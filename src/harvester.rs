use std::{
    collections::HashMap,
    ffi::OsString,
    io,
    net::{SocketAddr, TcpListener},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use color_eyre::Result;
use futures::future::join_all;
use itertools::Itertools;
use log::{debug, error, info, trace};
use r_neli::{LeaderEvent, Neli, NeliConfigParams};
use rdkafka::{
    producer::{DefaultProducerContext, FutureProducer, FutureRecord},
    util::Timeout,
    ClientConfig,
};

use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    ConnectOptions, Pool, Postgres,
};
use tokio::time;

#[derive(sqlx::FromRow)]
struct KafkaOutboxRecord {
    id: i64,
    kafka_topic: String,
    kafka_key: String,
    kafka_value: String,
}

pub struct HarvesterConfigParams {
    pub name: Option<String>,
    pub database_uri: PgConnectOptions,
    pub outbox_table: Option<String>,
    pub base_kafka_config: HashMap<String, String>,
    pub producer_kafka_config: Option<HashMap<String, String>>,
    pub leader_topic: String,
    pub leader_group_id: String,
    pub poll_duration: Option<Duration>,
    pub min_poll_duration: Option<Duration>,
    pub heartbeat_timeout: Option<Duration>,
    pub harvest_limit: Option<u32>,
    pub harvest_backoff: Option<Duration>,
    pub health_probe: Option<SocketAddr>,
    pub leader_probe: Option<SocketAddr>,
    pub db_connect_timeout: Option<Duration>,
}

#[derive(Clone)]
pub struct HarvesterConfig {
    pub name: String,
    pub database_uri: PgConnectOptions,
    pub outbox_table: String,
    pub base_kafka_config: HashMap<String, String>,
    pub producer_kafka_config: HashMap<String, String>,
    pub leader_topic: String,
    pub leader_group_id: String,
    pub poll_duration: Option<Duration>,
    pub min_poll_duration: Option<Duration>,
    pub heartbeat_timeout: Option<Duration>,
    pub harvest_limit: u32,
    pub harvest_backoff: Duration,
    pub health_probe: Option<SocketAddr>,
    pub leader_probe: Option<SocketAddr>,
    pub db_connect_timeout: Duration,
}

impl From<HarvesterConfigParams> for HarvesterConfig {
    fn from(params: HarvesterConfigParams) -> Self {
        HarvesterConfig {
            name: params.name.unwrap_or(format!(
                "{} {} {}",
                hostname::get()
                    .unwrap_or(OsString::from("unknown"))
                    .to_string_lossy(),
                std::process::id(),
                ulid::Ulid::new()
            )),
            database_uri: params.database_uri,
            outbox_table: params.outbox_table.unwrap_or(String::from("kafka_outbox")),
            base_kafka_config: params.base_kafka_config,
            producer_kafka_config: params.producer_kafka_config.unwrap_or(HashMap::new()),
            leader_topic: params.leader_topic,
            leader_group_id: params.leader_group_id,
            poll_duration: params.poll_duration,
            min_poll_duration: params.min_poll_duration,
            heartbeat_timeout: params.heartbeat_timeout,
            harvest_limit: params.harvest_limit.unwrap_or(1000),
            harvest_backoff: params.harvest_backoff.unwrap_or(Duration::from_secs(1)),
            health_probe: params.health_probe,
            leader_probe: params.leader_probe,
            db_connect_timeout: params.db_connect_timeout.unwrap_or(Duration::from_secs(5)),
        }
    }
}

#[derive(Clone)]
pub struct Harvester {
    config: HarvesterConfig,
    query_stmt: String,
    purge_stmt: String,
    state: Arc<HarvesterState>,
}

pub struct HarvesterState {
    is_running: AtomicBool,
    is_leader: AtomicBool,
}

impl Harvester {
    pub fn new(config: HarvesterConfigParams) -> Harvester {
        let config = HarvesterConfig::from(config);
        Harvester {
            query_stmt: format!(
                "SELECT id, kafka_topic, kafka_key, kafka_value FROM {} ORDER BY id ASC LIMIT {}",
                config.outbox_table, config.harvest_limit
            ),
            purge_stmt: format!("DELETE FROM {} WHERE id = ANY($1)", config.outbox_table),
            config,
            state: Arc::new(HarvesterState {
                is_running: AtomicBool::new(false),
                is_leader: AtomicBool::new(false),
            }),
        }
    }

    pub async fn start(&self) -> Result<()> {
        let mut options = self.config.database_uri.clone();
        options.disable_statement_logging();

        let db = PgPoolOptions::new()
            .connect_timeout(self.config.db_connect_timeout)
            .max_connections(2)
            .connect_lazy_with(options);

        let mut producer_config = ClientConfig::new();

        for (key, value) in self.config.base_kafka_config.iter() {
            producer_config.set(key.clone(), value.clone());
        }
        for (key, value) in self.config.producer_kafka_config.iter() {
            producer_config.set(key.clone(), value.clone());
        }

        let producer: FutureProducer<DefaultProducerContext> =
            producer_config.create_with_context(DefaultProducerContext)?;

        self.state.is_running.store(true, Ordering::Relaxed);
        self.state.is_leader.store(false, Ordering::SeqCst);
        let membership_check_handle = tokio::spawn(Harvester::membership_check(self.clone()));
        let health_probe_handle = tokio::spawn(Harvester::health_probe(self.clone()));
        let leader_probe_handle = tokio::spawn(Harvester::leader_probe(self.clone()));

        info!("[{}] Harvester started", self.config.name);

        while self.state.is_running.load(Ordering::Relaxed) {
            if self
                .state
                .is_leader
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                match self.harvest(db.clone(), &producer).await {
                    Ok(_) => {}
                    Err(err) => {
                        error!("Failed to harvest records: {}", err);
                        time::sleep(self.config.harvest_backoff).await;
                    }
                }
            } else {
                time::sleep(Duration::from_millis(100)).await;
            }
        }

        info!("[{}] Harvester shutting down", self.config.name);
        membership_check_handle.await?;

        drop(leader_probe_handle);
        drop(health_probe_handle);
        drop(producer);
        drop(db);

        info!("[{}] Harvester shut down", self.config.name);
        Ok(())
    }

    async fn harvest(
        &self,
        db: Pool<Postgres>,
        producer: &FutureProducer<DefaultProducerContext>,
    ) -> Result<()> {
        // Query records from db
        let start = Instant::now();
        let rows: Vec<KafkaOutboxRecord> = sqlx::query_as(self.query_stmt.as_str())
            .fetch_all(&db)
            .await?;

        let count = rows.len();
        if count == 0 {
            debug!(
                "[{}] No records found in outbox, waiting for {:.2} secs",
                self.config.name,
                self.config.harvest_backoff.as_secs_f32()
            );
            time::sleep(self.config.harvest_backoff).await;
        } else {
            debug!(
                "[{}] Preparing to publish {} records from outbox",
                self.config.name, count
            );
            // Group Records and send them simultaneously
            let (success, failure, largest_batch, success_ids) = join_all(
                rows.into_iter()
                    .group_by(|x| format!("{}:{}", x.kafka_topic, x.kafka_key))
                    .into_iter()
                    .map(|(key, batch)| self.batch_publish(key, &producer, batch.collect_vec())),
            )
            .await
            .into_iter()
            .fold((0, 0, 0, Vec::new()), |acc, x| {
                (
                    acc.0 + x.0,
                    acc.1 + x.1,
                    if acc.2 < x.0 + x.1 { x.0 + x.1 } else { acc.2 },
                    [x.2, acc.3].concat(),
                )
            });
            sqlx::query(self.purge_stmt.as_str())
                .bind(success_ids)
                .execute(&db)
                .await?;
            let duration = Instant::now() - start;
            info!(
    "[{}] Records published: (success: {}, failure: {}, largest batch: {}, duration: {:.3}s, rate: {:.3} m/s) ",
    self.config.name, success, failure, largest_batch, duration.as_secs_f32(), count as f32/ duration.as_secs_f32()
);
        }
        Ok(())
    }

    async fn batch_publish(
        &self,
        key: String,
        producer: &FutureProducer<DefaultProducerContext>,
        messages: Vec<KafkaOutboxRecord>,
    ) -> (u16, u16, Vec<i64>) {
        let mut success = 0;
        let mut failure = 0;
        let mut success_ids = Vec::new();

        for message in messages.iter() {
            // If any message in the batch fails skip all remaining records
            if failure > 0 {
                failure += 1;
                continue;
            }

            match producer
                .send(
                    FutureRecord::to(message.kafka_topic.as_str())
                        .key(message.kafka_key.as_bytes())
                        .payload(message.kafka_value.as_bytes()),
                    Timeout::Never,
                )
                .await
            {
                Ok(_) => {
                    trace!(
                        "[{}] Published message {} with key {} to {} successfully",
                        self.config.name,
                        message.id,
                        message.kafka_key,
                        message.kafka_topic
                    );
                    success_ids.push(message.id);
                    success += 1
                }
                Err((err, _)) => {
                    error!(
                        "[{}] Failed to publish message: {:?}",
                        self.config.name, err
                    );
                    failure += 1;
                }
            }
        }
        debug!(
            "[{}] Published batch {} with {} messages",
            self.config.name,
            key,
            messages.len()
        );
        (success, failure, success_ids)
    }

    async fn membership_check(harvester: Harvester) {
        let processor = {
            loop {
                match Neli::new(
                    NeliConfigParams {
                        name: format!("{}_neli", harvester.config.name),
                        kafka_config: harvester.config.base_kafka_config.clone(),
                        leader_group_id: harvester.config.leader_group_id.clone(),
                        leader_topic: harvester.config.leader_topic.clone(),
                        heartbeat_timeout: harvester.config.heartbeat_timeout,
                        min_poll_duration: harvester.config.min_poll_duration,
                        poll_duration: harvester.config.poll_duration,
                    },
                    |events| {
                        events.iter().find(|x| match x {
                            LeaderEvent::LeaderAcquired(0) => {
                                info!(
                                    "[{}] Harvester leader status acquired on '{}'",
                                    harvester.config.name, harvester.config.leader_topic
                                );
                                true
                            }
                            LeaderEvent::LeaderFenced(0) => {
                                info!(
                                    "[{}] Harvester leader status fenced  on '{}'",
                                    harvester.config.name, harvester.config.leader_topic
                                );
                                true
                            }
                            LeaderEvent::LeaderRevoked(0) => {
                                info!(
                                    "[{}] Harvester leader status lost  on '{}'",
                                    harvester.config.name, harvester.config.leader_topic
                                );
                                true
                            }
                            _ => false,
                        });
                    },
                ) {
                    Ok(processor) => break processor,
                    Err(err) => {
                        error!("Failed to start background NELI server: {}", err);
                    }
                }
            }
        };
        info!("[{}] Started background NELI server", harvester.config.name);
        while harvester.state.is_running.load(Ordering::Relaxed) {
            if let Some(leader_status) = processor.pulse(Duration::from_millis(500)).await.unwrap()
            {
                harvester
                    .state
                    .is_leader
                    .store(leader_status[0], std::sync::atomic::Ordering::SeqCst);
            } else {
                harvester
                    .state
                    .is_leader
                    .store(false, std::sync::atomic::Ordering::SeqCst);
            }
        }
        processor.close();
        drop(processor);
        info!("[{}] Stopped background NELI server", harvester.config.name);
    }

    async fn health_probe(harvester: Harvester) {
        if let Some(addr) = harvester.config.health_probe {
            let listener = TcpListener::bind(addr).unwrap();
            listener
                .set_nonblocking(true)
                .expect("Failed to set non-blocking tcp listener");
            info!(
                "[{}] Started liveness check on port {}",
                harvester.config.name, addr
            );
            for stream in listener.incoming() {
                if harvester.state.is_running.load(Ordering::Relaxed) {
                    match stream {
                        Ok(_) => {
                            debug!("[{}] Liveness check completed", harvester.config.name);
                        }
                        Err(ref err) if err.kind() == io::ErrorKind::WouldBlock => {
                            time::sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                        Err(err) => {
                            error!(
                                "[{}] Failed to complete liveness check: {}",
                                harvester.config.name, err
                            )
                        }
                    }
                } else {
                    break;
                }
            }
            info!(
                "[{}] Stopped liveness check on port {}",
                harvester.config.name, addr
            );
        }
    }
    async fn leader_probe(harvester: Harvester) {
        if let Some(addr) = harvester.config.leader_probe {
            info!(
                "[{}] Started leadership check on port {}",
                harvester.config.name, addr
            );

            let mut leadership_listener: Option<TcpListener> = None;
            loop {
                if harvester.state.is_running.load(Ordering::Relaxed) {
                    if harvester.state.is_leader.load(Ordering::Relaxed) {
                        if let Some(ref listener) = leadership_listener {
                            let stream = listener.accept();
                            match stream {
                                Ok(_) => {
                                    debug!(
                                        "[{}] Leadership check completed",
                                        harvester.config.name
                                    );
                                }
                                Err(ref err) if err.kind() == io::ErrorKind::WouldBlock => {
                                    time::sleep(Duration::from_millis(100)).await;
                                }
                                Err(err) => {
                                    error!(
                                        "[{}] Failed to complete leadership check: {}",
                                        harvester.config.name, err
                                    )
                                }
                            }
                        } else {
                            let listener = TcpListener::bind(addr).unwrap();
                            listener
                                .set_nonblocking(true)
                                .expect("Failed to set non-blocking tcp listener");
                            leadership_listener = Some(listener);
                        }
                    } else {
                        if leadership_listener.is_some() {
                            leadership_listener = None;
                        } else {
                            time::sleep(Duration::from_millis(100)).await;
                        }
                    };
                } else {
                    break;
                }
            }
            info!(
                "[{}] Stopped leadership check on port {}",
                harvester.config.name, addr
            );
        }
    }

    pub async fn stop(&self) {
        self.state.is_running.store(false, Ordering::Relaxed);
    }
}
