mod harvester;
mod settings;
use color_eyre::Result;
use env_logger::Env;
use futures::stream::StreamExt;
use harvester::{Harvester, HarvesterConfigParams};
use settings::APP;
use signal_hook::consts::{SIGHUP, SIGINT, SIGQUIT, SIGTERM};
use signal_hook_tokio::Signals;
use std::sync::Arc;

async fn handle_signals(harvester: Arc<Harvester>, signals: Signals) {
    let mut signals = signals.fuse();
    while let Some(signal) = signals.next().await {
        match signal {
            SIGTERM | SIGINT | SIGQUIT => {
                // Shutdown the system;
                harvester.stop().await;
            }
            _ => unreachable!(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("r_neli=trace,r_harvest=trace"))
        .init();
    color_eyre::install()?;
    let settings = APP.clone();

    let harvester = Arc::new(Harvester::new(HarvesterConfigParams {
        name: settings.name,
        database_uri: settings.database_uri,
        outbox_table: settings.outbox_table,
        base_kafka_config: settings.base_kafka_config.into(),
        producer_kafka_config: settings.producer_kafka_config,
        leader_topic: settings.leader_topic,
        leader_group_id: settings.leader_group_id,
        poll_duration: settings.limits.poll_duration,
        min_poll_duration: settings.limits.min_poll_duration,
        heartbeat_timeout: settings.limits.heartbeat_timeout,
        harvest_limit: settings.limits.harvest_limit,
        db_connect_timeout: settings.limits.db_connect_timeout,
        harvest_backoff: settings.limits.harvest_backoff,
        health_probe: settings.health_probe,
        leader_probe: settings.leader_probe,
    }));

    let signals = Signals::new(&[SIGHUP, SIGTERM, SIGINT, SIGQUIT])?;
    let handle = signals.handle();
    let signals_task = tokio::spawn(handle_signals(harvester.clone(), signals));

    harvester.start().await?;

    handle.close();
    signals_task.await?;

    Ok(())
}
