use color_eyre::{eyre::WrapErr, Result};
use config::{Config, File};
use lazy_static::lazy_static;
use serde::Deserialize;
use sqlx::postgres::PgConnectOptions;
use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use std::time::Duration;

static DEFAULT_CONFIG_FILE: &'static str = "harvester.yml";

lazy_static! {
    pub static ref APP: Settings = Settings::new().unwrap();
}
mod postgres_uri {
    use std::str::FromStr;

    use serde::{self, Deserialize, Deserializer};
    use sqlx::postgres::PgConnectOptions;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<PgConnectOptions, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        PgConnectOptions::from_str(s.as_str()).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    pub name: Option<String>,
    #[serde(deserialize_with = "postgres_uri::deserialize")]
    pub database_uri: PgConnectOptions,
    pub outbox_table: Option<String>,
    pub base_kafka_config: BaseKafkaConfig,
    pub producer_kafka_config: Option<HashMap<String, String>>,
    pub leader_topic: String,
    pub leader_group_id: String,
    pub health_probe: Option<SocketAddr>,
    #[serde(default)]
    pub limits: HarvesterLimits,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BaseKafkaConfig {
    #[serde(rename = "bootstrap.servers")]
    bootstrap_servers: String,
    #[serde(flatten)]
    extra_config: HashMap<String, String>,
}

impl Into<HashMap<String, String>> for BaseKafkaConfig {
    fn into(self) -> HashMap<String, String> {
        let mut config = self.extra_config.clone();
        config.insert(String::from("bootstrap.servers"), self.bootstrap_servers);
        config
    }
}

#[derive(Clone, Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct HarvesterLimits {
    #[serde(with = "humantime_serde", default)]
    pub poll_duration: Option<Duration>,
    #[serde(with = "humantime_serde", default)]
    pub min_poll_duration: Option<Duration>,
    #[serde(with = "humantime_serde", default)]
    pub heartbeat_timeout: Option<Duration>,
    #[serde(with = "humantime_serde", default)]
    pub harvest_backoff: Option<Duration>,
    #[serde(with = "humantime_serde", default)]
    pub db_connect_timeout: Option<Duration>,
    #[serde(default)]
    pub harvest_limit: Option<u32>,
}

impl Settings {
    pub fn new() -> Result<Settings> {
        let mut settings = Config::new();

        let config_path = env::var("HARVEST_CONFIG").unwrap_or(DEFAULT_CONFIG_FILE.to_string());
        settings
            .merge(File::with_name(&config_path))
            .wrap_err("Please make sure your configuration is present")?;

        return settings
            .try_into()
            .wrap_err_with(|| format!("Failed to read configuration file: {}", config_path));
    }
}
