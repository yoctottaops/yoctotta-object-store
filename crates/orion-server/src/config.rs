use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default = "default_listen")]
    pub listen: String,

    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,

    /// Metadata backend: "sqlite", "postgres", or "raft".
    #[serde(default = "default_meta_backend")]
    pub meta_backend: String,

    /// SQLite-specific config.
    #[serde(default)]
    pub meta_sqlite: MetaSqliteConfig,

    /// PostgreSQL-specific config.
    #[serde(default)]
    pub meta_postgres: MetaPostgresConfig,

    /// Raft-replicated SQLite config.
    #[serde(default)]
    pub meta_raft: MetaRaftConfig,

    #[serde(default)]
    pub extensions: ExtensionsConfig,

    /// Authentication configuration.
    #[serde(default)]
    pub auth: AuthConfig,

    /// Metrics collection configuration.
    #[serde(default)]
    pub metrics: MetricsCollectorConfig,
}

#[derive(Debug, Deserialize)]
pub struct MetaSqliteConfig {
    /// Path to SQLite database file. Relative to data_dir if not absolute.
    #[serde(default = "default_sqlite_filename")]
    pub filename: String,
}

impl Default for MetaSqliteConfig {
    fn default() -> Self {
        Self {
            filename: default_sqlite_filename(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct MetaPostgresConfig {
    /// PostgreSQL connection URL.
    #[serde(default = "default_pg_url")]
    pub url: String,
    /// Max connections in the pool.
    #[serde(default = "default_pg_max_conn")]
    pub max_connections: u32,
    /// Min idle connections.
    #[serde(default = "default_pg_min_conn")]
    pub min_connections: u32,
    /// Connection timeout in seconds.
    #[serde(default = "default_pg_timeout")]
    pub connect_timeout_secs: u64,
    /// Run migrations on startup.
    #[serde(default = "default_true")]
    pub run_migrations: bool,
}

impl Default for MetaPostgresConfig {
    fn default() -> Self {
        Self {
            url: default_pg_url(),
            max_connections: default_pg_max_conn(),
            min_connections: default_pg_min_conn(),
            connect_timeout_secs: default_pg_timeout(),
            run_migrations: true,
        }
    }
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct MetaRaftConfig {
    /// This node's unique ID.
    #[serde(default = "default_node_id")]
    pub node_id: u64,
    /// Address for Raft RPC (separate from S3 API port).
    #[serde(default = "default_raft_listen")]
    pub listen_addr: String,
    /// SQLite filename for the replicated metadata database.
    #[serde(default = "default_raft_db")]
    pub db_filename: String,
    /// Initial cluster members: [[node_id, "host:port"], ...]
    #[serde(default)]
    pub initial_members: Vec<(u64, String)>,
    /// Bootstrap the cluster (only set true on ONE node, first time only).
    #[serde(default)]
    pub bootstrap: bool,
    /// Read consistency: "local" (fast, eventual) or "leader" (linearizable).
    #[serde(default = "default_read_consistency")]
    pub read_consistency: String,
    #[serde(default = "default_election_min")]
    pub election_timeout_min: u64,
    #[serde(default = "default_election_max")]
    pub election_timeout_max: u64,
    #[serde(default = "default_heartbeat")]
    pub heartbeat_interval: u64,
    #[serde(default = "default_snapshot_threshold")]
    pub snapshot_threshold: u64,
}

impl Default for MetaRaftConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            listen_addr: default_raft_listen(),
            db_filename: default_raft_db(),
            initial_members: Vec::new(),
            bootstrap: false,
            read_consistency: default_read_consistency(),
            election_timeout_min: 300,
            election_timeout_max: 600,
            heartbeat_interval: 100,
            snapshot_threshold: 5000,
        }
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct ExtensionsConfig {
    #[serde(default)]
    pub rag: Option<toml::Value>,
    #[serde(default)]
    pub trigger: Option<toml::Value>,
}

#[derive(Debug, Deserialize, Default)]
pub struct AuthConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_session_expiry")]
    pub session_expiry_secs: u64,
    #[serde(default)]
    pub users: Vec<AuthUserConfig>,
}

#[derive(Debug, Deserialize)]
pub struct AuthUserConfig {
    pub username: String,
    pub password_hash: String,
}

#[derive(Debug, Deserialize)]
pub struct MetricsCollectorConfig {
    #[serde(default = "default_sample_interval")]
    pub sample_interval_secs: u64,
    #[serde(default = "default_retention")]
    pub retention_secs: u64,
}

impl Default for MetricsCollectorConfig {
    fn default() -> Self {
        Self {
            sample_interval_secs: default_sample_interval(),
            retention_secs: default_retention(),
        }
    }
}

// ── Defaults ──

fn default_session_expiry() -> u64 { 3600 }
fn default_sample_interval() -> u64 { 30 }
fn default_retention() -> u64 { 18000 }
fn default_listen() -> String { "0.0.0.0:9000".into() }
fn default_data_dir() -> PathBuf { PathBuf::from("./data") }
fn default_meta_backend() -> String { "sqlite".into() }
fn default_sqlite_filename() -> String { "orion.db".into() }
fn default_pg_url() -> String { "postgres://orion:orion@localhost:5432/orion".into() }
fn default_pg_max_conn() -> u32 { 20 }
fn default_pg_min_conn() -> u32 { 2 }
fn default_pg_timeout() -> u64 { 10 }
fn default_true() -> bool { true }
fn default_node_id() -> u64 { 1 }
fn default_raft_listen() -> String { "0.0.0.0:9001".into() }
fn default_raft_db() -> String { "orion-raft.db".into() }
fn default_read_consistency() -> String { "local".into() }
fn default_election_min() -> u64 { 300 }
fn default_election_max() -> u64 { 600 }
fn default_heartbeat() -> u64 { 100 }
fn default_snapshot_threshold() -> u64 { 5000 }

impl Default for Config {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            data_dir: default_data_dir(),
            meta_backend: default_meta_backend(),
            meta_sqlite: MetaSqliteConfig::default(),
            meta_postgres: MetaPostgresConfig::default(),
            meta_raft: MetaRaftConfig::default(),
            extensions: ExtensionsConfig::default(),
            auth: AuthConfig::default(),
            metrics: MetricsCollectorConfig::default(),
        }
    }
}
