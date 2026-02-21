mod config;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tracing_subscriber::EnvFilter;

use orion_core::{Extension, ExtensionRegistry, MetadataStore};
use orion_proto_s3::{MetricsConfig, S3Server};
use orion_store_fs::FsStore;

use crate::config::Config;

#[derive(Parser)]
#[command(name = "yoctotta", about = "Yoctotta Object Store - S3-compatible distributed object storage")]
struct Cli {
    /// Path to configuration file.
    #[arg(short, long, default_value = "orion.toml")]
    config: PathBuf,

    /// Listen address (overrides config).
    #[arg(short, long)]
    listen: Option<String>,

    /// Data directory (overrides config).
    #[arg(short, long)]
    data: Option<PathBuf>,

    /// Metadata backend: sqlite, postgres, raft (overrides config).
    #[arg(long)]
    meta: Option<String>,

    /// PostgreSQL URL (shortcut, overrides config).
    #[arg(long)]
    pg_url: Option<String>,

    /// Raft node ID (overrides config).
    #[arg(long)]
    node_id: Option<u64>,

    /// Bootstrap Raft cluster (only on first node, first time).
    #[arg(long)]
    bootstrap: bool,

    /// Disable all extensions.
    #[arg(long)]
    no_extensions: bool,

    /// Disable RAG extension only.
    #[arg(long)]
    no_rag: bool,

    /// Disable trigger extension only.
    #[arg(long)]
    no_triggers: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    // Load config.
    let mut config = if cli.config.exists() {
        let contents = std::fs::read_to_string(&cli.config)?;
        toml::from_str::<Config>(&contents)?
    } else {
        tracing::info!("No config file found, using defaults");
        Config::default()
    };

    // Apply CLI overrides.
    if let Some(listen) = cli.listen {
        config.listen = listen;
    }
    if let Some(data) = cli.data {
        config.data_dir = data;
    }
    if let Some(meta) = cli.meta {
        config.meta_backend = meta;
    }
    if let Some(pg_url) = cli.pg_url {
        config.meta_postgres.url = pg_url;
    }
    if let Some(node_id) = cli.node_id {
        config.meta_raft.node_id = node_id;
    }
    if cli.bootstrap {
        config.meta_raft.bootstrap = true;
    }

    // Ensure data directory exists.
    std::fs::create_dir_all(&config.data_dir)?;

    // Initialize storage backend.
    let store = Arc::new(FsStore::new(&config.data_dir).await?);
    let buckets: Arc<dyn orion_core::BucketManager> = store.clone();

    // ── Initialize metadata backend ──
    let meta: Arc<dyn MetadataStore> = match config.meta_backend.as_str() {
        #[cfg(feature = "meta-sqlite")]
        "sqlite" => {
            let db_path = if config.meta_sqlite.filename.starts_with('/') {
                PathBuf::from(&config.meta_sqlite.filename)
            } else {
                config.data_dir.join(&config.meta_sqlite.filename)
            };
            tracing::info!(path = %db_path.display(), "Using SQLite metadata backend");
            Arc::new(orion_meta_sqlite::SqliteMetaStore::new(&db_path)?)
        }

        #[cfg(feature = "meta-postgres")]
        "postgres" | "pg" => {
            tracing::info!(url = %config.meta_postgres.url, "Using PostgreSQL metadata backend");
            let pg_config = orion_meta_postgres::PgMetaStoreConfig {
                url: config.meta_postgres.url.clone(),
                max_connections: config.meta_postgres.max_connections,
                min_connections: config.meta_postgres.min_connections,
                connect_timeout_secs: config.meta_postgres.connect_timeout_secs,
                run_migrations: config.meta_postgres.run_migrations,
            };
            Arc::new(orion_meta_postgres::PgMetaStore::new(&pg_config).await?)
        }

        #[cfg(feature = "meta-raft")]
        "raft" => {
            let db_path = config.data_dir.join(&config.meta_raft.db_filename);
            tracing::info!(
                node_id = config.meta_raft.node_id,
                db = %db_path.display(),
                members = config.meta_raft.initial_members.len(),
                "Using Raft-replicated SQLite metadata backend"
            );
            let raft_config = orion_meta_raft::RaftMetaStoreConfig {
                node_id: config.meta_raft.node_id,
                listen_addr: config.meta_raft.listen_addr.clone(),
                db_path,
                initial_members: config.meta_raft.initial_members.clone(),
                read_consistency: config.meta_raft.read_consistency.clone(),
                election_timeout_min: config.meta_raft.election_timeout_min,
                election_timeout_max: config.meta_raft.election_timeout_max,
                heartbeat_interval: config.meta_raft.heartbeat_interval,
                snapshot_threshold: config.meta_raft.snapshot_threshold,
            };
            let raft_store = orion_meta_raft::RaftMetaStore::new(raft_config).await?;

            if config.meta_raft.bootstrap {
                tracing::info!("Bootstrapping Raft cluster...");
                raft_store.bootstrap().await?;
            }

            Arc::new(raft_store)
        }

        other => {
            anyhow::bail!(
                "Unknown metadata backend '{}'. Available: sqlite{}{}",
                other,
                if cfg!(feature = "meta-postgres") { ", postgres" } else { "" },
                if cfg!(feature = "meta-raft") { ", raft" } else { "" },
            );
        }
    };

    // ── Initialize extensions ──
    let mut registry = ExtensionRegistry::new();

    if !cli.no_extensions {
        #[cfg(feature = "rag")]
        if !cli.no_rag {
            let embedder = Arc::new(orion_ext_rag::FastEmbedder::try_new()?);
            let vector_store = Arc::new(
                orion_ext_rag::SqliteVectorStore::new(
                    &config.data_dir,
                    orion_ext_rag::FastEmbedder::DIMS,
                )?
            );
            let mut rag = orion_ext_rag::RagExtension::new(embedder, vector_store, store.clone());

            let rag_config = config
                .extensions
                .rag
                .unwrap_or(toml::Value::Table(toml::map::Map::new()));
            rag.init(&toml::Value::Table({
                let mut t = toml::map::Map::new();
                t.insert("rag".into(), rag_config);
                t
            }))
            .await?;

            registry.register(Box::new(rag));
        }

        #[cfg(feature = "trigger")]
        if !cli.no_triggers {
            let mut trigger = orion_ext_trigger::TriggerExtension::new();

            let trigger_config = config
                .extensions
                .trigger
                .unwrap_or(toml::Value::Table(toml::map::Map::new()));
            trigger
                .init(&toml::Value::Table({
                    let mut t = toml::map::Map::new();
                    t.insert("trigger".into(), trigger_config);
                    t
                }))
                .await?;

            registry.register(Box::new(trigger));
        }
    }

    let extensions = Arc::new(registry);

    // ── Startup banner ──
    tracing::info!("╔══════════════════════════════════════════╗");
    tracing::info!("║       YOCTOTTA OBJECT STORE              ║");
    tracing::info!("╚══════════════════════════════════════════╝");
    tracing::info!(
        listen = config.listen,
        data = %config.data_dir.display(),
        meta_backend = config.meta_backend,
        "Starting server"
    );

    for ext in extensions.list() {
        tracing::info!(
            name = ext.name,
            version = ext.version,
            enabled = ext.enabled,
            "Extension loaded"
        );
    }

    // ── Initialize auth (optional) ──
    let auth = if config.auth.enabled {
        tracing::info!(
            users = config.auth.users.len(),
            session_expiry = config.auth.session_expiry_secs,
            "Authentication enabled"
        );
        let auth_config = orion_ext_auth::AuthConfig {
            enabled: true,
            session_expiry_secs: config.auth.session_expiry_secs,
            users: config
                .auth
                .users
                .iter()
                .map(|u| orion_ext_auth::UserConfig {
                    username: u.username.clone(),
                    password_hash: u.password_hash.clone(),
                })
                .collect(),
        };
        Some(orion_ext_auth::AuthProvider::new(auth_config))
    } else {
        None
    };

    let metrics_config = MetricsConfig {
        sample_interval_secs: config.metrics.sample_interval_secs,
        retention_secs: config.metrics.retention_secs,
    };

    // ── Start S3 server ──
    let addr: SocketAddr = config.listen.parse()?;
    let server = S3Server::new(store, buckets, meta, extensions.clone(), addr, metrics_config, auth, &config.data_dir);

    let ext_shutdown = extensions.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("Shutting down...");
        ext_shutdown.shutdown_all().await;
        std::process::exit(0);
    });

    server.run().await?;

    Ok(())
}
