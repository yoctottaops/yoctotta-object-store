use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::collections::HashMap;

use orion_core::*;

use crate::migrations;

/// Configuration for the PostgreSQL metadata store.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct PgMetaStoreConfig {
    /// PostgreSQL connection URL.
    /// e.g. "postgres://orion:password@localhost:5432/orion"
    pub url: String,

    /// Maximum number of connections in the pool.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,

    /// Minimum number of idle connections to maintain.
    #[serde(default = "default_min_connections")]
    pub min_connections: u32,

    /// Connection timeout in seconds.
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_secs: u64,

    /// Whether to run migrations on startup.
    #[serde(default = "default_run_migrations")]
    pub run_migrations: bool,
}

fn default_max_connections() -> u32 {
    20
}
fn default_min_connections() -> u32 {
    2
}
fn default_connect_timeout() -> u64 {
    10
}
fn default_run_migrations() -> bool {
    true
}

impl Default for PgMetaStoreConfig {
    fn default() -> Self {
        Self {
            url: "postgres://orion:orion@localhost:5432/orion".into(),
            max_connections: default_max_connections(),
            min_connections: default_min_connections(),
            connect_timeout_secs: default_connect_timeout(),
            run_migrations: default_run_migrations(),
        }
    }
}

/// PostgreSQL-backed metadata store.
///
/// Uses connection pooling via sqlx for concurrent access.
/// Supports horizontal scaling through PostgreSQL replicas
/// (read replicas for list/get, primary for writes).
///
/// For very large deployments, the objects table can be
/// partitioned by bucket — see migration 003.
pub struct PgMetaStore {
    pool: PgPool,
}

impl PgMetaStore {
    /// Create a new PgMetaStore and connect to the database.
    pub async fn new(config: &PgMetaStoreConfig) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .min_connections(config.min_connections)
            .acquire_timeout(std::time::Duration::from_secs(config.connect_timeout_secs))
            .connect(&config.url)
            .await
            .map_err(|e| OrionError::Metadata(format!("Failed to connect to PostgreSQL: {}", e)))?;

        if config.run_migrations {
            migrations::run_migrations(&pool)
                .await
                .map_err(|e| OrionError::Metadata(format!("Migration failed: {}", e)))?;
        }

        tracing::info!(
            max_conn = config.max_connections,
            "PostgreSQL metadata store initialized"
        );

        Ok(Self { pool })
    }

    /// Create from an existing pool (useful for testing).
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Get a reference to the underlying connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Health check — pings the database.
    pub async fn health(&self) -> Result<bool> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map(|_| true)
            .map_err(|e| OrionError::Metadata(format!("Health check failed: {}", e)))
    }
}

#[async_trait]
impl MetadataStore for PgMetaStore {
    async fn create_bucket(&self, info: &BucketInfo) -> Result<()> {
        sqlx::query(
            "INSERT INTO buckets (name, created_at, region) VALUES ($1, $2, $3)"
        )
        .bind(&info.name)
        .bind(info.created_at)
        .bind(&info.region)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            if let sqlx::Error::Database(ref db_err) = e {
                if db_err.code().as_deref() == Some("23505") {
                    return OrionError::BucketAlreadyExists(info.name.clone());
                }
            }
            OrionError::Metadata(e.to_string())
        })?;
        Ok(())
    }

    async fn delete_bucket(&self, name: &str) -> Result<()> {
        let result = sqlx::query("DELETE FROM buckets WHERE name = $1")
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(|e| OrionError::Metadata(e.to_string()))?;

        if result.rows_affected() == 0 {
            return Err(OrionError::BucketNotFound(name.to_string()));
        }
        Ok(())
    }

    async fn get_bucket(&self, name: &str) -> Result<BucketInfo> {
        let row: (String, DateTime<Utc>, Option<String>) = sqlx::query_as(
            "SELECT name, created_at, region FROM buckets WHERE name = $1"
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| OrionError::Metadata(e.to_string()))?
        .ok_or_else(|| OrionError::BucketNotFound(name.to_string()))?;

        Ok(BucketInfo {
            name: row.0,
            created_at: row.1,
            region: row.2,
        })
    }

    async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        let rows: Vec<(String, DateTime<Utc>, Option<String>)> = sqlx::query_as(
            "SELECT name, created_at, region FROM buckets ORDER BY name"
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| OrionError::Metadata(e.to_string()))?;

        Ok(rows
            .into_iter()
            .map(|(name, created_at, region)| BucketInfo {
                name,
                created_at,
                region,
            })
            .collect())
    }

    async fn bucket_exists(&self, name: &str) -> Result<bool> {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM buckets WHERE name = $1)"
        )
        .bind(name)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| OrionError::Metadata(e.to_string()))?;

        Ok(exists)
    }

    async fn put_object_meta(&self, bucket: &str, key: &str, meta: &ObjectMeta) -> Result<()> {
        let user_meta_json = serde_json::to_value(&meta.user_meta)
            .map_err(|e| OrionError::Metadata(e.to_string()))?;

        sqlx::query(
            "INSERT INTO objects (bucket, key, size, etag, content_type, created_at, modified_at, user_meta, checksum, version_id)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
             ON CONFLICT (bucket, key) DO UPDATE SET
                size = EXCLUDED.size,
                etag = EXCLUDED.etag,
                content_type = EXCLUDED.content_type,
                modified_at = EXCLUDED.modified_at,
                user_meta = EXCLUDED.user_meta,
                checksum = EXCLUDED.checksum,
                version_id = EXCLUDED.version_id"
        )
        .bind(bucket)
        .bind(key)
        .bind(meta.size as i64)
        .bind(&meta.etag)
        .bind(&meta.content_type)
        .bind(meta.created_at)
        .bind(meta.modified_at)
        .bind(&user_meta_json)
        .bind(&meta.checksum)
        .bind(&meta.version_id)
        .execute(&self.pool)
        .await
        .map_err(|e| OrionError::Metadata(e.to_string()))?;

        Ok(())
    }

    async fn get_object_meta(&self, bucket: &str, key: &str) -> Result<ObjectMeta> {
        let row: Option<ObjectRow> = sqlx::query_as(
            "SELECT size, etag, content_type, created_at, modified_at, user_meta, checksum, version_id
             FROM objects WHERE bucket = $1 AND key = $2"
        )
        .bind(bucket)
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| OrionError::Metadata(e.to_string()))?;

        row.map(|r| r.into_meta())
            .ok_or_else(|| OrionError::NotFound(format!("{}/{}", bucket, key)))
    }

    async fn delete_object_meta(&self, bucket: &str, key: &str) -> Result<()> {
        sqlx::query("DELETE FROM objects WHERE bucket = $1 AND key = $2")
            .bind(bucket)
            .bind(key)
            .execute(&self.pool)
            .await
            .map_err(|e| OrionError::Metadata(e.to_string()))?;
        Ok(())
    }

    async fn list_objects(&self, bucket: &str, opts: &ListOptions) -> Result<ListPage> {
        let prefix = opts.prefix.as_deref().unwrap_or("");
        let max_keys = opts.max_keys as i64;
        let prefix_pattern = format!("{}%", prefix);

        // Build query with optional cursor.
        let rows: Vec<ObjectKeyRow> = if let Some(cursor) = &opts.cursor {
            sqlx::query_as(
                "SELECT key, size, etag, content_type, created_at, modified_at, user_meta, checksum, version_id
                 FROM objects
                 WHERE bucket = $1 AND key LIKE $2 AND key > $3
                 ORDER BY key
                 LIMIT $4"
            )
            .bind(bucket)
            .bind(&prefix_pattern)
            .bind(cursor)
            .bind(max_keys + 1)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query_as(
                "SELECT key, size, etag, content_type, created_at, modified_at, user_meta, checksum, version_id
                 FROM objects
                 WHERE bucket = $1 AND key LIKE $2
                 ORDER BY key
                 LIMIT $3"
            )
            .bind(bucket)
            .bind(&prefix_pattern)
            .bind(max_keys + 1)
            .fetch_all(&self.pool)
            .await
        }
        .map_err(|e| OrionError::Metadata(e.to_string()))?;

        let is_truncated = rows.len() as i64 > max_keys;
        let entries: Vec<ListEntry> = rows
            .into_iter()
            .take(max_keys as usize)
            .map(|r| r.into_list_entry())
            .collect();

        let next_cursor = if is_truncated {
            entries.last().map(|e| e.key.clone())
        } else {
            None
        };

        // Handle delimiter for common prefixes.
        let mut common_prefixes = Vec::new();
        let mut filtered_entries = entries;

        if let Some(delimiter) = &opts.delimiter {
            let mut seen = std::collections::HashSet::new();
            filtered_entries.retain(|entry| {
                let after_prefix = &entry.key[prefix.len()..];
                if let Some(pos) = after_prefix.find(delimiter.as_str()) {
                    let cp = format!("{}{}{}", prefix, &after_prefix[..pos], delimiter);
                    if seen.insert(cp.clone()) {
                        common_prefixes.push(CommonPrefix { prefix: cp });
                    }
                    false
                } else {
                    true
                }
            });
        }

        Ok(ListPage {
            entries: filtered_entries,
            common_prefixes,
            next_cursor,
            is_truncated,
        })
    }

    async fn object_exists(&self, bucket: &str, key: &str) -> Result<bool> {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM objects WHERE bucket = $1 AND key = $2)"
        )
        .bind(bucket)
        .bind(key)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| OrionError::Metadata(e.to_string()))?;

        Ok(exists)
    }

    async fn object_count(&self, bucket: &str) -> Result<u64> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM objects WHERE bucket = $1"
        )
        .bind(bucket)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| OrionError::Metadata(e.to_string()))?;

        Ok(count as u64)
    }

    async fn total_size(&self, bucket: &str) -> Result<u64> {
        let size: Option<i64> = sqlx::query_scalar(
            "SELECT SUM(size) FROM objects WHERE bucket = $1"
        )
        .bind(bucket)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| OrionError::Metadata(e.to_string()))?;

        Ok(size.unwrap_or(0) as u64)
    }
}

// ── Internal row types for sqlx mapping ──

#[derive(sqlx::FromRow)]
struct ObjectRow {
    size: i64,
    etag: String,
    content_type: String,
    created_at: DateTime<Utc>,
    modified_at: DateTime<Utc>,
    user_meta: serde_json::Value,
    checksum: Option<String>,
    version_id: Option<String>,
}

impl ObjectRow {
    fn into_meta(self) -> ObjectMeta {
        let user_meta: HashMap<String, String> =
            serde_json::from_value(self.user_meta).unwrap_or_default();
        ObjectMeta {
            size: self.size as u64,
            etag: self.etag,
            content_type: self.content_type,
            created_at: self.created_at,
            modified_at: self.modified_at,
            user_meta,
            checksum: self.checksum,
            version_id: self.version_id,
        }
    }
}

#[derive(sqlx::FromRow)]
struct ObjectKeyRow {
    key: String,
    size: i64,
    etag: String,
    content_type: String,
    created_at: DateTime<Utc>,
    modified_at: DateTime<Utc>,
    user_meta: serde_json::Value,
    checksum: Option<String>,
    version_id: Option<String>,
}

impl ObjectKeyRow {
    fn into_list_entry(self) -> ListEntry {
        let user_meta: HashMap<String, String> =
            serde_json::from_value(self.user_meta).unwrap_or_default();
        ListEntry {
            key: self.key,
            meta: ObjectMeta {
                size: self.size as u64,
                etag: self.etag,
                content_type: self.content_type,
                created_at: self.created_at,
                modified_at: self.modified_at,
                user_meta,
                checksum: self.checksum,
                version_id: self.version_id,
            },
        }
    }
}
