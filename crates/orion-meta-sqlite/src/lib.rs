use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};
use std::path::Path;
use std::sync::Mutex;

use orion_core::*;

pub struct SqliteMetaStore {
    conn: Mutex<Connection>,
}

impl SqliteMetaStore {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path).map_err(|e| OrionError::Metadata(e.to_string()))?;

        conn.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS buckets (
                name TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                region TEXT
            );

            CREATE TABLE IF NOT EXISTS objects (
                bucket TEXT NOT NULL,
                key TEXT NOT NULL,
                size INTEGER NOT NULL,
                etag TEXT NOT NULL,
                content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
                created_at TEXT NOT NULL,
                modified_at TEXT NOT NULL,
                user_meta TEXT DEFAULT '{}',
                checksum TEXT,
                version_id TEXT,
                PRIMARY KEY (bucket, key),
                FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_objects_prefix ON objects(bucket, key);
            ",
        )
        .map_err(|e| OrionError::Metadata(e.to_string()))?;

        tracing::info!("SQLite metadata store initialized");
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// In-memory database for testing.
    pub fn in_memory() -> Result<Self> {
        Self::new(":memory:")
    }
}

#[async_trait]
impl MetadataStore for SqliteMetaStore {
    async fn create_bucket(&self, info: &BucketInfo) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO buckets (name, created_at, region) VALUES (?1, ?2, ?3)",
            params![info.name, info.created_at.to_rfc3339(), info.region],
        )
        .map_err(|e| {
            if let rusqlite::Error::SqliteFailure(err, _) = &e {
                if err.code == rusqlite::ErrorCode::ConstraintViolation {
                    return OrionError::BucketAlreadyExists(info.name.clone());
                }
            }
            OrionError::Metadata(e.to_string())
        })?;
        Ok(())
    }

    async fn delete_bucket(&self, name: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let affected = conn
            .execute("DELETE FROM buckets WHERE name = ?1", params![name])
            .map_err(|e| OrionError::Metadata(e.to_string()))?;
        if affected == 0 {
            return Err(OrionError::BucketNotFound(name.to_string()));
        }
        Ok(())
    }

    async fn get_bucket(&self, name: &str) -> Result<BucketInfo> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT name, created_at, region FROM buckets WHERE name = ?1",
            params![name],
            |row| {
                let created_str: String = row.get(1)?;
                Ok(BucketInfo {
                    name: row.get(0)?,
                    created_at: DateTime::parse_from_rfc3339(&created_str)
                        .unwrap_or_default()
                        .with_timezone(&Utc),
                    region: row.get(2)?,
                })
            },
        )
        .map_err(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => {
                OrionError::BucketNotFound(name.to_string())
            }
            _ => OrionError::Metadata(e.to_string()),
        })
    }

    async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT name, created_at, region FROM buckets ORDER BY name")
            .map_err(|e| OrionError::Metadata(e.to_string()))?;
        let rows = stmt
            .query_map([], |row| {
                let created_str: String = row.get(1)?;
                Ok(BucketInfo {
                    name: row.get(0)?,
                    created_at: DateTime::parse_from_rfc3339(&created_str)
                        .unwrap_or_default()
                        .with_timezone(&Utc),
                    region: row.get(2)?,
                })
            })
            .map_err(|e| OrionError::Metadata(e.to_string()))?;

        let mut buckets = Vec::new();
        for row in rows {
            buckets.push(row.map_err(|e| OrionError::Metadata(e.to_string()))?);
        }
        Ok(buckets)
    }

    async fn bucket_exists(&self, name: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM buckets WHERE name = ?1",
                params![name],
                |row| row.get(0),
            )
            .map_err(|e| OrionError::Metadata(e.to_string()))?;
        Ok(count > 0)
    }

    async fn put_object_meta(&self, bucket: &str, key: &str, meta: &ObjectMeta) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let user_meta_json =
            serde_json::to_string(&meta.user_meta).unwrap_or_else(|_| "{}".into());
        conn.execute(
            "INSERT OR REPLACE INTO objects
             (bucket, key, size, etag, content_type, created_at, modified_at, user_meta, checksum, version_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                bucket,
                key,
                meta.size as i64,
                meta.etag,
                meta.content_type,
                meta.created_at.to_rfc3339(),
                meta.modified_at.to_rfc3339(),
                user_meta_json,
                meta.checksum,
                meta.version_id,
            ],
        )
        .map_err(|e| OrionError::Metadata(e.to_string()))?;
        Ok(())
    }

    async fn get_object_meta(&self, bucket: &str, key: &str) -> Result<ObjectMeta> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT size, etag, content_type, created_at, modified_at, user_meta, checksum, version_id
             FROM objects WHERE bucket = ?1 AND key = ?2",
            params![bucket, key],
            |row| {
                let created_str: String = row.get(3)?;
                let modified_str: String = row.get(4)?;
                let user_meta_str: String = row.get(5)?;
                Ok(ObjectMeta {
                    size: row.get::<_, i64>(0)? as u64,
                    etag: row.get(1)?,
                    content_type: row.get(2)?,
                    created_at: DateTime::parse_from_rfc3339(&created_str)
                        .unwrap_or_default()
                        .with_timezone(&Utc),
                    modified_at: DateTime::parse_from_rfc3339(&modified_str)
                        .unwrap_or_default()
                        .with_timezone(&Utc),
                    user_meta: serde_json::from_str(&user_meta_str).unwrap_or_default(),
                    checksum: row.get(6)?,
                    version_id: row.get(7)?,
                })
            },
        )
        .map_err(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => {
                OrionError::NotFound(format!("{}/{}", bucket, key))
            }
            _ => OrionError::Metadata(e.to_string()),
        })
    }

    async fn delete_object_meta(&self, bucket: &str, key: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM objects WHERE bucket = ?1 AND key = ?2",
            params![bucket, key],
        )
        .map_err(|e| OrionError::Metadata(e.to_string()))?;
        Ok(())
    }

    async fn list_objects(&self, bucket: &str, opts: &ListOptions) -> Result<ListPage> {
        let conn = self.conn.lock().unwrap();
        let prefix = opts.prefix.as_deref().unwrap_or("");
        let max_keys = opts.max_keys as usize;
        let prefix_pattern = format!("{}%", prefix);

        let mut sql = String::from(
            "SELECT key, size, etag, content_type, created_at, modified_at, user_meta, checksum, version_id
             FROM objects WHERE bucket = ?1 AND key LIKE ?2",
        );

        if let Some(cursor) = &opts.cursor {
            sql.push_str(&format!(" AND key > '{}'", cursor.replace('\'', "''")));
        }

        sql.push_str(" ORDER BY key");
        sql.push_str(&format!(" LIMIT {}", max_keys + 1));

        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| OrionError::Metadata(e.to_string()))?;

        let rows = stmt
            .query_map(params![bucket, prefix_pattern], |row| {
                let created_str: String = row.get(4)?;
                let modified_str: String = row.get(5)?;
                let user_meta_str: String = row.get(6)?;
                Ok(ListEntry {
                    key: row.get(0)?,
                    meta: ObjectMeta {
                        size: row.get::<_, i64>(1)? as u64,
                        etag: row.get(2)?,
                        content_type: row.get(3)?,
                        created_at: DateTime::parse_from_rfc3339(&created_str)
                            .unwrap_or_default()
                            .with_timezone(&Utc),
                        modified_at: DateTime::parse_from_rfc3339(&modified_str)
                            .unwrap_or_default()
                            .with_timezone(&Utc),
                        user_meta: serde_json::from_str(&user_meta_str).unwrap_or_default(),
                        checksum: row.get(7)?,
                        version_id: row.get(8)?,
                    },
                })
            })
            .map_err(|e| OrionError::Metadata(e.to_string()))?;

        let mut entries = Vec::new();
        for row in rows {
            entries.push(row.map_err(|e| OrionError::Metadata(e.to_string()))?);
        }

        let is_truncated = entries.len() > max_keys;
        if is_truncated {
            entries.truncate(max_keys);
        }
        let next_cursor = if is_truncated {
            entries.last().map(|e| e.key.clone())
        } else {
            None
        };

        // Handle delimiter for common prefixes.
        let mut common_prefixes = Vec::new();
        if let Some(delimiter) = &opts.delimiter {
            let mut seen = std::collections::HashSet::new();
            entries.retain(|entry| {
                let after_prefix = &entry.key[prefix.len()..];
                if let Some(pos) = after_prefix.find(delimiter.as_str()) {
                    let cp = format!("{}{}{}", prefix, &after_prefix[..pos], delimiter);
                    if seen.insert(cp.clone()) {
                        common_prefixes.push(CommonPrefix { prefix: cp });
                    }
                    false // Remove from entries, it's represented by the common prefix.
                } else {
                    true
                }
            });
        }

        Ok(ListPage {
            entries,
            common_prefixes,
            next_cursor,
            is_truncated,
        })
    }

    async fn object_exists(&self, bucket: &str, key: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM objects WHERE bucket = ?1 AND key = ?2",
                params![bucket, key],
                |row| row.get(0),
            )
            .map_err(|e| OrionError::Metadata(e.to_string()))?;
        Ok(count > 0)
    }

    async fn object_count(&self, bucket: &str) -> Result<u64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM objects WHERE bucket = ?1",
                params![bucket],
                |row| row.get(0),
            )
            .map_err(|e| OrionError::Metadata(e.to_string()))?;
        Ok(count as u64)
    }

    async fn total_size(&self, bucket: &str) -> Result<u64> {
        let conn = self.conn.lock().unwrap();
        let size: i64 = conn
            .query_row(
                "SELECT COALESCE(SUM(size), 0) FROM objects WHERE bucket = ?1",
                params![bucket],
                |row| row.get(0),
            )
            .map_err(|e| OrionError::Metadata(e.to_string()))?;
        Ok(size as u64)
    }
}
