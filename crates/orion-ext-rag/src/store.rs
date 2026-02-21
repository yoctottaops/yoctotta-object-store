use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

use orion_core::{OrionError, Result};
use rusqlite::{params, Connection, OptionalExtension};

use crate::search::{SearchQuery, SearchResult};

/// A stored vector entry with metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorEntry {
    /// Unique ID for this vector (typically: "{bucket}/{key}#{chunk_index}").
    pub id: String,
    /// The source object key.
    pub bucket: String,
    pub key: String,
    /// Chunk index within the source document.
    pub chunk_index: u32,
    /// The original text chunk.
    pub text: String,
    /// The embedding vector.
    pub vector: Vec<f32>,
}

/// Pluggable vector store trait.
/// Default: in-memory. Can be swapped for USearch, Qdrant, pgvector, etc.
#[async_trait]
pub trait VectorStore: Send + Sync + 'static {
    /// Insert or update a vector entry.
    async fn upsert(&self, entry: VectorEntry) -> Result<()>;

    /// Delete all vectors for a given object.
    async fn delete_by_key(&self, bucket: &str, key: &str) -> Result<u32>;

    /// Search for similar vectors.
    async fn search(&self, query: &SearchQuery) -> Result<Vec<SearchResult>>;

    /// Number of vectors stored.
    async fn count(&self) -> Result<u64>;
}

// ── SqliteVectorStore (persistent, production) ────────────────────────────

/// Persistent vector store backed by SQLite (source of truth) + USearch HNSW
/// (fast approximate nearest-neighbor search). The HNSW index is rebuilt from
/// SQLite on startup. Only embeddings and chunk identification are stored —
/// no text blobs — keeping the DB lean.
pub struct SqliteVectorStore {
    inner: Arc<Mutex<StoreInner>>,
}

struct StoreInner {
    conn: Connection,
    index: usearch::Index,
}

// Safety: StoreInner is only accessed through Mutex, ensuring exclusive access.
// usearch::Index wraps FFI but is safe to send when externally synchronized.
unsafe impl Send for StoreInner {}

/// Map a rusqlite error to an OrionError.
fn db_err(msg: impl std::fmt::Display) -> OrionError {
    OrionError::Extension {
        extension: "rag".into(),
        message: msg.to_string(),
    }
}

/// Convert &[f32] → Vec<u8> (little-endian) for SQLite BLOB storage.
fn f32_to_bytes(v: &[f32]) -> Vec<u8> {
    v.iter().flat_map(|f| f.to_le_bytes()).collect()
}

/// Convert &[u8] BLOB → Vec<f32> (little-endian).
fn bytes_to_f32(b: &[u8]) -> Vec<f32> {
    b.chunks_exact(4)
        .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
        .collect()
}

impl SqliteVectorStore {
    /// Open or create a persistent vector store at `{data_dir}/rag_vectors.db`.
    /// The HNSW index is rebuilt from all stored vectors on startup.
    pub fn new(data_dir: &Path, dims: usize) -> Result<Self> {
        let db_path = data_dir.join("rag_vectors.db");
        let conn = Connection::open(&db_path).map_err(|e| db_err(format!("open DB: {e}")))?;

        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             CREATE TABLE IF NOT EXISTS vectors (
                 uid         INTEGER PRIMARY KEY AUTOINCREMENT,
                 id          TEXT UNIQUE NOT NULL,
                 bucket      TEXT NOT NULL,
                 key         TEXT NOT NULL,
                 chunk_index INTEGER NOT NULL,
                 vector      BLOB NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_vectors_bucket_key ON vectors(bucket, key);",
        )
        .map_err(|e| db_err(format!("init schema: {e}")))?;

        // Create USearch HNSW index.
        let mut opts = usearch::IndexOptions::default();
        opts.dimensions = dims;
        opts.metric = usearch::MetricKind::Cos;
        opts.quantization = usearch::ScalarKind::F32;

        let index =
            usearch::new_index(&opts).map_err(|e| db_err(format!("create HNSW index: {e}")))?;

        // Rebuild HNSW from all stored vectors.
        let row_count: u64 = conn
            .query_row("SELECT COUNT(*) FROM vectors", [], |row| row.get(0))
            .map_err(|e| db_err(format!("count: {e}")))?;

        let capacity = (row_count as usize).max(1024);
        index
            .reserve(capacity)
            .map_err(|e| db_err(format!("reserve: {e}")))?;

        if row_count > 0 {
            let mut stmt = conn
                .prepare("SELECT uid, vector FROM vectors")
                .map_err(|e| db_err(format!("prepare rebuild: {e}")))?;
            let mut rows = stmt
                .query([])
                .map_err(|e| db_err(format!("query rebuild: {e}")))?;

            let mut loaded = 0u64;
            while let Some(row) = rows.next().map_err(|e| db_err(format!("row: {e}")))? {
                let uid: i64 = row.get(0).map_err(|e| db_err(format!("uid: {e}")))?;
                let blob: Vec<u8> = row.get(1).map_err(|e| db_err(format!("blob: {e}")))?;
                let vector = bytes_to_f32(&blob);
                index
                    .add(uid as u64, &vector)
                    .map_err(|e| db_err(format!("add to HNSW: {e}")))?;
                loaded += 1;
            }
            tracing::info!(vectors = loaded, dims, "Rebuilt HNSW index from SQLite");
        }

        Ok(Self {
            inner: Arc::new(Mutex::new(StoreInner { conn, index })),
        })
    }
}

#[async_trait]
impl VectorStore for SqliteVectorStore {
    async fn upsert(&self, entry: VectorEntry) -> Result<()> {
        let inner = self.inner.clone();

        tokio::task::spawn_blocking(move || {
            let mut guard = inner.lock().unwrap();
            let StoreInner { conn, index } = &mut *guard;

            let blob = f32_to_bytes(&entry.vector);

            // Check if this id already exists.
            let existing_uid: Option<i64> = conn
                .query_row(
                    "SELECT uid FROM vectors WHERE id = ?1",
                    params![entry.id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| db_err(format!("upsert lookup: {e}")))?;

            if let Some(old_uid) = existing_uid {
                // Remove old vector from HNSW, update SQLite row, re-add to HNSW.
                let _ = index.remove(old_uid as u64);

                conn.execute(
                    "UPDATE vectors SET bucket=?1, key=?2, chunk_index=?3, vector=?4 WHERE uid=?5",
                    params![entry.bucket, entry.key, entry.chunk_index, blob, old_uid],
                )
                .map_err(|e| db_err(format!("upsert update: {e}")))?;

                index
                    .add(old_uid as u64, &entry.vector)
                    .map_err(|e| db_err(format!("HNSW re-add: {e}")))?;
            } else {
                // Insert new row.
                conn.execute(
                    "INSERT INTO vectors (id, bucket, key, chunk_index, vector) VALUES (?1,?2,?3,?4,?5)",
                    params![entry.id, entry.bucket, entry.key, entry.chunk_index, blob],
                )
                .map_err(|e| db_err(format!("upsert insert: {e}")))?;

                let uid = conn.last_insert_rowid() as u64;

                // Ensure HNSW has capacity.
                if index.size() >= index.capacity() {
                    let new_cap = (index.capacity() * 2).max(1024);
                    index
                        .reserve(new_cap)
                        .map_err(|e| db_err(format!("reserve: {e}")))?;
                }

                index
                    .add(uid, &entry.vector)
                    .map_err(|e| db_err(format!("HNSW add: {e}")))?;
            }

            Ok(())
        })
        .await
        .map_err(|e| OrionError::Internal(format!("upsert task panicked: {e}")))?
    }

    async fn delete_by_key(&self, bucket: &str, key: &str) -> Result<u32> {
        let inner = self.inner.clone();
        let bucket = bucket.to_string();
        let key = key.to_string();

        tokio::task::spawn_blocking(move || {
            let mut guard = inner.lock().unwrap();
            let StoreInner { conn, index } = &mut *guard;

            // Get UIDs to remove from HNSW.
            let mut stmt = conn
                .prepare("SELECT uid FROM vectors WHERE bucket = ?1 AND key = ?2")
                .map_err(|e| db_err(format!("delete prepare: {e}")))?;

            let uids: Vec<i64> = stmt
                .query_map(params![bucket, key], |row| row.get(0))
                .map_err(|e| db_err(format!("delete query: {e}")))?
                .filter_map(|r| r.ok())
                .collect();

            let count = uids.len() as u32;

            for uid in &uids {
                let _ = index.remove(*uid as u64);
            }

            conn.execute(
                "DELETE FROM vectors WHERE bucket = ?1 AND key = ?2",
                params![bucket, key],
            )
            .map_err(|e| db_err(format!("delete: {e}")))?;

            Ok(count)
        })
        .await
        .map_err(|e| OrionError::Internal(format!("delete task panicked: {e}")))?
    }

    async fn search(&self, query: &SearchQuery) -> Result<Vec<SearchResult>> {
        let inner = self.inner.clone();
        let query = query.clone();

        tokio::task::spawn_blocking(move || {
            let guard = inner.lock().unwrap();
            let StoreInner { conn, index } = &*guard;

            if index.size() == 0 {
                return Ok(Vec::new());
            }

            // Over-fetch when filtering to ensure enough post-filter results.
            let has_filters = query.bucket.is_some() || query.key_prefix.is_some();
            let fetch_k = if has_filters {
                (query.top_k * 10).min(index.size())
            } else {
                query.top_k.min(index.size())
            };

            let matches = index
                .search(&query.vector, fetch_k)
                .map_err(|e| db_err(format!("HNSW search: {e}")))?;

            let min_score = query.min_score.unwrap_or(0.0);
            let mut results = Vec::with_capacity(query.top_k);

            for (&uid, &distance) in matches.keys.iter().zip(matches.distances.iter()) {
                let score = 1.0 - distance; // cosine distance → similarity

                if score < min_score {
                    continue;
                }

                // Fetch metadata from SQLite.
                let row = conn.query_row(
                    "SELECT id, bucket, key, chunk_index FROM vectors WHERE uid = ?1",
                    params![uid as i64],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, u32>(3)?,
                        ))
                    },
                );

                let (id, bucket, obj_key, chunk_index) = match row {
                    Ok(r) => r,
                    Err(_) => continue, // stale HNSW entry
                };

                // Apply filters.
                if let Some(ref b) = query.bucket {
                    if bucket != *b {
                        continue;
                    }
                }
                if let Some(ref prefix) = query.key_prefix {
                    if !obj_key.starts_with(prefix.as_str()) {
                        continue;
                    }
                }

                results.push(SearchResult {
                    id,
                    bucket,
                    key: obj_key,
                    chunk_index,
                    text: String::new(), // text reconstructed on-the-fly at higher level
                    score,
                });

                if results.len() >= query.top_k {
                    break;
                }
            }

            Ok(results)
        })
        .await
        .map_err(|e| OrionError::Internal(format!("search task panicked: {e}")))?
    }

    async fn count(&self) -> Result<u64> {
        let inner = self.inner.clone();

        tokio::task::spawn_blocking(move || {
            let guard = inner.lock().unwrap();
            guard
                .conn
                .query_row("SELECT COUNT(*) FROM vectors", [], |row| row.get(0))
                .map_err(|e| db_err(format!("count: {e}")))
        })
        .await
        .map_err(|e| OrionError::Internal(format!("count task panicked: {e}")))?
    }
}

// ── InMemoryVectorStore (testing) ─────────────────────────────────────────

/// Simple in-memory vector store using brute-force cosine similarity.
/// Fine for small datasets and development/testing.
pub struct InMemoryVectorStore {
    entries: RwLock<HashMap<String, VectorEntry>>,
}

impl InMemoryVectorStore {
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for InMemoryVectorStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl VectorStore for InMemoryVectorStore {
    async fn upsert(&self, entry: VectorEntry) -> Result<()> {
        let mut entries = self.entries.write().unwrap();
        entries.insert(entry.id.clone(), entry);
        Ok(())
    }

    async fn delete_by_key(&self, bucket: &str, key: &str) -> Result<u32> {
        let mut entries = self.entries.write().unwrap();
        let before = entries.len();
        entries.retain(|_, e| !(e.bucket == bucket && e.key == key));
        Ok((before - entries.len()) as u32)
    }

    async fn search(&self, query: &SearchQuery) -> Result<Vec<SearchResult>> {
        let entries = self.entries.read().unwrap();

        let mut results: Vec<(f32, &VectorEntry)> = entries
            .values()
            .filter(|e| {
                query
                    .bucket
                    .as_ref()
                    .map_or(true, |b| e.bucket == *b)
            })
            .filter(|e| {
                query
                    .key_prefix
                    .as_ref()
                    .map_or(true, |p| e.key.starts_with(p))
            })
            .map(|e| (cosine_similarity(&query.vector, &e.vector), e))
            .collect();

        results.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

        let min_score = query.min_score.unwrap_or(0.0);

        Ok(results
            .into_iter()
            .filter(|(score, _)| *score >= min_score)
            .take(query.top_k)
            .map(|(score, entry)| SearchResult {
                id: entry.id.clone(),
                bucket: entry.bucket.clone(),
                key: entry.key.clone(),
                chunk_index: entry.chunk_index,
                text: entry.text.clone(),
                score,
            })
            .collect())
    }

    async fn count(&self) -> Result<u64> {
        let entries = self.entries.read().unwrap();
        Ok(entries.len() as u64)
    }
}

fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() {
        return 0.0;
    }
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }
    dot / (norm_a * norm_b)
}
