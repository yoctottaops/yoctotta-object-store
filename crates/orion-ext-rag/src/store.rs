use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::RwLock;

use orion_core::Result;

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

/// Simple in-memory vector store using brute-force cosine similarity.
/// Fine for small datasets and development. Use USearch/HNSW for production.
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
                // Apply bucket filter if specified.
                query
                    .bucket
                    .as_ref()
                    .map_or(true, |b| e.bucket == *b)
            })
            .filter(|e| {
                // Apply prefix filter if specified.
                query
                    .key_prefix
                    .as_ref()
                    .map_or(true, |p| e.key.starts_with(p))
            })
            .map(|e| (cosine_similarity(&query.vector, &e.vector), e))
            .collect();

        // Sort by similarity descending.
        results.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

        // Apply score threshold.
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
