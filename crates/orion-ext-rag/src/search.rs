use serde::{Deserialize, Serialize};

/// A vector search query.
#[derive(Debug, Clone)]
pub struct SearchQuery {
    /// The query embedding vector.
    pub vector: Vec<f32>,
    /// Number of results to return.
    pub top_k: usize,
    /// Minimum similarity score (0.0 to 1.0).
    pub min_score: Option<f32>,
    /// Filter by bucket.
    pub bucket: Option<String>,
    /// Filter by key prefix.
    pub key_prefix: Option<String>,
}

/// A search result with relevance score.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    /// Vector entry ID.
    pub id: String,
    /// Source bucket.
    pub bucket: String,
    /// Source object key.
    pub key: String,
    /// Chunk index within source object.
    pub chunk_index: u32,
    /// The matched text chunk.
    pub text: String,
    /// Cosine similarity score (0.0 to 1.0).
    pub score: f32,
}
