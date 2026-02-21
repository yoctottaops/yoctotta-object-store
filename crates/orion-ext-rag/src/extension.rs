use async_trait::async_trait;
use std::sync::Arc;

use orion_core::extension::*;
use orion_core::stream::collect_stream;
use orion_core::types::*;
use orion_core::{Result, StorageBackend};

use crate::embedder::Embedder;
use crate::search::{SearchQuery, SearchResult};
use crate::store::{VectorEntry, VectorStore};

/// Configuration for the RAG extension.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct RagConfig {
    /// Maximum chunk size in characters for text splitting.
    #[serde(default = "default_chunk_size")]
    pub chunk_size: usize,
    /// Overlap between chunks in characters.
    #[serde(default = "default_chunk_overlap")]
    pub chunk_overlap: usize,
    /// Content types to index (glob patterns).
    #[serde(default = "default_indexable_types")]
    pub indexable_types: Vec<String>,
    /// Buckets to index. Empty = all buckets.
    #[serde(default)]
    pub buckets: Vec<String>,
}

fn default_chunk_size() -> usize {
    512
}
fn default_chunk_overlap() -> usize {
    64
}
fn default_indexable_types() -> Vec<String> {
    vec![
        "text/*".into(),
        "application/json".into(),
        "application/xml".into(),
    ]
}

impl Default for RagConfig {
    fn default() -> Self {
        Self {
            chunk_size: default_chunk_size(),
            chunk_overlap: default_chunk_overlap(),
            indexable_types: default_indexable_types(),
            buckets: Vec::new(),
        }
    }
}

/// The RAG extension: automatically indexes stored objects into a vector store
/// and provides semantic search over the indexed content.
pub struct RagExtension {
    embedder: Arc<dyn Embedder>,
    vector_store: Arc<dyn VectorStore>,
    storage: Arc<dyn StorageBackend>,
    config: RagConfig,
    enabled: bool,
}

impl RagExtension {
    pub fn new(
        embedder: Arc<dyn Embedder>,
        vector_store: Arc<dyn VectorStore>,
        storage: Arc<dyn StorageBackend>,
    ) -> Self {
        Self {
            embedder,
            vector_store,
            storage,
            config: RagConfig::default(),
            enabled: true,
        }
    }

    /// Semantic search across indexed objects.
    /// Text is reconstructed on-the-fly from the original objects.
    pub async fn search(&self, query_text: &str, top_k: usize) -> Result<Vec<SearchResult>> {
        let vector = self.embedder.embed(query_text).await?;
        let query = SearchQuery {
            vector,
            top_k,
            min_score: Some(0.1),
            bucket: None,
            key_prefix: None,
        };
        let mut results = self.vector_store.search(&query).await?;
        self.fill_text(&mut results).await;
        Ok(results)
    }

    /// Search within a specific bucket.
    pub async fn search_bucket(
        &self,
        query_text: &str,
        bucket: &str,
        top_k: usize,
    ) -> Result<Vec<SearchResult>> {
        let vector = self.embedder.embed(query_text).await?;
        let query = SearchQuery {
            vector,
            top_k,
            min_score: Some(0.1),
            bucket: Some(bucket.to_string()),
            key_prefix: None,
        };
        let mut results = self.vector_store.search(&query).await?;
        self.fill_text(&mut results).await;
        Ok(results)
    }

    /// Reconstruct text for search results by fetching original objects
    /// and extracting the matching chunk. Fails gracefully — if an object
    /// is gone or unreadable, the text field stays empty.
    async fn fill_text(&self, results: &mut [SearchResult]) {
        for result in results.iter_mut() {
            if !result.text.is_empty() {
                continue; // already populated (e.g. from InMemoryVectorStore)
            }

            let key = ObjectKey {
                bucket: result.bucket.clone(),
                key: result.key.clone(),
            };

            let text = match self.fetch_chunk(&key, result.chunk_index).await {
                Ok(t) => t,
                Err(_) => continue, // object deleted or unreadable — leave text empty
            };

            result.text = text;
        }
    }

    /// Fetch a specific chunk from an object by reading and re-chunking it.
    async fn fetch_chunk(&self, key: &ObjectKey, chunk_index: u32) -> Result<String> {
        let (stream, _meta) = self.storage.get(key).await?;
        let data = collect_stream(stream).await?;

        let text = std::str::from_utf8(&data)
            .map_err(|_| orion_core::OrionError::Internal("non-UTF8 content".into()))?;

        let chunks = self.chunk_text(text);
        chunks
            .into_iter()
            .nth(chunk_index as usize)
            .ok_or_else(|| orion_core::OrionError::Internal("chunk index out of range".into()))
    }

    /// Check if a content type should be indexed.
    fn should_index(&self, content_type: &str, bucket: &str) -> bool {
        // Check bucket filter.
        if !self.config.buckets.is_empty() && !self.config.buckets.contains(&bucket.to_string()) {
            return false;
        }

        // Check content type against patterns.
        self.config.indexable_types.iter().any(|pattern| {
            if pattern.ends_with("/*") {
                let prefix = &pattern[..pattern.len() - 2];
                content_type.starts_with(prefix)
            } else {
                content_type == pattern
            }
        })
    }

    /// Split text into overlapping chunks.
    fn chunk_text(&self, text: &str) -> Vec<String> {
        let chars: Vec<char> = text.chars().collect();
        if chars.len() <= self.config.chunk_size {
            return vec![text.to_string()];
        }

        let mut chunks = Vec::new();
        let mut start = 0;

        while start < chars.len() {
            let end = (start + self.config.chunk_size).min(chars.len());
            let chunk: String = chars[start..end].iter().collect();
            chunks.push(chunk);

            if end >= chars.len() {
                break;
            }
            start += self.config.chunk_size - self.config.chunk_overlap;
        }

        chunks
    }

    /// Index a document's content.
    async fn index_document(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
    ) -> Result<u32> {
        let text = match std::str::from_utf8(data) {
            Ok(t) => t.to_string(),
            Err(_) => {
                tracing::debug!(key, "Skipping non-UTF8 content");
                return Ok(0);
            }
        };

        if text.trim().is_empty() {
            return Ok(0);
        }

        let chunks = self.chunk_text(&text);
        let count = chunks.len() as u32;

        for (i, chunk) in chunks.iter().enumerate() {
            let vector = self.embedder.embed(chunk).await?;
            let entry = VectorEntry {
                id: format!("{}/{}#{}", bucket, key, i),
                bucket: bucket.to_string(),
                key: key.to_string(),
                chunk_index: i as u32,
                text: String::new(), // text not stored — reconstructed on-the-fly during search
                vector,
            };
            self.vector_store.upsert(entry).await?;
        }

        tracing::info!(bucket, key, chunks = count, "Indexed document");
        Ok(count)
    }
}

#[async_trait]
impl orion_core::Searchable for RagExtension {
    async fn search_text(
        &self,
        query: &str,
        top_k: usize,
        bucket: Option<&str>,
    ) -> orion_core::Result<Vec<orion_core::SearchHit>> {
        let results = if let Some(b) = bucket {
            self.search_bucket(query, b, top_k).await?
        } else {
            self.search(query, top_k).await?
        };
        Ok(results
            .into_iter()
            .map(|r| orion_core::SearchHit {
                id: r.id,
                bucket: r.bucket,
                key: r.key,
                chunk_index: r.chunk_index,
                text: r.text,
                score: r.score,
            })
            .collect())
    }
}

#[async_trait]
impl Extension for RagExtension {
    fn info(&self) -> ExtensionInfo {
        ExtensionInfo {
            name: "rag".into(),
            version: "0.1.0".into(),
            description: "Vector search over stored objects".into(),
            enabled: self.enabled,
        }
    }

    async fn init(&mut self, config: &toml::Value) -> Result<()> {
        if let Some(rag_config) = config.get("rag") {
            self.config = rag_config
                .clone()
                .try_into()
                .map_err(|e: toml::de::Error| {
                    orion_core::OrionError::Extension {
                        extension: "rag".into(),
                        message: format!("Invalid config: {}", e),
                    }
                })?;
        }
        tracing::info!(
            model = self.embedder.model_name(),
            dims = self.embedder.dimensions(),
            chunk_size = self.config.chunk_size,
            "RAG extension initialized"
        );
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        let count = self.vector_store.count().await?;
        tracing::info!(vectors = count, "RAG extension shutting down");
        Ok(())
    }

    fn hooks(&self) -> &dyn ExtensionHooks {
        self
    }

    fn searchable(&self) -> Option<&dyn orion_core::Searchable> {
        Some(self)
    }
}

#[async_trait]
impl ExtensionHooks for RagExtension {
    async fn post_write(
        &self,
        key: &ObjectKey,
        meta: &ObjectMeta,
        data: &[u8],
        _ctx: &ExtensionContext,
    ) -> Result<()> {
        if !self.should_index(&meta.content_type, &key.bucket) {
            return Ok(());
        }

        // Re-index: delete old vectors first.
        self.vector_store.delete_by_key(&key.bucket, &key.key).await?;

        // Index new content.
        self.index_document(&key.bucket, &key.key, data).await?;

        Ok(())
    }

    async fn post_delete(
        &self,
        key: &ObjectKey,
        _ctx: &ExtensionContext,
    ) -> Result<()> {
        let removed = self
            .vector_store
            .delete_by_key(&key.bucket, &key.key)
            .await?;
        if removed > 0 {
            tracing::info!(
                bucket = key.bucket,
                key = key.key,
                vectors = removed,
                "Removed vectors for deleted object"
            );
        }
        Ok(())
    }
}
