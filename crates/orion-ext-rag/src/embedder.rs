use std::sync::Arc;

use async_trait::async_trait;
use orion_core::{OrionError, Result};

/// Trait for converting text into embedding vectors.
/// Implementations can use local models (fastembed, ONNX) or
/// remote APIs (OpenAI, Cohere, etc).
#[async_trait]
pub trait Embedder: Send + Sync + 'static {
    /// Embed a single text string.
    async fn embed(&self, text: &str) -> Result<Vec<f32>>;

    /// Batch embed multiple texts (can be more efficient for some backends).
    async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        let mut results = Vec::with_capacity(texts.len());
        for text in texts {
            results.push(self.embed(text).await?);
        }
        Ok(results)
    }

    /// Dimensionality of the embedding vectors.
    fn dimensions(&self) -> usize;

    /// Model name/identifier for logging.
    fn model_name(&self) -> &str;
}

// ── FastEmbedder ──────────────────────────────────────────────────────────

/// Production embedder using fastembed (ONNX Runtime, CPU-optimized).
/// Uses all-MiniLM-L6-v2: 384 dimensions, ~22 MB, very fast on CPU.
///
/// All inference is offloaded to `spawn_blocking` so the async runtime
/// is never blocked — normal operations proceed unaffected.
pub struct FastEmbedder {
    model: Arc<fastembed::TextEmbedding>,
}

impl FastEmbedder {
    /// Create a new FastEmbedder. Downloads the model on first use.
    pub fn try_new() -> Result<Self> {
        let options = fastembed::InitOptions::new(fastembed::EmbeddingModel::AllMiniLML6V2)
            .with_show_download_progress(true);

        let model = fastembed::TextEmbedding::try_new(options).map_err(|e| {
            OrionError::Internal(format!("failed to initialize embedding model: {e}"))
        })?;

        tracing::info!(
            model = "all-MiniLM-L6-v2",
            dims = 384,
            "FastEmbedder initialized (CPU, ONNX)"
        );

        Ok(Self {
            model: Arc::new(model),
        })
    }
}

#[async_trait]
impl Embedder for FastEmbedder {
    async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        let model = self.model.clone();
        let text = text.to_string();

        let result = tokio::task::spawn_blocking(move || {
            model.embed(vec![text.as_str()], None)
        })
        .await
        .map_err(|e| OrionError::Internal(format!("embedding task panicked: {e}")))?
        .map_err(|e| OrionError::Internal(format!("embedding failed: {e}")))?;

        result
            .into_iter()
            .next()
            .ok_or_else(|| OrionError::Internal("embedding returned no vectors".into()))
    }

    async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        let model = self.model.clone();
        let owned: Vec<String> = texts.iter().map(|s| s.to_string()).collect();

        let result = tokio::task::spawn_blocking(move || {
            let refs: Vec<&str> = owned.iter().map(|s| s.as_str()).collect();
            model.embed(refs, None)
        })
        .await
        .map_err(|e| OrionError::Internal(format!("embedding task panicked: {e}")))?
        .map_err(|e| OrionError::Internal(format!("batch embedding failed: {e}")))?;

        Ok(result)
    }

    fn dimensions(&self) -> usize {
        384
    }

    fn model_name(&self) -> &str {
        "all-MiniLM-L6-v2"
    }
}

// ── MockEmbedder (testing only) ───────────────────────────────────────────

/// A simple mock embedder for testing.
/// Produces deterministic vectors based on text hash.
pub struct MockEmbedder {
    dims: usize,
}

impl MockEmbedder {
    pub fn new(dims: usize) -> Self {
        Self { dims }
    }
}

#[async_trait]
impl Embedder for MockEmbedder {
    async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        // Simple hash-based pseudo-embedding for testing.
        let mut vec = vec![0.0f32; self.dims];
        for (i, byte) in text.bytes().enumerate() {
            vec[i % self.dims] += (byte as f32) / 255.0;
        }
        // Normalize.
        let norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for v in &mut vec {
                *v /= norm;
            }
        }
        Ok(vec)
    }

    fn dimensions(&self) -> usize {
        self.dims
    }

    fn model_name(&self) -> &str {
        "mock-embedder"
    }
}
