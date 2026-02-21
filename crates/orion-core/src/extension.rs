use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::error::Result;
use crate::stream::DataStream;
use crate::types::*;

/// What an extension hook wants to do with the request.
pub enum HookAction {
    /// Continue processing normally.
    Continue,
    /// Block the operation (returns 403 to client).
    Deny(String),
    /// Transform the data stream (e.g. encryption, compression).
    Transform(DataStream),
}

impl std::fmt::Debug for HookAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Continue => write!(f, "Continue"),
            Self::Deny(msg) => f.debug_tuple("Deny").field(msg).finish(),
            Self::Transform(_) => write!(f, "Transform(<stream>)"),
        }
    }
}

/// Context passed to extension hooks with request details.
#[derive(Debug, Clone)]
pub struct ExtensionContext {
    /// Arbitrary key-value data extensions can pass between hooks.
    pub data: HashMap<String, String>,
}

impl ExtensionContext {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }
}

impl Default for ExtensionContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Lifecycle hooks that extensions can implement.
/// All methods have default no-op implementations — extensions only
/// override what they care about.
#[async_trait]
pub trait ExtensionHooks: Send + Sync {
    /// Called before an object is written to storage.
    /// Can inspect/modify data or deny the write.
    async fn pre_write(
        &self,
        _key: &ObjectKey,
        _opts: &PutOptions,
        _ctx: &mut ExtensionContext,
    ) -> Result<HookAction> {
        Ok(HookAction::Continue)
    }

    /// Called after an object is successfully written.
    /// Receives the final metadata. Cannot block.
    async fn post_write(
        &self,
        _key: &ObjectKey,
        _meta: &ObjectMeta,
        _data: &[u8],
        _ctx: &ExtensionContext,
    ) -> Result<()> {
        Ok(())
    }

    /// Called before an object is read from storage.
    async fn pre_read(
        &self,
        _key: &ObjectKey,
        _ctx: &mut ExtensionContext,
    ) -> Result<HookAction> {
        Ok(HookAction::Continue)
    }

    /// Called after an object is read (for auditing, caching, etc).
    async fn post_read(
        &self,
        _key: &ObjectKey,
        _meta: &ObjectMeta,
        _ctx: &ExtensionContext,
    ) -> Result<()> {
        Ok(())
    }

    /// Called before an object is deleted.
    async fn pre_delete(
        &self,
        _key: &ObjectKey,
        _ctx: &mut ExtensionContext,
    ) -> Result<HookAction> {
        Ok(HookAction::Continue)
    }

    /// Called after an object is deleted.
    async fn post_delete(
        &self,
        _key: &ObjectKey,
        _ctx: &ExtensionContext,
    ) -> Result<()> {
        Ok(())
    }

    /// Called on any storage event (superset — includes bucket events too).
    async fn on_event(&self, _event: &StorageEvent) -> Result<()> {
        Ok(())
    }
}

/// Extension metadata and configuration schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtensionInfo {
    pub name: String,
    pub version: String,
    pub description: String,
    /// Whether this extension is currently enabled.
    pub enabled: bool,
}

/// A search result returned by searchable extensions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchHit {
    pub id: String,
    pub bucket: String,
    pub key: String,
    pub chunk_index: u32,
    pub text: String,
    pub score: f32,
}

/// Optional trait for extensions that provide semantic search.
#[async_trait]
pub trait Searchable: Send + Sync {
    async fn search_text(
        &self,
        query: &str,
        top_k: usize,
        bucket: Option<&str>,
    ) -> Result<Vec<SearchHit>>;
}

/// The main extension trait — combines lifecycle management with hooks.
#[async_trait]
pub trait Extension: Send + Sync + 'static {
    /// Extension metadata.
    fn info(&self) -> ExtensionInfo;

    /// Initialize the extension with its configuration.
    async fn init(&mut self, config: &toml::Value) -> Result<()>;

    /// Graceful shutdown.
    async fn shutdown(&self) -> Result<()>;

    /// Return the hooks this extension provides.
    fn hooks(&self) -> &dyn ExtensionHooks;

    /// Health check — extensions can report their own health.
    async fn health(&self) -> Result<bool> {
        Ok(true)
    }

    /// If this extension supports search, return a reference to its Searchable impl.
    fn searchable(&self) -> Option<&dyn Searchable> {
        None
    }
}

/// Registry that manages all loaded extensions and dispatches hooks.
pub struct ExtensionRegistry {
    extensions: Vec<Box<dyn Extension>>,
}

impl ExtensionRegistry {
    pub fn new() -> Self {
        Self {
            extensions: Vec::new(),
        }
    }

    pub fn register(&mut self, ext: Box<dyn Extension>) {
        tracing::info!("Registered extension: {}", ext.info().name);
        self.extensions.push(ext);
    }

    /// Get all enabled extensions.
    fn enabled(&self) -> impl Iterator<Item = &dyn Extension> {
        self.extensions
            .iter()
            .filter(|e| e.info().enabled)
            .map(|e| e.as_ref())
    }

    /// Dispatch pre_write hooks. Returns Deny on first denial.
    pub async fn dispatch_pre_write(
        &self,
        key: &ObjectKey,
        opts: &PutOptions,
        ctx: &mut ExtensionContext,
    ) -> Result<HookAction> {
        for ext in self.enabled() {
            match ext.hooks().pre_write(key, opts, ctx).await? {
                HookAction::Continue => continue,
                action => return Ok(action),
            }
        }
        Ok(HookAction::Continue)
    }

    /// Dispatch post_write hooks to all enabled extensions.
    pub async fn dispatch_post_write(
        &self,
        key: &ObjectKey,
        meta: &ObjectMeta,
        data: &[u8],
        ctx: &ExtensionContext,
    ) {
        for ext in self.enabled() {
            if let Err(e) = ext.hooks().post_write(key, meta, data, ctx).await {
                tracing::error!(
                    extension = ext.info().name,
                    error = %e,
                    "post_write hook failed"
                );
            }
        }
    }

    /// Dispatch pre_delete hooks.
    pub async fn dispatch_pre_delete(
        &self,
        key: &ObjectKey,
        ctx: &mut ExtensionContext,
    ) -> Result<HookAction> {
        for ext in self.enabled() {
            match ext.hooks().pre_delete(key, ctx).await? {
                HookAction::Continue => continue,
                action => return Ok(action),
            }
        }
        Ok(HookAction::Continue)
    }

    /// Dispatch post_delete hooks.
    pub async fn dispatch_post_delete(&self, key: &ObjectKey, ctx: &ExtensionContext) {
        for ext in self.enabled() {
            if let Err(e) = ext.hooks().post_delete(key, ctx).await {
                tracing::error!(
                    extension = ext.info().name,
                    error = %e,
                    "post_delete hook failed"
                );
            }
        }
    }

    /// Dispatch a storage event to all extensions.
    pub async fn dispatch_event(&self, event: &StorageEvent) {
        for ext in self.enabled() {
            if let Err(e) = ext.hooks().on_event(event).await {
                tracing::error!(
                    extension = ext.info().name,
                    error = %e,
                    "on_event hook failed"
                );
            }
        }
    }

    /// Shutdown all extensions gracefully.
    pub async fn shutdown_all(&self) {
        for ext in self.extensions.iter().rev() {
            if let Err(e) = ext.shutdown().await {
                tracing::error!(
                    extension = ext.info().name,
                    error = %e,
                    "extension shutdown failed"
                );
            }
        }
    }

    /// List all registered extensions.
    pub fn list(&self) -> Vec<ExtensionInfo> {
        self.extensions.iter().map(|e| e.info()).collect()
    }

    /// Run health checks on all extensions, returning (info, healthy) pairs.
    pub async fn health_all(&self) -> Vec<(ExtensionInfo, bool)> {
        let mut results = Vec::new();
        for ext in &self.extensions {
            let healthy = ext.health().await.unwrap_or(false);
            results.push((ext.info(), healthy));
        }
        results
    }

    /// Find the first searchable extension and run a search.
    pub async fn search(
        &self,
        query: &str,
        top_k: usize,
        bucket: Option<&str>,
    ) -> Result<Vec<SearchHit>> {
        for ext in self.enabled() {
            if let Some(searchable) = ext.searchable() {
                return searchable.search_text(query, top_k, bucket).await;
            }
        }
        Err(crate::OrionError::Extension {
            extension: "search".into(),
            message: "No searchable extension is available".into(),
        })
    }
}

impl Default for ExtensionRegistry {
    fn default() -> Self {
        Self::new()
    }
}
