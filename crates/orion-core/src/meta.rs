use async_trait::async_trait;

use crate::error::Result;
use crate::types::*;

/// Metadata store provides indexed access to object and bucket metadata.
///
/// This is separate from StorageBackend because metadata needs different
/// access patterns (listing, searching, filtering) than raw byte storage.
/// The default implementation uses SQLite, but could be swapped for
/// PostgreSQL, etcd, or a distributed KV store for clustering.
#[async_trait]
pub trait MetadataStore: Send + Sync + 'static {
    // ── Bucket operations ──

    async fn create_bucket(&self, info: &BucketInfo) -> Result<()>;
    async fn delete_bucket(&self, name: &str) -> Result<()>;
    async fn get_bucket(&self, name: &str) -> Result<BucketInfo>;
    async fn list_buckets(&self) -> Result<Vec<BucketInfo>>;
    async fn bucket_exists(&self, name: &str) -> Result<bool>;

    // ── Object metadata operations ──

    async fn put_object_meta(
        &self,
        bucket: &str,
        key: &str,
        meta: &ObjectMeta,
    ) -> Result<()>;

    async fn get_object_meta(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<ObjectMeta>;

    async fn delete_object_meta(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<()>;

    async fn list_objects(
        &self,
        bucket: &str,
        opts: &ListOptions,
    ) -> Result<ListPage>;

    async fn object_exists(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<bool>;

    // ── Stats ──

    async fn object_count(&self, bucket: &str) -> Result<u64>;
    async fn total_size(&self, bucket: &str) -> Result<u64>;
}
