use async_trait::async_trait;

use crate::error::Result;
use crate::stream::DataStream;
use crate::types::*;

/// The core storage backend trait.
///
/// Implementations handle the actual byte storage — filesystem, block devices,
/// cloud storage, etc. The interface is intentionally minimal: bytes in, bytes out.
/// All higher-level concerns (metadata indexing, S3 protocol, extensions) live
/// in other layers.
///
/// Streaming is mandatory — implementations must handle objects larger than
/// available RAM by processing data in chunks.
#[async_trait]
pub trait StorageBackend: Send + Sync + 'static {
    /// Store an object. Data is provided as a stream of chunks.
    /// Returns the final metadata (with computed etag, checksum, etc).
    async fn put(
        &self,
        key: &ObjectKey,
        data: DataStream,
        opts: &PutOptions,
    ) -> Result<ObjectMeta>;

    /// Retrieve an object as a stream of chunks plus its metadata.
    async fn get(&self, key: &ObjectKey) -> Result<(DataStream, ObjectMeta)>;

    /// Delete an object.
    async fn delete(&self, key: &ObjectKey) -> Result<()>;

    /// Get object metadata without retrieving the body.
    async fn head(&self, key: &ObjectKey) -> Result<ObjectMeta>;

    /// Check if an object exists.
    async fn exists(&self, key: &ObjectKey) -> Result<bool> {
        match self.head(key).await {
            Ok(_) => Ok(true),
            Err(crate::OrionError::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// List objects with pagination and optional prefix/delimiter filtering.
    async fn list(&self, bucket: &str, opts: &ListOptions) -> Result<ListPage>;

    /// Get backend capacity and health statistics.
    async fn stats(&self) -> Result<BackendStats>;

    /// Backend name for logging/config purposes.
    fn name(&self) -> &str;
}

/// Bucket management operations — separated from object storage
/// because some backends may handle buckets differently (e.g. as directories,
/// database tables, or namespaces).
#[async_trait]
pub trait BucketManager: Send + Sync + 'static {
    async fn create_bucket(&self, name: &str) -> Result<BucketInfo>;
    async fn delete_bucket(&self, name: &str) -> Result<()>;
    async fn head_bucket(&self, name: &str) -> Result<BucketInfo>;
    async fn list_buckets(&self) -> Result<Vec<BucketInfo>>;
}
