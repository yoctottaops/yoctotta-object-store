use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Unique object identifier within a bucket.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectKey {
    pub bucket: String,
    pub key: String,
}

impl ObjectKey {
    pub fn new(bucket: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            key: key.into(),
        }
    }
}

impl std::fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.bucket, self.key)
    }
}

/// Metadata associated with a stored object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectMeta {
    pub size: u64,
    pub etag: String,
    pub content_type: String,
    pub created_at: DateTime<Utc>,
    pub modified_at: DateTime<Utc>,
    /// User-defined metadata (x-amz-meta-* headers).
    pub user_meta: HashMap<String, String>,
    /// Storage-level checksum (CRC32, SHA256, etc).
    pub checksum: Option<String>,
    /// Version ID (None if versioning is disabled).
    pub version_id: Option<String>,
}

impl Default for ObjectMeta {
    fn default() -> Self {
        let now = Utc::now();
        Self {
            size: 0,
            etag: String::new(),
            content_type: "application/octet-stream".into(),
            created_at: now,
            modified_at: now,
            user_meta: HashMap::new(),
            checksum: None,
            version_id: None,
        }
    }
}

/// Bucket information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketInfo {
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub region: Option<String>,
}

/// A single entry in a list operation result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListEntry {
    pub key: String,
    pub meta: ObjectMeta,
}

/// Common prefix entry for S3 list operations (folder simulation).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommonPrefix {
    pub prefix: String,
}

/// Paginated list result.
#[derive(Debug, Clone)]
pub struct ListPage {
    pub entries: Vec<ListEntry>,
    pub common_prefixes: Vec<CommonPrefix>,
    pub next_cursor: Option<String>,
    pub is_truncated: bool,
}

/// Backend health and capacity statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackendStats {
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub object_count: u64,
    pub is_healthy: bool,
}

/// Options for put operations.
#[derive(Debug, Clone, Default)]
pub struct PutOptions {
    pub content_type: Option<String>,
    pub user_meta: HashMap<String, String>,
    /// Expected content-md5 for integrity verification.
    pub content_md5: Option<String>,
}

/// Options for list operations.
#[derive(Debug, Clone)]
pub struct ListOptions {
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub cursor: Option<String>,
    pub max_keys: u32,
}

impl Default for ListOptions {
    fn default() -> Self {
        Self {
            prefix: None,
            delimiter: None,
            cursor: None,
            max_keys: 1000,
        }
    }
}

/// Storage event types emitted by the core for extensions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StorageEvent {
    ObjectCreated {
        key: ObjectKey,
        meta: ObjectMeta,
    },
    ObjectDeleted {
        key: ObjectKey,
    },
    ObjectAccessed {
        key: ObjectKey,
    },
    BucketCreated {
        name: String,
    },
    BucketDeleted {
        name: String,
    },
}

impl StorageEvent {
    /// S3-compatible event name (e.g. "s3:ObjectCreated:Put").
    pub fn event_name(&self) -> &'static str {
        match self {
            StorageEvent::ObjectCreated { .. } => "s3:ObjectCreated:Put",
            StorageEvent::ObjectDeleted { .. } => "s3:ObjectRemoved:Delete",
            StorageEvent::ObjectAccessed { .. } => "s3:ObjectAccessed:Get",
            StorageEvent::BucketCreated { .. } => "s3:BucketCreated",
            StorageEvent::BucketDeleted { .. } => "s3:BucketRemoved",
        }
    }

    /// The bucket this event relates to.
    pub fn bucket(&self) -> &str {
        match self {
            StorageEvent::ObjectCreated { key, .. } => &key.bucket,
            StorageEvent::ObjectDeleted { key } => &key.bucket,
            StorageEvent::ObjectAccessed { key } => &key.bucket,
            StorageEvent::BucketCreated { name } => name,
            StorageEvent::BucketDeleted { name } => name,
        }
    }
}
