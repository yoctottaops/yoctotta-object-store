use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configuration for the trigger extension.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct TriggerConfig {
    /// List of trigger rules.
    #[serde(default)]
    pub rules: Vec<TriggerRule>,
    /// Default timeout for webhook calls in seconds.
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    /// Maximum concurrent webhook dispatches.
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent: usize,
    /// Number of retry attempts on failure.
    #[serde(default = "default_retries")]
    pub retries: u32,
}

fn default_timeout() -> u64 {
    10
}
fn default_max_concurrent() -> usize {
    16
}
fn default_retries() -> u32 {
    3
}

/// A trigger rule: when event X happens on bucket/prefix Y, fire webhook Z.
#[derive(Debug, Clone, Deserialize)]
pub struct TriggerRule {
    /// Rule name for logging.
    pub name: String,
    /// Events that activate this trigger (S3 event names).
    /// e.g. ["s3:ObjectCreated:Put", "s3:ObjectRemoved:Delete"]
    pub events: Vec<String>,
    /// Bucket filter (exact match). Empty = all buckets.
    #[serde(default)]
    pub bucket: Option<String>,
    /// Key prefix filter. Empty = all keys.
    #[serde(default)]
    pub key_prefix: Option<String>,
    /// Key suffix filter (e.g. ".json"). Empty = all keys.
    #[serde(default)]
    pub key_suffix: Option<String>,
    /// Webhook URL to call.
    pub url: String,
    /// HTTP method (default: POST).
    #[serde(default = "default_method")]
    pub method: String,
    /// Additional headers to send.
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// Whether this rule is enabled.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

fn default_method() -> String {
    "POST".into()
}
fn default_enabled() -> bool {
    true
}

/// The webhook payload sent to the configured URL.
/// Follows a structure similar to S3 event notifications.
#[derive(Debug, Clone, Serialize)]
pub struct WebhookPayload {
    /// Event name (e.g. "s3:ObjectCreated:Put").
    pub event: String,
    /// Timestamp of the event.
    pub timestamp: DateTime<Utc>,
    /// Bucket name.
    pub bucket: String,
    /// Object key (if applicable).
    pub key: Option<String>,
    /// Object size in bytes (if applicable).
    pub size: Option<u64>,
    /// Object ETag (if applicable).
    pub etag: Option<String>,
    /// Content type (if applicable).
    pub content_type: Option<String>,
    /// The trigger rule name that matched.
    pub rule: String,
    /// Unique delivery ID for idempotency.
    pub delivery_id: String,
}
