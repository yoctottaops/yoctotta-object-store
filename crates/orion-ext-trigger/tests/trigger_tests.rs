use orion_core::extension::*;
use orion_ext_trigger::*;

// ── TriggerConfig tests ──

#[test]
fn trigger_config_defaults() {
    let config = TriggerConfig::default();
    assert!(config.rules.is_empty());
    // #[derive(Default)] gives u64::default() = 0, not serde defaults
    assert_eq!(config.timeout_secs, 0);
    assert_eq!(config.max_concurrent, 0);
    assert_eq!(config.retries, 0);
}

#[test]
fn trigger_config_deserialize() {
    let toml_str = r#"
        timeout_secs = 5
        max_concurrent = 8
        retries = 2

        [[rules]]
        name = "notify-upload"
        events = ["s3:ObjectCreated:Put"]
        bucket = "uploads"
        url = "http://localhost:8080/webhook"
        method = "POST"
        enabled = true

        [[rules]]
        name = "catch-all"
        events = ["*"]
        url = "http://localhost:8080/all"
    "#;

    let config: TriggerConfig = toml::from_str(toml_str).unwrap();
    assert_eq!(config.timeout_secs, 5);
    assert_eq!(config.max_concurrent, 8);
    assert_eq!(config.retries, 2);
    assert_eq!(config.rules.len(), 2);

    let rule = &config.rules[0];
    assert_eq!(rule.name, "notify-upload");
    assert_eq!(rule.events, vec!["s3:ObjectCreated:Put"]);
    assert_eq!(rule.bucket, Some("uploads".into()));
    assert_eq!(rule.url, "http://localhost:8080/webhook");
    assert_eq!(rule.method, "POST");
    assert!(rule.enabled);

    let rule2 = &config.rules[1];
    assert_eq!(rule2.name, "catch-all");
    assert!(rule2.bucket.is_none());
    assert!(rule2.key_prefix.is_none());
    assert!(rule2.key_suffix.is_none());
}

#[test]
fn trigger_rule_with_filters() {
    let toml_str = r#"
        name = "json-only"
        events = ["s3:ObjectCreated:Put"]
        bucket = "data"
        key_prefix = "input/"
        key_suffix = ".json"
        url = "http://localhost:8080/hook"
        method = "PUT"
        enabled = true

        [headers]
        Authorization = "Bearer token123"
        X-Custom = "value"
    "#;

    let rule: TriggerRule = toml::from_str(toml_str).unwrap();
    assert_eq!(rule.name, "json-only");
    assert_eq!(rule.key_prefix, Some("input/".into()));
    assert_eq!(rule.key_suffix, Some(".json".into()));
    assert_eq!(rule.method, "PUT");
    assert_eq!(rule.headers.get("Authorization").unwrap(), "Bearer token123");
    assert_eq!(rule.headers.get("X-Custom").unwrap(), "value");
}

#[test]
fn trigger_rule_defaults() {
    let toml_str = r#"
        name = "minimal"
        events = ["s3:ObjectCreated:Put"]
        url = "http://localhost:8080/hook"
    "#;

    let rule: TriggerRule = toml::from_str(toml_str).unwrap();
    assert_eq!(rule.method, "POST"); // default
    assert!(rule.enabled); // default true
    assert!(rule.bucket.is_none());
    assert!(rule.key_prefix.is_none());
    assert!(rule.key_suffix.is_none());
    assert!(rule.headers.is_empty());
}

// ── WebhookPayload tests ──

#[test]
fn webhook_payload_serializes() {
    let payload = WebhookPayload {
        event: "s3:ObjectCreated:Put".into(),
        timestamp: chrono::Utc::now(),
        bucket: "my-bucket".into(),
        key: Some("path/to/file.txt".into()),
        size: Some(1024),
        etag: Some("\"abc123\"".into()),
        content_type: Some("text/plain".into()),
        rule: "my-rule".into(),
        delivery_id: "delivery-001".into(),
    };

    let json = serde_json::to_string(&payload).unwrap();
    assert!(json.contains("s3:ObjectCreated:Put"));
    assert!(json.contains("my-bucket"));
    assert!(json.contains("path/to/file.txt"));
    assert!(json.contains("1024"));
}

#[test]
fn webhook_payload_bucket_event() {
    let payload = WebhookPayload {
        event: "s3:BucketCreated".into(),
        timestamp: chrono::Utc::now(),
        bucket: "new-bucket".into(),
        key: None,
        size: None,
        etag: None,
        content_type: None,
        rule: "bucket-watcher".into(),
        delivery_id: "del-002".into(),
    };

    let json = serde_json::to_string(&payload).unwrap();
    assert!(json.contains("new-bucket"));
    assert!(json.contains("null") || json.contains(r#""key":null"#));
}

// ── TriggerExtension tests ──

#[test]
fn trigger_extension_info() {
    let ext = TriggerExtension::new();
    let info = ext.info();
    assert_eq!(info.name, "trigger");
    assert!(info.enabled);
}

#[tokio::test]
async fn trigger_extension_shutdown() {
    let ext = TriggerExtension::new();
    ext.shutdown().await.unwrap();
}

#[tokio::test]
async fn trigger_extension_health() {
    let ext = TriggerExtension::new();
    assert!(ext.health().await.unwrap());
}

#[tokio::test]
async fn trigger_on_event_no_rules() {
    let ext = TriggerExtension::new();
    let event = orion_core::types::StorageEvent::BucketCreated {
        name: "test".into(),
    };
    // Should succeed with no matching rules (no webhooks to fire)
    ext.hooks().on_event(&event).await.unwrap();
}
