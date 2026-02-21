use std::collections::HashMap;
use tempfile::TempDir;

use orion_core::*;
use orion_core::stream::{collect_stream, vec_to_stream};
use orion_store_fs::FsStore;

async fn setup() -> (TempDir, FsStore) {
    let dir = TempDir::new().unwrap();
    let store = FsStore::new(dir.path()).await.unwrap();
    (dir, store)
}

fn test_data(content: &[u8]) -> DataStream {
    vec_to_stream(content.to_vec())
}

// ── BucketManager tests ──

#[tokio::test]
async fn create_and_list_buckets() {
    let (_dir, store) = setup().await;

    let info = store.create_bucket("my-bucket").await.unwrap();
    assert_eq!(info.name, "my-bucket");

    let buckets = store.list_buckets().await.unwrap();
    assert_eq!(buckets.len(), 1);
    assert_eq!(buckets[0].name, "my-bucket");
}

#[tokio::test]
async fn create_bucket_duplicate() {
    let (_dir, store) = setup().await;
    store.create_bucket("b").await.unwrap();

    let err = store.create_bucket("b").await.unwrap_err();
    assert!(matches!(err, OrionError::BucketAlreadyExists(_)));
}

#[tokio::test]
async fn delete_bucket() {
    let (_dir, store) = setup().await;
    store.create_bucket("b").await.unwrap();
    store.delete_bucket("b").await.unwrap();

    let buckets = store.list_buckets().await.unwrap();
    assert!(buckets.is_empty());
}

#[tokio::test]
async fn delete_nonexistent_bucket() {
    let (_dir, store) = setup().await;
    let err = store.delete_bucket("nope").await.unwrap_err();
    assert!(matches!(err, OrionError::BucketNotFound(_)));
}

#[tokio::test]
async fn head_bucket() {
    let (_dir, store) = setup().await;
    store.create_bucket("b").await.unwrap();

    let info = store.head_bucket("b").await.unwrap();
    assert_eq!(info.name, "b");
}

#[tokio::test]
async fn head_nonexistent_bucket() {
    let (_dir, store) = setup().await;
    let err = store.head_bucket("nope").await.unwrap_err();
    assert!(matches!(err, OrionError::BucketNotFound(_)));
}

// ── StorageBackend tests ──

#[tokio::test]
async fn put_and_get_object() {
    let (_dir, store) = setup().await;
    store.create_bucket("b").await.unwrap();

    let key = ObjectKey::new("b", "hello.txt");
    let opts = PutOptions {
        content_type: Some("text/plain".into()),
        ..Default::default()
    };

    let meta = store
        .put(&key, test_data(b"hello world"), &opts)
        .await
        .unwrap();

    assert_eq!(meta.size, 11);
    assert_eq!(meta.content_type, "text/plain");
    assert!(meta.etag.starts_with('"'));
    assert!(meta.etag.ends_with('"'));
    assert!(meta.checksum.is_some());

    // Get it back
    let (stream, get_meta) = store.get(&key).await.unwrap();
    let data = collect_stream(stream).await.unwrap();
    assert_eq!(data, b"hello world");
    assert_eq!(get_meta.size, 11);
}

#[tokio::test]
async fn put_default_content_type() {
    let (_dir, store) = setup().await;
    store.create_bucket("b").await.unwrap();

    let key = ObjectKey::new("b", "data.bin");
    let meta = store
        .put(&key, test_data(b"bytes"), &PutOptions::default())
        .await
        .unwrap();

    assert_eq!(meta.content_type, "application/octet-stream");
}

#[tokio::test]
async fn get_nonexistent_object() {
    let (_dir, store) = setup().await;
    store.create_bucket("b").await.unwrap();

    let key = ObjectKey::new("b", "nope.txt");
    match store.get(&key).await {
        Err(OrionError::NotFound(_)) => {}
        Err(other) => panic!("Expected NotFound, got {:?}", other),
        Ok(_) => panic!("Expected error, got Ok"),
    }
}

#[tokio::test]
async fn delete_object() {
    let (_dir, store) = setup().await;
    store.create_bucket("b").await.unwrap();

    let key = ObjectKey::new("b", "file.txt");
    store
        .put(&key, test_data(b"data"), &PutOptions::default())
        .await
        .unwrap();

    store.delete(&key).await.unwrap();

    match store.get(&key).await {
        Err(OrionError::NotFound(_)) => {}
        Err(other) => panic!("Expected NotFound, got {:?}", other),
        Ok(_) => panic!("Expected error, got Ok"),
    }
}

#[tokio::test]
async fn delete_nonexistent_object() {
    let (_dir, store) = setup().await;
    store.create_bucket("b").await.unwrap();

    let key = ObjectKey::new("b", "nope.txt");
    let err = store.delete(&key).await.unwrap_err();
    assert!(matches!(err, OrionError::NotFound(_)));
}

#[tokio::test]
async fn head_object() {
    let (_dir, store) = setup().await;
    store.create_bucket("b").await.unwrap();

    let key = ObjectKey::new("b", "file.txt");
    store
        .put(&key, test_data(b"content"), &PutOptions::default())
        .await
        .unwrap();

    let meta = store.head(&key).await.unwrap();
    assert_eq!(meta.size, 7);
}

#[tokio::test]
async fn exists_check() {
    let (_dir, store) = setup().await;
    store.create_bucket("b").await.unwrap();

    let key = ObjectKey::new("b", "file.txt");
    assert!(!store.exists(&key).await.unwrap());

    store
        .put(&key, test_data(b"data"), &PutOptions::default())
        .await
        .unwrap();
    assert!(store.exists(&key).await.unwrap());
}

#[tokio::test]
async fn put_with_user_meta() {
    let (_dir, store) = setup().await;
    store.create_bucket("b").await.unwrap();

    let key = ObjectKey::new("b", "doc.txt");
    let mut user_meta = HashMap::new();
    user_meta.insert("author".into(), "test".into());

    let opts = PutOptions {
        user_meta,
        ..Default::default()
    };

    store.put(&key, test_data(b"doc"), &opts).await.unwrap();

    let meta = store.head(&key).await.unwrap();
    assert_eq!(meta.user_meta.get("author").unwrap(), "test");
}

#[tokio::test]
async fn overwrite_object() {
    let (_dir, store) = setup().await;
    store.create_bucket("b").await.unwrap();

    let key = ObjectKey::new("b", "file.txt");
    store
        .put(&key, test_data(b"first"), &PutOptions::default())
        .await
        .unwrap();
    store
        .put(&key, test_data(b"second version"), &PutOptions::default())
        .await
        .unwrap();

    let (stream, meta) = store.get(&key).await.unwrap();
    let data = collect_stream(stream).await.unwrap();
    assert_eq!(data, b"second version");
    assert_eq!(meta.size, 14);
}

#[tokio::test]
async fn nested_key_paths() {
    let (_dir, store) = setup().await;
    store.create_bucket("b").await.unwrap();

    let key = ObjectKey::new("b", "deep/nested/path/file.txt");
    store
        .put(&key, test_data(b"nested"), &PutOptions::default())
        .await
        .unwrap();

    let (stream, _meta) = store.get(&key).await.unwrap();
    let data = collect_stream(stream).await.unwrap();
    assert_eq!(data, b"nested");
}

#[tokio::test]
async fn stats() {
    let (_dir, store) = setup().await;
    store.create_bucket("b").await.unwrap();

    let key = ObjectKey::new("b", "f.txt");
    store
        .put(&key, test_data(b"hello"), &PutOptions::default())
        .await
        .unwrap();

    let stats = store.stats().await.unwrap();
    assert!(stats.is_healthy);
    assert_eq!(stats.object_count, 1);
    assert_eq!(stats.used_bytes, 5);
}

#[tokio::test]
async fn backend_name() {
    let (_dir, store) = setup().await;
    assert_eq!(store.name(), "filesystem");
}

#[tokio::test]
async fn empty_object() {
    let (_dir, store) = setup().await;
    store.create_bucket("b").await.unwrap();

    let key = ObjectKey::new("b", "empty.txt");
    let meta = store
        .put(&key, test_data(b""), &PutOptions::default())
        .await
        .unwrap();
    assert_eq!(meta.size, 0);

    let (stream, _) = store.get(&key).await.unwrap();
    let data = collect_stream(stream).await.unwrap();
    assert!(data.is_empty());
}

#[tokio::test]
async fn large_object() {
    let (_dir, store) = setup().await;
    store.create_bucket("b").await.unwrap();

    let large_data = vec![42u8; 256 * 1024]; // 256KB
    let key = ObjectKey::new("b", "large.bin");
    let meta = store
        .put(&key, test_data(&large_data), &PutOptions::default())
        .await
        .unwrap();
    assert_eq!(meta.size, 256 * 1024);

    let (stream, _) = store.get(&key).await.unwrap();
    let data = collect_stream(stream).await.unwrap();
    assert_eq!(data.len(), 256 * 1024);
    assert_eq!(data, large_data);
}

#[tokio::test]
async fn list_objects_basic() {
    let (_dir, store) = setup().await;
    store.create_bucket("b").await.unwrap();

    for name in &["a.txt", "b.txt", "c.txt"] {
        let key = ObjectKey::new("b", *name);
        store
            .put(&key, test_data(b"data"), &PutOptions::default())
            .await
            .unwrap();
    }

    let page = store
        .list("b", &ListOptions::default())
        .await
        .unwrap();
    assert_eq!(page.entries.len(), 3);
    assert!(!page.is_truncated);
}

#[tokio::test]
async fn list_objects_nonexistent_bucket() {
    let (_dir, store) = setup().await;
    let err = store
        .list("nope", &ListOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(err, OrionError::BucketNotFound(_)));
}
