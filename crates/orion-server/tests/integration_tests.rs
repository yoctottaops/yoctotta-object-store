//! Component integration tests.
//!
//! Wire up FsStore + SqliteMetaStore and exercise them together through
//! their trait interfaces — no HTTP layer involved.

use std::sync::Arc;

use orion_core::*;
use orion_ext_rag::VectorStore;
use orion_meta_sqlite::SqliteMetaStore;
use orion_store_fs::FsStore;

use tempfile::TempDir;

async fn setup() -> (TempDir, Arc<FsStore>, Arc<SqliteMetaStore>) {
    let tmp = TempDir::new().unwrap();
    let store = Arc::new(FsStore::new(tmp.path()).await.unwrap());
    let meta = Arc::new(SqliteMetaStore::in_memory().unwrap());
    (tmp, store, meta)
}

// ── Bucket lifecycle ──

#[tokio::test]
async fn bucket_create_list_delete() {
    let (_tmp, store, meta) = setup().await;

    // Create bucket via storage backend
    let info = store.create_bucket("test-bucket").await.unwrap();
    assert_eq!(info.name, "test-bucket");

    // Register in metadata store (as S3Handler does)
    meta.create_bucket(&info).await.unwrap();

    // Both should list it
    let store_buckets = store.list_buckets().await.unwrap();
    let meta_buckets = meta.list_buckets().await.unwrap();
    assert_eq!(store_buckets.len(), 1);
    assert_eq!(meta_buckets.len(), 1);
    assert_eq!(store_buckets[0].name, "test-bucket");
    assert_eq!(meta_buckets[0].name, "test-bucket");

    // Head bucket should succeed
    store.head_bucket("test-bucket").await.unwrap();
    meta.bucket_exists("test-bucket").await.unwrap();

    // Delete from both
    store.delete_bucket("test-bucket").await.unwrap();
    meta.delete_bucket("test-bucket").await.unwrap();

    // Both empty now
    assert!(store.list_buckets().await.unwrap().is_empty());
    assert!(meta.list_buckets().await.unwrap().is_empty());
}

#[tokio::test]
async fn bucket_create_duplicate_errors() {
    let (_tmp, store, meta) = setup().await;

    let info = store.create_bucket("dup").await.unwrap();
    meta.create_bucket(&info).await.unwrap();

    // Second create should fail
    let err = store.create_bucket("dup").await.unwrap_err();
    assert!(matches!(err, OrionError::BucketAlreadyExists(_)));
}

// ── Object put / get / delete roundtrip ──

#[tokio::test]
async fn object_put_get_roundtrip() {
    let (_tmp, store, meta) = setup().await;

    // Setup bucket
    let info = store.create_bucket("data").await.unwrap();
    meta.create_bucket(&info).await.unwrap();

    // Put object
    let key = ObjectKey::new("data", "hello.txt");
    let body = b"Hello, Orion!";
    let opts = PutOptions {
        content_type: Some("text/plain".into()),
        ..Default::default()
    };
    let data_stream = orion_core::stream::single_chunk_stream(bytes::Bytes::from_static(body));
    let obj_meta = store.put(&key, data_stream, &opts).await.unwrap();

    // Register in metadata
    meta.put_object_meta("data", "hello.txt", &obj_meta)
        .await
        .unwrap();

    // Verify metadata consistency
    assert_eq!(obj_meta.size, body.len() as u64);
    assert_eq!(obj_meta.content_type, "text/plain");

    let stored_meta = meta.get_object_meta("data", "hello.txt").await.unwrap();
    assert_eq!(stored_meta.size, obj_meta.size);
    assert_eq!(stored_meta.etag, obj_meta.etag);
    assert_eq!(stored_meta.content_type, obj_meta.content_type);

    // Get object and verify content
    let (stream, get_meta) = store.get(&key).await.unwrap();
    let data = orion_core::stream::collect_stream(stream).await.unwrap();
    assert_eq!(data, body);
    assert_eq!(get_meta.size, body.len() as u64);

    // Head should also work
    let head_meta = store.head(&key).await.unwrap();
    assert_eq!(head_meta.size, body.len() as u64);
    assert_eq!(head_meta.content_type, "text/plain");
}

#[tokio::test]
async fn object_delete_removes_from_both_stores() {
    let (_tmp, store, meta) = setup().await;

    let info = store.create_bucket("bucket").await.unwrap();
    meta.create_bucket(&info).await.unwrap();

    let key = ObjectKey::new("bucket", "to-delete.txt");
    let stream = orion_core::stream::single_chunk_stream(bytes::Bytes::from_static(b"data"));
    let obj_meta = store.put(&key, stream, &PutOptions::default()).await.unwrap();
    meta.put_object_meta("bucket", "to-delete.txt", &obj_meta)
        .await
        .unwrap();

    // Delete from both stores
    store.delete(&key).await.unwrap();
    meta.delete_object_meta("bucket", "to-delete.txt")
        .await
        .unwrap();

    // Object gone from storage
    assert!(!store.exists(&key).await.unwrap());

    // Object gone from metadata
    assert!(!meta.object_exists("bucket", "to-delete.txt").await.unwrap());
}

#[tokio::test]
async fn object_overwrite() {
    let (_tmp, store, meta) = setup().await;

    let info = store.create_bucket("b").await.unwrap();
    meta.create_bucket(&info).await.unwrap();

    let key = ObjectKey::new("b", "file.txt");

    // First write
    let s1 = orion_core::stream::single_chunk_stream(bytes::Bytes::from_static(b"version-1"));
    let m1 = store.put(&key, s1, &PutOptions::default()).await.unwrap();
    meta.put_object_meta("b", "file.txt", &m1).await.unwrap();

    // Overwrite
    let s2 = orion_core::stream::single_chunk_stream(bytes::Bytes::from_static(b"version-2-longer"));
    let m2 = store.put(&key, s2, &PutOptions::default()).await.unwrap();
    meta.put_object_meta("b", "file.txt", &m2).await.unwrap();

    // Read back: should get version 2
    let (stream, _) = store.get(&key).await.unwrap();
    let data = orion_core::stream::collect_stream(stream).await.unwrap();
    assert_eq!(data, b"version-2-longer");

    // Meta should also reflect new size
    let stored = meta.get_object_meta("b", "file.txt").await.unwrap();
    assert_eq!(stored.size, b"version-2-longer".len() as u64);

    // Still only 1 object
    assert_eq!(meta.object_count("b").await.unwrap(), 1);
}

// ── Listing consistency ──

#[tokio::test]
async fn list_objects_consistent_between_stores() {
    let (_tmp, store, meta) = setup().await;

    let info = store.create_bucket("files").await.unwrap();
    meta.create_bucket(&info).await.unwrap();

    let objects = &[
        ("docs/readme.md", "# Hello"),
        ("docs/api.md", "## API"),
        ("images/logo.png", "PNG_DATA"),
        ("config.toml", "[server]"),
    ];

    for (key, content) in objects {
        let obj_key = ObjectKey::new("files", *key);
        let stream =
            orion_core::stream::single_chunk_stream(bytes::Bytes::from(content.as_bytes()));
        let obj_meta = store
            .put(&obj_key, stream, &PutOptions::default())
            .await
            .unwrap();
        meta.put_object_meta("files", key, &obj_meta)
            .await
            .unwrap();
    }

    // List all - metadata store used for listing
    let all = meta
        .list_objects("files", &ListOptions::default())
        .await
        .unwrap();
    assert_eq!(all.entries.len(), 4);

    // List with prefix
    let docs = meta
        .list_objects(
            "files",
            &ListOptions {
                prefix: Some("docs/".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(docs.entries.len(), 2);
    assert!(docs.entries.iter().all(|e| e.key.starts_with("docs/")));

    // List with delimiter (get common prefixes)
    let with_delim = meta
        .list_objects(
            "files",
            &ListOptions {
                delimiter: Some("/".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    // Should have config.toml as entry + docs/ and images/ as common prefixes
    assert_eq!(with_delim.entries.len(), 1);
    assert_eq!(with_delim.entries[0].key, "config.toml");
    assert_eq!(with_delim.common_prefixes.len(), 2);

    let prefixes: Vec<&str> = with_delim.common_prefixes.iter().map(|p| p.prefix.as_str()).collect();
    assert!(prefixes.contains(&"docs/"));
    assert!(prefixes.contains(&"images/"));
}

#[tokio::test]
async fn list_objects_pagination() {
    let (_tmp, store, meta) = setup().await;

    let info = store.create_bucket("pag").await.unwrap();
    meta.create_bucket(&info).await.unwrap();

    // Create 5 objects
    for i in 0..5 {
        let key_str = format!("file-{:02}.txt", i);
        let obj_key = ObjectKey::new("pag", &key_str);
        let stream = orion_core::stream::single_chunk_stream(bytes::Bytes::from(format!("data-{}", i)));
        let obj_meta = store
            .put(&obj_key, stream, &PutOptions::default())
            .await
            .unwrap();
        meta.put_object_meta("pag", &key_str, &obj_meta)
            .await
            .unwrap();
    }

    // Page 1: 2 items
    let page1 = meta
        .list_objects(
            "pag",
            &ListOptions {
                max_keys: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(page1.entries.len(), 2);
    assert!(page1.is_truncated);
    assert!(page1.next_cursor.is_some());

    // Page 2: next 2 items
    let page2 = meta
        .list_objects(
            "pag",
            &ListOptions {
                max_keys: 2,
                cursor: page1.next_cursor.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(page2.entries.len(), 2);
    assert!(page2.is_truncated);

    // Page 3: last 1 item
    let page3 = meta
        .list_objects(
            "pag",
            &ListOptions {
                max_keys: 2,
                cursor: page2.next_cursor.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(page3.entries.len(), 1);
    assert!(!page3.is_truncated);

    // All keys across pages should be unique and cover the 5 objects
    let mut all_keys: Vec<String> = page1
        .entries
        .iter()
        .chain(page2.entries.iter())
        .chain(page3.entries.iter())
        .map(|e| e.key.clone())
        .collect();
    all_keys.sort();
    assert_eq!(all_keys.len(), 5);
}

// ── Stats ──

#[tokio::test]
async fn stats_reflect_stored_data() {
    let (_tmp, store, meta) = setup().await;

    let info = store.create_bucket("stats").await.unwrap();
    meta.create_bucket(&info).await.unwrap();

    let key = ObjectKey::new("stats", "big.bin");
    let data = vec![42u8; 4096];
    let stream = orion_core::stream::single_chunk_stream(bytes::Bytes::from(data.clone()));
    let obj_meta = store.put(&key, stream, &PutOptions::default()).await.unwrap();
    meta.put_object_meta("stats", "big.bin", &obj_meta)
        .await
        .unwrap();

    let backend_stats = store.stats().await.unwrap();
    assert!(backend_stats.is_healthy);
    assert!(backend_stats.used_bytes >= 4096);
    assert!(backend_stats.object_count >= 1);

    let meta_count = meta.object_count("stats").await.unwrap();
    let meta_size = meta.total_size("stats").await.unwrap();
    assert_eq!(meta_count, 1);
    assert_eq!(meta_size, 4096);
}

// ── User metadata roundtrip ──

#[tokio::test]
async fn user_metadata_roundtrip() {
    let (_tmp, store, meta) = setup().await;

    let info = store.create_bucket("umeta").await.unwrap();
    meta.create_bucket(&info).await.unwrap();

    let key = ObjectKey::new("umeta", "tagged.txt");
    let mut user_meta = std::collections::HashMap::new();
    user_meta.insert("author".into(), "orion".into());
    user_meta.insert("version".into(), "1.0".into());

    let opts = PutOptions {
        content_type: Some("text/plain".into()),
        user_meta,
        ..Default::default()
    };

    let stream = orion_core::stream::single_chunk_stream(bytes::Bytes::from_static(b"tagged content"));
    let obj_meta = store.put(&key, stream, &opts).await.unwrap();

    // Store returns user metadata
    assert_eq!(obj_meta.user_meta.get("author").unwrap(), "orion");
    assert_eq!(obj_meta.user_meta.get("version").unwrap(), "1.0");

    // Save to metadata store and retrieve
    meta.put_object_meta("umeta", "tagged.txt", &obj_meta)
        .await
        .unwrap();
    let retrieved = meta.get_object_meta("umeta", "tagged.txt").await.unwrap();
    assert_eq!(retrieved.user_meta.get("author").unwrap(), "orion");
    assert_eq!(retrieved.user_meta.get("version").unwrap(), "1.0");
}

// ── Extension integration ──

#[tokio::test]
async fn extension_registry_pre_write_hook_deny() {
    use orion_core::extension::*;

    let registry = ExtensionRegistry::new();

    // Empty registry allows everything
    let key = ObjectKey::new("b", "k");
    let opts = PutOptions::default();
    let mut ctx = ExtensionContext::new();
    let action = registry.dispatch_pre_write(&key, &opts, &mut ctx).await.unwrap();
    assert!(matches!(action, HookAction::Continue));
}

#[tokio::test]
async fn rag_extension_indexes_through_hooks() {
    use orion_core::extension::*;

    let embedder = Arc::new(orion_ext_rag::MockEmbedder::new(16));
    let vector_store = Arc::new(orion_ext_rag::InMemoryVectorStore::new());
    let rag = orion_ext_rag::RagExtension::new(embedder, vector_store.clone());

    let (_tmp, store, meta) = setup().await;

    let info = store.create_bucket("rag-test").await.unwrap();
    meta.create_bucket(&info).await.unwrap();

    // Put a text object
    let key = ObjectKey::new("rag-test", "document.txt");
    let content = b"The quick brown fox jumps over the lazy dog";
    let opts = PutOptions {
        content_type: Some("text/plain".into()),
        ..Default::default()
    };
    let stream = orion_core::stream::single_chunk_stream(bytes::Bytes::from_static(content));
    let obj_meta = store.put(&key, stream, &opts).await.unwrap();
    meta.put_object_meta("rag-test", "document.txt", &obj_meta)
        .await
        .unwrap();

    // Trigger RAG indexing via post_write hook (as S3Handler does)
    let ctx = ExtensionContext::new();
    rag.hooks()
        .post_write(&key, &obj_meta, content, &ctx)
        .await
        .unwrap();

    // Vectors should be indexed
    let count = VectorStore::count(vector_store.as_ref()).await.unwrap();
    assert!(count > 0);

    // Search should find the document
    let results = rag.search("brown fox", 5).await.unwrap();
    assert!(!results.is_empty());
    assert_eq!(results[0].bucket, "rag-test");

    // Delete the object and trigger post_delete hook
    store.delete(&key).await.unwrap();
    meta.delete_object_meta("rag-test", "document.txt")
        .await
        .unwrap();
    rag.hooks().post_delete(&key, &ctx).await.unwrap();

    // Vectors should be removed
    let count = VectorStore::count(vector_store.as_ref()).await.unwrap();
    assert_eq!(count, 0);
}

// ── Multi-bucket operations ──

#[tokio::test]
async fn multi_bucket_isolation() {
    let (_tmp, store, meta) = setup().await;

    // Create two buckets
    for name in &["alpha", "bravo"] {
        let info = store.create_bucket(name).await.unwrap();
        meta.create_bucket(&info).await.unwrap();
    }

    // Put same key in both buckets
    for bucket in &["alpha", "bravo"] {
        let key = ObjectKey::new(*bucket, "shared-name.txt");
        let content = format!("content from {}", bucket);
        let stream =
            orion_core::stream::single_chunk_stream(bytes::Bytes::from(content.into_bytes()));
        let obj_meta = store
            .put(&key, stream, &PutOptions::default())
            .await
            .unwrap();
        meta.put_object_meta(bucket, "shared-name.txt", &obj_meta)
            .await
            .unwrap();
    }

    // Each bucket has its own object
    assert_eq!(meta.object_count("alpha").await.unwrap(), 1);
    assert_eq!(meta.object_count("bravo").await.unwrap(), 1);

    // Content is different
    let key_a = ObjectKey::new("alpha", "shared-name.txt");
    let key_b = ObjectKey::new("bravo", "shared-name.txt");

    let (s_a, _) = store.get(&key_a).await.unwrap();
    let (s_b, _) = store.get(&key_b).await.unwrap();
    let data_a = orion_core::stream::collect_stream(s_a).await.unwrap();
    let data_b = orion_core::stream::collect_stream(s_b).await.unwrap();

    assert_eq!(data_a, b"content from alpha");
    assert_eq!(data_b, b"content from bravo");

    // Delete from alpha doesn't affect bravo
    store.delete(&key_a).await.unwrap();
    meta.delete_object_meta("alpha", "shared-name.txt")
        .await
        .unwrap();

    assert!(!store.exists(&key_a).await.unwrap());
    assert!(store.exists(&key_b).await.unwrap());
    assert_eq!(meta.object_count("alpha").await.unwrap(), 0);
    assert_eq!(meta.object_count("bravo").await.unwrap(), 1);
}

// ── Nested / deep paths ──

#[tokio::test]
async fn deeply_nested_paths() {
    let (_tmp, store, meta) = setup().await;

    let info = store.create_bucket("deep").await.unwrap();
    meta.create_bucket(&info).await.unwrap();

    let key = ObjectKey::new("deep", "a/b/c/d/e/file.txt");
    let stream = orion_core::stream::single_chunk_stream(bytes::Bytes::from_static(b"deep content"));
    let obj_meta = store
        .put(&key, stream, &PutOptions::default())
        .await
        .unwrap();
    meta.put_object_meta("deep", "a/b/c/d/e/file.txt", &obj_meta)
        .await
        .unwrap();

    // Retrieve
    let (stream, _) = store.get(&key).await.unwrap();
    let data = orion_core::stream::collect_stream(stream).await.unwrap();
    assert_eq!(data, b"deep content");

    // List with prefix at each level
    for prefix in &["a/", "a/b/", "a/b/c/", "a/b/c/d/", "a/b/c/d/e/"] {
        let page = meta
            .list_objects(
                "deep",
                &ListOptions {
                    prefix: Some(prefix.to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            page.entries.len(),
            1,
            "prefix {} should match the object",
            prefix
        );
    }
}

// ── Storage events ──

#[tokio::test]
async fn storage_event_properties() {
    let key = ObjectKey::new("bucket", "key.txt");
    let meta = ObjectMeta::default();

    let event = StorageEvent::ObjectCreated {
        key: key.clone(),
        meta: meta.clone(),
    };
    assert_eq!(event.event_name(), "s3:ObjectCreated:Put");
    assert_eq!(event.bucket(), "bucket");

    let del_event = StorageEvent::ObjectDeleted { key };
    assert_eq!(del_event.event_name(), "s3:ObjectRemoved:Delete");
    assert_eq!(del_event.bucket(), "bucket");

    let bucket_event = StorageEvent::BucketCreated {
        name: "new".into(),
    };
    assert_eq!(bucket_event.event_name(), "s3:BucketCreated");
    assert_eq!(bucket_event.bucket(), "new");
}

// ── Error handling consistency ──

#[tokio::test]
async fn get_nonexistent_object_errors() {
    let (_tmp, store, _meta) = setup().await;

    store.create_bucket("err").await.unwrap();
    let key = ObjectKey::new("err", "nope.txt");
    match store.get(&key).await {
        Err(OrionError::NotFound(_)) => {}
        Err(other) => panic!("Expected NotFound, got {:?}", other),
        Ok(_) => panic!("Expected error, got Ok"),
    }
}

#[tokio::test]
async fn get_from_nonexistent_bucket_errors() {
    let (_tmp, store, _meta) = setup().await;

    let key = ObjectKey::new("no-such-bucket", "file.txt");
    match store.get(&key).await {
        Err(OrionError::BucketNotFound(_)) | Err(OrionError::NotFound(_)) => {}
        Err(other) => panic!("Expected BucketNotFound or NotFound, got {:?}", other),
        Ok(_) => panic!("Expected error, got Ok"),
    }
}

#[tokio::test]
async fn delete_nonexistent_bucket_errors() {
    let (_tmp, store, _meta) = setup().await;

    let err = store.delete_bucket("ghost").await.unwrap_err();
    assert!(
        matches!(&err, OrionError::BucketNotFound(_) | OrionError::NotFound(_)),
        "Expected BucketNotFound or NotFound, got {:?}",
        err
    );
}
