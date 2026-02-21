use chrono::Utc;
use std::collections::HashMap;

use orion_core::*;
use orion_meta_sqlite::SqliteMetaStore;

fn make_meta(size: u64, key_suffix: &str) -> ObjectMeta {
    ObjectMeta {
        size,
        etag: format!("\"etag-{}\"", key_suffix),
        content_type: "text/plain".into(),
        created_at: Utc::now(),
        modified_at: Utc::now(),
        user_meta: HashMap::new(),
        checksum: Some("sha256-abc".into()),
        version_id: None,
    }
}

#[tokio::test]
async fn create_and_get_bucket() {
    let store = SqliteMetaStore::in_memory().unwrap();
    let info = BucketInfo {
        name: "test-bucket".into(),
        created_at: Utc::now(),
        region: Some("us-east-1".into()),
    };
    store.create_bucket(&info).await.unwrap();

    let fetched = store.get_bucket("test-bucket").await.unwrap();
    assert_eq!(fetched.name, "test-bucket");
    assert_eq!(fetched.region, Some("us-east-1".into()));
}

#[tokio::test]
async fn create_bucket_duplicate() {
    let store = SqliteMetaStore::in_memory().unwrap();
    let info = BucketInfo {
        name: "dup".into(),
        created_at: Utc::now(),
        region: None,
    };
    store.create_bucket(&info).await.unwrap();

    let err = store.create_bucket(&info).await.unwrap_err();
    assert!(matches!(err, OrionError::BucketAlreadyExists(_)));
}

#[tokio::test]
async fn delete_bucket() {
    let store = SqliteMetaStore::in_memory().unwrap();
    let info = BucketInfo {
        name: "to-delete".into(),
        created_at: Utc::now(),
        region: None,
    };
    store.create_bucket(&info).await.unwrap();
    store.delete_bucket("to-delete").await.unwrap();

    let err = store.get_bucket("to-delete").await.unwrap_err();
    assert!(matches!(err, OrionError::BucketNotFound(_)));
}

#[tokio::test]
async fn delete_nonexistent_bucket() {
    let store = SqliteMetaStore::in_memory().unwrap();
    let err = store.delete_bucket("nope").await.unwrap_err();
    assert!(matches!(err, OrionError::BucketNotFound(_)));
}

#[tokio::test]
async fn list_buckets_empty() {
    let store = SqliteMetaStore::in_memory().unwrap();
    let buckets = store.list_buckets().await.unwrap();
    assert!(buckets.is_empty());
}

#[tokio::test]
async fn list_buckets_ordered() {
    let store = SqliteMetaStore::in_memory().unwrap();
    for name in &["charlie", "alpha", "bravo"] {
        store
            .create_bucket(&BucketInfo {
                name: name.to_string(),
                created_at: Utc::now(),
                region: None,
            })
            .await
            .unwrap();
    }
    let buckets = store.list_buckets().await.unwrap();
    let names: Vec<_> = buckets.iter().map(|b| b.name.as_str()).collect();
    assert_eq!(names, vec!["alpha", "bravo", "charlie"]);
}

#[tokio::test]
async fn bucket_exists() {
    let store = SqliteMetaStore::in_memory().unwrap();
    assert!(!store.bucket_exists("test").await.unwrap());

    store
        .create_bucket(&BucketInfo {
            name: "test".into(),
            created_at: Utc::now(),
            region: None,
        })
        .await
        .unwrap();

    assert!(store.bucket_exists("test").await.unwrap());
}

#[tokio::test]
async fn put_and_get_object_meta() {
    let store = SqliteMetaStore::in_memory().unwrap();
    store
        .create_bucket(&BucketInfo {
            name: "b".into(),
            created_at: Utc::now(),
            region: None,
        })
        .await
        .unwrap();

    let meta = make_meta(1024, "file1");
    store.put_object_meta("b", "file1.txt", &meta).await.unwrap();

    let fetched = store.get_object_meta("b", "file1.txt").await.unwrap();
    assert_eq!(fetched.size, 1024);
    assert_eq!(fetched.content_type, "text/plain");
    assert_eq!(fetched.checksum, Some("sha256-abc".into()));
}

#[tokio::test]
async fn get_nonexistent_object_meta() {
    let store = SqliteMetaStore::in_memory().unwrap();
    store
        .create_bucket(&BucketInfo {
            name: "b".into(),
            created_at: Utc::now(),
            region: None,
        })
        .await
        .unwrap();

    let err = store.get_object_meta("b", "nope").await.unwrap_err();
    assert!(matches!(err, OrionError::NotFound(_)));
}

#[tokio::test]
async fn put_object_meta_upsert() {
    let store = SqliteMetaStore::in_memory().unwrap();
    store
        .create_bucket(&BucketInfo {
            name: "b".into(),
            created_at: Utc::now(),
            region: None,
        })
        .await
        .unwrap();

    let meta1 = make_meta(100, "v1");
    store.put_object_meta("b", "key", &meta1).await.unwrap();

    let meta2 = make_meta(200, "v2");
    store.put_object_meta("b", "key", &meta2).await.unwrap();

    let fetched = store.get_object_meta("b", "key").await.unwrap();
    assert_eq!(fetched.size, 200);
}

#[tokio::test]
async fn delete_object_meta() {
    let store = SqliteMetaStore::in_memory().unwrap();
    store
        .create_bucket(&BucketInfo {
            name: "b".into(),
            created_at: Utc::now(),
            region: None,
        })
        .await
        .unwrap();

    store
        .put_object_meta("b", "key", &make_meta(10, "x"))
        .await
        .unwrap();
    store.delete_object_meta("b", "key").await.unwrap();

    let err = store.get_object_meta("b", "key").await.unwrap_err();
    assert!(matches!(err, OrionError::NotFound(_)));
}

#[tokio::test]
async fn object_exists() {
    let store = SqliteMetaStore::in_memory().unwrap();
    store
        .create_bucket(&BucketInfo {
            name: "b".into(),
            created_at: Utc::now(),
            region: None,
        })
        .await
        .unwrap();

    assert!(!store.object_exists("b", "key").await.unwrap());

    store
        .put_object_meta("b", "key", &make_meta(1, "x"))
        .await
        .unwrap();
    assert!(store.object_exists("b", "key").await.unwrap());
}

#[tokio::test]
async fn object_count_and_total_size() {
    let store = SqliteMetaStore::in_memory().unwrap();
    store
        .create_bucket(&BucketInfo {
            name: "b".into(),
            created_at: Utc::now(),
            region: None,
        })
        .await
        .unwrap();

    assert_eq!(store.object_count("b").await.unwrap(), 0);
    assert_eq!(store.total_size("b").await.unwrap(), 0);

    store
        .put_object_meta("b", "k1", &make_meta(100, "a"))
        .await
        .unwrap();
    store
        .put_object_meta("b", "k2", &make_meta(200, "b"))
        .await
        .unwrap();

    assert_eq!(store.object_count("b").await.unwrap(), 2);
    assert_eq!(store.total_size("b").await.unwrap(), 300);
}

#[tokio::test]
async fn list_objects_basic() {
    let store = SqliteMetaStore::in_memory().unwrap();
    store
        .create_bucket(&BucketInfo {
            name: "b".into(),
            created_at: Utc::now(),
            region: None,
        })
        .await
        .unwrap();

    for i in 0..5 {
        let key = format!("file{}.txt", i);
        store
            .put_object_meta("b", &key, &make_meta(i * 10, &key))
            .await
            .unwrap();
    }

    let page = store
        .list_objects("b", &ListOptions::default())
        .await
        .unwrap();
    assert_eq!(page.entries.len(), 5);
    assert!(!page.is_truncated);
    assert!(page.next_cursor.is_none());
}

#[tokio::test]
async fn list_objects_with_prefix() {
    let store = SqliteMetaStore::in_memory().unwrap();
    store
        .create_bucket(&BucketInfo {
            name: "b".into(),
            created_at: Utc::now(),
            region: None,
        })
        .await
        .unwrap();

    store
        .put_object_meta("b", "photos/cat.jpg", &make_meta(100, "cat"))
        .await
        .unwrap();
    store
        .put_object_meta("b", "photos/dog.jpg", &make_meta(200, "dog"))
        .await
        .unwrap();
    store
        .put_object_meta("b", "docs/readme.md", &make_meta(50, "readme"))
        .await
        .unwrap();

    let opts = ListOptions {
        prefix: Some("photos/".into()),
        ..Default::default()
    };
    let page = store.list_objects("b", &opts).await.unwrap();
    assert_eq!(page.entries.len(), 2);
    assert!(page.entries.iter().all(|e| e.key.starts_with("photos/")));
}

#[tokio::test]
async fn list_objects_with_delimiter() {
    let store = SqliteMetaStore::in_memory().unwrap();
    store
        .create_bucket(&BucketInfo {
            name: "b".into(),
            created_at: Utc::now(),
            region: None,
        })
        .await
        .unwrap();

    store
        .put_object_meta("b", "a/1.txt", &make_meta(10, "1"))
        .await
        .unwrap();
    store
        .put_object_meta("b", "a/2.txt", &make_meta(20, "2"))
        .await
        .unwrap();
    store
        .put_object_meta("b", "b/3.txt", &make_meta(30, "3"))
        .await
        .unwrap();
    store
        .put_object_meta("b", "root.txt", &make_meta(40, "r"))
        .await
        .unwrap();

    let opts = ListOptions {
        delimiter: Some("/".into()),
        ..Default::default()
    };
    let page = store.list_objects("b", &opts).await.unwrap();

    // "root.txt" is the only entry without delimiter after prefix
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].key, "root.txt");

    // "a/" and "b/" are common prefixes
    let prefixes: Vec<_> = page.common_prefixes.iter().map(|cp| &cp.prefix).collect();
    assert!(prefixes.contains(&&"a/".to_string()));
    assert!(prefixes.contains(&&"b/".to_string()));
}

#[tokio::test]
async fn list_objects_pagination() {
    let store = SqliteMetaStore::in_memory().unwrap();
    store
        .create_bucket(&BucketInfo {
            name: "b".into(),
            created_at: Utc::now(),
            region: None,
        })
        .await
        .unwrap();

    for i in 0..10 {
        let key = format!("key{:02}", i);
        store
            .put_object_meta("b", &key, &make_meta(i, &key))
            .await
            .unwrap();
    }

    // First page
    let opts = ListOptions {
        max_keys: 3,
        ..Default::default()
    };
    let page = store.list_objects("b", &opts).await.unwrap();
    assert_eq!(page.entries.len(), 3);
    assert!(page.is_truncated);
    assert!(page.next_cursor.is_some());

    // Second page
    let opts = ListOptions {
        max_keys: 3,
        cursor: page.next_cursor,
        ..Default::default()
    };
    let page2 = store.list_objects("b", &opts).await.unwrap();
    assert_eq!(page2.entries.len(), 3);
    assert!(page2.is_truncated);
}

#[tokio::test]
async fn cascade_delete_objects_with_bucket() {
    let store = SqliteMetaStore::in_memory().unwrap();
    store
        .create_bucket(&BucketInfo {
            name: "b".into(),
            created_at: Utc::now(),
            region: None,
        })
        .await
        .unwrap();

    store
        .put_object_meta("b", "k1", &make_meta(10, "1"))
        .await
        .unwrap();
    store
        .put_object_meta("b", "k2", &make_meta(20, "2"))
        .await
        .unwrap();

    store.delete_bucket("b").await.unwrap();

    // Objects should be gone (CASCADE)
    // Re-create bucket to test
    store
        .create_bucket(&BucketInfo {
            name: "b".into(),
            created_at: Utc::now(),
            region: None,
        })
        .await
        .unwrap();
    assert_eq!(store.object_count("b").await.unwrap(), 0);
}

#[tokio::test]
async fn user_meta_roundtrip() {
    let store = SqliteMetaStore::in_memory().unwrap();
    store
        .create_bucket(&BucketInfo {
            name: "b".into(),
            created_at: Utc::now(),
            region: None,
        })
        .await
        .unwrap();

    let mut user_meta = HashMap::new();
    user_meta.insert("author".into(), "alice".into());
    user_meta.insert("project".into(), "orion".into());

    let meta = ObjectMeta {
        user_meta,
        ..make_meta(42, "x")
    };

    store.put_object_meta("b", "k", &meta).await.unwrap();
    let fetched = store.get_object_meta("b", "k").await.unwrap();
    assert_eq!(fetched.user_meta.get("author").unwrap(), "alice");
    assert_eq!(fetched.user_meta.get("project").unwrap(), "orion");
}
