use std::sync::Arc;

use orion_core::extension::*;
use orion_core::types::*;
use orion_ext_rag::*;

fn make_rag() -> RagExtension {
    let embedder = Arc::new(MockEmbedder::new(16));
    let store = Arc::new(InMemoryVectorStore::new());
    RagExtension::new(embedder, store)
}

// ── MockEmbedder tests ──

#[tokio::test]
async fn mock_embedder_dimensions() {
    let emb = MockEmbedder::new(32);
    assert_eq!(emb.dimensions(), 32);
    assert_eq!(emb.model_name(), "mock-embedder");
}

#[tokio::test]
async fn mock_embedder_produces_correct_dims() {
    let emb = MockEmbedder::new(8);
    let vec = emb.embed("hello").await.unwrap();
    assert_eq!(vec.len(), 8);
}

#[tokio::test]
async fn mock_embedder_normalized() {
    let emb = MockEmbedder::new(16);
    let vec = emb.embed("test text").await.unwrap();
    let norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
    assert!((norm - 1.0).abs() < 0.001, "Vector should be normalized, got norm={}", norm);
}

#[tokio::test]
async fn mock_embedder_deterministic() {
    let emb = MockEmbedder::new(16);
    let v1 = emb.embed("same text").await.unwrap();
    let v2 = emb.embed("same text").await.unwrap();
    assert_eq!(v1, v2);
}

#[tokio::test]
async fn mock_embedder_different_texts_different_vectors() {
    let emb = MockEmbedder::new(16);
    let v1 = emb.embed("hello").await.unwrap();
    let v2 = emb.embed("world").await.unwrap();
    assert_ne!(v1, v2);
}

#[tokio::test]
async fn mock_embedder_batch() {
    let emb = MockEmbedder::new(8);
    let results = emb.embed_batch(&["hello", "world"]).await.unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].len(), 8);
    assert_eq!(results[1].len(), 8);
}

// ── InMemoryVectorStore tests ──

#[tokio::test]
async fn vector_store_upsert_and_count() {
    let store = InMemoryVectorStore::new();
    assert_eq!(store.count().await.unwrap(), 0);

    store
        .upsert(VectorEntry {
            id: "b/k#0".into(),
            bucket: "b".into(),
            key: "k".into(),
            chunk_index: 0,
            text: "hello".into(),
            vector: vec![1.0, 0.0, 0.0],
        })
        .await
        .unwrap();

    assert_eq!(store.count().await.unwrap(), 1);
}

#[tokio::test]
async fn vector_store_upsert_overwrites() {
    let store = InMemoryVectorStore::new();

    for text in &["first", "second"] {
        store
            .upsert(VectorEntry {
                id: "b/k#0".into(),
                bucket: "b".into(),
                key: "k".into(),
                chunk_index: 0,
                text: text.to_string(),
                vector: vec![1.0, 0.0],
            })
            .await
            .unwrap();
    }

    // Should still be 1 entry (upsert)
    assert_eq!(store.count().await.unwrap(), 1);
}

#[tokio::test]
async fn vector_store_delete_by_key() {
    let store = InMemoryVectorStore::new();

    for i in 0..3 {
        store
            .upsert(VectorEntry {
                id: format!("b/k#{}", i),
                bucket: "b".into(),
                key: "k".into(),
                chunk_index: i,
                text: format!("chunk {}", i),
                vector: vec![1.0, 0.0],
            })
            .await
            .unwrap();
    }

    // Add another key
    store
        .upsert(VectorEntry {
            id: "b/other#0".into(),
            bucket: "b".into(),
            key: "other".into(),
            chunk_index: 0,
            text: "other".into(),
            vector: vec![0.0, 1.0],
        })
        .await
        .unwrap();

    assert_eq!(store.count().await.unwrap(), 4);

    let removed = store.delete_by_key("b", "k").await.unwrap();
    assert_eq!(removed, 3);
    assert_eq!(store.count().await.unwrap(), 1);
}

#[tokio::test]
async fn vector_store_search_basic() {
    let store = InMemoryVectorStore::new();

    store
        .upsert(VectorEntry {
            id: "b/a#0".into(),
            bucket: "b".into(),
            key: "a".into(),
            chunk_index: 0,
            text: "relevant".into(),
            vector: vec![1.0, 0.0, 0.0],
        })
        .await
        .unwrap();

    store
        .upsert(VectorEntry {
            id: "b/b#0".into(),
            bucket: "b".into(),
            key: "b".into(),
            chunk_index: 0,
            text: "not relevant".into(),
            vector: vec![0.0, 1.0, 0.0],
        })
        .await
        .unwrap();

    let query = SearchQuery {
        vector: vec![1.0, 0.0, 0.0],
        top_k: 1,
        min_score: None,
        bucket: None,
        key_prefix: None,
    };

    let results = store.search(&query).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].key, "a");
    assert!((results[0].score - 1.0).abs() < 0.001);
}

#[tokio::test]
async fn vector_store_search_with_bucket_filter() {
    let store = InMemoryVectorStore::new();

    store
        .upsert(VectorEntry {
            id: "b1/k#0".into(),
            bucket: "b1".into(),
            key: "k".into(),
            chunk_index: 0,
            text: "hello".into(),
            vector: vec![1.0, 0.0],
        })
        .await
        .unwrap();

    store
        .upsert(VectorEntry {
            id: "b2/k#0".into(),
            bucket: "b2".into(),
            key: "k".into(),
            chunk_index: 0,
            text: "world".into(),
            vector: vec![1.0, 0.0],
        })
        .await
        .unwrap();

    let query = SearchQuery {
        vector: vec![1.0, 0.0],
        top_k: 10,
        min_score: None,
        bucket: Some("b1".into()),
        key_prefix: None,
    };

    let results = store.search(&query).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].bucket, "b1");
}

#[tokio::test]
async fn vector_store_search_min_score() {
    let store = InMemoryVectorStore::new();

    store
        .upsert(VectorEntry {
            id: "b/k#0".into(),
            bucket: "b".into(),
            key: "k".into(),
            chunk_index: 0,
            text: "orthogonal".into(),
            vector: vec![0.0, 1.0],
        })
        .await
        .unwrap();

    let query = SearchQuery {
        vector: vec![1.0, 0.0],
        top_k: 10,
        min_score: Some(0.5),
        bucket: None,
        key_prefix: None,
    };

    let results = store.search(&query).await.unwrap();
    assert!(results.is_empty(), "Orthogonal vectors should be filtered by min_score");
}

// ── RagExtension tests ──

#[tokio::test]
async fn rag_extension_info() {
    let rag = make_rag();
    let info = rag.info();
    assert_eq!(info.name, "rag");
    assert!(info.enabled);
}

#[tokio::test]
async fn rag_extension_init_default() {
    let mut rag = make_rag();
    let config = toml::Value::Table(toml::map::Map::new());
    rag.init(&config).await.unwrap();
}

#[tokio::test]
async fn rag_extension_shutdown() {
    let rag = make_rag();
    rag.shutdown().await.unwrap();
}

#[tokio::test]
async fn rag_extension_health() {
    let rag = make_rag();
    assert!(rag.health().await.unwrap());
}

#[tokio::test]
async fn rag_post_write_indexes_text() {
    let embedder = Arc::new(MockEmbedder::new(16));
    let store = Arc::new(InMemoryVectorStore::new());
    let rag = RagExtension::new(embedder, store.clone());

    let key = ObjectKey::new("test-bucket", "doc.txt");
    let meta = ObjectMeta {
        content_type: "text/plain".into(),
        ..Default::default()
    };
    let ctx = ExtensionContext::new();

    rag.post_write(&key, &meta, b"hello world", &ctx)
        .await
        .unwrap();

    assert!(store.count().await.unwrap() > 0);
}

#[tokio::test]
async fn rag_post_write_skips_binary() {
    let embedder = Arc::new(MockEmbedder::new(16));
    let store = Arc::new(InMemoryVectorStore::new());
    let rag = RagExtension::new(embedder, store.clone());

    let key = ObjectKey::new("bucket", "image.png");
    let meta = ObjectMeta {
        content_type: "image/png".into(),
        ..Default::default()
    };
    let ctx = ExtensionContext::new();

    rag.post_write(&key, &meta, b"\x89PNG\r\n", &ctx)
        .await
        .unwrap();

    assert_eq!(store.count().await.unwrap(), 0);
}

#[tokio::test]
async fn rag_post_delete_removes_vectors() {
    let embedder = Arc::new(MockEmbedder::new(16));
    let store = Arc::new(InMemoryVectorStore::new());
    let rag = RagExtension::new(embedder, store.clone());

    // Index a doc
    let key = ObjectKey::new("bucket", "doc.txt");
    let meta = ObjectMeta {
        content_type: "text/plain".into(),
        ..Default::default()
    };
    let ctx = ExtensionContext::new();
    rag.post_write(&key, &meta, b"some text content", &ctx)
        .await
        .unwrap();
    assert!(store.count().await.unwrap() > 0);

    // Delete
    rag.post_delete(&key, &ctx).await.unwrap();
    assert_eq!(store.count().await.unwrap(), 0);
}

#[tokio::test]
async fn rag_search() {
    let embedder = Arc::new(MockEmbedder::new(16));
    let store = Arc::new(InMemoryVectorStore::new());
    let rag = RagExtension::new(embedder, store.clone());

    // Index documents
    let key = ObjectKey::new("bucket", "doc.txt");
    let meta = ObjectMeta {
        content_type: "text/plain".into(),
        ..Default::default()
    };
    let ctx = ExtensionContext::new();
    rag.post_write(&key, &meta, b"the quick brown fox", &ctx)
        .await
        .unwrap();

    // Search
    let results = rag.search("quick brown", 5).await.unwrap();
    assert!(!results.is_empty());
    assert_eq!(results[0].bucket, "bucket");
}

#[tokio::test]
async fn rag_search_bucket_filter() {
    let embedder = Arc::new(MockEmbedder::new(16));
    let store = Arc::new(InMemoryVectorStore::new());
    let rag = RagExtension::new(embedder, store.clone());

    let ctx = ExtensionContext::new();
    let meta = ObjectMeta {
        content_type: "text/plain".into(),
        ..Default::default()
    };

    rag.post_write(
        &ObjectKey::new("b1", "a.txt"),
        &meta,
        b"content for bucket one",
        &ctx,
    )
    .await
    .unwrap();

    rag.post_write(
        &ObjectKey::new("b2", "b.txt"),
        &meta,
        b"content for bucket two",
        &ctx,
    )
    .await
    .unwrap();

    let results = rag.search_bucket("content", "b1", 10).await.unwrap();
    assert!(results.iter().all(|r| r.bucket == "b1"));
}

#[tokio::test]
async fn rag_indexes_json_content_type() {
    let embedder = Arc::new(MockEmbedder::new(16));
    let store = Arc::new(InMemoryVectorStore::new());
    let rag = RagExtension::new(embedder, store.clone());

    let key = ObjectKey::new("bucket", "data.json");
    let meta = ObjectMeta {
        content_type: "application/json".into(),
        ..Default::default()
    };
    let ctx = ExtensionContext::new();

    rag.post_write(&key, &meta, b"{\"key\": \"value\"}", &ctx)
        .await
        .unwrap();

    assert!(store.count().await.unwrap() > 0);
}

#[tokio::test]
async fn rag_post_write_reindexes() {
    let embedder = Arc::new(MockEmbedder::new(16));
    let store = Arc::new(InMemoryVectorStore::new());
    let rag = RagExtension::new(embedder, store.clone());

    let key = ObjectKey::new("bucket", "doc.txt");
    let meta = ObjectMeta {
        content_type: "text/plain".into(),
        ..Default::default()
    };
    let ctx = ExtensionContext::new();

    // First write
    rag.post_write(&key, &meta, b"version one", &ctx)
        .await
        .unwrap();
    let count1 = store.count().await.unwrap();

    // Overwrite - should delete old vectors and create new ones
    rag.post_write(&key, &meta, b"version two", &ctx)
        .await
        .unwrap();
    let count2 = store.count().await.unwrap();

    assert_eq!(count1, count2, "Re-index should result in same number of vectors");
}
