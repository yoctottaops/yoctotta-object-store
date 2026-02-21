//! End-to-end HTTP tests.
//!
//! Spin up a real S3Server backed by FsStore + SqliteMetaStore and exercise it
//! through HTTP requests using reqwest.

use std::net::SocketAddr;
use std::sync::Arc;

use orion_core::*;
use orion_meta_sqlite::SqliteMetaStore;
use orion_proto_s3::{MetricsConfig, S3Server};
use orion_store_fs::FsStore;
use tempfile::TempDir;

/// Start a test S3 server on a random port and return (base_url, background_handle, _tmpdir).
async fn start_server() -> (String, tokio::task::JoinHandle<()>, TempDir) {
    let tmp = TempDir::new().unwrap();
    let store = Arc::new(FsStore::new(tmp.path()).await.unwrap());
    let buckets: Arc<dyn BucketManager> = store.clone();
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetaStore::in_memory().unwrap());
    let extensions = Arc::new(ExtensionRegistry::new());

    // Bind to port 0 to get a random free port.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    // Drop the listener so the server can bind to the same port.
    drop(listener);

    let server = S3Server::new(store, buckets, meta, extensions, addr, MetricsConfig::default(), None, tmp.path());

    let handle = tokio::spawn(async move {
        let _ = server.run().await;
    });

    // Give the server a moment to start listening.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let base_url = format!("http://{}", addr);
    (base_url, handle, tmp)
}

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

// ── Bucket operations ──

#[tokio::test]
async fn e2e_create_and_list_buckets() {
    let (base, _handle, _tmp) = start_server().await;
    let c = client();

    // List buckets: should be empty
    let resp = c.get(&base).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.contains("ListAllMyBucketsResult"));
    assert!(!body.contains("<Bucket>"));

    // Create a bucket
    let resp = c
        .put(&format!("{}/my-bucket", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // List buckets: should have one
    let resp = c.get(&base).send().await.unwrap();
    let body = resp.text().await.unwrap();
    assert!(body.contains("<Name>my-bucket</Name>"));
}

#[tokio::test]
async fn e2e_head_bucket() {
    let (base, _handle, _tmp) = start_server().await;
    let c = client();

    // HEAD non-existent bucket
    let resp = c
        .head(&format!("{}/nonexistent", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    // Create bucket, then HEAD
    c.put(&format!("{}/exists", base)).send().await.unwrap();
    let resp = c
        .head(&format!("{}/exists", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn e2e_delete_bucket() {
    let (base, _handle, _tmp) = start_server().await;
    let c = client();

    // Create and delete
    c.put(&format!("{}/to-delete", base)).send().await.unwrap();
    let resp = c
        .delete(&format!("{}/to-delete", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // HEAD should now 404
    let resp = c
        .head(&format!("{}/to-delete", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn e2e_create_duplicate_bucket() {
    let (base, _handle, _tmp) = start_server().await;
    let c = client();

    c.put(&format!("{}/dup", base)).send().await.unwrap();
    let resp = c.put(&format!("{}/dup", base)).send().await.unwrap();
    assert_eq!(resp.status(), 409);
    let body = resp.text().await.unwrap();
    assert!(body.contains("BucketAlreadyExists"));
}

// ── Object operations ──

#[tokio::test]
async fn e2e_put_and_get_object() {
    let (base, _handle, _tmp) = start_server().await;
    let c = client();

    // Create bucket first
    c.put(&format!("{}/data", base)).send().await.unwrap();

    // Put object
    let resp = c
        .put(&format!("{}/data/hello.txt", base))
        .header("content-type", "text/plain")
        .body("Hello, World!")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let etag = resp.headers().get("etag").unwrap().to_str().unwrap().to_string();
    assert!(!etag.is_empty());

    // Get object
    let resp = c
        .get(&format!("{}/data/hello.txt", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("content-type").unwrap().to_str().unwrap(),
        "text/plain"
    );
    let body = resp.text().await.unwrap();
    assert_eq!(body, "Hello, World!");
}

#[tokio::test]
async fn e2e_head_object() {
    let (base, _handle, _tmp) = start_server().await;
    let c = client();

    c.put(&format!("{}/b", base)).send().await.unwrap();
    c.put(&format!("{}/b/doc.txt", base))
        .header("content-type", "text/plain")
        .body("test content")
        .send()
        .await
        .unwrap();

    let resp = c
        .head(&format!("{}/b/doc.txt", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("content-type").unwrap().to_str().unwrap(),
        "text/plain"
    );
    let size: u64 = resp
        .headers()
        .get("content-length")
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(size, 12); // "test content".len()
}

#[tokio::test]
async fn e2e_delete_object() {
    let (base, _handle, _tmp) = start_server().await;
    let c = client();

    c.put(&format!("{}/b", base)).send().await.unwrap();
    c.put(&format!("{}/b/obj.txt", base))
        .body("data")
        .send()
        .await
        .unwrap();

    // Delete
    let resp = c
        .delete(&format!("{}/b/obj.txt", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // GET should now 404
    let resp = c
        .get(&format!("{}/b/obj.txt", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn e2e_get_nonexistent_object() {
    let (base, _handle, _tmp) = start_server().await;
    let c = client();

    c.put(&format!("{}/b", base)).send().await.unwrap();

    let resp = c
        .get(&format!("{}/b/nope.txt", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
    let body = resp.text().await.unwrap();
    assert!(body.contains("NoSuchKey"));
}

// ── Object with user metadata ──

#[tokio::test]
async fn e2e_user_metadata() {
    let (base, _handle, _tmp) = start_server().await;
    let c = client();

    c.put(&format!("{}/meta-bucket", base))
        .send()
        .await
        .unwrap();

    // Put with x-amz-meta-* headers
    c.put(&format!("{}/meta-bucket/tagged.txt", base))
        .header("content-type", "text/plain")
        .header("x-amz-meta-author", "test-user")
        .header("x-amz-meta-project", "orion")
        .body("tagged content")
        .send()
        .await
        .unwrap();

    // GET the object back
    let resp = c
        .get(&format!("{}/meta-bucket/tagged.txt", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert_eq!(body, "tagged content");
}

// ── List objects ──

#[tokio::test]
async fn e2e_list_objects() {
    let (base, _handle, _tmp) = start_server().await;
    let c = client();

    c.put(&format!("{}/listing", base)).send().await.unwrap();

    // Put some objects
    for name in &["a.txt", "b.txt", "dir/c.txt", "dir/d.txt"] {
        c.put(&format!("{}/listing/{}", base, name))
            .body("data")
            .send()
            .await
            .unwrap();
    }

    // List all objects
    let resp = c
        .get(&format!("{}/listing", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.contains("ListBucketResult"));
    assert!(body.contains("<KeyCount>4</KeyCount>"));
}

// ── Overwrite object ──

#[tokio::test]
async fn e2e_overwrite_object() {
    let (base, _handle, _tmp) = start_server().await;
    let c = client();

    c.put(&format!("{}/ow", base)).send().await.unwrap();

    // Write v1
    c.put(&format!("{}/ow/file.txt", base))
        .body("version1")
        .send()
        .await
        .unwrap();

    // Write v2
    c.put(&format!("{}/ow/file.txt", base))
        .body("version2-updated")
        .send()
        .await
        .unwrap();

    // Read back should be v2
    let resp = c
        .get(&format!("{}/ow/file.txt", base))
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    assert_eq!(body, "version2-updated");
}

// ── Multiple objects in same bucket ──

#[tokio::test]
async fn e2e_multiple_objects() {
    let (base, _handle, _tmp) = start_server().await;
    let c = client();

    c.put(&format!("{}/multi", base)).send().await.unwrap();

    let objects = vec![
        ("file1.txt", "content1"),
        ("file2.txt", "content2"),
        ("file3.txt", "content3"),
    ];

    for (name, content) in &objects {
        c.put(&format!("{}/multi/{}", base, name))
            .body(*content)
            .send()
            .await
            .unwrap();
    }

    // Verify each object individually
    for (name, expected) in &objects {
        let resp = c
            .get(&format!("{}/multi/{}", base, name))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body = resp.text().await.unwrap();
        assert_eq!(body, *expected);
    }
}

// ── Binary data ──

#[tokio::test]
async fn e2e_binary_data_roundtrip() {
    let (base, _handle, _tmp) = start_server().await;
    let c = client();

    c.put(&format!("{}/bin", base)).send().await.unwrap();

    // Create binary data with all byte values
    let data: Vec<u8> = (0..=255).collect();

    c.put(&format!("{}/bin/bytes.bin", base))
        .header("content-type", "application/octet-stream")
        .body(data.clone())
        .send()
        .await
        .unwrap();

    let resp = c
        .get(&format!("{}/bin/bytes.bin", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.to_vec(), data);
}

// ── Large object ──

#[tokio::test]
async fn e2e_large_object() {
    let (base, _handle, _tmp) = start_server().await;
    let c = client();

    c.put(&format!("{}/large", base)).send().await.unwrap();

    // 1 MB of data
    let data = vec![0xABu8; 1024 * 1024];

    c.put(&format!("{}/large/big.bin", base))
        .body(data.clone())
        .send()
        .await
        .unwrap();

    let resp = c
        .get(&format!("{}/large/big.bin", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.len(), 1024 * 1024);
    assert!(body.iter().all(|&b| b == 0xAB));
}

// ── Empty object ──

#[tokio::test]
async fn e2e_empty_object() {
    let (base, _handle, _tmp) = start_server().await;
    let c = client();

    c.put(&format!("{}/empty-bucket", base))
        .send()
        .await
        .unwrap();

    c.put(&format!("{}/empty-bucket/empty.txt", base))
        .body("")
        .send()
        .await
        .unwrap();

    let resp = c
        .get(&format!("{}/empty-bucket/empty.txt", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.bytes().await.unwrap();
    assert!(body.is_empty());
}

// ── XML response format ──

#[tokio::test]
async fn e2e_xml_declaration() {
    let (base, _handle, _tmp) = start_server().await;
    let c = client();

    // ListBuckets response should have XML declaration
    let resp = c.get(&base).send().await.unwrap();
    let body = resp.text().await.unwrap();
    assert!(
        body.starts_with("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"),
        "Response should start with XML declaration"
    );
}

// ── Error response format ──

#[tokio::test]
async fn e2e_error_response_format() {
    let (base, _handle, _tmp) = start_server().await;
    let c = client();

    c.put(&format!("{}/err-test", base)).send().await.unwrap();

    let resp = c
        .get(&format!("{}/err-test/no-such-key", base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
    let body = resp.text().await.unwrap();

    // Verify XML error format
    assert!(body.contains("<Code>NoSuchKey</Code>"));
    assert!(body.contains("<Message>"));
    assert!(body.contains("<RequestId>"));
}
