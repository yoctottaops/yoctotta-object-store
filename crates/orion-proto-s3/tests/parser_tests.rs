use http::Method;
use orion_proto_s3::parser::{parse_request, S3Operation};

// ── Service-level ──

#[test]
fn list_buckets() {
    let op = parse_request(&Method::GET, "/", None);
    assert!(matches!(op, S3Operation::ListBuckets));
}

#[test]
fn unsupported_root() {
    let op = parse_request(&Method::POST, "/", None);
    assert!(matches!(op, S3Operation::Unsupported(_)));
}

// ── Bucket operations ──

#[test]
fn create_bucket() {
    let op = parse_request(&Method::PUT, "/my-bucket", None);
    match op {
        S3Operation::CreateBucket { bucket } => assert_eq!(bucket, "my-bucket"),
        _ => panic!("Expected CreateBucket, got {:?}", op),
    }
}

#[test]
fn delete_bucket() {
    let op = parse_request(&Method::DELETE, "/my-bucket", None);
    match op {
        S3Operation::DeleteBucket { bucket } => assert_eq!(bucket, "my-bucket"),
        _ => panic!("Expected DeleteBucket"),
    }
}

#[test]
fn head_bucket() {
    let op = parse_request(&Method::HEAD, "/test-bucket", None);
    match op {
        S3Operation::HeadBucket { bucket } => assert_eq!(bucket, "test-bucket"),
        _ => panic!("Expected HeadBucket"),
    }
}

#[test]
fn list_objects_default() {
    let op = parse_request(&Method::GET, "/my-bucket", None);
    match op {
        S3Operation::ListObjects { bucket, params } => {
            assert_eq!(bucket, "my-bucket");
            assert!(params.prefix.is_none());
            assert!(params.delimiter.is_none());
            assert_eq!(params.list_type, 1);
        }
        _ => panic!("Expected ListObjects"),
    }
}

#[test]
fn list_objects_v2_with_params() {
    let op = parse_request(
        &Method::GET,
        "/bucket",
        Some("list-type=2&prefix=photos/&delimiter=/&max-keys=10"),
    );
    match op {
        S3Operation::ListObjects { bucket, params } => {
            assert_eq!(bucket, "bucket");
            assert_eq!(params.prefix, Some("photos/".into()));
            assert_eq!(params.delimiter, Some("/".into()));
            assert_eq!(params.max_keys, Some(10));
            assert_eq!(params.list_type, 2);
        }
        _ => panic!("Expected ListObjects"),
    }
}

#[test]
fn list_objects_with_continuation_token() {
    let op = parse_request(
        &Method::GET,
        "/bucket",
        Some("continuation-token=abc123&start-after=file.txt"),
    );
    match op {
        S3Operation::ListObjects { params, .. } => {
            assert_eq!(params.continuation_token, Some("abc123".into()));
            assert_eq!(params.start_after, Some("file.txt".into()));
        }
        _ => panic!("Expected ListObjects"),
    }
}

// ── Object operations ──

#[test]
fn get_object() {
    let op = parse_request(&Method::GET, "/bucket/path/to/file.txt", None);
    match op {
        S3Operation::GetObject { bucket, key } => {
            assert_eq!(bucket, "bucket");
            assert_eq!(key, "path/to/file.txt");
        }
        _ => panic!("Expected GetObject"),
    }
}

#[test]
fn put_object() {
    let op = parse_request(&Method::PUT, "/bucket/file.txt", None);
    match op {
        S3Operation::PutObject { bucket, key } => {
            assert_eq!(bucket, "bucket");
            assert_eq!(key, "file.txt");
        }
        _ => panic!("Expected PutObject"),
    }
}

#[test]
fn delete_object() {
    let op = parse_request(&Method::DELETE, "/bucket/file.txt", None);
    match op {
        S3Operation::DeleteObject { bucket, key } => {
            assert_eq!(bucket, "bucket");
            assert_eq!(key, "file.txt");
        }
        _ => panic!("Expected DeleteObject"),
    }
}

#[test]
fn head_object() {
    let op = parse_request(&Method::HEAD, "/bucket/file.txt", None);
    match op {
        S3Operation::HeadObject { bucket, key } => {
            assert_eq!(bucket, "bucket");
            assert_eq!(key, "file.txt");
        }
        _ => panic!("Expected HeadObject"),
    }
}

// ── Multipart operations ──

#[test]
fn create_multipart_upload() {
    let op = parse_request(&Method::POST, "/bucket/file.txt", Some("uploads"));
    match op {
        S3Operation::CreateMultipartUpload { bucket, key } => {
            assert_eq!(bucket, "bucket");
            assert_eq!(key, "file.txt");
        }
        _ => panic!("Expected CreateMultipartUpload, got {:?}", op),
    }
}

#[test]
fn upload_part() {
    let op = parse_request(
        &Method::PUT,
        "/bucket/file.txt",
        Some("uploadId=abc&partNumber=3"),
    );
    match op {
        S3Operation::UploadPart {
            bucket,
            key,
            upload_id,
            part_number,
        } => {
            assert_eq!(bucket, "bucket");
            assert_eq!(key, "file.txt");
            assert_eq!(upload_id, "abc");
            assert_eq!(part_number, 3);
        }
        _ => panic!("Expected UploadPart"),
    }
}

#[test]
fn complete_multipart_upload() {
    let op = parse_request(
        &Method::POST,
        "/bucket/file.txt",
        Some("uploadId=xyz"),
    );
    match op {
        S3Operation::CompleteMultipartUpload {
            bucket,
            key,
            upload_id,
        } => {
            assert_eq!(bucket, "bucket");
            assert_eq!(key, "file.txt");
            assert_eq!(upload_id, "xyz");
        }
        _ => panic!("Expected CompleteMultipartUpload"),
    }
}

#[test]
fn abort_multipart_upload() {
    let op = parse_request(
        &Method::DELETE,
        "/bucket/file.txt",
        Some("uploadId=xyz"),
    );
    match op {
        S3Operation::AbortMultipartUpload {
            bucket,
            key,
            upload_id,
        } => {
            assert_eq!(bucket, "bucket");
            assert_eq!(key, "file.txt");
            assert_eq!(upload_id, "xyz");
        }
        _ => panic!("Expected AbortMultipartUpload"),
    }
}

// ── Edge cases ──

#[test]
fn percent_encoded_path() {
    let op = parse_request(&Method::GET, "/bucket/my%20file%2Fpath.txt", None);
    match op {
        S3Operation::GetObject { bucket, key } => {
            assert_eq!(bucket, "bucket");
            assert_eq!(key, "my file/path.txt");
        }
        _ => panic!("Expected GetObject"),
    }
}

#[test]
fn unsupported_method_on_object() {
    let op = parse_request(&Method::PATCH, "/bucket/key", None);
    assert!(matches!(op, S3Operation::Unsupported(_)));
}

#[test]
fn unsupported_post_without_query() {
    let op = parse_request(&Method::POST, "/bucket/key", None);
    assert!(matches!(op, S3Operation::Unsupported(_)));
}
