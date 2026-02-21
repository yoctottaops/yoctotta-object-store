use chrono::Utc;
use orion_core::*;
use orion_proto_s3::xml;

#[test]
fn list_buckets_xml_empty() {
    let result = xml::list_buckets_xml(&[]);
    assert!(result.contains("ListAllMyBucketsResult"));
    assert!(result.contains("<Buckets>"));
    assert!(result.contains("</Buckets>"));
    assert!(!result.contains("<Bucket>"));
}

#[test]
fn list_buckets_xml_with_buckets() {
    let buckets = vec![
        BucketInfo {
            name: "alpha".into(),
            created_at: Utc::now(),
            region: None,
        },
        BucketInfo {
            name: "bravo".into(),
            created_at: Utc::now(),
            region: None,
        },
    ];

    let result = xml::list_buckets_xml(&buckets);
    assert!(result.contains("<Name>alpha</Name>"));
    assert!(result.contains("<Name>bravo</Name>"));
    assert!(result.contains("<CreationDate>"));
}

#[test]
fn list_objects_v2_xml_empty() {
    let page = ListPage {
        entries: vec![],
        common_prefixes: vec![],
        next_cursor: None,
        is_truncated: false,
    };

    let result = xml::list_objects_v2_xml("my-bucket", &page, None, None, 1000, None);
    assert!(result.contains("<Name>my-bucket</Name>"));
    assert!(result.contains("<IsTruncated>false</IsTruncated>"));
    assert!(result.contains("<KeyCount>0</KeyCount>"));
    assert!(result.contains("<MaxKeys>1000</MaxKeys>"));
}

#[test]
fn list_objects_v2_xml_with_entries() {
    let page = ListPage {
        entries: vec![
            ListEntry {
                key: "file1.txt".into(),
                meta: ObjectMeta {
                    size: 1024,
                    etag: "\"abc\"".into(),
                    ..Default::default()
                },
            },
            ListEntry {
                key: "file2.txt".into(),
                meta: ObjectMeta {
                    size: 2048,
                    etag: "\"def\"".into(),
                    ..Default::default()
                },
            },
        ],
        common_prefixes: vec![],
        next_cursor: None,
        is_truncated: false,
    };

    let result = xml::list_objects_v2_xml("b", &page, None, None, 1000, None);
    assert!(result.contains("<Key>file1.txt</Key>"));
    assert!(result.contains("<Size>1024</Size>"));
    assert!(result.contains("<Key>file2.txt</Key>"));
    assert!(result.contains("<Size>2048</Size>"));
    assert!(result.contains("<StorageClass>STANDARD</StorageClass>"));
    assert!(result.contains("<KeyCount>2</KeyCount>"));
}

#[test]
fn list_objects_v2_xml_with_prefix_and_delimiter() {
    let page = ListPage {
        entries: vec![],
        common_prefixes: vec![CommonPrefix {
            prefix: "photos/".into(),
        }],
        next_cursor: None,
        is_truncated: false,
    };

    let result =
        xml::list_objects_v2_xml("b", &page, Some(""), Some("/"), 1000, None);
    assert!(result.contains("<Delimiter>/</Delimiter>"));
    assert!(result.contains("<CommonPrefixes>"));
    assert!(result.contains("<Prefix>photos/</Prefix>"));
}

#[test]
fn list_objects_v2_xml_truncated_with_token() {
    let page = ListPage {
        entries: vec![ListEntry {
            key: "a.txt".into(),
            meta: ObjectMeta::default(),
        }],
        common_prefixes: vec![],
        next_cursor: Some("a.txt".into()),
        is_truncated: true,
    };

    let result =
        xml::list_objects_v2_xml("b", &page, None, None, 1, Some("prev-token"));
    assert!(result.contains("<IsTruncated>true</IsTruncated>"));
    assert!(result.contains("<ContinuationToken>prev-token</ContinuationToken>"));
    assert!(result.contains("<NextContinuationToken>a.txt</NextContinuationToken>"));
}

#[test]
fn error_xml_basic() {
    let result = xml::error_xml("NoSuchKey", "The specified key does not exist.", "/bucket/key", "req-123");
    assert!(result.contains("<Code>NoSuchKey</Code>"));
    assert!(result.contains("<Message>The specified key does not exist.</Message>"));
    assert!(result.contains("<Resource>/bucket/key</Resource>"));
    assert!(result.contains("<RequestId>req-123</RequestId>"));
}

#[test]
fn error_xml_escaping() {
    let result = xml::error_xml("Error", "message with <special> & \"chars\"", "/r", "id");
    assert!(result.contains("&lt;special&gt;"));
    assert!(result.contains("&amp;"));
    assert!(result.contains("&quot;chars&quot;"));
}

#[test]
fn copy_object_xml() {
    let result = xml::copy_object_xml("\"abc123\"", "2024-01-01T00:00:00Z");
    assert!(result.contains("<CopyObjectResult>"));
    assert!(result.contains("<ETag>&quot;abc123&quot;</ETag>"));
    assert!(result.contains("<LastModified>2024-01-01T00:00:00Z</LastModified>"));
}

#[test]
fn initiate_multipart_xml() {
    let result = xml::initiate_multipart_xml("bucket", "key.txt", "upload-id-123");
    assert!(result.contains("<InitiateMultipartUploadResult>"));
    assert!(result.contains("<Bucket>bucket</Bucket>"));
    assert!(result.contains("<Key>key.txt</Key>"));
    assert!(result.contains("<UploadId>upload-id-123</UploadId>"));
}

#[test]
fn complete_multipart_xml() {
    let result = xml::complete_multipart_xml(
        "http://s3.example.com/bucket/key",
        "bucket",
        "key",
        "\"etag\"",
    );
    assert!(result.contains("<CompleteMultipartUploadResult>"));
    assert!(result.contains("<Location>http://s3.example.com/bucket/key</Location>"));
    assert!(result.contains("<Bucket>bucket</Bucket>"));
    assert!(result.contains("<Key>key</Key>"));
}

#[test]
fn xml_has_proper_declaration() {
    let result = xml::list_buckets_xml(&[]);
    assert!(result.starts_with("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"));

    let result = xml::error_xml("Code", "Msg", "/", "id");
    assert!(result.starts_with("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"));
}
