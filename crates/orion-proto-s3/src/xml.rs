use orion_core::{BucketInfo, ListPage};

/// Build ListAllMyBucketsResult XML.
pub fn list_buckets_xml(buckets: &[BucketInfo]) -> String {
    let mut xml = String::from(r#"<?xml version="1.0" encoding="UTF-8"?>"#);
    xml.push_str(r#"<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#);
    xml.push_str("<Owner><ID>orion</ID><DisplayName>orion</DisplayName></Owner>");
    xml.push_str("<Buckets>");
    for b in buckets {
        xml.push_str("<Bucket>");
        xml.push_str(&format!("<Name>{}</Name>", escape_xml(&b.name)));
        xml.push_str(&format!(
            "<CreationDate>{}</CreationDate>",
            b.created_at.to_rfc3339()
        ));
        xml.push_str("</Bucket>");
    }
    xml.push_str("</Buckets>");
    xml.push_str("</ListAllMyBucketsResult>");
    xml
}

/// Build ListBucketResult XML (ListObjectsV2).
pub fn list_objects_v2_xml(
    bucket: &str,
    page: &ListPage,
    prefix: Option<&str>,
    delimiter: Option<&str>,
    max_keys: u32,
    continuation_token: Option<&str>,
) -> String {
    let mut xml = String::from(r#"<?xml version="1.0" encoding="UTF-8"?>"#);
    xml.push_str(r#"<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#);
    xml.push_str(&format!("<Name>{}</Name>", escape_xml(bucket)));
    xml.push_str(&format!(
        "<Prefix>{}</Prefix>",
        escape_xml(prefix.unwrap_or(""))
    ));
    xml.push_str(&format!("<MaxKeys>{}</MaxKeys>", max_keys));
    xml.push_str(&format!(
        "<IsTruncated>{}</IsTruncated>",
        page.is_truncated
    ));
    xml.push_str("<KeyCount>");
    xml.push_str(&page.entries.len().to_string());
    xml.push_str("</KeyCount>");

    if let Some(delim) = delimiter {
        xml.push_str(&format!("<Delimiter>{}</Delimiter>", escape_xml(delim)));
    }
    if let Some(token) = continuation_token {
        xml.push_str(&format!(
            "<ContinuationToken>{}</ContinuationToken>",
            escape_xml(token)
        ));
    }
    if let Some(next) = &page.next_cursor {
        xml.push_str(&format!(
            "<NextContinuationToken>{}</NextContinuationToken>",
            escape_xml(next)
        ));
    }

    for entry in &page.entries {
        xml.push_str("<Contents>");
        xml.push_str(&format!("<Key>{}</Key>", escape_xml(&entry.key)));
        xml.push_str(&format!(
            "<LastModified>{}</LastModified>",
            entry.meta.modified_at.to_rfc3339()
        ));
        xml.push_str(&format!("<ETag>{}</ETag>", escape_xml(&entry.meta.etag)));
        xml.push_str(&format!("<Size>{}</Size>", entry.meta.size));
        xml.push_str("<StorageClass>STANDARD</StorageClass>");
        xml.push_str("</Contents>");
    }

    for cp in &page.common_prefixes {
        xml.push_str("<CommonPrefixes>");
        xml.push_str(&format!("<Prefix>{}</Prefix>", escape_xml(&cp.prefix)));
        xml.push_str("</CommonPrefixes>");
    }

    xml.push_str("</ListBucketResult>");
    xml
}

/// Build S3 error response XML.
pub fn error_xml(code: &str, message: &str, resource: &str, request_id: &str) -> String {
    let mut xml = String::from(r#"<?xml version="1.0" encoding="UTF-8"?>"#);
    xml.push_str("<Error>");
    xml.push_str(&format!("<Code>{}</Code>", escape_xml(code)));
    xml.push_str(&format!("<Message>{}</Message>", escape_xml(message)));
    xml.push_str(&format!("<Resource>{}</Resource>", escape_xml(resource)));
    xml.push_str(&format!(
        "<RequestId>{}</RequestId>",
        escape_xml(request_id)
    ));
    xml.push_str("</Error>");
    xml
}

/// Build CopyObjectResult XML.
pub fn copy_object_xml(etag: &str, last_modified: &str) -> String {
    let mut xml = String::from(r#"<?xml version="1.0" encoding="UTF-8"?>"#);
    xml.push_str("<CopyObjectResult>");
    xml.push_str(&format!("<ETag>{}</ETag>", escape_xml(etag)));
    xml.push_str(&format!(
        "<LastModified>{}</LastModified>",
        escape_xml(last_modified)
    ));
    xml.push_str("</CopyObjectResult>");
    xml
}

/// Build InitiateMultipartUploadResult XML.
pub fn initiate_multipart_xml(bucket: &str, key: &str, upload_id: &str) -> String {
    let mut xml = String::from(r#"<?xml version="1.0" encoding="UTF-8"?>"#);
    xml.push_str("<InitiateMultipartUploadResult>");
    xml.push_str(&format!("<Bucket>{}</Bucket>", escape_xml(bucket)));
    xml.push_str(&format!("<Key>{}</Key>", escape_xml(key)));
    xml.push_str(&format!(
        "<UploadId>{}</UploadId>",
        escape_xml(upload_id)
    ));
    xml.push_str("</InitiateMultipartUploadResult>");
    xml
}

/// Build CompleteMultipartUploadResult XML.
pub fn complete_multipart_xml(
    location: &str,
    bucket: &str,
    key: &str,
    etag: &str,
) -> String {
    let mut xml = String::from(r#"<?xml version="1.0" encoding="UTF-8"?>"#);
    xml.push_str("<CompleteMultipartUploadResult>");
    xml.push_str(&format!("<Location>{}</Location>", escape_xml(location)));
    xml.push_str(&format!("<Bucket>{}</Bucket>", escape_xml(bucket)));
    xml.push_str(&format!("<Key>{}</Key>", escape_xml(key)));
    xml.push_str(&format!("<ETag>{}</ETag>", escape_xml(etag)));
    xml.push_str("</CompleteMultipartUploadResult>");
    xml
}

fn escape_xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
