use http::Method;
use percent_encoding::percent_decode_str;

/// Parsed S3 request - maps HTTP method + path to an S3 operation.
#[derive(Debug)]
pub enum S3Operation {
    // ── Service ──
    ListBuckets,

    // ── Bucket operations ──
    CreateBucket { bucket: String },
    DeleteBucket { bucket: String },
    HeadBucket { bucket: String },
    ListObjects { bucket: String, params: ListParams },

    // ── Object operations ──
    GetObject { bucket: String, key: String },
    PutObject { bucket: String, key: String },
    DeleteObject { bucket: String, key: String },
    HeadObject { bucket: String, key: String },
    CopyObject { bucket: String, key: String },

    // ── Multipart ──
    CreateMultipartUpload { bucket: String, key: String },
    UploadPart { bucket: String, key: String, upload_id: String, part_number: u32 },
    CompleteMultipartUpload { bucket: String, key: String, upload_id: String },
    AbortMultipartUpload { bucket: String, key: String, upload_id: String },

    // ── Management UI ──
    ManagementUI,
    ManagementApiStats,
    ManagementApiExtensions,
    ManagementApiSearch,
    ManagementApiMetrics,
    ManagementApiLogin,
    ManagementApiLogout,
    ManagementApiCors,

    // ── Not implemented ──
    Unsupported(String),
}

#[derive(Debug, Default)]
pub struct ListParams {
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub max_keys: Option<u32>,
    pub continuation_token: Option<String>,
    pub start_after: Option<String>,
    pub list_type: u8, // 1 or 2
}

/// Parse an incoming HTTP request into an S3 operation.
/// Supports both path-style (/{bucket}/{key}) and will later support
/// virtual-hosted-style ({bucket}.s3.endpoint/{key}).
pub fn parse_request(method: &Method, path: &str, query: Option<&str>) -> S3Operation {
    let decoded_path = percent_decode_str(path).decode_utf8_lossy().to_string();
    let trimmed = decoded_path.trim_start_matches('/');

    // Intercept management paths (/_/*) before S3 routing.
    // Bucket names cannot start with '_', so no collision with S3 operations.
    if trimmed.starts_with("_/") || trimmed == "_" {
        return match (method, trimmed) {
            (&Method::GET, "_/ui") | (&Method::GET, "_/ui/") => S3Operation::ManagementUI,
            (&Method::GET, "_/api/stats") => S3Operation::ManagementApiStats,
            (&Method::GET, "_/api/extensions") => S3Operation::ManagementApiExtensions,
            (&Method::GET, "_/api/metrics") => S3Operation::ManagementApiMetrics,
            (&Method::POST, "_/api/search") => S3Operation::ManagementApiSearch,
            (&Method::POST, "_/api/login") => S3Operation::ManagementApiLogin,
            (&Method::POST, "_/api/logout") => S3Operation::ManagementApiLogout,
            (&Method::OPTIONS, _) => S3Operation::ManagementApiCors,
            _ => S3Operation::Unsupported(format!("{} /{}", method, trimmed)),
        };
    }

    // Parse query parameters.
    let query_params = parse_query(query);

    // Root path: service-level operations.
    if trimmed.is_empty() {
        return match *method {
            Method::GET => S3Operation::ListBuckets,
            _ => S3Operation::Unsupported(format!("{} /", method)),
        };
    }

    // Split into bucket and key.
    let (bucket, key) = match trimmed.split_once('/') {
        Some((b, k)) => (b.to_string(), Some(k.to_string())),
        None => (trimmed.to_string(), None),
    };

    // Check for multipart upload query params.
    let upload_id = query_params.get("uploadId").cloned();
    let part_number = query_params.get("partNumber").and_then(|p| p.parse().ok());

    match (method.clone(), key) {
        // ── Bucket-level operations (no key) ──
        (Method::PUT, None) => S3Operation::CreateBucket { bucket },
        (Method::DELETE, None) => S3Operation::DeleteBucket { bucket },
        (Method::HEAD, None) => S3Operation::HeadBucket { bucket },
        (Method::GET, None) => {
            let list_type = query_params
                .get("list-type")
                .and_then(|v| v.parse().ok())
                .unwrap_or(1);
            S3Operation::ListObjects {
                bucket,
                params: ListParams {
                    prefix: query_params.get("prefix").cloned(),
                    delimiter: query_params.get("delimiter").cloned(),
                    max_keys: query_params.get("max-keys").and_then(|v| v.parse().ok()),
                    continuation_token: query_params.get("continuation-token").cloned(),
                    start_after: query_params.get("start-after").cloned(),
                    list_type,
                },
            }
        }

        // ── Object-level operations ──
        (Method::GET, Some(key)) => S3Operation::GetObject { bucket, key },

        (Method::PUT, Some(key)) => {
            // Check for multipart upload part.
            if let (Some(uid), Some(pn)) = (upload_id, part_number) {
                S3Operation::UploadPart {
                    bucket,
                    key,
                    upload_id: uid,
                    part_number: pn,
                }
            } else {
                S3Operation::PutObject { bucket, key }
            }
        }

        (Method::DELETE, Some(key)) => {
            if let Some(uid) = upload_id {
                S3Operation::AbortMultipartUpload {
                    bucket,
                    key,
                    upload_id: uid,
                }
            } else {
                S3Operation::DeleteObject { bucket, key }
            }
        }

        (Method::HEAD, Some(key)) => S3Operation::HeadObject { bucket, key },

        (Method::POST, Some(key)) => {
            if query_params.contains_key("uploads") {
                S3Operation::CreateMultipartUpload { bucket, key }
            } else if let Some(uid) = upload_id {
                S3Operation::CompleteMultipartUpload {
                    bucket,
                    key,
                    upload_id: uid,
                }
            } else {
                S3Operation::Unsupported(format!("POST /{}/{}", bucket, key))
            }
        }

        (method, key) => S3Operation::Unsupported(format!(
            "{} /{}/{}",
            method,
            bucket,
            key.unwrap_or_default()
        )),
    }
}

fn parse_query(query: Option<&str>) -> std::collections::HashMap<String, String> {
    let mut params = std::collections::HashMap::new();
    if let Some(q) = query {
        for pair in q.split('&') {
            if let Some((k, v)) = pair.split_once('=') {
                params.insert(
                    percent_decode_str(k).decode_utf8_lossy().to_string(),
                    percent_decode_str(v).decode_utf8_lossy().to_string(),
                );
            } else {
                // Query param with no value (e.g. "?uploads").
                params.insert(
                    percent_decode_str(pair).decode_utf8_lossy().to_string(),
                    String::new(),
                );
            }
        }
    }
    params
}
