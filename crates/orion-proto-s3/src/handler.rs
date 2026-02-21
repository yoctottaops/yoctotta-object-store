use bytes::Bytes;
use http::{Request, Response};
use http_body_util::{BodyExt, Full};
use std::collections::HashMap;
use std::sync::Arc;

use orion_core::*;
use orion_ext_auth::AuthProvider;

use crate::error_response;
use crate::management;
use crate::metrics::MetricsCollector;
use crate::parser::{self, S3Operation};
use crate::xml;

/// S3 request handler backed by pluggable storage, metadata, and extensions.
pub struct S3Handler {
    pub store: Arc<dyn StorageBackend>,
    pub buckets: Arc<dyn BucketManager>,
    pub meta: Arc<dyn MetadataStore>,
    pub extensions: Arc<ExtensionRegistry>,
    pub metrics: Arc<MetricsCollector>,
    pub auth: Option<Arc<AuthProvider>>,
}

impl S3Handler {
    pub async fn handle(
        &self,
        req: Request<hyper::body::Incoming>,
    ) -> Response<Full<Bytes>> {
        self.metrics.increment_requests();

        let method = req.method().clone();
        let uri = req.uri().clone();
        let path = uri.path();
        let query = uri.query();
        let headers = req.headers().clone();

        let op = parser::parse_request(&method, path, query);

        tracing::debug!(?op, "S3 operation");

        // Auth check: skip for UI page, login, CORS preflight.
        if let Some(ref auth) = self.auth {
            if auth.is_enabled() {
                let skip_auth = matches!(
                    op,
                    S3Operation::ManagementUI
                        | S3Operation::ManagementApiLogin
                        | S3Operation::ManagementApiCors
                );
                if !skip_auth {
                    let token = management::extract_session_token(&headers);
                    let valid = if let Some(ref t) = token {
                        auth.validate_session(t).await.is_some()
                    } else {
                        false
                    };
                    if !valid {
                        return management::json_unauthorized();
                    }
                }
            }
        }

        match op {
            // ── Service ──
            S3Operation::ListBuckets => self.handle_list_buckets().await,

            // ── Bucket ──
            S3Operation::CreateBucket { bucket } => self.handle_create_bucket(&bucket).await,
            S3Operation::DeleteBucket { bucket } => self.handle_delete_bucket(&bucket).await,
            S3Operation::HeadBucket { bucket } => self.handle_head_bucket(&bucket).await,
            S3Operation::ListObjects { bucket, params } => {
                self.handle_list_objects(&bucket, params).await
            }

            // ── Object ──
            S3Operation::GetObject { bucket, key } => {
                self.handle_get_object(&bucket, &key).await
            }
            S3Operation::PutObject { bucket, key } => {
                self.handle_put_object(&bucket, &key, &headers, req).await
            }
            S3Operation::DeleteObject { bucket, key } => {
                self.handle_delete_object(&bucket, &key).await
            }
            S3Operation::HeadObject { bucket, key } => {
                self.handle_head_object(&bucket, &key).await
            }

            // ── Not yet implemented ──
            S3Operation::CopyObject { .. }
            | S3Operation::CreateMultipartUpload { .. }
            | S3Operation::UploadPart { .. }
            | S3Operation::CompleteMultipartUpload { .. }
            | S3Operation::AbortMultipartUpload { .. } => {
                error_response::not_implemented(&format!("{:?}", op))
            }

            // ── Management UI ──
            S3Operation::ManagementUI => management::handle_ui(),
            S3Operation::ManagementApiStats => management::handle_api_stats(self).await,
            S3Operation::ManagementApiExtensions => management::handle_api_extensions(self).await,
            S3Operation::ManagementApiSearch => management::handle_api_search(self, req).await,
            S3Operation::ManagementApiMetrics => management::handle_api_metrics(self).await,
            S3Operation::ManagementApiLogin => management::handle_api_login(self, req).await,
            S3Operation::ManagementApiLogout => management::handle_api_logout(self, req).await,
            S3Operation::ManagementApiCors => management::handle_cors(),

            S3Operation::Unsupported(desc) => error_response::not_implemented(&desc),
        }
    }

    async fn handle_list_buckets(&self) -> Response<Full<Bytes>> {
        match self.buckets.list_buckets().await {
            Ok(buckets) => {
                let body = xml::list_buckets_xml(&buckets);
                Response::builder()
                    .status(200)
                    .header("Content-Type", "application/xml")
                    .body(Full::new(Bytes::from(body)))
                    .unwrap()
            }
            Err(e) => error_response::error_response(&e, "/"),
        }
    }

    async fn handle_create_bucket(&self, bucket: &str) -> Response<Full<Bytes>> {
        match self.buckets.create_bucket(bucket).await {
            Ok(info) => {
                // Also register in metadata store.
                if let Err(e) = self.meta.create_bucket(&info).await {
                    tracing::error!(error = %e, "Failed to register bucket in metadata");
                }

                let event = StorageEvent::BucketCreated {
                    name: bucket.to_string(),
                };
                self.extensions.dispatch_event(&event).await;

                Response::builder()
                    .status(200)
                    .header("Location", format!("/{}", bucket))
                    .body(Full::new(Bytes::new()))
                    .unwrap()
            }
            Err(e) => error_response::error_response(&e, bucket),
        }
    }

    async fn handle_delete_bucket(&self, bucket: &str) -> Response<Full<Bytes>> {
        match self.buckets.delete_bucket(bucket).await {
            Ok(()) => {
                let _ = self.meta.delete_bucket(bucket).await;
                let event = StorageEvent::BucketDeleted {
                    name: bucket.to_string(),
                };
                self.extensions.dispatch_event(&event).await;

                Response::builder()
                    .status(204)
                    .body(Full::new(Bytes::new()))
                    .unwrap()
            }
            Err(e) => error_response::error_response(&e, bucket),
        }
    }

    async fn handle_head_bucket(&self, bucket: &str) -> Response<Full<Bytes>> {
        match self.buckets.head_bucket(bucket).await {
            Ok(_) => Response::builder()
                .status(200)
                .body(Full::new(Bytes::new()))
                .unwrap(),
            Err(e) => error_response::error_response(&e, bucket),
        }
    }

    async fn handle_list_objects(
        &self,
        bucket: &str,
        params: parser::ListParams,
    ) -> Response<Full<Bytes>> {
        let opts = ListOptions {
            prefix: params.prefix.clone(),
            delimiter: params.delimiter.clone(),
            cursor: params.continuation_token.clone().or(params.start_after.clone()),
            max_keys: params.max_keys.unwrap_or(1000),
        };

        match self.meta.list_objects(bucket, &opts).await {
            Ok(page) => {
                let body = xml::list_objects_v2_xml(
                    bucket,
                    &page,
                    params.prefix.as_deref(),
                    params.delimiter.as_deref(),
                    opts.max_keys,
                    params.continuation_token.as_deref(),
                );
                Response::builder()
                    .status(200)
                    .header("Content-Type", "application/xml")
                    .body(Full::new(Bytes::from(body)))
                    .unwrap()
            }
            Err(e) => error_response::error_response(&e, bucket),
        }
    }

    async fn handle_get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Response<Full<Bytes>> {
        let obj_key = ObjectKey::new(bucket, key);

        // Pre-read hooks.
        let mut ctx = ExtensionContext::new();
        match self.extensions.dispatch_pre_write(&obj_key, &PutOptions::default(), &mut ctx).await {
            Ok(HookAction::Deny(reason)) => {
                return error_response::error_response(
                    &OrionError::AccessDenied(reason),
                    &obj_key.to_string(),
                );
            }
            Err(e) => return error_response::error_response(&e, &obj_key.to_string()),
            _ => {}
        }

        match self.store.get(&obj_key).await {
            Ok((stream, meta)) => {
                // Collect stream (for now — streaming response comes next).
                let data = match orion_core::stream::collect_stream(stream).await {
                    Ok(d) => d,
                    Err(e) => {
                        return error_response::error_response(&e, &obj_key.to_string());
                    }
                };

                let event = StorageEvent::ObjectAccessed { key: obj_key };
                self.extensions.dispatch_event(&event).await;

                Response::builder()
                    .status(200)
                    .header("Content-Type", &meta.content_type)
                    .header("Content-Length", data.len())
                    .header("ETag", &meta.etag)
                    .header("Last-Modified", meta.modified_at.to_rfc2822())
                    .body(Full::new(Bytes::from(data)))
                    .unwrap()
            }
            Err(e) => error_response::error_response(&e, &obj_key.to_string()),
        }
    }

    async fn handle_put_object(
        &self,
        bucket: &str,
        key: &str,
        headers: &http::HeaderMap,
        req: Request<hyper::body::Incoming>,
    ) -> Response<Full<Bytes>> {
        let obj_key = ObjectKey::new(bucket, key);

        let content_type = headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(String::from);

        // Extract user metadata from x-amz-meta-* headers.
        let mut user_meta = HashMap::new();
        for (k, v) in headers.iter() {
            if let Some(meta_key) = k.as_str().strip_prefix("x-amz-meta-") {
                if let Ok(val) = v.to_str() {
                    user_meta.insert(meta_key.to_string(), val.to_string());
                }
            }
        }

        let opts = PutOptions {
            content_type,
            user_meta,
            content_md5: headers
                .get("content-md5")
                .and_then(|v| v.to_str().ok())
                .map(String::from),
        };

        // Pre-write hooks.
        let mut ctx = ExtensionContext::new();
        match self.extensions.dispatch_pre_write(&obj_key, &opts, &mut ctx).await {
            Ok(HookAction::Deny(reason)) => {
                return error_response::error_response(
                    &OrionError::AccessDenied(reason),
                    &obj_key.to_string(),
                );
            }
            Err(e) => return error_response::error_response(&e, &obj_key.to_string()),
            _ => {}
        }

        // Collect body (streaming put will be added).
        let body_bytes = match req.collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(e) => {
                return error_response::error_response(
                    &OrionError::Internal(e.to_string()),
                    &obj_key.to_string(),
                );
            }
        };

        let data_for_hooks = body_bytes.to_vec();
        let data_stream = orion_core::stream::single_chunk_stream(body_bytes);

        match self.store.put(&obj_key, data_stream, &opts).await {
            Ok(meta) => {
                // Register in metadata store.
                if let Err(e) = self.meta.put_object_meta(bucket, key, &meta).await {
                    tracing::error!(error = %e, "Failed to write object metadata");
                }

                // Post-write hooks (including RAG indexing, triggers, etc).
                self.extensions
                    .dispatch_post_write(&obj_key, &meta, &data_for_hooks, &ctx)
                    .await;

                let event = StorageEvent::ObjectCreated {
                    key: obj_key,
                    meta: meta.clone(),
                };
                self.extensions.dispatch_event(&event).await;

                Response::builder()
                    .status(200)
                    .header("ETag", &meta.etag)
                    .body(Full::new(Bytes::new()))
                    .unwrap()
            }
            Err(e) => error_response::error_response(&e, &format!("{}/{}", bucket, key)),
        }
    }

    async fn handle_delete_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Response<Full<Bytes>> {
        let obj_key = ObjectKey::new(bucket, key);

        // Pre-delete hooks.
        let mut ctx = ExtensionContext::new();
        match self.extensions.dispatch_pre_delete(&obj_key, &mut ctx).await {
            Ok(HookAction::Deny(reason)) => {
                return error_response::error_response(
                    &OrionError::AccessDenied(reason),
                    &obj_key.to_string(),
                );
            }
            Err(e) => return error_response::error_response(&e, &obj_key.to_string()),
            _ => {}
        }

        match self.store.delete(&obj_key).await {
            Ok(()) => {
                let _ = self.meta.delete_object_meta(bucket, key).await;

                self.extensions.dispatch_post_delete(&obj_key, &ctx).await;

                let event = StorageEvent::ObjectDeleted { key: obj_key };
                self.extensions.dispatch_event(&event).await;

                Response::builder()
                    .status(204)
                    .body(Full::new(Bytes::new()))
                    .unwrap()
            }
            Err(e) => error_response::error_response(&e, &format!("{}/{}", bucket, key)),
        }
    }

    async fn handle_head_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Response<Full<Bytes>> {
        let obj_key = ObjectKey::new(bucket, key);

        match self.store.head(&obj_key).await {
            Ok(meta) => Response::builder()
                .status(200)
                .header("Content-Type", &meta.content_type)
                .header("Content-Length", meta.size)
                .header("ETag", &meta.etag)
                .header("Last-Modified", meta.modified_at.to_rfc2822())
                .body(Full::new(Bytes::new()))
                .unwrap(),
            Err(e) => error_response::error_response(&e, &obj_key.to_string()),
        }
    }
}
