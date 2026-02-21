#[cfg(test)]
mod types_tests {
    use super::super::types::*;
    use std::collections::HashMap;

    #[test]
    fn object_key_new() {
        let key = ObjectKey::new("my-bucket", "some/key.txt");
        assert_eq!(key.bucket, "my-bucket");
        assert_eq!(key.key, "some/key.txt");
    }

    #[test]
    fn object_key_display() {
        let key = ObjectKey::new("b", "k");
        assert_eq!(format!("{}", key), "b/k");
    }

    #[test]
    fn object_key_equality() {
        let a = ObjectKey::new("b", "k");
        let b = ObjectKey::new("b", "k");
        let c = ObjectKey::new("b", "other");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn object_key_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(ObjectKey::new("b", "k"));
        assert!(set.contains(&ObjectKey::new("b", "k")));
        assert!(!set.contains(&ObjectKey::new("b", "other")));
    }

    #[test]
    fn object_meta_default() {
        let meta = ObjectMeta::default();
        assert_eq!(meta.size, 0);
        assert_eq!(meta.content_type, "application/octet-stream");
        assert!(meta.user_meta.is_empty());
        assert!(meta.checksum.is_none());
        assert!(meta.version_id.is_none());
    }

    #[test]
    fn put_options_default() {
        let opts = PutOptions::default();
        assert!(opts.content_type.is_none());
        assert!(opts.user_meta.is_empty());
        assert!(opts.content_md5.is_none());
    }

    #[test]
    fn list_options_default() {
        let opts = ListOptions::default();
        assert!(opts.prefix.is_none());
        assert!(opts.delimiter.is_none());
        assert!(opts.cursor.is_none());
        assert_eq!(opts.max_keys, 1000);
    }

    #[test]
    fn storage_event_names() {
        let key = ObjectKey::new("bucket", "key");
        let meta = ObjectMeta::default();

        let e = StorageEvent::ObjectCreated {
            key: key.clone(),
            meta,
        };
        assert_eq!(e.event_name(), "s3:ObjectCreated:Put");
        assert_eq!(e.bucket(), "bucket");

        let e = StorageEvent::ObjectDeleted { key: key.clone() };
        assert_eq!(e.event_name(), "s3:ObjectRemoved:Delete");

        let e = StorageEvent::ObjectAccessed { key };
        assert_eq!(e.event_name(), "s3:ObjectAccessed:Get");

        let e = StorageEvent::BucketCreated {
            name: "b".into(),
        };
        assert_eq!(e.event_name(), "s3:BucketCreated");
        assert_eq!(e.bucket(), "b");

        let e = StorageEvent::BucketDeleted {
            name: "b".into(),
        };
        assert_eq!(e.event_name(), "s3:BucketRemoved");
    }

    #[test]
    fn bucket_info_serialization() {
        let info = BucketInfo {
            name: "test".into(),
            created_at: chrono::Utc::now(),
            region: Some("us-east-1".into()),
        };
        let json = serde_json::to_string(&info).unwrap();
        let deser: BucketInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.name, "test");
        assert_eq!(deser.region, Some("us-east-1".into()));
    }

    #[test]
    fn object_meta_with_user_meta() {
        let mut user_meta = HashMap::new();
        user_meta.insert("author".into(), "test".into());

        let meta = ObjectMeta {
            user_meta,
            ..Default::default()
        };

        let json = serde_json::to_string(&meta).unwrap();
        let deser: ObjectMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.user_meta.get("author").unwrap(), "test");
    }
}

#[cfg(test)]
mod error_tests {
    use super::super::error::OrionError;

    #[test]
    fn s3_error_codes() {
        assert_eq!(
            OrionError::NotFound("x".into()).s3_error_code(),
            "NoSuchKey"
        );
        assert_eq!(
            OrionError::BucketNotFound("x".into()).s3_error_code(),
            "NoSuchBucket"
        );
        assert_eq!(
            OrionError::BucketAlreadyExists("x".into()).s3_error_code(),
            "BucketAlreadyExists"
        );
        assert_eq!(OrionError::NoSpace.s3_error_code(), "InsufficientStorage");
        assert_eq!(
            OrionError::InvalidArgument("x".into()).s3_error_code(),
            "InvalidArgument"
        );
        assert_eq!(
            OrionError::ChecksumMismatch {
                expected: "a".into(),
                actual: "b".into()
            }
            .s3_error_code(),
            "BadDigest"
        );
        assert_eq!(
            OrionError::AuthFailed("x".into()).s3_error_code(),
            "InvalidAccessKeyId"
        );
        assert_eq!(
            OrionError::AccessDenied("x".into()).s3_error_code(),
            "AccessDenied"
        );
        assert_eq!(OrionError::HookDenied.s3_error_code(), "AccessDenied");
        assert_eq!(
            OrionError::Internal("x".into()).s3_error_code(),
            "InternalError"
        );
        assert_eq!(
            OrionError::Metadata("x".into()).s3_error_code(),
            "InternalError"
        );
    }

    #[test]
    fn http_status_codes() {
        assert_eq!(OrionError::NotFound("x".into()).http_status(), 404);
        assert_eq!(OrionError::BucketNotFound("x".into()).http_status(), 404);
        assert_eq!(
            OrionError::BucketAlreadyExists("x".into()).http_status(),
            409
        );
        assert_eq!(
            OrionError::ObjectAlreadyExists("x".into()).http_status(),
            409
        );
        assert_eq!(OrionError::NoSpace.http_status(), 507);
        assert_eq!(OrionError::InvalidArgument("x".into()).http_status(), 400);
        assert_eq!(
            OrionError::ChecksumMismatch {
                expected: "a".into(),
                actual: "b".into()
            }
            .http_status(),
            400
        );
        assert_eq!(OrionError::AuthFailed("x".into()).http_status(), 401);
        assert_eq!(OrionError::AccessDenied("x".into()).http_status(), 403);
        assert_eq!(OrionError::HookDenied.http_status(), 403);
        assert_eq!(OrionError::Internal("x".into()).http_status(), 500);
    }

    #[test]
    fn error_display() {
        let e = OrionError::NotFound("my-key".into());
        assert!(e.to_string().contains("my-key"));

        let e = OrionError::ChecksumMismatch {
            expected: "abc".into(),
            actual: "def".into(),
        };
        let msg = e.to_string();
        assert!(msg.contains("abc"));
        assert!(msg.contains("def"));
    }
}

#[cfg(test)]
mod stream_tests {
    use super::super::stream::*;
    use bytes::Bytes;

    #[tokio::test]
    async fn single_chunk_roundtrip() {
        let data = Bytes::from("hello world");
        let stream = single_chunk_stream(data.clone());
        let collected = collect_stream(stream).await.unwrap();
        assert_eq!(collected, b"hello world");
    }

    #[tokio::test]
    async fn vec_to_stream_roundtrip() {
        let data = vec![1, 2, 3, 4, 5];
        let stream = vec_to_stream(data.clone());
        let collected = collect_stream(stream).await.unwrap();
        assert_eq!(collected, data);
    }

    #[tokio::test]
    async fn empty_stream_collects_empty() {
        let stream = empty_stream();
        let collected = collect_stream(stream).await.unwrap();
        assert!(collected.is_empty());
    }

    #[tokio::test]
    async fn large_data_stream() {
        let data = vec![42u8; 1024 * 1024]; // 1MB
        let stream = vec_to_stream(data.clone());
        let collected = collect_stream(stream).await.unwrap();
        assert_eq!(collected.len(), 1024 * 1024);
        assert_eq!(collected, data);
    }
}

#[cfg(test)]
mod extension_tests {
    use super::super::extension::*;
    use super::super::types::*;

    #[test]
    fn extension_context_default() {
        let ctx = ExtensionContext::new();
        assert!(ctx.data.is_empty());
    }

    #[test]
    fn extension_context_with_data() {
        let mut ctx = ExtensionContext::new();
        ctx.data.insert("key".into(), "value".into());
        assert_eq!(ctx.data.get("key").unwrap(), "value");
    }

    #[test]
    fn hook_action_debug() {
        let action = HookAction::Continue;
        assert_eq!(format!("{:?}", action), "Continue");

        let action = HookAction::Deny("forbidden".into());
        let debug = format!("{:?}", action);
        assert!(debug.contains("Deny"));
        assert!(debug.contains("forbidden"));
    }

    #[test]
    fn extension_registry_empty() {
        let registry = ExtensionRegistry::new();
        assert!(registry.list().is_empty());
    }

    #[tokio::test]
    async fn extension_registry_dispatch_continue() {
        let registry = ExtensionRegistry::new();
        let key = ObjectKey::new("b", "k");
        let opts = PutOptions::default();
        let mut ctx = ExtensionContext::new();

        // No extensions registered, should continue.
        let result = registry.dispatch_pre_write(&key, &opts, &mut ctx).await;
        assert!(result.is_ok());
        match result.unwrap() {
            HookAction::Continue => {}
            _ => panic!("Expected Continue"),
        }
    }

    #[tokio::test]
    async fn extension_registry_dispatch_pre_delete_empty() {
        let registry = ExtensionRegistry::new();
        let key = ObjectKey::new("b", "k");
        let mut ctx = ExtensionContext::new();

        let result = registry.dispatch_pre_delete(&key, &mut ctx).await;
        assert!(result.is_ok());
    }
}
