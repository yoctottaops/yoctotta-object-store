use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tracing::instrument;

use orion_core::*;

/// Filesystem-based storage backend.
///
/// Layout:
///   {root}/{bucket}/{key}          — object data
///   {root}/{bucket}/.meta/{key}    — JSON metadata
///
/// Buckets are directories. Keys can contain '/' which creates subdirectories.
/// This is a straightforward layout optimized for simplicity and debuggability —
/// you can inspect stored objects with standard unix tools.
pub struct FsStore {
    root: PathBuf,
}

impl FsStore {
    pub async fn new(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).await?;
        tracing::info!(path = %root.display(), "Filesystem store initialized");
        Ok(Self { root })
    }

    fn data_path(&self, key: &ObjectKey) -> PathBuf {
        self.root.join(&key.bucket).join(&key.key)
    }

    fn meta_path(&self, key: &ObjectKey) -> PathBuf {
        self.root
            .join(&key.bucket)
            .join(".meta")
            .join(&key.key)
    }

    fn bucket_path(&self, bucket: &str) -> PathBuf {
        self.root.join(bucket)
    }

    async fn write_meta(&self, key: &ObjectKey, meta: &ObjectMeta) -> Result<()> {
        let meta_path = self.meta_path(key);
        if let Some(parent) = meta_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let json = serde_json::to_vec_pretty(meta)
            .map_err(|e| OrionError::Internal(e.to_string()))?;
        fs::write(&meta_path, &json).await?;
        Ok(())
    }

    async fn read_meta(&self, key: &ObjectKey) -> Result<ObjectMeta> {
        let meta_path = self.meta_path(key);
        let data = fs::read(&meta_path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                OrionError::NotFound(key.to_string())
            } else {
                OrionError::Io(e)
            }
        })?;
        serde_json::from_slice(&data).map_err(|e| OrionError::Metadata(e.to_string()))
    }
}

#[async_trait]
impl StorageBackend for FsStore {
    #[instrument(skip(self, data, opts), fields(key = %key))]
    async fn put(
        &self,
        key: &ObjectKey,
        mut data: DataStream,
        opts: &PutOptions,
    ) -> Result<ObjectMeta> {
        let data_path = self.data_path(key);
        if let Some(parent) = data_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        // Stream data to file while computing hash.
        let mut file = fs::File::create(&data_path).await?;
        let mut hasher = Sha256::new();
        let mut size: u64 = 0;

        while let Some(chunk) = data.next().await {
            let chunk = chunk?;
            hasher.update(&chunk);
            size += chunk.len() as u64;
            file.write_all(&chunk).await?;
        }
        file.flush().await?;

        let hash = hex::encode(hasher.finalize());
        let etag = format!("\"{}\"", &hash[..32]); // S3 etags are quoted hex

        let now = Utc::now();
        let meta = ObjectMeta {
            size,
            etag,
            content_type: opts
                .content_type
                .clone()
                .unwrap_or_else(|| "application/octet-stream".into()),
            created_at: now,
            modified_at: now,
            user_meta: opts.user_meta.clone(),
            checksum: Some(hash),
            version_id: None,
        };

        self.write_meta(key, &meta).await?;

        tracing::debug!(size, "Object stored");
        Ok(meta)
    }

    #[instrument(skip(self), fields(key = %key))]
    async fn get(&self, key: &ObjectKey) -> Result<(DataStream, ObjectMeta)> {
        let meta = self.read_meta(key).await?;
        let data_path = self.data_path(key);

        let file = fs::File::open(&data_path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                OrionError::NotFound(key.to_string())
            } else {
                OrionError::Io(e)
            }
        })?;

        // Stream file in 64KB chunks.
        let stream = tokio_util::io::ReaderStream::new(tokio::io::BufReader::new(file));
        let mapped = stream.map(|r: std::result::Result<Bytes, std::io::Error>| r.map_err(OrionError::Io));

        Ok((Box::pin(mapped), meta))
    }

    #[instrument(skip(self), fields(key = %key))]
    async fn delete(&self, key: &ObjectKey) -> Result<()> {
        let data_path = self.data_path(key);
        let meta_path = self.meta_path(key);

        // Delete data file.
        fs::remove_file(&data_path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                OrionError::NotFound(key.to_string())
            } else {
                OrionError::Io(e)
            }
        })?;

        // Delete metadata (best effort).
        let _ = fs::remove_file(&meta_path).await;

        tracing::debug!("Object deleted");
        Ok(())
    }

    #[instrument(skip(self), fields(key = %key))]
    async fn head(&self, key: &ObjectKey) -> Result<ObjectMeta> {
        self.read_meta(key).await
    }

    #[instrument(skip(self), fields(bucket = bucket))]
    async fn list(&self, bucket: &str, opts: &ListOptions) -> Result<ListPage> {
        let bucket_path = self.bucket_path(bucket);
        if !bucket_path.exists() {
            return Err(OrionError::BucketNotFound(bucket.to_string()));
        }

        let prefix = opts.prefix.as_deref().unwrap_or("");
        let delimiter = opts.delimiter.as_deref();
        let max_keys = opts.max_keys as usize;

        let mut entries = Vec::new();
        let mut common_prefixes = Vec::new();
        let mut seen_prefixes = std::collections::HashSet::new();

        // Walk the directory tree.
        self.walk_dir(&bucket_path, &bucket_path, prefix, delimiter, &mut entries, &mut common_prefixes, &mut seen_prefixes).await?;

        // Sort by key.
        entries.sort_by(|a, b| a.key.cmp(&b.key));

        // Apply cursor (start-after).
        let start_idx = if let Some(cursor) = &opts.cursor {
            entries.iter().position(|e| e.key.as_str() > cursor.as_str()).unwrap_or(entries.len())
        } else {
            0
        };

        let entries: Vec<_> = entries.into_iter().skip(start_idx).collect();
        let is_truncated = entries.len() > max_keys;
        let entries: Vec<_> = entries.into_iter().take(max_keys).collect();
        let next_cursor = if is_truncated {
            entries.last().map(|e| e.key.clone())
        } else {
            None
        };

        common_prefixes.sort_by(|a, b| a.prefix.cmp(&b.prefix));

        Ok(ListPage {
            entries,
            common_prefixes,
            next_cursor,
            is_truncated,
        })
    }

    async fn stats(&self) -> Result<BackendStats> {
        // Walk root to compute stats. For production, cache this.
        let mut total_size = 0u64;
        let mut count = 0u64;

        let mut stack = vec![self.root.clone()];
        while let Some(dir) = stack.pop() {
            let mut rd = fs::read_dir(&dir).await?;
            while let Some(entry) = rd.next_entry().await? {
                let ft = entry.file_type().await?;
                if ft.is_dir() {
                    let name = entry.file_name();
                    if name.to_str().map_or(true, |n| n != ".meta") {
                        stack.push(entry.path());
                    }
                } else if ft.is_file() {
                    let path = entry.path();
                    // Skip metadata files.
                    if !path.to_str().map_or(false, |p| p.contains("/.meta/")) {
                        let meta = entry.metadata().await?;
                        total_size += meta.len();
                        count += 1;
                    }
                }
            }
        }

        Ok(BackendStats {
            total_bytes: 0, // Would need statvfs for actual disk capacity
            used_bytes: total_size,
            object_count: count,
            is_healthy: true,
        })
    }

    fn name(&self) -> &str {
        "filesystem"
    }
}

impl FsStore {
    #[async_recursion::async_recursion]
    async fn walk_dir(
        &self,
        base: &Path,
        dir: &Path,
        prefix: &str,
        delimiter: Option<&str>,
        entries: &mut Vec<ListEntry>,
        common_prefixes: &mut Vec<CommonPrefix>,
        seen_prefixes: &mut std::collections::HashSet<String>,
    ) -> Result<()> {
        let mut rd = match fs::read_dir(dir).await {
            Ok(rd) => rd,
            Err(_) => return Ok(()),
        };

        while let Some(entry) = rd.next_entry().await? {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            // Skip .meta directories.
            if name_str == ".meta" {
                continue;
            }

            let ft = entry.file_type().await?;
            let rel = entry
                .path()
                .strip_prefix(base)
                .unwrap_or(&entry.path())
                .to_string_lossy()
                .to_string();

            if ft.is_dir() {
                if let Some(delim) = delimiter {
                    let dir_prefix = format!("{}{}", rel, delim);
                    if dir_prefix.starts_with(prefix) && !seen_prefixes.contains(&dir_prefix) {
                        seen_prefixes.insert(dir_prefix.clone());
                        common_prefixes.push(CommonPrefix {
                            prefix: dir_prefix,
                        });
                    }
                } else {
                    self.walk_dir(base, &entry.path(), prefix, delimiter, entries, common_prefixes, seen_prefixes).await?;
                }
            } else if ft.is_file() && rel.starts_with(prefix) {
                let key = ObjectKey::new(
                    dir.strip_prefix(&self.root)
                        .unwrap_or(dir)
                        .iter()
                        .next()
                        .map(|s| s.to_string_lossy().to_string())
                        .unwrap_or_default(),
                    &rel,
                );
                if let Ok(meta) = self.read_meta(&key).await {
                    entries.push(ListEntry {
                        key: rel,
                        meta,
                    });
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl BucketManager for FsStore {
    async fn create_bucket(&self, name: &str) -> Result<BucketInfo> {
        let path = self.bucket_path(name);
        if path.exists() {
            return Err(OrionError::BucketAlreadyExists(name.to_string()));
        }
        fs::create_dir_all(&path).await?;
        fs::create_dir_all(path.join(".meta")).await?;

        let info = BucketInfo {
            name: name.to_string(),
            created_at: Utc::now(),
            region: None,
        };
        Ok(info)
    }

    async fn delete_bucket(&self, name: &str) -> Result<()> {
        let path = self.bucket_path(name);
        if !path.exists() {
            return Err(OrionError::BucketNotFound(name.to_string()));
        }
        fs::remove_dir_all(&path).await?;
        Ok(())
    }

    async fn head_bucket(&self, name: &str) -> Result<BucketInfo> {
        let path = self.bucket_path(name);
        if !path.exists() {
            return Err(OrionError::BucketNotFound(name.to_string()));
        }
        let fs_meta = fs::metadata(&path).await?;
        let created: DateTime<Utc> = fs_meta.created().unwrap_or(std::time::SystemTime::now()).into();
        Ok(BucketInfo {
            name: name.to_string(),
            created_at: created,
            region: None,
        })
    }

    async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        let mut buckets = Vec::new();
        let mut rd = fs::read_dir(&self.root).await?;
        while let Some(entry) = rd.next_entry().await? {
            if entry.file_type().await?.is_dir() {
                let name = entry.file_name().to_string_lossy().to_string();
                if !name.starts_with('.') {
                    let fs_meta = entry.metadata().await?;
                    let created: DateTime<Utc> = fs_meta.created().unwrap_or(std::time::SystemTime::now()).into();
                    buckets.push(BucketInfo {
                        name,
                        created_at: created,
                        region: None,
                    });
                }
            }
        }
        Ok(buckets)
    }
}
