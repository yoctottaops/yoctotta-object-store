use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use openraft::storage::RaftStateMachine;
use openraft::{
    Entry, EntryPayload, LogId, OptionalSend, RaftSnapshotBuilder,
    Snapshot, SnapshotMeta, StorageError, StoredMembership,
};
use tokio::sync::RwLock;

use orion_core::{BucketInfo, MetadataStore, ObjectMeta};
use orion_meta_sqlite::SqliteMetaStore;

use crate::types::*;

/// The Raft state machine backed by SQLite.
///
/// Every Raft log entry is a MetaCommand that gets applied to the local
/// SQLite database. Snapshots are copies of the SQLite file.
pub struct MetaStateMachine {
    /// Path to the SQLite database file.
    db_path: PathBuf,
    /// The underlying SQLite metadata store.
    pub(crate) inner: Arc<SqliteMetaStore>,
    /// Last applied log entry.
    last_applied: RwLock<Option<LogId<NodeId>>>,
    /// Last membership configuration.
    last_membership: RwLock<StoredMembership<NodeId, openraft::BasicNode>>,
    /// Snapshot index counter.
    snapshot_idx: RwLock<u64>,
}

impl MetaStateMachine {
    pub fn new(db_path: impl AsRef<Path>) -> orion_core::Result<Self> {
        let db_path = db_path.as_ref().to_path_buf();
        let inner = Arc::new(SqliteMetaStore::new(&db_path)?);
        Ok(Self {
            db_path,
            inner,
            last_applied: RwLock::new(None),
            last_membership: RwLock::new(StoredMembership::default()),
            snapshot_idx: RwLock::new(0),
        })
    }

    /// Apply a single metadata command to the local SQLite store.
    async fn apply_command(&self, cmd: &MetaCommand) -> MetaResponse {
        let result = match cmd {
            MetaCommand::CreateBucket {
                name,
                created_at,
                region,
            } => {
                let created = chrono::DateTime::parse_from_rfc3339(created_at)
                    .map(|dt| dt.with_timezone(&chrono::Utc))
                    .unwrap_or_else(|_| chrono::Utc::now());

                let info = BucketInfo {
                    name: name.clone(),
                    created_at: created,
                    region: region.clone(),
                };
                self.inner.create_bucket(&info).await
            }

            MetaCommand::DeleteBucket { name } => self.inner.delete_bucket(name).await,

            MetaCommand::PutObjectMeta {
                bucket,
                key,
                size,
                etag,
                content_type,
                created_at,
                modified_at,
                user_meta,
                checksum,
                version_id,
            } => {
                let created = chrono::DateTime::parse_from_rfc3339(created_at)
                    .map(|dt| dt.with_timezone(&chrono::Utc))
                    .unwrap_or_else(|_| chrono::Utc::now());
                let modified = chrono::DateTime::parse_from_rfc3339(modified_at)
                    .map(|dt| dt.with_timezone(&chrono::Utc))
                    .unwrap_or_else(|_| chrono::Utc::now());

                let meta = ObjectMeta {
                    size: *size,
                    etag: etag.clone(),
                    content_type: content_type.clone(),
                    created_at: created,
                    modified_at: modified,
                    user_meta: user_meta.clone(),
                    checksum: checksum.clone(),
                    version_id: version_id.clone(),
                };
                self.inner.put_object_meta(bucket, key, &meta).await
            }

            MetaCommand::DeleteObjectMeta { bucket, key } => {
                self.inner.delete_object_meta(bucket, key).await
            }
        };

        match result {
            Ok(_) => MetaResponse::Ok,
            Err(e) => {
                tracing::warn!(error = %e, "State machine command failed");
                MetaResponse::Error(e.to_string())
            }
        }
    }

    /// Get a reference to the underlying SQLite store for reads.
    pub fn sqlite(&self) -> &SqliteMetaStore {
        &self.inner
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<MetaStateMachine> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        // Read the SQLite database file as the snapshot.
        let db_bytes = tokio::fs::read(&self.db_path)
            .await
            .map_err(|e| StorageError::from_io_error(
                openraft::ErrorSubject::StateMachine,
                openraft::ErrorVerb::Read,
                std::io::Error::new(std::io::ErrorKind::Other, e),
            ))?;

        let last_applied = self.last_applied.read().await.clone();
        let last_membership = self.last_membership.read().await.clone();

        let mut idx = self.snapshot_idx.write().await;
        *idx += 1;
        let snapshot_id = format!(
            "{}-{}-{}",
            last_applied
                .map(|la| la.index.to_string())
                .unwrap_or_else(|| "0".into()),
            last_applied
                .map(|la| la.leader_id.term.to_string())
                .unwrap_or_else(|| "0".into()),
            *idx
        );

        let snapshot_meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership,
            snapshot_id,
        };

        let snapshot_data = MetaSnapshot {
            db_bytes,
            last_applied_log: last_applied,
            last_membership: self.last_membership.read().await.clone(),
        };

        let data = serde_json::to_vec(&snapshot_data)
            .map_err(|e| StorageError::from_io_error(
                openraft::ErrorSubject::StateMachine,
                openraft::ErrorVerb::Read,
                std::io::Error::new(std::io::ErrorKind::Other, e),
            ))?;

        Ok(Snapshot {
            meta: snapshot_meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for Arc<MetaStateMachine> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<NodeId>>,
            StoredMembership<NodeId, openraft::BasicNode>,
        ),
        StorageError<NodeId>,
    > {
        let last_applied = self.last_applied.read().await.clone();
        let membership = self.last_membership.read().await.clone();
        Ok((last_applied, membership))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<MetaResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
    {
        let mut responses = Vec::new();

        for entry in entries {
            // Update last applied.
            *self.last_applied.write().await = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => {
                    responses.push(MetaResponse::Ok);
                }
                EntryPayload::Normal(cmd) => {
                    let resp = self.apply_command(&cmd).await;
                    responses.push(resp);
                }
                EntryPayload::Membership(membership) => {
                    *self.last_membership.write().await =
                        StoredMembership::new(Some(entry.log_id), membership);
                    responses.push(MetaResponse::Ok);
                }
            }
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();

        let snapshot_data: MetaSnapshot = serde_json::from_slice(&data)
            .map_err(|e| StorageError::from_io_error(
                openraft::ErrorSubject::StateMachine,
                openraft::ErrorVerb::Read,
                std::io::Error::new(std::io::ErrorKind::Other, e),
            ))?;

        // Write the snapshot's SQLite database to disk.
        tokio::fs::write(&self.db_path, &snapshot_data.db_bytes)
            .await
            .map_err(|e| StorageError::from_io_error(
                openraft::ErrorSubject::StateMachine,
                openraft::ErrorVerb::Write,
                e,
            ))?;

        // Update state.
        *self.last_applied.write().await = meta.last_log_id;
        *self.last_membership.write().await = meta.last_membership.clone();

        tracing::info!(
            last_log = ?meta.last_log_id,
            "Snapshot installed"
        );

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        // Build a fresh snapshot from current state.
        let mut builder = self.clone();
        let snapshot = RaftSnapshotBuilder::build_snapshot(&mut builder).await?;
        Ok(Some(snapshot))
    }
}
