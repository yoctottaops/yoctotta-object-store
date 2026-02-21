use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use openraft::{BasicNode, Config, Raft};

use orion_core::*;

use crate::log_store::MemLogStore;
use crate::network::RaftNetworkTransport;
use crate::state_machine::MetaStateMachine;
use crate::types::*;

/// Configuration for the Raft-replicated metadata store.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct RaftMetaStoreConfig {
    /// This node's ID (must be unique in the cluster).
    pub node_id: NodeId,
    /// This node's advertised address (host:port for Raft RPC).
    pub listen_addr: String,
    /// Path to the SQLite database for metadata.
    pub db_path: PathBuf,
    /// Initial cluster members: list of (node_id, address) pairs.
    /// Only used during initial cluster bootstrap.
    #[serde(default)]
    pub initial_members: Vec<(NodeId, String)>,
    /// Read consistency: "local" for fast stale reads, "leader" for linearizable.
    #[serde(default = "default_read_consistency")]
    pub read_consistency: String,
    /// Raft election timeout range in milliseconds.
    #[serde(default = "default_election_timeout_min")]
    pub election_timeout_min: u64,
    #[serde(default = "default_election_timeout_max")]
    pub election_timeout_max: u64,
    /// Raft heartbeat interval in milliseconds.
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval: u64,
    /// Number of log entries before triggering a snapshot.
    #[serde(default = "default_snapshot_threshold")]
    pub snapshot_threshold: u64,
}

fn default_read_consistency() -> String {
    "local".into()
}
fn default_election_timeout_min() -> u64 {
    300
}
fn default_election_timeout_max() -> u64 {
    600
}
fn default_heartbeat_interval() -> u64 {
    100
}
fn default_snapshot_threshold() -> u64 {
    5000
}

/// Raft-replicated metadata store.
///
/// Writes go through Raft consensus (leader -> replicate -> commit -> apply).
/// Reads go directly to local SQLite (fast, eventually consistent) or
/// through the leader (linearizable, slower).
///
/// Each node in the cluster runs a full copy of the metadata in SQLite.
/// The Raft log ensures all nodes converge to the same state.
pub struct RaftMetaStore {
    /// The openraft instance.
    raft: Raft<TypeConfig>,
    /// The local state machine (SQLite).
    state_machine: Arc<MetaStateMachine>,
    /// Network layer for Raft RPC.
    network: Arc<RaftNetworkTransport>,
    /// Configuration.
    config: RaftMetaStoreConfig,
}

type OrionRaft = Raft<TypeConfig>;

impl RaftMetaStore {
    /// Create and start a new Raft metadata store node.
    pub async fn new(config: RaftMetaStoreConfig) -> orion_core::Result<Self> {
        // Build openraft config.
        let raft_config = Config {
            election_timeout_min: config.election_timeout_min,
            election_timeout_max: config.election_timeout_max,
            heartbeat_interval: config.heartbeat_interval,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(config.snapshot_threshold),
            ..Default::default()
        };
        let raft_config = Arc::new(raft_config.validate().map_err(|e| {
            OrionError::Internal(format!("Invalid Raft config: {}", e))
        })?);

        // Create components.
        let state_machine = Arc::new(MetaStateMachine::new(&config.db_path)?);
        let log_store = MemLogStore::new();
        let network = Arc::new(RaftNetworkTransport::new());

        // Add known nodes to network.
        for (id, addr) in &config.initial_members {
            network.add_node(*id, addr.clone()).await;
        }

        // Create Raft instance.
        let raft = OrionRaft::new(
            config.node_id,
            raft_config,
            network.clone(),
            log_store,
            state_machine.clone(),
        )
        .await
        .map_err(|e| OrionError::Internal(format!("Failed to create Raft: {}", e)))?;

        tracing::info!(
            node_id = config.node_id,
            addr = config.listen_addr,
            members = config.initial_members.len(),
            "Raft metadata store initialized"
        );

        Ok(Self {
            raft,
            state_machine,
            network,
            config,
        })
    }

    /// Bootstrap the cluster with the initial set of members.
    /// Call this on ONE node when first starting the cluster.
    pub async fn bootstrap(&self) -> orion_core::Result<()> {
        let mut members = std::collections::BTreeMap::new();
        for (id, addr) in &self.config.initial_members {
            members.insert(*id, BasicNode { addr: addr.clone() });
        }
        // Also add self.
        members.insert(
            self.config.node_id,
            BasicNode {
                addr: self.config.listen_addr.clone(),
            },
        );

        self.raft
            .initialize(members)
            .await
            .map_err(|e| OrionError::Internal(format!("Bootstrap failed: {}", e)))?;

        tracing::info!("Cluster bootstrapped");
        Ok(())
    }

    /// Submit a write command through Raft consensus.
    async fn write(&self, cmd: MetaCommand) -> orion_core::Result<()> {
        let resp = self
            .raft
            .client_write(cmd)
            .await
            .map_err(|e| OrionError::Internal(format!("Raft write failed: {}", e)))?;

        match resp.data {
            MetaResponse::Ok => Ok(()),
            MetaResponse::Error(msg) => Err(OrionError::Metadata(msg)),
        }
    }

    /// Read from local SQLite (eventually consistent).
    fn local_store(&self) -> &orion_meta_sqlite::SqliteMetaStore {
        self.state_machine.sqlite()
    }

    /// Get the Raft instance (for mounting HTTP endpoints).
    pub fn raft(&self) -> &OrionRaft {
        &self.raft
    }

    /// Get the network layer (for adding nodes dynamically).
    pub fn network(&self) -> &Arc<RaftNetworkTransport> {
        &self.network
    }

    /// Check if this node is the current leader.
    pub async fn is_leader(&self) -> bool {
        self.raft.ensure_linearizable().await.is_ok()
    }

    /// Get current cluster metrics.
    pub fn metrics(&self) -> openraft::RaftMetrics<NodeId, openraft::BasicNode> {
        self.raft.metrics().borrow().clone()
    }
}

#[async_trait]
impl MetadataStore for RaftMetaStore {
    // ── Bucket operations (writes go through Raft) ──

    async fn create_bucket(&self, info: &BucketInfo) -> orion_core::Result<()> {
        self.write(MetaCommand::CreateBucket {
            name: info.name.clone(),
            created_at: info.created_at.to_rfc3339(),
            region: info.region.clone(),
        })
        .await
    }

    async fn delete_bucket(&self, name: &str) -> orion_core::Result<()> {
        self.write(MetaCommand::DeleteBucket {
            name: name.to_string(),
        })
        .await
    }

    // ── Reads go to local SQLite ──

    async fn get_bucket(&self, name: &str) -> orion_core::Result<BucketInfo> {
        self.local_store().get_bucket(name).await
    }

    async fn list_buckets(&self) -> orion_core::Result<Vec<BucketInfo>> {
        self.local_store().list_buckets().await
    }

    async fn bucket_exists(&self, name: &str) -> orion_core::Result<bool> {
        self.local_store().bucket_exists(name).await
    }

    // ── Object metadata (writes through Raft, reads local) ──

    async fn put_object_meta(
        &self,
        bucket: &str,
        key: &str,
        meta: &ObjectMeta,
    ) -> orion_core::Result<()> {
        self.write(MetaCommand::PutObjectMeta {
            bucket: bucket.to_string(),
            key: key.to_string(),
            size: meta.size,
            etag: meta.etag.clone(),
            content_type: meta.content_type.clone(),
            created_at: meta.created_at.to_rfc3339(),
            modified_at: meta.modified_at.to_rfc3339(),
            user_meta: meta.user_meta.clone(),
            checksum: meta.checksum.clone(),
            version_id: meta.version_id.clone(),
        })
        .await
    }

    async fn get_object_meta(
        &self,
        bucket: &str,
        key: &str,
    ) -> orion_core::Result<ObjectMeta> {
        self.local_store().get_object_meta(bucket, key).await
    }

    async fn delete_object_meta(
        &self,
        bucket: &str,
        key: &str,
    ) -> orion_core::Result<()> {
        self.write(MetaCommand::DeleteObjectMeta {
            bucket: bucket.to_string(),
            key: key.to_string(),
        })
        .await
    }

    async fn list_objects(
        &self,
        bucket: &str,
        opts: &ListOptions,
    ) -> orion_core::Result<ListPage> {
        self.local_store().list_objects(bucket, opts).await
    }

    async fn object_exists(
        &self,
        bucket: &str,
        key: &str,
    ) -> orion_core::Result<bool> {
        self.local_store().object_exists(bucket, key).await
    }

    async fn object_count(&self, bucket: &str) -> orion_core::Result<u64> {
        self.local_store().object_count(bucket).await
    }

    async fn total_size(&self, bucket: &str) -> orion_core::Result<u64> {
        self.local_store().total_size(bucket).await
    }
}
