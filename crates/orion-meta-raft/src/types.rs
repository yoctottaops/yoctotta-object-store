use std::collections::HashMap;
use std::io::Cursor;

use openraft::BasicNode;
use serde::{Deserialize, Serialize};

/// Node identifier in the Raft cluster.
pub type NodeId = u64;

// ── Raft type config ──

openraft::declare_raft_types!(
    pub TypeConfig:
        D = MetaCommand,
        R = MetaResponse,
        NodeId = NodeId,
        Node = BasicNode,
        SnapshotData = Cursor<Vec<u8>>,
);

// ── Commands ──

/// A metadata operation that goes through Raft consensus.
/// Every write to the metadata store becomes one of these commands,
/// serialized into the Raft log, and applied to each node's local SQLite.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetaCommand {
    // ── Bucket operations ──
    CreateBucket {
        name: String,
        created_at: String, // RFC3339
        region: Option<String>,
    },
    DeleteBucket {
        name: String,
    },

    // ── Object metadata operations ──
    PutObjectMeta {
        bucket: String,
        key: String,
        size: u64,
        etag: String,
        content_type: String,
        created_at: String,
        modified_at: String,
        user_meta: HashMap<String, String>,
        checksum: Option<String>,
        version_id: Option<String>,
    },
    DeleteObjectMeta {
        bucket: String,
        key: String,
    },
}

/// Response from applying a command to the state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetaResponse {
    Ok,
    Error(String),
}

// ── Snapshot data ──

/// Snapshot of the entire metadata state.
/// For SQLite, this is just the serialized database file.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MetaSnapshot {
    /// The SQLite database as raw bytes.
    pub db_bytes: Vec<u8>,
    /// Last applied log index at snapshot time.
    pub last_applied_log: Option<openraft::LogId<NodeId>>,
    /// Last membership config at snapshot time.
    pub last_membership: openraft::StoredMembership<NodeId, BasicNode>,
}
