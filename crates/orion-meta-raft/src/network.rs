use std::collections::BTreeMap;
use std::sync::Arc;

use openraft::error::{
    InstallSnapshotError, RPCError, RaftError,
};
use openraft::network::{RPCOption, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use tokio::sync::RwLock;

use crate::types::*;

/// HTTP-based Raft network transport.
///
/// Each Raft RPC (AppendEntries, Vote, InstallSnapshot) is mapped to an
/// HTTP POST endpoint on the target node:
///   POST http://{node_addr}/raft/append
///   POST http://{node_addr}/raft/vote
///   POST http://{node_addr}/raft/snapshot
pub struct RaftNetworkTransport {
    /// Known nodes in the cluster: node_id -> address.
    nodes: Arc<RwLock<BTreeMap<NodeId, String>>>,
    client: reqwest::Client,
}

impl RaftNetworkTransport {
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(RwLock::new(BTreeMap::new())),
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .build()
                .expect("Failed to build HTTP client"),
        }
    }

    pub async fn add_node(&self, id: NodeId, addr: String) {
        self.nodes.write().await.insert(id, addr);
    }

    #[allow(dead_code)]
    async fn node_addr(&self, target: NodeId) -> Option<String> {
        self.nodes.read().await.get(&target).cloned()
    }
}

impl Default for RaftNetworkTransport {
    fn default() -> Self {
        Self::new()
    }
}

/// A connection to a single remote Raft node.
pub struct RaftNodeConnection {
    #[allow(dead_code)]
    target: NodeId,
    addr: String,
    client: reqwest::Client,
}

impl RaftNetworkFactory<TypeConfig> for Arc<RaftNetworkTransport> {
    type Network = RaftNodeConnection;

    async fn new_client(
        &mut self,
        target: NodeId,
        node: &BasicNode,
    ) -> Self::Network {
        RaftNodeConnection {
            target,
            addr: node.addr.clone(),
            client: self.client.clone(),
        }
    }
}

impl openraft::network::RaftNetwork<TypeConfig> for RaftNodeConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId>>,
    > {
        let url = format!("http://{}/raft/append", self.addr);
        let resp = self
            .client
            .post(&url)
            .json(&req)
            .send()
            .await
            .map_err(|e| new_rpc_error(e))?;

        let result: AppendEntriesResponse<NodeId> = resp
            .json()
            .await
            .map_err(|e| new_rpc_error(e))?;

        Ok(result)
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let url = format!("http://{}/raft/snapshot", self.addr);
        let resp = self
            .client
            .post(&url)
            .json(&req)
            .send()
            .await
            .map_err(|e| new_rpc_error_snapshot(e))?;

        let result: InstallSnapshotResponse<NodeId> = resp
            .json()
            .await
            .map_err(|e| new_rpc_error_snapshot(e))?;

        Ok(result)
    }

    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<
        VoteResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId>>,
    > {
        let url = format!("http://{}/raft/vote", self.addr);
        let resp = self
            .client
            .post(&url)
            .json(&req)
            .send()
            .await
            .map_err(|e| new_rpc_error(e))?;

        let result: VoteResponse<NodeId> = resp
            .json()
            .await
            .map_err(|e| new_rpc_error(e))?;

        Ok(result)
    }
}

fn new_rpc_error<E: std::error::Error + 'static>(
    e: E,
) -> RPCError<NodeId, BasicNode, RaftError<NodeId>> {
    RPCError::Unreachable(openraft::error::Unreachable::new(&e))
}

fn new_rpc_error_snapshot<E: std::error::Error + 'static>(
    e: E,
) -> RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>> {
    RPCError::Unreachable(openraft::error::Unreachable::new(&e))
}
