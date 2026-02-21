use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::RaftLogStorage;
use openraft::{
    Entry, LogId, LogState, OptionalSend, RaftLogReader, StorageError,
    Vote,
};
use tokio::sync::RwLock;

use crate::types::*;

/// In-memory Raft log store with vote persistence.
///
/// For production, this should be backed by persistent storage (e.g. another
/// SQLite file or WAL). The in-memory version is sufficient for development
/// and small clusters where snapshot recovery is acceptable on restart.
pub struct MemLogStore {
    vote: RwLock<Option<Vote<NodeId>>>,
    log: RwLock<BTreeMap<u64, Entry<TypeConfig>>>,
    committed: RwLock<Option<LogId<NodeId>>>,
}

impl MemLogStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            vote: RwLock::new(None),
            log: RwLock::new(BTreeMap::new()),
            committed: RwLock::new(None),
        })
    }
}

impl Default for MemLogStore {
    fn default() -> Self {
        Self {
            vote: RwLock::new(None),
            log: RwLock::new(BTreeMap::new()),
            committed: RwLock::new(None),
        }
    }
}

impl RaftLogReader<TypeConfig> for Arc<MemLogStore> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let log = self.log.read().await;
        let entries: Vec<_> = log.range(range).map(|(_, v)| v.clone()).collect();
        Ok(entries)
    }
}

impl RaftLogStorage<TypeConfig> for Arc<MemLogStore> {
    type LogReader = Self;

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let log = self.log.read().await;
        let last = log.iter().next_back().map(|(_, v)| v.log_id);

        Ok(LogState {
            last_purged_log_id: None,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(
        &mut self,
        vote: &Vote<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        *self.vote.write().await = Some(*vote);
        Ok(())
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.vote.read().await.clone())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        *self.committed.write().await = committed;
        Ok(())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: openraft::storage::LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
    {
        let mut log = self.log.write().await;
        for entry in entries {
            log.insert(entry.log_id.index, entry);
        }
        // In-memory: immediately "flushed".
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut log = self.log.write().await;
        let keys: Vec<u64> = log
            .range(log_id.index..)
            .map(|(k, _)| *k)
            .collect();
        for k in keys {
            log.remove(&k);
        }
        Ok(())
    }

    async fn purge(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut log = self.log.write().await;
        let keys: Vec<u64> = log
            .range(..=log_id.index)
            .map(|(k, _)| *k)
            .collect();
        for k in keys {
            log.remove(&k);
        }
        Ok(())
    }
}
