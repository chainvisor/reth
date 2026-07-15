use crate::{
    metrics::PersistenceMetrics,
    persistence_fence::{PersistenceCheckpointSnapshot, PersistenceFenceError},
};
use alloy_consensus::BlockHeader;
use alloy_eips::BlockNumHash;
use crossbeam_channel::Sender as CrossbeamSender;
use reth_chain_state::ExecutedBlock;
use reth_errors::ProviderError;
use reth_ethereum_primitives::EthPrimitives;
use reth_primitives_traits::{FastInstant as Instant, NodePrimitives};
use reth_provider::{
    providers::ProviderNodeTypes, BlockExecutionWriter, BlockHashReader, ChainStateBlockWriter,
    DBProvider, DatabaseProviderFactory, ProviderFactory, SaveBlocksMode,
};
use reth_prune::{PrunerError, PrunerWithFactory};
use reth_stages_api::{MetricEvent, MetricEventsSender};
use reth_tasks::spawn_os_thread;
use std::{
    sync::{
        mpsc::{Receiver, SendError, Sender},
        Arc,
    },
    thread::JoinHandle,
    time::Duration,
};
use thiserror::Error;
use tracing::{debug, error, instrument};

/// Unified result of any persistence operation.
#[derive(Debug)]
pub struct PersistenceResult {
    /// The last block that was persisted, if any.
    pub last_block: Option<BlockNumHash>,
    /// The commit duration, only available for save-blocks operations.
    pub commit_duration: Option<Duration>,
}

/// Typed completion sent back to the engine tree for every persistence action.
pub type PersistenceActionResult = Result<PersistenceResult, PersistenceError>;

/// Writes parts of reth's in memory tree state to the database and static files.
///
/// This is meant to be a spawned service that listens for various incoming persistence operations,
/// performing those actions on disk, and returning the result in a channel.
///
/// This should be spawned in its own thread with [`std::thread::spawn`], since this performs
/// blocking I/O operations in an endless loop.
#[derive(Debug)]
pub struct PersistenceService<N>
where
    N: ProviderNodeTypes,
{
    /// The provider factory to use
    provider: ProviderFactory<N>,
    /// Incoming requests
    incoming: Receiver<PersistenceAction<N::Primitives>>,
    /// The pruner
    pruner: PrunerWithFactory<ProviderFactory<N>>,
    /// metrics
    metrics: PersistenceMetrics,
    /// Sender for sync metrics - we only submit sync metrics for persisted blocks
    sync_metrics_tx: MetricEventsSender,
    /// Pending finalized block number to be committed with the next block save.
    /// This avoids triggering a separate fsync for each finalized block update.
    pending_finalized_block: Option<u64>,
    /// Pending safe block number to be committed with the next block save.
    /// This avoids triggering a separate fsync for each safe block update.
    pending_safe_block: Option<u64>,
}

impl<N> PersistenceService<N>
where
    N: ProviderNodeTypes,
{
    /// Create a new persistence service
    pub fn new(
        provider: ProviderFactory<N>,
        incoming: Receiver<PersistenceAction<N::Primitives>>,
        pruner: PrunerWithFactory<ProviderFactory<N>>,
        sync_metrics_tx: MetricEventsSender,
    ) -> Self {
        Self {
            provider,
            incoming,
            pruner,
            metrics: PersistenceMetrics::default(),
            sync_metrics_tx,
            pending_finalized_block: None,
            pending_safe_block: None,
        }
    }
}

impl<N> PersistenceService<N>
where
    N: ProviderNodeTypes,
{
    /// This is the main loop, that will listen to database events and perform the requested
    /// database actions
    pub fn run(mut self) -> Result<(), PersistenceError> {
        // If the receiver errors then senders have disconnected, so the loop should then end.
        while let Ok(action) = self.incoming.recv() {
            match action {
                PersistenceAction::RemoveBlocksAbove(new_tip_num, sender) => {
                    let result = self
                        .on_remove_blocks_above(new_tip_num)
                        .map(|last_block| PersistenceResult { last_block, commit_duration: None });
                    if result.is_ok() {
                        let _ = self
                            .sync_metrics_tx
                            .send(MetricEvent::SyncHeight { height: new_tip_num });
                    }
                    let failed = result.is_err();
                    if let Err(err) = &result {
                        error!(target: "engine::persistence", %err, "Persistence action failed; stopping service");
                    }
                    let _ = sender.send(result);
                    if failed {
                        return Ok(())
                    }
                }
                PersistenceAction::SaveBlocks(blocks, expected_last_persisted, sender) => {
                    let result = self.on_save_blocks(blocks, expected_last_persisted);
                    let result_number = result
                        .as_ref()
                        .ok()
                        .and_then(|result| result.last_block.map(|block| block.number));
                    let failed = result.is_err();
                    if let Err(err) = &result {
                        error!(target: "engine::persistence", %err, "Persistence action failed; stopping service");
                    }
                    let _ = sender.send(result);

                    if let Some(block_number) = result_number {
                        // send new sync metrics based on saved blocks
                        let _ = self
                            .sync_metrics_tx
                            .send(MetricEvent::SyncHeight { height: block_number });
                    }
                    if failed {
                        return Ok(())
                    }
                }
                PersistenceAction::SaveFinalizedBlock(finalized_block) => {
                    self.pending_finalized_block = Some(finalized_block);
                }
                PersistenceAction::SaveSafeBlock(safe_block) => {
                    self.pending_safe_block = Some(safe_block);
                }
            }
        }
        Ok(())
    }

    #[instrument(level = "debug", target = "engine::persistence", skip_all, fields(%new_tip_num))]
    fn on_remove_blocks_above(
        &self,
        new_tip_num: u64,
    ) -> Result<Option<BlockNumHash>, PersistenceError> {
        debug!(target: "engine::persistence", ?new_tip_num, "Removing blocks");
        let start_time = Instant::now();
        let provider_rw = self.provider.database_provider_rw()?;

        let new_tip_hash = provider_rw.block_hash(new_tip_num)?;
        provider_rw.remove_block_and_execution_above(new_tip_num)?;
        provider_rw.commit()?;

        debug!(target: "engine::persistence", ?new_tip_num, ?new_tip_hash, "Removed blocks from disk");
        self.metrics.remove_blocks_above_duration_seconds.record(start_time.elapsed());
        Ok(new_tip_hash.map(|hash| BlockNumHash { hash, number: new_tip_num }))
    }

    #[instrument(level = "debug", target = "engine::persistence", skip_all, fields(block_count = blocks.len()))]
    fn on_save_blocks(
        &mut self,
        blocks: Vec<ExecutedBlock<N::Primitives>>,
        expected_last_persisted: BlockNumHash,
    ) -> Result<PersistenceResult, PersistenceError> {
        let first_block = blocks
            .first()
            .map(|block| (block.recovered_block.num_hash(), block.recovered_block.parent_hash()));
        let last_block = blocks.last().map(|b| b.recovered_block.num_hash());
        let block_count = blocks.len();

        let pending_finalized = self.pending_finalized_block.take();
        let pending_safe = self.pending_safe_block.take();

        debug!(target: "engine::persistence", ?block_count, first=?first_block, last=?last_block, "Saving range of blocks");

        let start_time = Instant::now();

        if let Some(last) = last_block {
            let provider_rw = self.provider.database_provider_rw()?;
            let snapshot = PersistenceCheckpointSnapshot::read(&provider_rw)?;
            let finish = match snapshot.validate(expected_last_persisted) {
                Ok(finish) => finish,
                Err(err) => {
                    self.metrics.fence_rejections.increment(1);
                    return Err(err.into())
                }
            };
            if let Some((first, parent_hash)) = first_block &&
                let Err(err) = PersistenceCheckpointSnapshot::validate_first_block(
                    finish,
                    first,
                    parent_hash,
                )
            {
                self.metrics.fence_rejections.increment(1);
                return Err(err.into())
            }
            for pair in blocks.windows(2) {
                let previous = pair[0].recovered_block.num_hash();
                let next = pair[1].recovered_block.num_hash();
                if let Err(err) = PersistenceCheckpointSnapshot::validate_next_block(
                    previous,
                    next,
                    pair[1].recovered_block.parent_hash(),
                ) {
                    self.metrics.fence_rejections.increment(1);
                    return Err(err.into())
                }
            }
            provider_rw.save_blocks(blocks, SaveBlocksMode::Full)?;

            if let Some(finalized) = pending_finalized {
                provider_rw.save_finalized_block_number(finalized.min(last.number))?;
                if finalized > last.number {
                    self.pending_finalized_block = Some(finalized);
                }
            }
            if let Some(safe) = pending_safe {
                provider_rw.save_safe_block_number(safe.min(last.number))?;
                if safe > last.number {
                    self.pending_safe_block = Some(safe);
                }
            }

            provider_rw.commit()?;
            debug!(target: "engine::persistence", first=?first_block, last=?last_block, "Saved range of blocks");

            // Run the pruner in a separate provider so it reads committed RocksDB state
            // that includes the history entries written by save_blocks above.
            //
            // The pruner reads the indices from rocksdb, filters it, and writes to indices, so it
            // must be able to read anything written by save_blocks.
            if self.pruner.is_pruning_needed(last.number) {
                debug!(target: "engine::persistence", block_num=?last.number, "Running pruner");
                let prune_start = Instant::now();
                let provider_rw = self.provider.database_provider_rw()?;
                let _ = self.pruner.run_with_provider(&provider_rw, last.number)?;
                provider_rw.commit()?;
                debug!(target: "engine::persistence", tip=?last.number, "Finished pruning after saving blocks");
                self.metrics.prune_before_duration_seconds.record(prune_start.elapsed());
            }
        }

        let elapsed = start_time.elapsed();
        self.metrics.save_blocks_batch_size.record(block_count as f64);
        self.metrics.save_blocks_duration_seconds.record(elapsed);

        Ok(PersistenceResult { last_block, commit_duration: Some(elapsed) })
    }
}

/// One of the errors that can happen when using the persistence service.
#[derive(Debug, Error)]
pub enum PersistenceError {
    /// Engine persistence attempted to overlap a pipeline-owned range.
    #[error(transparent)]
    Fence(#[from] PersistenceFenceError),

    /// A pruner error
    #[error(transparent)]
    PrunerError(#[from] PrunerError),

    /// A provider error
    #[error(transparent)]
    ProviderError(#[from] ProviderError),
}

/// A signal to the persistence service that part of the tree state can be persisted.
#[derive(Debug)]
pub enum PersistenceAction<N: NodePrimitives = EthPrimitives> {
    /// The section of tree state that should be persisted. These blocks are expected in order of
    /// increasing block number.
    ///
    /// First, header, transaction, and receipt-related data should be written to static files.
    /// Then the execution history-related data will be written to the database.
    SaveBlocks(Vec<ExecutedBlock<N>>, BlockNumHash, CrossbeamSender<PersistenceActionResult>),

    /// Removes block data above the given block number from the database.
    ///
    /// This will first update checkpoints from the database, then remove actual block data from
    /// static files.
    RemoveBlocksAbove(u64, CrossbeamSender<PersistenceActionResult>),

    /// Update the persisted finalized block on disk
    SaveFinalizedBlock(u64),

    /// Update the persisted safe block on disk
    SaveSafeBlock(u64),
}

/// A handle to the persistence service
#[derive(Debug, Clone)]
pub struct PersistenceHandle<N: NodePrimitives = EthPrimitives> {
    /// The channel used to communicate with the persistence service
    sender: Sender<PersistenceAction<N>>,
    /// Guard that joins the service thread when all handles are dropped.
    /// Uses `Arc` so the handle remains `Clone`.
    _service_guard: Arc<ServiceGuard>,
}

impl<T: NodePrimitives> PersistenceHandle<T> {
    /// Create a new [`PersistenceHandle`] from a [`Sender<PersistenceAction>`].
    ///
    /// This is intended for testing purposes where you want to mock the persistence service.
    /// For production use, prefer [`spawn_service`](Self::spawn_service).
    pub fn new(sender: Sender<PersistenceAction<T>>) -> Self {
        Self { sender, _service_guard: Arc::new(ServiceGuard(None)) }
    }

    /// Create a new [`PersistenceHandle`], and spawn the persistence service.
    ///
    /// The returned handle can be cloned and shared. When all clones are dropped, the service
    /// thread will be joined, ensuring graceful shutdown before resources (like `RocksDB`) are
    /// released.
    pub fn spawn_service<N>(
        provider_factory: ProviderFactory<N>,
        pruner: PrunerWithFactory<ProviderFactory<N>>,
        sync_metrics_tx: MetricEventsSender,
    ) -> PersistenceHandle<N::Primitives>
    where
        N: ProviderNodeTypes,
    {
        // create the initial channels
        let (db_service_tx, db_service_rx) = std::sync::mpsc::channel();

        // spawn the persistence service
        let db_service =
            PersistenceService::new(provider_factory, db_service_rx, pruner, sync_metrics_tx);
        let join_handle = spawn_os_thread("persistence", || {
            if let Err(err) = db_service.run() {
                error!(target: "engine::persistence", ?err, "Persistence service failed");
            }
        });

        PersistenceHandle {
            sender: db_service_tx,
            _service_guard: Arc::new(ServiceGuard(Some(join_handle))),
        }
    }

    /// Sends a specific [`PersistenceAction`] in the contained channel. The caller is responsible
    /// for creating any channels for the given action.
    pub fn send_action(
        &self,
        action: PersistenceAction<T>,
    ) -> Result<(), SendError<PersistenceAction<T>>> {
        self.sender.send(action)
    }

    /// Tells the persistence service to save a certain list of finalized blocks. The blocks are
    /// assumed to be ordered by block number.
    ///
    /// This returns the latest hash that has been saved, allowing removal of that block and any
    /// previous blocks from in-memory data structures. This value is returned in the receiver end
    /// of the sender argument.
    ///
    /// If there are no blocks to persist, then `None` is sent in the sender.
    pub fn save_blocks(
        &self,
        blocks: Vec<ExecutedBlock<T>>,
        expected_last_persisted: BlockNumHash,
        tx: CrossbeamSender<PersistenceActionResult>,
    ) -> Result<(), SendError<PersistenceAction<T>>> {
        self.send_action(PersistenceAction::SaveBlocks(blocks, expected_last_persisted, tx))
    }

    /// Queues the finalized block number to be persisted on disk.
    ///
    /// The update is deferred and will be committed together with the next [`Self::save_blocks`]
    /// call to avoid triggering a separate fsync for each update.
    pub fn save_finalized_block_number(
        &self,
        finalized_block: u64,
    ) -> Result<(), SendError<PersistenceAction<T>>> {
        self.send_action(PersistenceAction::SaveFinalizedBlock(finalized_block))
    }

    /// Queues the safe block number to be persisted on disk.
    ///
    /// The update is deferred and will be committed together with the next [`Self::save_blocks`]
    /// call to avoid triggering a separate fsync for each update.
    pub fn save_safe_block_number(
        &self,
        safe_block: u64,
    ) -> Result<(), SendError<PersistenceAction<T>>> {
        self.send_action(PersistenceAction::SaveSafeBlock(safe_block))
    }

    /// Tells the persistence service to remove blocks above a certain block number. The removed
    /// blocks are returned by the service.
    ///
    /// When the operation completes, the new tip hash is returned in the receiver end of the sender
    /// argument.
    pub fn remove_blocks_above(
        &self,
        block_num: u64,
        tx: CrossbeamSender<PersistenceActionResult>,
    ) -> Result<(), SendError<PersistenceAction<T>>> {
        self.send_action(PersistenceAction::RemoveBlocksAbove(block_num, tx))
    }
}

/// Guard that joins the persistence service thread when dropped.
///
/// This ensures graceful shutdown - the service thread completes before resources like
/// `RocksDB` are released. Stored in an `Arc` inside [`PersistenceHandle`] so the handle
/// can be cloned while sharing the same guard.
struct ServiceGuard(Option<JoinHandle<()>>);

impl std::fmt::Debug for ServiceGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ServiceGuard").field(&self.0.as_ref().map(|_| "...")).finish()
    }
}

impl Drop for ServiceGuard {
    fn drop(&mut self) {
        if let Some(join_handle) = self.0.take() {
            let _ = join_handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{B256, U256};
    use reth_chain_state::test_utils::TestBlockBuilder;
    use reth_exex_types::FinishedExExHeight;
    use reth_provider::{
        providers::{ProviderFactoryBuilder, ReadOnlyConfig},
        test_utils::{create_test_provider_factory, MockNodeTypes, MockNodeTypesWithDB},
        AccountReader, BlockBodyIndicesProvider, ChainSpecProvider, HeaderProvider,
        StageCheckpointReader, StageCheckpointWriter, StorageSettingsCache,
        TryIntoHistoricalStateProvider,
    };
    use reth_prune::Pruner;
    use reth_stages_api::StageId;
    use std::ops::Range;
    use tokio::sync::mpsc::unbounded_channel;

    fn executed_chain(range: Range<u64>, mut parent_hash: B256) -> Vec<ExecutedBlock> {
        let mut builder = TestBlockBuilder::eth();
        range
            .map(|number| {
                let block = builder.get_executed_block_with_number(number, parent_hash);
                parent_hash = block.recovered_block().hash();
                block
            })
            .collect()
    }

    fn persistence_service(
        provider: ProviderFactory<MockNodeTypesWithDB>,
    ) -> PersistenceService<MockNodeTypesWithDB> {
        let (_finished_exex_height_tx, finished_exex_height_rx) =
            tokio::sync::watch::channel(FinishedExExHeight::NoExExs);
        let pruner =
            Pruner::new_with_factory(provider.clone(), vec![], 5, 0, None, finished_exex_height_rx);
        let (_action_tx, action_rx) = std::sync::mpsc::channel();
        let (sync_metrics_tx, _sync_metrics_rx) = unbounded_channel();
        PersistenceService::new(provider, action_rx, pruner, sync_metrics_tx)
    }

    fn seeded_provider(
        block_count: u64,
        persisted_count: usize,
    ) -> (ProviderFactory<MockNodeTypesWithDB>, Vec<ExecutedBlock<EthPrimitives>>) {
        let provider = create_test_provider_factory();
        let blocks = executed_chain(0..block_count, B256::ZERO);
        let provider_rw = provider.database_provider_rw().unwrap();
        provider_rw.save_blocks(blocks[..persisted_count].to_vec(), SaveBlocksMode::Full).unwrap();
        provider_rw.commit().unwrap();
        (provider, blocks)
    }

    fn default_persistence_handle() -> (PersistenceHandle<EthPrimitives>, BlockNumHash) {
        let provider = create_test_provider_factory();
        let genesis = TestBlockBuilder::eth().get_executed_block_with_number(0, B256::random());
        let genesis_num_hash = genesis.recovered_block().num_hash();
        let provider_rw = provider.database_provider_rw().unwrap();
        provider_rw.save_blocks(vec![genesis], SaveBlocksMode::Full).unwrap();
        provider_rw.commit().unwrap();

        let (_finished_exex_height_tx, finished_exex_height_rx) =
            tokio::sync::watch::channel(FinishedExExHeight::NoExExs);

        let pruner =
            Pruner::new_with_factory(provider.clone(), vec![], 5, 0, None, finished_exex_height_rx);

        let (sync_metrics_tx, _sync_metrics_rx) = unbounded_channel();
        (
            PersistenceHandle::<EthPrimitives>::spawn_service(provider, pruner, sync_metrics_tx),
            genesis_num_hash,
        )
    }

    #[test]
    fn test_save_blocks_empty() {
        reth_tracing::init_test_tracing();
        let (handle, genesis) = default_persistence_handle();

        let blocks = vec![];
        let (tx, rx) = crossbeam_channel::bounded(1);

        handle.save_blocks(blocks, genesis, tx).unwrap();

        let result = rx.recv().unwrap().unwrap();
        assert!(result.last_block.is_none());
    }

    #[test]
    fn test_save_blocks_single_block() {
        reth_tracing::init_test_tracing();
        let (handle, genesis) = default_persistence_handle();
        let block_number = 1;
        let mut test_block_builder = TestBlockBuilder::eth();
        let executed =
            test_block_builder.get_executed_block_with_number(block_number, genesis.hash);
        let block_hash = executed.recovered_block().hash();

        let blocks = vec![executed];
        let (tx, rx) = crossbeam_channel::bounded(1);

        handle.save_blocks(blocks, genesis, tx).unwrap();

        let result =
            rx.recv_timeout(std::time::Duration::from_secs(10)).expect("test timed out").unwrap();

        assert_eq!(block_hash, result.last_block.unwrap().hash);
    }

    #[test]
    fn test_save_blocks_multiple_blocks() {
        reth_tracing::init_test_tracing();
        let (handle, genesis) = default_persistence_handle();

        let blocks = executed_chain(1..6, genesis.hash);
        let last_hash = blocks.last().unwrap().recovered_block().hash();
        let (tx, rx) = crossbeam_channel::bounded(1);

        handle.save_blocks(blocks, genesis, tx).unwrap();
        let result = rx.recv().unwrap().unwrap();
        assert_eq!(last_hash, result.last_block.unwrap().hash);
    }

    #[test]
    fn test_save_blocks_multiple_calls() {
        reth_tracing::init_test_tracing();
        let (handle, genesis) = default_persistence_handle();

        let ranges = [1..2, 2..3, 3..5, 5..6];
        let blocks = executed_chain(1..6, genesis.hash);
        let mut expected_last_persisted = genesis;
        for range in ranges {
            let batch = blocks[(range.start - 1) as usize..(range.end - 1) as usize].to_vec();
            let last_hash = batch.last().unwrap().recovered_block().hash();
            let (tx, rx) = crossbeam_channel::bounded(1);

            handle.save_blocks(batch, expected_last_persisted, tx).unwrap();

            let result = rx.recv().unwrap().unwrap();
            assert_eq!(last_hash, result.last_block.unwrap().hash);
            expected_last_persisted = result.last_block.unwrap();
        }
    }

    #[test]
    fn service_fence_rejects_toctou_before_writing_any_block_data() {
        let (provider, blocks) = seeded_provider(2, 1);
        let provider_rw = provider.database_provider_rw().unwrap();
        provider_rw
            .save_stage_checkpoint(StageId::Bodies, reth_stages_api::StageCheckpoint::new(1))
            .unwrap();
        provider_rw.commit().unwrap();

        // This models the service seeing a different checkpoint frontier than the tree admitted.
        let mut service = persistence_service(provider.clone());
        let error = service
            .on_save_blocks(vec![blocks[1].clone()], blocks[0].recovered_block().num_hash())
            .expect_err("Bodies ahead of Finish must be rejected");
        assert!(matches!(
            error,
            PersistenceError::Fence(PersistenceFenceError::OwnerStageDoesNotMatchFinish {
                stage: StageId::Bodies,
                checkpoint: 1,
                finish: 0,
            })
        ));

        let provider_ro = provider.database_provider_ro().unwrap();
        assert_eq!(
            provider_ro.get_stage_checkpoint(StageId::Finish).unwrap().unwrap().block_number,
            0
        );
        assert!(provider_ro.block_body_indices(1).unwrap().is_none());
        assert!(provider_ro.sealed_header(1).unwrap().is_none());
    }

    #[test]
    fn service_fence_rejects_same_height_hash_divergence_before_writing() {
        let (provider, blocks) = seeded_provider(2, 1);
        let mut expected = blocks[0].recovered_block().num_hash();
        expected.hash = B256::random();

        let mut service = persistence_service(provider.clone());
        assert!(matches!(
            service.on_save_blocks(vec![blocks[1].clone()], expected),
            Err(PersistenceError::Fence(
                PersistenceFenceError::FinishHashDoesNotMatchLastPersisted { .. }
            ))
        ));

        let provider_ro = provider.database_provider_ro().unwrap();
        assert!(provider_ro.block_body_indices(1).unwrap().is_none());
        assert!(provider_ro.sealed_header(1).unwrap().is_none());
    }

    #[test]
    fn service_fence_rejects_overlap_gap_and_wrong_parent_without_writes() {
        for case in ["overlap", "gap", "wrong_parent"] {
            let (provider, blocks) = seeded_provider(1, 1);
            let finish = blocks[0].recovered_block().num_hash();
            let mut builder = TestBlockBuilder::eth();
            let candidate = match case {
                "overlap" => builder.get_executed_block_with_number(finish.number, finish.hash),
                "gap" => builder.get_executed_block_with_number(finish.number + 2, finish.hash),
                "wrong_parent" => {
                    builder.get_executed_block_with_number(finish.number + 1, B256::random())
                }
                _ => unreachable!(),
            };
            let candidate_number = candidate.recovered_block().number;
            let mut service = persistence_service(provider.clone());
            let error = service
                .on_save_blocks(vec![candidate], finish)
                .expect_err("unsafe range must be rejected");
            match case {
                "wrong_parent" => assert!(matches!(
                    error,
                    PersistenceError::Fence(
                        PersistenceFenceError::FirstBlockParentDoesNotMatchFinish { .. }
                    )
                )),
                _ => assert!(matches!(
                    error,
                    PersistenceError::Fence(
                        PersistenceFenceError::FirstBlockDoesNotExtendFinish { .. }
                    )
                )),
            }

            let provider_ro = provider.database_provider_ro().unwrap();
            assert_eq!(
                provider_ro.get_stage_checkpoint(StageId::Finish).unwrap().unwrap().block_number,
                finish.number
            );
            if candidate_number > finish.number {
                assert!(provider_ro.block_body_indices(candidate_number).unwrap().is_none());
                assert!(provider_ro.sealed_header(candidate_number).unwrap().is_none());
            }
        }
    }

    #[test]
    fn service_fence_rejects_second_block_overlap_gap_and_wrong_parent_without_writes() {
        for case in ["overlap", "gap", "wrong_parent"] {
            let (provider, blocks) = seeded_provider(1, 1);
            let finish = blocks[0].recovered_block().num_hash();
            let mut builder = TestBlockBuilder::eth();
            let first = builder.get_executed_block_with_number(finish.number + 1, finish.hash);
            let second = match case {
                "overlap" => builder.get_executed_block_with_number(
                    first.recovered_block().number,
                    first.recovered_block().hash(),
                ),
                "gap" => builder.get_executed_block_with_number(
                    first.recovered_block().number + 2,
                    first.recovered_block().hash(),
                ),
                "wrong_parent" => builder.get_executed_block_with_number(
                    first.recovered_block().number + 1,
                    B256::random(),
                ),
                _ => unreachable!(),
            };
            let second_number = second.recovered_block().number;
            let mut service = persistence_service(provider.clone());
            let error = service
                .on_save_blocks(vec![first, second], finish)
                .expect_err("malformed later batch block must be rejected");
            match case {
                "wrong_parent" => assert!(matches!(
                    error,
                    PersistenceError::Fence(
                        PersistenceFenceError::BatchBlockParentDoesNotMatchPrevious { .. }
                    )
                )),
                _ => assert!(matches!(
                    error,
                    PersistenceError::Fence(
                        PersistenceFenceError::BatchBlockDoesNotExtendPrevious { .. }
                    )
                )),
            }

            let provider_ro = provider.database_provider_ro().unwrap();
            assert!(provider_ro.block_body_indices(finish.number + 1).unwrap().is_none());
            assert!(provider_ro.sealed_header(finish.number + 1).unwrap().is_none());
            if second_number != finish.number + 1 {
                assert!(provider_ro.block_body_indices(second_number).unwrap().is_none());
                assert!(provider_ro.sealed_header(second_number).unwrap().is_none());
            }
        }
    }

    #[test]
    fn service_fence_accepts_era_lag_and_aligned_owner_append() {
        let (provider, blocks) = seeded_provider(3, 2);
        let provider_rw = provider.database_provider_rw().unwrap();
        provider_rw
            .save_stage_checkpoint(StageId::Era, reth_stages_api::StageCheckpoint::new(0))
            .unwrap();
        provider_rw.commit().unwrap();

        let mut service = persistence_service(provider.clone());
        let result = service
            .on_save_blocks(vec![blocks[2].clone()], blocks[1].recovered_block().num_hash())
            .expect("Era is independent and must not block Engine persistence");
        assert_eq!(result.last_block.map(|block| block.number), Some(2));

        let provider_ro = provider.database_provider_ro().unwrap();
        assert_eq!(
            provider_ro.get_stage_checkpoint(StageId::Finish).unwrap().unwrap().block_number,
            2
        );
        assert!(provider_ro.block_body_indices(2).unwrap().is_some());
    }

    #[test]
    fn service_thread_returns_typed_fence_error() {
        let (provider, blocks) = seeded_provider(2, 1);
        let provider_rw = provider.database_provider_rw().unwrap();
        provider_rw
            .save_stage_checkpoint(StageId::Bodies, reth_stages_api::StageCheckpoint::new(1))
            .unwrap();
        provider_rw.commit().unwrap();

        let (_finished_exex_height_tx, finished_exex_height_rx) =
            tokio::sync::watch::channel(FinishedExExHeight::NoExExs);
        let pruner =
            Pruner::new_with_factory(provider.clone(), vec![], 5, 0, None, finished_exex_height_rx);
        let (sync_metrics_tx, _sync_metrics_rx) = unbounded_channel();
        let handle =
            PersistenceHandle::<EthPrimitives>::spawn_service(provider, pruner, sync_metrics_tx);
        let (tx, rx) = crossbeam_channel::bounded(1);
        handle
            .save_blocks(vec![blocks[1].clone()], blocks[0].recovered_block().num_hash(), tx)
            .unwrap();

        assert!(matches!(
            rx.recv_timeout(Duration::from_secs(10)).expect("typed result timed out"),
            Err(PersistenceError::Fence(PersistenceFenceError::OwnerStageDoesNotMatchFinish {
                stage: StageId::Bodies,
                ..
            }))
        ));
    }

    /// Verifies that committing `save_blocks` history before running the pruner
    /// prevents the pruner from overwriting new entries.
    ///
    /// Previously, both `save_blocks` and the pruner pushed `RocksDB` batches before
    /// a single commit. Both read committed state, so the pruner didn't see the
    /// new entries and its batch overwrote them. The fix commits `save_blocks`
    /// first, then runs the pruner against committed state in a separate provider.
    #[test]
    fn test_save_blocks_then_prune_preserves_new_history() {
        use reth_db::{models::ShardedKey, tables, BlockNumberList};
        use reth_provider::RocksDBProviderFactory;

        reth_tracing::init_test_tracing();

        let provider_factory = create_test_provider_factory();
        let tracked_addr = alloy_primitives::Address::from([0xBE; 20]);

        // Phase 1: Establish baseline history for blocks 0..20.
        let rocksdb = provider_factory.rocksdb_provider();
        {
            let mut batch = rocksdb.batch();
            let initial_blocks: Vec<u64> = (0..20).collect();
            let shard = BlockNumberList::new_pre_sorted(initial_blocks.iter().copied());
            batch
                .put::<tables::AccountsHistory>(ShardedKey::new(tracked_addr, u64::MAX), &shard)
                .unwrap();
            batch.commit().unwrap();
        }

        // Phase 2: Simulate the fixed on_save_blocks flow.
        // Step 1: save_blocks appends new entries 20..25 and commits immediately.
        let mut batch1 = rocksdb.batch();
        batch1.append_account_history_shard(tracked_addr, 20..25u64).unwrap();
        batch1.commit().unwrap();

        // Step 2: Pruner runs AFTER commit, so it reads state that includes 20..25.
        // Prunes entries ≤ 14, leaving [15..25).
        let mut batch2 = rocksdb.batch();
        batch2.prune_account_history_to(tracked_addr, 14).unwrap();
        batch2.commit().unwrap();

        // Verify new entries survived pruning.
        let shards = rocksdb.account_history_shards(tracked_addr).unwrap();
        let entries: Vec<u64> = shards.iter().flat_map(|(_, list)| list.iter()).collect();
        let expected: Vec<u64> = (15..25).collect();
        assert_eq!(entries, expected, "new entries 20..25 must survive pruning");
    }

    #[test]
    fn test_read_only_consistency_across_reorg() {
        reth_tracing::init_test_tracing();

        // Allow opening the same MDBX env twice in-process
        reth_db::test_utils::enable_legacy_multiopen();

        let provider_factory = create_test_provider_factory();
        provider_factory.set_storage_settings_cache(reth_provider::StorageSettings::v2());

        // Open the secondary provider concurrently with the primary.
        let secondary = ProviderFactoryBuilder::<MockNodeTypes>::default()
            .open_read_only(
                provider_factory.chain_spec(),
                ReadOnlyConfig::from_datadir(provider_factory.db_ref().path()),
                reth_tasks::Runtime::test(),
            )
            .expect("failed to open read-only provider factory");
        secondary.set_storage_settings_cache(reth_provider::StorageSettings::v2());

        // --- Phase 1: Write blocks 0..3 via the primary ---
        let mut test_block_builder = TestBlockBuilder::eth().with_state();
        let signer = test_block_builder.signer;
        let blocks_a: Vec<_> = test_block_builder.get_executed_blocks(0..3).collect();
        let hash_a1 = blocks_a[1].recovered_block().hash();
        let hash_a2 = blocks_a[2].recovered_block().hash();

        // Compute expected signer state after each block from tx counts.
        let single_cost = TestBlockBuilder::<EthPrimitives>::single_tx_cost();
        let initial_balance = U256::from(10).pow(U256::from(18));
        let txs_in_block0 = blocks_a[0].recovered_block().body().transactions.len() as u64;
        let txs_in_block1 = blocks_a[1].recovered_block().body().transactions.len() as u64;

        let balance_after_block0 = initial_balance - single_cost * U256::from(txs_in_block0);
        let nonce_after_block0 = txs_in_block0;
        let balance_after_block1 = balance_after_block0 - single_cost * U256::from(txs_in_block1);
        let nonce_after_block1 = nonce_after_block0 + txs_in_block1;

        {
            let provider_rw = provider_factory.database_provider_rw().unwrap();
            provider_rw.save_blocks(blocks_a, SaveBlocksMode::Full).unwrap();
            provider_rw.commit().unwrap();
        }

        // Secondary catches up and sees all 3 blocks.
        // Hold this provider (and its MDBX RO tx) across the reorg to test snapshot isolation.
        let pre_reorg_provider = secondary.provider().unwrap();
        assert_eq!(
            pre_reorg_provider.sealed_header(2).unwrap().as_ref().map(|h| h.hash()),
            Some(hash_a2),
            "secondary must see block 2 after initial append"
        );

        // Check the primary can read its own historical state.
        {
            let primary_state_at_1 = provider_factory.history_by_block_number(1).unwrap();
            let primary_account = primary_state_at_1.basic_account(&signer).unwrap();
            assert!(primary_account.is_some(), "primary: signer must exist at block 1");
        }

        // Verify historical state at block 1 is accessible via changesets on the secondary.
        {
            let state_at_1 = secondary.history_by_block_number(1).unwrap();
            let account_at_1 = state_at_1.basic_account(&signer).unwrap();
            assert!(account_at_1.is_some(), "signer account must exist at block 1");
            let account_at_1 = account_at_1.unwrap();
            assert_eq!(account_at_1.balance, balance_after_block1, "signer balance at block 1");
            assert_eq!(account_at_1.nonce, nonce_after_block1, "signer nonce at block 1");
        }

        // --- Phase 2: Reorg — remove block 2 and append a different block 2 ---
        // Build the reorg block before starting the commit so we can write it in the
        // same thread after the unwind.
        let block_b2 = test_block_builder.get_executed_block_with_number(2, hash_a1);
        let hash_b2 = block_b2.recovered_block().hash();
        let txs_in_block_b2 = block_b2.recovered_block().body().transactions.len() as u64;
        assert_ne!(hash_a2, hash_b2, "reorg block must differ");

        // Expected signer state after the reorged block 2.
        let balance_after_reorg_block2 =
            balance_after_block1 - single_cost * U256::from(txs_in_block_b2);
        let nonce_after_reorg_block2 = nonce_after_block1 + txs_in_block_b2;

        // Spawn the reorg on a background thread because `commit_unwind` calls
        // `wait_for_pre_commit_readers()` which blocks until the secondary's held
        // RO tx is dropped.
        //
        // We want to keep provider factory around, otherwise it's gonna drop mdbx env before the
        // reorg thread is on
        #[expect(clippy::redundant_clone)]
        let pf = provider_factory.clone();
        let reorg_handle = std::thread::spawn(move || {
            let provider_rw = pf.database_provider_rw().unwrap();
            provider_rw.remove_block_and_execution_above(1).unwrap();
            provider_rw.commit().unwrap();

            let provider_rw = pf.database_provider_rw().unwrap();
            provider_rw.save_blocks(vec![block_b2], SaveBlocksMode::Full).unwrap();
            provider_rw.commit().unwrap();
        });

        // Give the reorg thread time to start and block on wait_for_pre_commit_readers.
        std::thread::sleep(std::time::Duration::from_millis(100));

        // The pre-reorg provider still holds its MDBX snapshot — it must still see
        // the OLD block 2 from before the reorg.
        assert_eq!(
            pre_reorg_provider.sealed_header(2).unwrap().as_ref().map(|h| h.hash()),
            Some(hash_a2),
            "pre-reorg provider must still see the original block 2"
        );
        assert_eq!(
            pre_reorg_provider.sealed_header(1).unwrap().as_ref().map(|h| h.hash()),
            Some(hash_a1),
            "pre-reorg provider must still see block 1"
        );

        // The held RO tx must still be able to read historical state at block 1 via
        // changesets, even though the reorg thread is about to rewrite block 2's data.
        // Consuming pre_reorg_provider here also unblocks the reorg commit.
        let state_at_1 = pre_reorg_provider.try_into_history_at_block(1).unwrap();
        let account = state_at_1.basic_account(&signer).unwrap();
        assert!(
            account.is_some(),
            "pre-reorg RO tx must still read signer at block 1 during reorg"
        );
        let account = account.unwrap();
        assert_eq!(
            account.balance, balance_after_block1,
            "pre-reorg RO tx: signer balance at block 1 during reorg"
        );
        assert_eq!(
            account.nonce, nonce_after_block1,
            "pre-reorg RO tx: signer nonce at block 1 during reorg"
        );
        drop(state_at_1);
        reorg_handle.join().expect("reorg thread panicked");

        // A new provider catches up and sees the reorged chain.
        let obs_header = secondary.provider().unwrap().sealed_header(2).unwrap();
        assert_eq!(
            obs_header.as_ref().map(|h| h.hash()),
            Some(hash_b2),
            "secondary must see the reorged block 2, not the old one"
        );

        // Block 1 should still be the original.
        let obs_header = secondary.provider().unwrap().sealed_header(1).unwrap();
        assert_eq!(
            obs_header.as_ref().map(|h| h.hash()),
            Some(hash_a1),
            "secondary must still see block 1"
        );

        // Verify historical state at block 1 is still accessible after the reorg.
        let state_at_1 = secondary.history_by_block_number(1).unwrap();
        let account_at_1 = state_at_1.basic_account(&signer).unwrap();
        assert!(account_at_1.is_some(), "signer account must exist at block 1 after reorg");
        let account_at_1 = account_at_1.unwrap();
        assert_eq!(
            account_at_1.balance, balance_after_block1,
            "signer balance at block 1 must survive reorg"
        );
        assert_eq!(
            account_at_1.nonce, nonce_after_block1,
            "signer nonce at block 1 must survive reorg"
        );

        // Verify the latest state (at block 2) reflects the reorged execution.
        let state_at_2 = secondary.history_by_block_number(2).unwrap();
        let account_at_2 = state_at_2.basic_account(&signer).unwrap();
        assert!(account_at_2.is_some(), "signer account must exist at block 2 after reorg");
        let account_at_2 = account_at_2.unwrap();
        assert_eq!(
            account_at_2.balance, balance_after_reorg_block2,
            "signer balance at block 2 must reflect reorged execution"
        );
        assert_eq!(
            account_at_2.nonce, nonce_after_reorg_block2,
            "signer nonce at block 2 must reflect reorged execution"
        );
    }
}
