//! Engine node related functionality.

use crate::{
    common::{Attached, LaunchContextWith, WithConfigs},
    hooks::NodeHooks,
    rpc::{EngineShutdown, EngineValidatorAddOn, EngineValidatorBuilder, RethRpcAddOns, RpcHandle},
    setup::build_networked_pipeline,
    AddOns, AddOnsContext, FullNode, LaunchContext, LaunchNode, NodeAdapter,
    NodeBuilderWithComponents, NodeComponents, NodeComponentsBuilder, NodeHandle, NodeTypesAdapter,
};
use alloy_consensus::BlockHeader;
use alloy_primitives::B256;
use futures::{stream::FusedStream, stream_select, FutureExt, StreamExt};
use reth_chainspec::{EthChainSpec, EthereumHardforks};
use reth_engine_tree::{
    chain::{ChainEvent, FromOrchestrator},
    engine::{EngineApiKind, EngineApiRequest, EngineRequestHandler},
    launch::build_engine_orchestrator,
    tree::{error::AdvancePersistenceError, TreeConfig},
};
use reth_engine_util::EngineMessageStreamExt;
use reth_exex::ExExManagerHandle;
use reth_network::{types::BlockRangeUpdate, NetworkSyncUpdater, SyncState};
use reth_network_api::BlockDownloaderProvider;
use reth_node_api::{
    BuiltPayload, ConsensusEngineHandle, FullNodeTypes, NodeTypes, NodeTypesWithDBAdapter,
};
use reth_node_core::{
    dirs::{ChainPath, DataDirPath},
    exit::NodeExitFuture,
    primitives::Head,
};
use reth_node_events::node;
use reth_provider::{
    providers::{BlockchainProvider, NodeTypesForProvider},
    BlockNumReader, HeaderProvider, RocksDBProviderFactory, StorageSettingsCache,
};
use reth_tasks::TaskExecutor;
use reth_tokio_util::EventSender;
use reth_tracing::tracing::{debug, error, info};
use reth_trie_db::ChangesetCache;
use std::{future::Future, pin::Pin, sync::Arc};
use tokio::sync::{mpsc::unbounded_channel, oneshot};
use tokio_stream::wrappers::UnboundedReceiverStream;

fn engine_shutdown_outcome_to_exit(
    outcome: &Result<(), AdvancePersistenceError>,
) -> eyre::Result<()> {
    match outcome {
        Ok(()) => Ok(()),
        Err(err) => Err(eyre::eyre!("Engine shutdown persistence failed: {err}")),
    }
}

fn ensure_initial_backfill_allowed(
    tree_config: &TreeConfig,
    initial_target: Option<B256>,
) -> eyre::Result<()> {
    if tree_config.pipeline_backfill_disabled() &&
        let Some(target) = initial_target
    {
        return Err(eyre::eyre!(
            "database requires initial pipeline convergence to {target}, but pipeline backfill is disabled"
        ))
    }
    Ok(())
}

/// The engine node launcher.
#[derive(Debug)]
pub struct EngineNodeLauncher {
    /// The task executor for the node.
    pub ctx: LaunchContext,

    /// Temporary configuration for engine tree.
    /// After engine is stabilized, this should be configured through node builder.
    pub engine_tree_config: TreeConfig,
}

impl EngineNodeLauncher {
    /// Create a new instance of the ethereum node launcher.
    pub const fn new(
        task_executor: TaskExecutor,
        data_dir: ChainPath<DataDirPath>,
        engine_tree_config: TreeConfig,
    ) -> Self {
        Self { ctx: LaunchContext::new(task_executor, data_dir), engine_tree_config }
    }

    async fn launch_node<T, CB, AO>(
        self,
        target: NodeBuilderWithComponents<T, CB, AO>,
    ) -> eyre::Result<NodeHandle<NodeAdapter<T, CB::Components>, AO>>
    where
        T: FullNodeTypes<
            Types: NodeTypesForProvider,
            Provider = BlockchainProvider<
                NodeTypesWithDBAdapter<<T as FullNodeTypes>::Types, <T as FullNodeTypes>::DB>,
            >,
        >,
        CB: NodeComponentsBuilder<T>,
        AO: RethRpcAddOns<NodeAdapter<T, CB::Components>>
            + EngineValidatorAddOn<NodeAdapter<T, CB::Components>>,
    {
        let Self { ctx, engine_tree_config } = self;
        let NodeBuilderWithComponents {
            adapter: NodeTypesAdapter { database },
            rocksdb_provider,
            components_builder,
            add_ons: AddOns { hooks, exexs: installed_exex, add_ons },
            config,
        } = target;
        let NodeHooks { on_component_initialized, on_node_started, .. } = hooks;

        // Create changeset cache that will be shared across the engine
        let changeset_cache = ChangesetCache::new();

        // setup the launch context
        let ctx = ctx
            .with_configured_globals(engine_tree_config.reserved_cpu_cores())
            // load the toml config
            .with_loaded_toml_config(config)?
            // add resolved peers
            .with_resolved_peers()?
            // attach the database
            .attach(database.clone())
            // ensure certain settings take effect
            .with_adjusted_configs()
            // Create the provider factory with changeset cache
            .with_provider_factory::<_, <CB::Components as NodeComponents<T>>::Evm>(changeset_cache.clone(), rocksdb_provider).await?
            .inspect(|_| {
                info!(target: "reth::cli", "Database opened");
            })
            .with_prometheus_server().await?
            .inspect(|this| {
                debug!(target: "reth::cli", chain=%this.chain_id(), genesis=?this.genesis_hash(), "Initializing genesis");
            })
            .with_genesis()?
            .inspect(|this: &LaunchContextWith<Attached<WithConfigs<<T::Types as NodeTypes>::ChainSpec>, _>>| {
                info!(target: "reth::cli", "\n{}", this.chain_spec().display_hardforks());
                let settings = this.provider_factory().cached_storage_settings();
                info!(target: "reth::cli", ?settings, "Loaded storage settings");
            })
            .with_metrics_task()
            // passing FullNodeTypes as type parameter here so that we can build
            // later the components.
            .with_blockchain_db::<T, _>(move |provider_factory| {
                Ok(BlockchainProvider::new(provider_factory)?)
            })?
            .with_components(components_builder, on_component_initialized).await?;

        // spawn exexs if any
        let maybe_exex_manager_handle = ctx.launch_exex(installed_exex).await?;

        // create pipeline
        let network_handle = ctx.components().network().clone();
        let network_client = network_handle.fetch_client().await?;
        let (consensus_engine_tx, consensus_engine_rx) = unbounded_channel();

        let node_config = ctx.node_config();

        // We always assume that node is syncing after a restart
        network_handle.update_sync_state(SyncState::Syncing);

        let max_block = ctx.max_block(network_client.clone()).await?;

        let static_file_producer = ctx.static_file_producer();
        let static_file_producer_events = static_file_producer.lock().events();
        info!(target: "reth::cli", "StaticFileProducer initialized");

        let consensus = Arc::new(ctx.components().consensus().clone());

        let pipeline = build_networked_pipeline(
            &ctx.toml_config().stages,
            network_client.clone(),
            consensus.clone(),
            ctx.provider_factory().clone(),
            ctx.task_executor(),
            ctx.sync_metrics_tx(),
            ctx.prune_config(),
            max_block,
            static_file_producer,
            ctx.components().evm_config().clone(),
            maybe_exex_manager_handle.clone().unwrap_or_else(ExExManagerHandle::empty),
            ctx.era_import_source(),
        )?;

        // The new engine writes directly to static files. This ensures that they're up to the tip.
        pipeline.move_to_static_files()?;

        let pipeline_events = pipeline.events();

        let mut pruner_builder = ctx.pruner_builder();
        if let Some(exex_manager_handle) = &maybe_exex_manager_handle {
            pruner_builder =
                pruner_builder.finished_exex_height(exex_manager_handle.finished_height());
        }
        let pruner = pruner_builder.build_with_provider_factory(ctx.provider_factory().clone());
        let pruner_events = pruner.events();
        info!(target: "reth::cli", prune_config=?ctx.prune_config(), "Pruner initialized");

        let event_sender = EventSender::default();

        let beacon_engine_handle = ConsensusEngineHandle::new(consensus_engine_tx.clone());

        // extract the jwt secret from the args if possible
        let jwt_secret = ctx.auth_jwt_secret()?;

        let add_ons_ctx = AddOnsContext {
            node: ctx.node_adapter().clone(),
            config: ctx.node_config(),
            beacon_engine_handle: beacon_engine_handle.clone(),
            jwt_secret,
            engine_events: event_sender.clone(),
        };
        let validator_builder = add_ons.engine_validator_builder();

        // Build the engine validator with all required components
        let engine_validator = validator_builder
            .clone()
            .build_tree_validator(&add_ons_ctx, engine_tree_config.clone(), changeset_cache.clone())
            .await?;

        // Create the consensus engine stream with optional reorg
        let consensus_engine_stream = UnboundedReceiverStream::from(consensus_engine_rx)
            .maybe_skip_fcu(node_config.debug.skip_fcu)
            .maybe_skip_new_payload(node_config.debug.skip_new_payload)
            .maybe_reorg(
                ctx.blockchain_db().clone(),
                ctx.components().evm_config().clone(),
                || async {
                    validator_builder
                        .build_tree_validator(
                            &add_ons_ctx,
                            engine_tree_config.clone(),
                            changeset_cache.clone(),
                        )
                        .await
                },
                node_config.debug.reorg_frequency,
                node_config.debug.reorg_depth,
            )
            .await?
            // Store messages _after_ skipping so that `replay-engine` command
            // would replay only the messages that were observed by the engine
            // during this run.
            .maybe_store_messages(node_config.debug.engine_api_store.clone());

        let engine_kind = if ctx.chain_spec().is_optimism() {
            EngineApiKind::OpStack
        } else {
            EngineApiKind::Ethereum
        };

        // Determine startup ownership before the tree thread and RPC surface are exposed. A known
        // target starts the tree in Pending; disabled backfill must fail launch rather than serve
        // an unconverged database.
        let initial_target = ctx.initial_backfill_target()?;
        ensure_initial_backfill_allowed(&engine_tree_config, initial_target)?;
        let initial_pipeline_target = initial_target.map(Into::into);

        let mut orchestrator = build_engine_orchestrator(
            engine_kind,
            consensus.clone(),
            network_client.clone(),
            Box::pin(consensus_engine_stream),
            pipeline,
            ctx.task_executor().clone(),
            ctx.provider_factory().clone(),
            ctx.blockchain_db().clone(),
            pruner,
            ctx.components().payload_builder_handle().clone(),
            engine_validator,
            initial_pipeline_target,
            engine_tree_config,
            ctx.sync_metrics_tx(),
            ctx.components().evm_config().clone(),
            changeset_cache,
            ctx.task_executor().clone(),
        )?;

        if let Some(initial_target) = initial_target {
            debug!(target: "reth::cli", %initial_target, "establish initial backfill ownership");
            orchestrator.start_backfill_sync(initial_target).map_err(|err| {
                eyre::eyre!(
                    "failed to establish initial pipeline ownership before RPC startup: {err}"
                )
            })?;
        }

        info!(target: "reth::cli", "Consensus engine initialized");

        #[expect(clippy::needless_continue)]
        let events = stream_select!(
            event_sender.new_listener().map(Into::into),
            pipeline_events.map(Into::into),
            ctx.consensus_layer_events(),
            pruner_events.map(Into::into),
            static_file_producer_events.map(Into::into),
        );

        ctx.task_executor().spawn_critical_task(
            "events task",
            node::handle_events(
                Some(Box::new(ctx.components().network().clone())),
                Some(ctx.head().number),
                events,
            ),
        );

        let RpcHandle {
            rpc_server_handles,
            rpc_registry,
            engine_events,
            beacon_engine_handle,
            engine_shutdown: _,
        } = add_ons.launch_add_ons(add_ons_ctx).await?;

        // Create engine shutdown handle
        let (engine_shutdown, shutdown_rx) = EngineShutdown::new();

        // Run consensus engine to completion
        let mut built_payloads = ctx
            .components()
            .payload_builder_handle()
            .subscribe()
            .await
            .map_err(|e| eyre::eyre!("Failed to subscribe to payload builder events: {:?}", e))?
            .into_built_payload_stream()
            .fuse();

        let chainspec = ctx.chain_spec();
        let provider = ctx.blockchain_db().clone();

        // chainvisor trusting-reader (pure-adopt): the engine never executes
        // payloads; instead this poller ADOPTS the writer's committed on-disk
        // head. Every 500ms it reads the on-disk tip block number
        // (`last_block_number()` = the DB's last block, NOT the in-memory head),
        // fetches that block's `SealedHeader`, and sets it as the canonical head.
        // "latest" state then auto-falls through to the on-disk
        // `LatestStateProvider`. Robust + best-effort: any error just retries on
        // the next tick. Default off; zero effect unless --engine.reader-trusting.
        if ctx.node_config().engine.reader_trusting {
            let poll_provider = provider.clone();
            ctx.task_executor().spawn_critical_task("trusting-reader head poller", async move {
                info!(target: "reth::cli", "trusting-reader: head-pointer poller started");
                let mut last_adopted: u64 = 0;
                loop {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    // On-disk tip the writer has committed (database last block).
                    let tip = match poll_provider.last_block_number() {
                        Ok(n) => n,
                        Err(_) => continue,
                    };
                    // Monotonic head guard. The writer's committed tip is
                    // FINAL (committed blocks don't roll back), so a `tip`
                    // far BELOW `last_adopted` is a transient static-file
                    // re-scan inconsistency (the force_refresh poll racing
                    // the chainvisor base-advance), NOT a real regression —
                    // adopting it would serve a stale (e.g. ~10h-old) head.
                    // Skip an unchanged tip and any drop deeper than a
                    // shallow reorg; still follow small (<= 64) reorgs.
                    const REORG_TOLERANCE: u64 = 64;
                    if tip == last_adopted ||
                        (tip < last_adopted && last_adopted - tip > REORG_TOLERANCE)
                    {
                        continue;
                    }
                    match poll_provider.sealed_header(tip) {
                        Ok(Some(header)) => {
                            poll_provider.canonical_in_memory_state().set_canonical_head(header);
                            last_adopted = tip;
                            debug!(
                                target: "reth::cli",
                                number = tip,
                                "trusting-reader: adopted writer-committed head"
                            );
                        }
                        // Not on-disk yet / read error: retry next tick.
                        Ok(None) | Err(_) => continue,
                    }
                }
            });
        }

        // chainvisor writer snapshot quiesce (FlushInPlace): SIGUSR1 first takes
        // the provider factory's cross-store writer barrier, so the snapshot can
        // only land BETWEEN complete static-file/RocksDB/MDBX transactions. It
        // then drains RocksDB memtables, touches CV_RETH_FLUSH_MARKER_PATH, and
        // HOLDS the barrier until the parent sends SIGUSR2 after FIFREEZE +
        // `lvcreate -s` + FITHAW. This is an application-atomic checkpoint, not
        // merely a crash-recoverable cut through the provider's ordered commit.
        // Default off and never armed on a reader.
        if std::env::var("CV_RETH_FLUSH_ON_SIGUSR1").as_deref() == Ok("1") {
            let marker_path = std::env::var("CV_RETH_FLUSH_MARKER_PATH").map_err(|_| {
                eyre::eyre!(
                    "CV_RETH_FLUSH_ON_SIGUSR1=1 requires CV_RETH_FLUSH_MARKER_PATH"
                )
            })?;
            let mut sigusr1 = tokio::signal::unix::signal(
                tokio::signal::unix::SignalKind::user_defined1(),
            )
            .map_err(|error| eyre::eyre!("install snapshot SIGUSR1 handler: {error}"))?;
            let mut sigusr2 = tokio::signal::unix::signal(
                tokio::signal::unix::SignalKind::user_defined2(),
            )
            .map_err(|error| eyre::eyre!("install snapshot SIGUSR2 handler: {error}"))?;
            let flush_provider = provider.clone();
            let snapshot_barrier = provider.cross_store_snapshot_barrier();
            snapshot_barrier.forbid_raw_transaction_escape();
            ctx.task_executor().spawn_critical_task(
                "cross-store snapshot signal handler",
                async move {
                    info!(target: "reth::cli", marker = %marker_path, "cross-store snapshot SIGUSR1/SIGUSR2 handler armed");
                    while sigusr1.recv().await.is_some() {
                        let started = std::time::Instant::now();
                        let barrier = snapshot_barrier.clone();
                        let snapshot_guard = match tokio::task::spawn_blocking(move || {
                            barrier.quiesce()
                        })
                        .await
                        {
                            Ok(guard) => guard,
                            Err(error) => {
                                error!(target: "reth::cli", %error, "cross-store snapshot barrier task failed");
                                return
                            }
                        };

                        match flush_provider.rocksdb_provider().flush_all_for_snapshot() {
                            Ok(()) => match std::fs::write(&marker_path, b"") {
                                Ok(()) => info!(
                                    target: "reth::cli",
                                    elapsed_ms = started.elapsed().as_millis() as u64,
                                    "cross-store snapshot barrier acquired and RocksDB flushed"
                                ),
                                Err(error) => error!(target: "reth::cli", %error, "cross-store snapshot marker write failed"),
                            },
                            Err(error) => error!(target: "reth::cli", %error, "cross-store snapshot RocksDB flush failed"),
                        }

                        // Pair every SIGUSR1 with exactly one SIGUSR2 even when
                        // flush/marker creation failed. The parent sends SIGUSR2
                        // on timeout and cancellation too, so no stale resume can
                        // be consumed by a later snapshot cycle.
                        if sigusr2.recv().await.is_none() {
                            error!(target: "reth::cli", "cross-store snapshot SIGUSR2 stream closed while barrier held");
                            drop(snapshot_guard);
                            return
                        }
                        drop(snapshot_guard);
                        info!(
                            target: "reth::cli",
                            held_ms = started.elapsed().as_millis() as u64,
                            "cross-store snapshot barrier released after SIGUSR2"
                        );
                    }
                    error!(target: "reth::cli", "cross-store snapshot SIGUSR1 stream closed");
                }
            );
        }

        let (exit, rx) = oneshot::channel();
        let terminate_after_backfill = ctx.terminate_after_initial_backfill();
        let startup_sync_state_idle = ctx.node_config().debug.startup_sync_state_idle;
        let initial_backfill_started = initial_target.is_some();

        info!(target: "reth::cli", "Starting consensus engine");
        let consensus_engine = move |mut on_graceful_shutdown| async move {
            if !initial_backfill_started && startup_sync_state_idle {
                network_handle.update_sync_state(SyncState::Idle);
            }

            let mut res = Ok(());
            let mut shutdown_rx = shutdown_rx.fuse();

            // advance the chain and await payloads built locally to add into the engine api
            // tree handler to prevent re-execution if that block is received as payload from
            // the CL
            loop {
                tokio::select! {
                    event = orchestrator.next() => {
                        let Some(event) = event else { break };
                        debug!(target: "reth::cli", "Event: {event}");
                        match event {
                            ChainEvent::BackfillSyncFinished => {
                                if terminate_after_backfill {
                                    debug!(target: "reth::cli", "Terminating after initial backfill");
                                    break
                                }
                                if startup_sync_state_idle {
                                    network_handle.update_sync_state(SyncState::Idle);
                                }
                            }
                            ChainEvent::BackfillSyncStarted => {
                                network_handle.update_sync_state(SyncState::Syncing);
                            }
                            ChainEvent::FatalError => {
                                error!(target: "reth::cli", "Fatal error in consensus engine");
                                res = Err(eyre::eyre!("Fatal error in consensus engine"));
                                break
                            }
                            ChainEvent::Handler(ev) => {
                                if let Some(head) = ev.canonical_header() {
                                    // Once we're progressing via live sync, we can consider the node is not syncing anymore
                                    network_handle.update_sync_state(SyncState::Idle);
                                    let head_block = Head {
                                        number: head.number(),
                                        hash: head.hash(),
                                        difficulty: head.difficulty(),
                                        timestamp: head.timestamp(),
                                        total_difficulty: chainspec.final_paris_total_difficulty()
                                            .filter(|_| chainspec.is_paris_active_at_block(head.number()))
                                            .unwrap_or_default(),
                                    };
                                    network_handle.update_status(head_block);

                                    let updated = BlockRangeUpdate {
                                        earliest: provider.earliest_block_number().unwrap_or_default(),
                                        latest: head.number(),
                                        latest_hash: head.hash(),
                                    };
                                    network_handle.update_block_range(updated);
                                }
                                event_sender.notify(ev);
                            }
                        }
                    }
                    payload = built_payloads.select_next_some(), if !built_payloads.is_terminated() => {
                        if let Some(executed_block) = payload.executed_block() {
                            debug!(target: "reth::cli", block=?executed_block.recovered_block.num_hash(),  "inserting built payload");
                            orchestrator.handler_mut().handler_mut().on_event(EngineApiRequest::InsertExecutedBlock(executed_block.into_executed_payload()).into());
                        }
                    }
                    shutdown_req = &mut shutdown_rx => {
                        if let Ok(req) = shutdown_req {
                            debug!(target: "reth::cli", "received engine shutdown request");
                            let (done_tx, done_rx) = oneshot::channel();
                            orchestrator.handler_mut().handler_mut().on_event(
                                FromOrchestrator::Terminate { tx: done_tx }.into()
                            );
                            let outcome = done_rx
                                .await
                                .unwrap_or(Err(AdvancePersistenceError::ChannelClosed));
                            let exit_result = engine_shutdown_outcome_to_exit(&outcome);
                            if req.done_tx.send(outcome).is_err() {
                                error!(target: "reth::cli", "Engine shutdown caller dropped before receiving persistence outcome");
                            }
                            if let Err(err) = exit_result {
                                error!(target: "reth::cli", %err, "Engine shutdown failed");
                                res = Err(err);
                            }
                            break;
                        }
                    }
                    _guard = &mut on_graceful_shutdown => {
                        // Shutdown signal received.
                        // Send Terminate so the engine OS thread can exit cleanly before we
                        // drop the orchestrator.
                        debug!(target: "reth::cli", "shutdown signal received, terminating engine");
                        let (done_tx, done_rx) = oneshot::channel();
                        orchestrator.handler_mut().handler_mut().on_event(
                            FromOrchestrator::Terminate { tx: done_tx }.into()
                        );
                        let outcome = done_rx
                            .await
                            .unwrap_or(Err(AdvancePersistenceError::ChannelClosed));
                        if let Err(err) = engine_shutdown_outcome_to_exit(&outcome) {
                            error!(target: "reth::cli", %err, "Engine shutdown failed");
                            res = Err(err);
                        }
                        break;
                    }
                }
            }

            let _ = exit.send(res);
        };
        ctx.task_executor()
            .spawn_critical_with_graceful_shutdown_signal("consensus engine", consensus_engine);

        let engine_events_for_ethstats = engine_events.new_listener();

        let full_node = FullNode {
            evm_config: ctx.components().evm_config().clone(),
            pool: ctx.components().pool().clone(),
            network: ctx.components().network().clone(),
            provider: ctx.node_adapter().provider.clone(),
            payload_builder_handle: ctx.components().payload_builder_handle().clone(),
            task_executor: ctx.task_executor().clone(),
            config: ctx.node_config().clone(),
            data_dir: ctx.data_dir().clone(),
            add_ons_handle: RpcHandle {
                rpc_server_handles,
                rpc_registry,
                engine_events,
                beacon_engine_handle,
                engine_shutdown,
            },
        };
        // Notify on node started
        on_node_started.on_event(FullNode::clone(&full_node))?;

        ctx.spawn_ethstats(engine_events_for_ethstats).await?;

        let handle = NodeHandle {
            node_exit_future: NodeExitFuture::new(async { rx.await? }),
            node: full_node,
        };

        Ok(handle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_shutdown_failure_maps_to_node_exit_error() {
        let outcome = Err(AdvancePersistenceError::UnsafeShutdownPipelineBusy);
        let error = engine_shutdown_outcome_to_exit(&outcome)
            .expect_err("persistence failure must fail node exit");
        assert!(error
            .to_string()
            .contains("refusing graceful persistence flush while pipeline backfill"));
    }

    #[test]
    fn engine_shutdown_success_maps_to_clean_node_exit() {
        assert!(engine_shutdown_outcome_to_exit(&Ok(())).is_ok());
    }

    #[test]
    fn disabled_pipeline_rejects_initial_target_before_startup() {
        let disabled = TreeConfig::default().with_min_blocks_for_pipeline_run(u64::MAX);
        let target = B256::repeat_byte(0x44);
        let error = ensure_initial_backfill_allowed(&disabled, Some(target))
            .expect_err("disabled pipeline cannot repair an initial target");
        assert!(error.to_string().contains(&target.to_string()));
        assert!(ensure_initial_backfill_allowed(&disabled, None).is_ok());
        assert!(ensure_initial_backfill_allowed(&TreeConfig::default(), Some(target)).is_ok());
    }
}

impl<T, CB, AO> LaunchNode<NodeBuilderWithComponents<T, CB, AO>> for EngineNodeLauncher
where
    T: FullNodeTypes<
        Types: NodeTypesForProvider,
        Provider = BlockchainProvider<
            NodeTypesWithDBAdapter<<T as FullNodeTypes>::Types, <T as FullNodeTypes>::DB>,
        >,
    >,
    CB: NodeComponentsBuilder<T> + 'static,
    AO: RethRpcAddOns<NodeAdapter<T, CB::Components>>
        + EngineValidatorAddOn<NodeAdapter<T, CB::Components>>
        + 'static,
{
    type Node = NodeHandle<NodeAdapter<T, CB::Components>, AO>;
    type Future = Pin<Box<dyn Future<Output = eyre::Result<Self::Node>> + Send>>;

    fn launch_node(self, target: NodeBuilderWithComponents<T, CB, AO>) -> Self::Future {
        Box::pin(self.launch_node(target))
    }
}
