//! Loads chain metadata.

use alloy_primitives::{U256, U64};
use alloy_rpc_types_eth::{Stage, SyncInfo, SyncStatus};
use futures::Future;
use reth_chainspec::ChainInfo;
use reth_errors::{RethError, RethResult};
use reth_network_api::NetworkInfo;
use reth_rpc_convert::RpcTxReq;
use reth_storage_api::{BlockNumReader, StageCheckpointReader, TransactionsProvider};

use crate::{helpers::EthSigner, EthApiTypes, RpcNodeCore};

/// `Eth` API trait.
///
/// Defines core functionality of the `eth` API implementation.
#[auto_impl::auto_impl(&, Arc)]
pub trait EthApiSpec: RpcNodeCore + EthApiTypes {
    /// Returns the block node is started on.
    fn starting_block(&self) -> U256;

    /// Returns the current ethereum protocol version.
    fn protocol_version(&self) -> impl Future<Output = RethResult<U64>> + Send {
        async move {
            let status = self.network().network_status().await.map_err(RethError::other)?;
            Ok(U64::from(status.protocol_version))
        }
    }

    /// Returns the chain id
    fn chain_id(&self) -> U64 {
        U64::from(self.network().chain_id())
    }

    /// Returns provider chain info
    fn chain_info(&self) -> RethResult<ChainInfo> {
        Ok(self.provider().chain_info()?)
    }

    /// Returns `true` if the network is undergoing sync.
    fn is_syncing(&self) -> bool {
        self.network().is_syncing()
    }

    /// Returns the [`SyncStatus`] of the network
    fn sync_status(&self) -> RethResult<SyncStatus> {
        // `chain_info()` includes the canonical in-memory Engine tree, while
        // the Finish checkpoint is the state another process will actually
        // reopen from disk. A fresh transient head must therefore continue to
        // report syncing until Finish catches it. This is especially important
        // for chainvisor's Backfill mode: Engine persistence is disabled while
        // the staged pipeline is the sole durable writer, and chainvisor uses
        // this gap to decide when an immediate-kill handoff is safe.
        let canonical_head = self.provider().chain_info()?.best_number;
        let checkpoints = self.provider().get_all_checkpoints()?;
        let durable_head = checkpoints
            .iter()
            .find(|(name, _)| name == "Finish")
            .map(|(_, checkpoint)| checkpoint.block_number)
            .unwrap_or_default();
        let status = if should_report_syncing(self.is_syncing(), durable_head, canonical_head) {
            let stages = checkpoints
                .into_iter()
                .map(|(name, checkpoint)| Stage { name, block: checkpoint.block_number })
                .collect();

            SyncStatus::Info(Box::new(SyncInfo {
                starting_block: self.starting_block(),
                current_block: U256::from(durable_head),
                highest_block: U256::from(canonical_head),
                warp_chunks_amount: None,
                warp_chunks_processed: None,
                stages: Some(stages),
            }))
        } else {
            SyncStatus::None
        };
        Ok(status)
    }
}

const fn should_report_syncing(
    network_syncing: bool,
    durable_head: u64,
    canonical_head: u64,
) -> bool {
    network_syncing || durable_head != canonical_head
}

/// A handle to [`EthSigner`]s with its generics set from [`TransactionsProvider`] and
/// [`reth_rpc_convert::RpcTypes`].
pub type SignersForRpc<Provider, Rpc> = parking_lot::RwLock<
    Vec<Box<dyn EthSigner<<Provider as TransactionsProvider>::Transaction, RpcTxReq<Rpc>>>>,
>;

#[cfg(test)]
mod tests {
    use super::should_report_syncing;

    #[test]
    fn transient_canonical_head_keeps_sync_status_active_until_durable() {
        assert!(should_report_syncing(false, 100, 101));
        assert!(should_report_syncing(true, 101, 101));
        assert!(!should_report_syncing(false, 101, 101));
    }
}
