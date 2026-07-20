//! Internal errors for the tree module.

use crate::{persistence::PersistenceError, persistence_fence::PersistenceFenceError};
use alloy_consensus::BlockHeader;
use alloy_eips::BlockNumHash;
use reth_consensus::ConsensusError;
use reth_errors::{BlockExecutionError, BlockValidationError, ProviderError};
use reth_evm::execute::InternalBlockExecutionError;
use reth_payload_primitives::NewPayloadError;
use reth_primitives_traits::{Block, BlockBody, SealedBlock};

/// This is an error that can come from advancing persistence.
#[derive(Debug, thiserror::Error)]
pub enum AdvancePersistenceError {
    /// The persistence channel was closed unexpectedly
    #[error("persistence channel closed")]
    ChannelClosed,
    /// A provider error
    #[error(transparent)]
    Provider(#[from] ProviderError),
    /// The persistence worker rejected an unsafe or failed disk action.
    #[error(transparent)]
    Persistence(#[from] PersistenceError),
    /// Checkpoints cannot be converged because pipeline backfill is disabled.
    #[error(
        "engine persistence fence cannot converge with pipeline backfill disabled: {mismatch}"
    )]
    FenceRepairUnavailable {
        /// Blocking checkpoint mismatch.
        mismatch: PersistenceFenceError,
    },
    /// No canonical block exists for the checkpoint convergence target.
    #[error("engine persistence fence cannot find block {block_number} for pipeline convergence")]
    FenceRepairTargetMissing {
        /// Highest checkpoint that must be converged.
        block_number: u64,
    },
    /// Dispatch of a required convergence run failed.
    #[error("engine persistence fence could not dispatch its pipeline convergence action")]
    FenceRepairDispatchFailed,
    /// A convergence run completed without strict durable progress toward its fixed target.
    #[error(
        "engine persistence fence convergence to {target:?} stalled: initial={initial}; previous Finish={previous_finish:?}; current Finish={current_finish:?}; current={current}"
    )]
    FenceRepairFailed {
        /// Pipeline target used by the convergence attempt.
        target: BlockNumHash,
        /// Mismatch that triggered the attempt.
        initial: PersistenceFenceError,
        /// Durable `Finish` observed before the completed convergence run.
        previous_finish: Option<u64>,
        /// Durable `Finish` observed after the completed convergence run.
        current_finish: Option<u64>,
        /// Mismatch still present after the attempt.
        current: PersistenceFenceError,
    },
    /// The tree constructed a batch that does not extend the admitted durable frontier.
    #[error("engine persistence batch rejected before dispatch: {mismatch}")]
    FenceBatchRejected {
        /// Blocking admission mismatch.
        mismatch: PersistenceFenceError,
    },
    /// Graceful shutdown may not flush an Engine tree across a divergent pipeline frontier.
    #[error("refusing unsafe graceful persistence flush: {mismatch}")]
    UnsafeShutdownFence {
        /// Blocking checkpoint mismatch.
        mismatch: PersistenceFenceError,
    },
    /// Graceful shutdown may not start Engine persistence while pipeline backfill owns the
    /// database.
    #[error("refusing graceful persistence flush while pipeline backfill is pending or active")]
    UnsafeShutdownPipelineBusy,
}

#[derive(thiserror::Error)]
#[error("Failed to insert block (hash={}, number={}, parent_hash={}): {}",
    .block.hash(),
    .block.number(),
    .block.parent_hash(),
    .kind)]
struct InsertBlockErrorData<B: Block> {
    block: SealedBlock<B>,
    #[source]
    kind: InsertBlockErrorKind,
}

impl<B: Block> std::fmt::Debug for InsertBlockErrorData<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InsertBlockError")
            .field("error", &self.kind)
            .field("hash", &self.block.hash())
            .field("number", &self.block.number())
            .field("parent_hash", &self.block.parent_hash())
            .field("num_txs", &self.block.body().transactions().len())
            .finish_non_exhaustive()
    }
}

impl<B: Block> InsertBlockErrorData<B> {
    const fn new(block: SealedBlock<B>, kind: InsertBlockErrorKind) -> Self {
        Self { block, kind }
    }

    fn boxed(block: SealedBlock<B>, kind: InsertBlockErrorKind) -> Box<Self> {
        Box::new(Self::new(block, kind))
    }
}

/// Error thrown when inserting a block failed because the block is considered invalid.
#[derive(thiserror::Error)]
#[error(transparent)]
pub struct InsertBlockError<B: Block> {
    inner: Box<InsertBlockErrorData<B>>,
}

// === impl InsertBlockErrorTwo ===

impl<B: Block> InsertBlockError<B> {
    /// Create a new `InsertInvalidBlockErrorTwo`
    pub fn new(block: SealedBlock<B>, kind: InsertBlockErrorKind) -> Self {
        Self { inner: InsertBlockErrorData::boxed(block, kind) }
    }

    /// Create a new `InsertInvalidBlockError` from a consensus error
    pub fn consensus_error(error: ConsensusError, block: SealedBlock<B>) -> Self {
        Self::new(block, InsertBlockErrorKind::Consensus(error))
    }

    /// Consumes the error and returns the block that resulted in the error
    #[inline]
    pub fn into_block(self) -> SealedBlock<B> {
        self.inner.block
    }

    /// Returns the error kind
    #[inline]
    pub const fn kind(&self) -> &InsertBlockErrorKind {
        &self.inner.kind
    }

    /// Returns the block that resulted in the error
    #[inline]
    pub const fn block(&self) -> &SealedBlock<B> {
        &self.inner.block
    }

    /// Consumes the type and returns the block and error kind.
    #[inline]
    pub fn split(self) -> (SealedBlock<B>, InsertBlockErrorKind) {
        let inner = *self.inner;
        (inner.block, inner.kind)
    }
}

impl<B: Block> std::fmt::Debug for InsertBlockError<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.inner, f)
    }
}

/// All error variants possible when inserting a block
#[derive(Debug, thiserror::Error)]
pub enum InsertBlockErrorKind {
    /// Block violated consensus rules.
    #[error(transparent)]
    Consensus(#[from] ConsensusError),
    /// Block execution failed.
    #[error(transparent)]
    Execution(#[from] BlockExecutionError),
    /// Provider error.
    #[error(transparent)]
    Provider(#[from] ProviderError),
    /// Other errors.
    #[error(transparent)]
    Other(#[from] Box<dyn core::error::Error + Send + Sync + 'static>),
}

impl InsertBlockErrorKind {
    /// Returns an [`InsertBlockValidationError`] if the error is caused by an invalid block.
    ///
    /// Returns an [`InsertBlockFatalError`] if the error is caused by an error that is not
    /// validation related or is otherwise fatal.
    ///
    /// This is intended to be used to determine if we should respond `INVALID` as a response when
    /// processing a new block.
    pub fn ensure_validation_error(
        self,
    ) -> Result<InsertBlockValidationError, InsertBlockFatalError> {
        match self {
            Self::Consensus(err) => Ok(InsertBlockValidationError::Consensus(err)),
            // other execution errors that are considered internal errors
            Self::Execution(err) => {
                match err {
                    BlockExecutionError::Validation(err) => {
                        Ok(InsertBlockValidationError::Validation(err))
                    }
                    // these are internal errors, not caused by an invalid block
                    BlockExecutionError::Internal(error) => {
                        Err(InsertBlockFatalError::BlockExecutionError(error))
                    }
                }
            }
            Self::Provider(err) => Err(InsertBlockFatalError::Provider(err)),
            Self::Other(err) => Err(InternalBlockExecutionError::Other(err).into()),
        }
    }
}

/// Error variants that are not caused by invalid blocks
#[derive(Debug, thiserror::Error)]
pub enum InsertBlockFatalError {
    /// A provider error
    #[error(transparent)]
    Provider(#[from] ProviderError),
    /// An internal / fatal block execution error
    #[error(transparent)]
    BlockExecutionError(#[from] InternalBlockExecutionError),
    /// A fatal persistence failure while processing an Engine API request.
    #[error(transparent)]
    Persistence(#[from] AdvancePersistenceError),
    /// The exact persistence failure was returned through the reth_newPayload response channel;
    /// the tree must still stop so it cannot continue after losing persistence coordination.
    #[error("fatal reth_newPayload persistence failure was delivered to the caller")]
    RethNewPayloadPersistenceFailureDelivered,
    /// The orchestrator dropped the Pending acknowledgement before the tree could deliver it.
    #[error("backfill-pending acknowledgement receiver closed")]
    BackfillPendingAcknowledgementClosed,
    /// A pipeline task reported Started without a preceding acknowledged Pending handoff.
    #[error("pipeline backfill started without an acknowledged pending handoff")]
    BackfillStartedWithoutPending,
    /// A pipeline task reported Started even though pipeline backfill is disabled.
    #[error("pipeline backfill started while pipeline backfill is disabled")]
    BackfillStartedWhileDisabled,
}

/// Error variants that are caused by invalid blocks
#[derive(Debug, thiserror::Error)]
pub enum InsertBlockValidationError {
    /// Block violated consensus rules.
    #[error(transparent)]
    Consensus(#[from] ConsensusError),
    /// Validation error, transparently wrapping [`BlockValidationError`]
    #[error(transparent)]
    Validation(#[from] BlockValidationError),
}

/// Errors that may occur when inserting a payload.
#[derive(Debug, thiserror::Error)]
pub enum InsertPayloadError<B: Block> {
    /// Block validation error
    #[error(transparent)]
    Block(#[from] InsertBlockError<B>),
    /// Payload validation error
    #[error(transparent)]
    Payload(#[from] NewPayloadError),
}
