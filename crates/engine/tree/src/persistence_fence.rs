use alloy_eips::BlockNumHash;
use alloy_primitives::B256;
use reth_errors::ProviderResult;
use reth_provider::{BlockHashReader, DBProvider, StageCheckpointReader};
use reth_stages_api::StageId;

/// A pipeline stage whose tables overlap with full Engine API block persistence.
#[derive(Clone, Copy, Debug)]
struct PersistenceOwnerStage {
    stage: StageId,
}

/// Pipeline stages whose tables overlap with full Engine API block persistence.
///
/// `Era` is deliberately excluded because it is an independent optional import cursor. `Finish`
/// is the durable watermark against which every installed owner stage is compared.
/// `PruneSenderRecovery` is omitted entirely unless sender-recovery pruning is configured because
/// a stale checkpoint from a prior configuration is not repairable by a pipeline without the
/// stage.
const ENGINE_PERSISTENCE_OWNER_STAGES: [PersistenceOwnerStage; 13] = [
    PersistenceOwnerStage { stage: StageId::Headers },
    PersistenceOwnerStage { stage: StageId::Bodies },
    PersistenceOwnerStage { stage: StageId::SenderRecovery },
    PersistenceOwnerStage { stage: StageId::Execution },
    PersistenceOwnerStage { stage: StageId::PruneSenderRecovery },
    PersistenceOwnerStage { stage: StageId::MerkleUnwind },
    PersistenceOwnerStage { stage: StageId::AccountHashing },
    PersistenceOwnerStage { stage: StageId::StorageHashing },
    PersistenceOwnerStage { stage: StageId::MerkleExecute },
    PersistenceOwnerStage { stage: StageId::TransactionLookup },
    PersistenceOwnerStage { stage: StageId::IndexStorageHistory },
    PersistenceOwnerStage { stage: StageId::IndexAccountHistory },
    PersistenceOwnerStage { stage: StageId::Prune },
];

/// One stage checkpoint captured in the persistence admission snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
struct PersistenceOwnerCheckpoint {
    stage: StageId,
    checkpoint: Option<u64>,
}

/// One atomic view of the pipeline checkpoints and durable block identity relevant to Engine API
/// persistence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PersistenceCheckpointSnapshot {
    finish: Option<u64>,
    finish_hash: Option<B256>,
    owners: Vec<PersistenceOwnerCheckpoint>,
}

impl PersistenceCheckpointSnapshot {
    /// Read all persistence-owning checkpoints and the `Finish` block hash through one provider
    /// transaction.
    pub(crate) fn read<P>(provider: &P) -> ProviderResult<Self>
    where
        P: StageCheckpointReader + BlockHashReader + DBProvider,
    {
        let finish = provider
            .get_stage_checkpoint(StageId::Finish)?
            .map(|checkpoint| checkpoint.block_number);
        let finish_hash = match finish {
            Some(finish) => provider.block_hash(finish)?,
            None => None,
        };
        let mut owners = Vec::with_capacity(ENGINE_PERSISTENCE_OWNER_STAGES.len());
        for owner in ENGINE_PERSISTENCE_OWNER_STAGES {
            if owner.stage == StageId::PruneSenderRecovery &&
                provider.prune_modes_ref().sender_recovery.is_none()
            {
                continue
            }
            owners.push(PersistenceOwnerCheckpoint {
                stage: owner.stage,
                checkpoint: provider
                    .get_stage_checkpoint(owner.stage)?
                    .map(|checkpoint| checkpoint.block_number),
            });
        }
        Ok(Self { finish, finish_hash, owners })
    }

    /// Require `Finish` to identify the tree's expected durable parent and every installed
    /// overlapping stage to be exactly aligned with that durable watermark.
    pub(crate) fn validate(
        &self,
        expected_last_persisted: BlockNumHash,
    ) -> Result<BlockNumHash, PersistenceFenceError> {
        let finish = self.finish.ok_or(PersistenceFenceError::MissingFinish)?;
        if finish != expected_last_persisted.number {
            return Err(PersistenceFenceError::FinishDoesNotMatchLastPersisted {
                finish,
                last_persisted: expected_last_persisted.number,
            })
        }

        let finish_hash =
            self.finish_hash.ok_or(PersistenceFenceError::MissingFinishBlockHash { finish })?;
        let finish_num_hash = BlockNumHash::new(finish, finish_hash);
        if finish_num_hash.hash != expected_last_persisted.hash {
            return Err(PersistenceFenceError::FinishHashDoesNotMatchLastPersisted {
                finish: finish_num_hash,
                last_persisted: expected_last_persisted,
            })
        }

        for owner in &self.owners {
            let Some(checkpoint) = owner.checkpoint else {
                return Err(PersistenceFenceError::MissingOwnerStage { stage: owner.stage, finish })
            };
            if checkpoint != finish {
                return Err(PersistenceFenceError::OwnerStageDoesNotMatchFinish {
                    stage: owner.stage,
                    checkpoint,
                    finish,
                })
            }
        }

        Ok(finish_num_hash)
    }

    /// Require the first Engine API block to extend the exact durable `Finish` block without a
    /// number gap, overlap, or parent-hash fork.
    pub(crate) fn validate_first_block(
        finish: BlockNumHash,
        first_block: BlockNumHash,
        first_parent_hash: B256,
    ) -> Result<(), PersistenceFenceError> {
        if finish.number.checked_add(1) != Some(first_block.number) {
            return Err(PersistenceFenceError::FirstBlockDoesNotExtendFinish {
                first_block: first_block.number,
                finish: finish.number,
            })
        }
        if first_parent_hash != finish.hash {
            return Err(PersistenceFenceError::FirstBlockParentDoesNotMatchFinish {
                first_block,
                parent_hash: first_parent_hash,
                finish,
            })
        }
        Ok(())
    }

    /// Require one later batch block to extend the immediately preceding block.
    pub(crate) fn validate_next_block(
        previous: BlockNumHash,
        next: BlockNumHash,
        next_parent_hash: B256,
    ) -> Result<(), PersistenceFenceError> {
        if previous.number.checked_add(1) != Some(next.number) {
            return Err(PersistenceFenceError::BatchBlockDoesNotExtendPrevious {
                previous,
                block: next,
            })
        }
        if next_parent_hash != previous.hash {
            return Err(PersistenceFenceError::BatchBlockParentDoesNotMatchPrevious {
                previous,
                block: next,
                parent_hash: next_parent_hash,
            })
        }
        Ok(())
    }

    /// Highest checkpoint that a convergence pipeline must cover.
    pub(crate) fn highest_checkpoint(&self, last_persisted: u64) -> u64 {
        self.owners
            .iter()
            .filter_map(|owner| owner.checkpoint)
            .chain(self.finish)
            .fold(last_persisted, u64::max)
    }
}

/// A precise reason why Engine API persistence is not allowed to own the next block range.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PersistenceFenceError {
    /// The durable pipeline watermark is absent.
    #[error("Finish checkpoint is missing")]
    MissingFinish,
    /// The provider and tree disagree about the last durable block number.
    #[error("Finish checkpoint {finish} does not match last persisted block {last_persisted}")]
    FinishDoesNotMatchLastPersisted {
        /// On-disk Finish checkpoint.
        finish: u64,
        /// Tree persistence state's expected durable block number.
        last_persisted: u64,
    },
    /// No canonical block hash exists for the on-disk `Finish` checkpoint.
    #[error("canonical block hash for Finish checkpoint {finish} is missing")]
    MissingFinishBlockHash {
        /// On-disk Finish checkpoint.
        finish: u64,
    },
    /// The provider and tree disagree about the durable block hash at the same height.
    #[error("Finish block {finish:?} does not match last persisted block {last_persisted:?}")]
    FinishHashDoesNotMatchLastPersisted {
        /// On-disk Finish block identity.
        finish: BlockNumHash,
        /// Tree persistence state's expected durable block identity.
        last_persisted: BlockNumHash,
    },
    /// A required overlapping stage has no checkpoint.
    #[error("owner stage {stage} is missing while Finish is {finish}")]
    MissingOwnerStage {
        /// Missing stage.
        stage: StageId,
        /// On-disk Finish checkpoint.
        finish: u64,
    },
    /// An installed overlapping stage is either ahead of or behind Finish.
    #[error("owner stage {stage} checkpoint {checkpoint} does not match Finish {finish}")]
    OwnerStageDoesNotMatchFinish {
        /// Divergent stage.
        stage: StageId,
        /// Divergent checkpoint.
        checkpoint: u64,
        /// On-disk Finish checkpoint.
        finish: u64,
    },
    /// The submitted batch overlaps or skips the durable frontier.
    #[error("first Engine persistence block {first_block} does not extend Finish {finish}")]
    FirstBlockDoesNotExtendFinish {
        /// First submitted block number.
        first_block: u64,
        /// On-disk Finish checkpoint.
        finish: u64,
    },
    /// The submitted batch's first block is on a different branch from the durable frontier.
    #[error(
        "first Engine persistence block {first_block:?} has parent {parent_hash}, not Finish {finish:?}"
    )]
    FirstBlockParentDoesNotMatchFinish {
        /// First submitted block identity.
        first_block: BlockNumHash,
        /// First submitted block's parent hash.
        parent_hash: B256,
        /// On-disk Finish block identity.
        finish: BlockNumHash,
    },
    /// A later block in the submitted batch overlaps or skips its predecessor.
    #[error(
        "Engine persistence batch block {block:?} does not extend previous block {previous:?}"
    )]
    BatchBlockDoesNotExtendPrevious {
        /// Immediately preceding batch block.
        previous: BlockNumHash,
        /// Non-contiguous batch block.
        block: BlockNumHash,
    },
    /// A later block in the submitted batch is on a different branch from its predecessor.
    #[error(
        "Engine persistence batch block {block:?} has parent {parent_hash}, not previous block {previous:?}"
    )]
    BatchBlockParentDoesNotMatchPrevious {
        /// Immediately preceding batch block.
        previous: BlockNumHash,
        /// Batch block with the wrong parent.
        block: BlockNumHash,
        /// Batch block's declared parent hash.
        parent_hash: B256,
    },
}

impl PersistenceFenceError {
    /// Stage associated with this rejection, for bounded-cardinality metrics and logs.
    pub(crate) const fn stage(&self) -> StageId {
        match self {
            Self::MissingFinish |
            Self::FinishDoesNotMatchLastPersisted { .. } |
            Self::MissingFinishBlockHash { .. } |
            Self::FinishHashDoesNotMatchLastPersisted { .. } |
            Self::FirstBlockDoesNotExtendFinish { .. } |
            Self::FirstBlockParentDoesNotMatchFinish { .. } |
            Self::BatchBlockDoesNotExtendPrevious { .. } |
            Self::BatchBlockParentDoesNotMatchPrevious { .. } => StageId::Finish,
            Self::MissingOwnerStage { stage, .. } |
            Self::OwnerStageDoesNotMatchFinish { stage, .. } => *stage,
        }
    }

    /// Stable reason label for metrics.
    pub(crate) const fn reason(&self) -> &'static str {
        match self {
            Self::MissingFinish => "missing_finish",
            Self::FinishDoesNotMatchLastPersisted { .. } => "finish_last_persisted_mismatch",
            Self::MissingFinishBlockHash { .. } => "missing_finish_hash",
            Self::FinishHashDoesNotMatchLastPersisted { .. } => "finish_hash_mismatch",
            Self::MissingOwnerStage { .. } => "missing_owner",
            Self::OwnerStageDoesNotMatchFinish { .. } => "owner_finish_mismatch",
            Self::FirstBlockDoesNotExtendFinish { .. } => "non_contiguous_batch",
            Self::FirstBlockParentDoesNotMatchFinish { .. } => "wrong_parent",
            Self::BatchBlockDoesNotExtendPrevious { .. } => "non_contiguous_batch",
            Self::BatchBlockParentDoesNotMatchPrevious { .. } => "wrong_parent",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_provider::test_utils::MockEthProvider;
    use reth_prune_types::{PruneMode, PruneModes};
    use reth_stages_api::StageCheckpoint;

    fn aligned(block: BlockNumHash) -> PersistenceCheckpointSnapshot {
        PersistenceCheckpointSnapshot {
            finish: Some(block.number),
            finish_hash: Some(block.hash),
            owners: ENGINE_PERSISTENCE_OWNER_STAGES
                .into_iter()
                .map(|owner| PersistenceOwnerCheckpoint {
                    stage: owner.stage,
                    checkpoint: Some(block.number),
                })
                .collect(),
        }
    }

    #[test]
    fn every_required_owner_stage_rejects_missing_ahead_and_behind() {
        const FINISH: u64 = 100;
        let expected = BlockNumHash::new(FINISH, B256::repeat_byte(0x11));
        for owner in ENGINE_PERSISTENCE_OWNER_STAGES {
            for checkpoint in [None, Some(FINISH - 1), Some(FINISH + 1)] {
                let mut snapshot = aligned(expected);
                snapshot
                    .owners
                    .iter_mut()
                    .find(|candidate| candidate.stage == owner.stage)
                    .expect("owner stage exists")
                    .checkpoint = checkpoint;

                let error = snapshot.validate(expected).expect_err("divergence must be rejected");
                assert_eq!(error.stage(), owner.stage);
                match checkpoint {
                    None => {
                        assert!(matches!(error, PersistenceFenceError::MissingOwnerStage { .. }))
                    }
                    Some(_) => assert!(matches!(
                        error,
                        PersistenceFenceError::OwnerStageDoesNotMatchFinish { .. }
                    )),
                }
            }
        }
    }

    fn provider_with_sender_prune_checkpoint(
        sender_recovery: Option<PruneMode>,
        checkpoint: Option<u64>,
    ) -> (MockEthProvider, BlockNumHash) {
        const FINISH: u64 = 100;
        let provider = MockEthProvider::default()
            .with_genesis_block()
            .with_prune_modes(PruneModes { sender_recovery, ..Default::default() });
        let finish_hash = B256::repeat_byte(0x77);
        provider.add_header(
            finish_hash,
            alloy_consensus::Header { number: FINISH, ..Default::default() },
        );
        provider.set_stage_checkpoint(StageId::Finish, StageCheckpoint::new(FINISH));
        for owner in ENGINE_PERSISTENCE_OWNER_STAGES {
            if owner.stage != StageId::PruneSenderRecovery {
                provider.set_stage_checkpoint(owner.stage, StageCheckpoint::new(FINISH));
            }
        }
        if let Some(checkpoint) = checkpoint {
            provider.set_stage_checkpoint(
                StageId::PruneSenderRecovery,
                StageCheckpoint::new(checkpoint),
            );
        }
        let finish = BlockNumHash::new(FINISH, finish_hash);
        (provider, finish)
    }

    #[test]
    fn sender_recovery_prune_checkpoint_tracks_configured_pipeline_ownership() {
        for stale_checkpoint in [None, Some(99), Some(101)] {
            let (provider, finish) = provider_with_sender_prune_checkpoint(None, stale_checkpoint);
            let snapshot = PersistenceCheckpointSnapshot::read(&provider).unwrap();
            assert_eq!(snapshot.validate(finish), Ok(finish));
            assert_eq!(snapshot.highest_checkpoint(finish.number), finish.number);
        }

        for checkpoint in [None, Some(99), Some(101)] {
            let (provider, finish) =
                provider_with_sender_prune_checkpoint(Some(PruneMode::Full), checkpoint);
            let error = PersistenceCheckpointSnapshot::read(&provider)
                .unwrap()
                .validate(finish)
                .expect_err("configured owner must be aligned");
            if checkpoint.is_none() {
                assert!(matches!(
                    error,
                    PersistenceFenceError::MissingOwnerStage {
                        stage: StageId::PruneSenderRecovery,
                        ..
                    }
                ));
            } else {
                assert!(matches!(
                    error,
                    PersistenceFenceError::OwnerStageDoesNotMatchFinish {
                        stage: StageId::PruneSenderRecovery,
                        ..
                    }
                ));
            }
        }

        let (provider, finish) =
            provider_with_sender_prune_checkpoint(Some(PruneMode::Full), Some(100));
        assert_eq!(
            PersistenceCheckpointSnapshot::read(&provider).unwrap().validate(finish),
            Ok(finish)
        );
    }

    #[test]
    fn finish_must_exist_and_match_full_last_persisted_identity() {
        let expected = BlockNumHash::new(100, B256::repeat_byte(0x33));
        let mut snapshot = aligned(expected);
        snapshot.finish = None;
        snapshot.finish_hash = None;
        assert_eq!(snapshot.validate(expected), Err(PersistenceFenceError::MissingFinish));

        let snapshot = aligned(expected);
        assert_eq!(
            snapshot.validate(BlockNumHash::new(99, expected.hash)),
            Err(PersistenceFenceError::FinishDoesNotMatchLastPersisted {
                finish: 100,
                last_persisted: 99,
            })
        );

        let mut snapshot = aligned(expected);
        snapshot.finish_hash = None;
        assert_eq!(
            snapshot.validate(expected),
            Err(PersistenceFenceError::MissingFinishBlockHash { finish: 100 })
        );

        let wrong_hash = BlockNumHash::new(100, B256::repeat_byte(0x44));
        assert_eq!(
            aligned(expected).validate(wrong_hash),
            Err(PersistenceFenceError::FinishHashDoesNotMatchLastPersisted {
                finish: expected,
                last_persisted: wrong_hash,
            })
        );
    }

    #[test]
    fn first_block_must_be_finish_plus_one_on_the_same_branch() {
        let finish = BlockNumHash::new(100, B256::repeat_byte(0x55));
        let first = BlockNumHash::new(101, B256::repeat_byte(0x66));
        assert!(
            PersistenceCheckpointSnapshot::validate_first_block(finish, first, finish.hash).is_ok()
        );
        assert!(PersistenceCheckpointSnapshot::validate_first_block(
            finish,
            BlockNumHash::new(100, first.hash),
            finish.hash
        )
        .is_err());
        assert!(PersistenceCheckpointSnapshot::validate_first_block(
            finish,
            BlockNumHash::new(102, first.hash),
            finish.hash
        )
        .is_err());
        assert!(matches!(
            PersistenceCheckpointSnapshot::validate_first_block(
                finish,
                first,
                B256::repeat_byte(0x77)
            ),
            Err(PersistenceFenceError::FirstBlockParentDoesNotMatchFinish { .. })
        ));
        let max = BlockNumHash::new(u64::MAX, finish.hash);
        assert!(PersistenceCheckpointSnapshot::validate_first_block(max, max, max.hash).is_err());
    }

    #[test]
    fn every_later_batch_block_must_extend_its_immediate_predecessor() {
        let previous = BlockNumHash::new(101, B256::repeat_byte(0x88));
        let next = BlockNumHash::new(102, B256::repeat_byte(0x99));
        assert!(PersistenceCheckpointSnapshot::validate_next_block(previous, next, previous.hash)
            .is_ok());
        assert!(matches!(
            PersistenceCheckpointSnapshot::validate_next_block(
                previous,
                BlockNumHash::new(103, next.hash),
                previous.hash
            ),
            Err(PersistenceFenceError::BatchBlockDoesNotExtendPrevious { .. })
        ));
        assert!(matches!(
            PersistenceCheckpointSnapshot::validate_next_block(
                previous,
                next,
                B256::repeat_byte(0xaa)
            ),
            Err(PersistenceFenceError::BatchBlockParentDoesNotMatchPrevious { .. })
        ));
    }
}
