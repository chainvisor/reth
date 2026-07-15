use crate::{
    backfill::{BackfillAction, BackfillEvent, BackfillSync},
    tree::error::AdvancePersistenceError,
};
use futures::Stream;
use reth_stages_api::{ControlFlow, PipelineTarget};
use std::{
    fmt::{Display, Formatter, Result},
    pin::Pin,
    sync::mpsc,
    task::{Context, Poll},
};
use tracing::*;

/// Failure to establish the tree-side Pending state before scheduling pipeline backfill.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum BackfillPendingError {
    /// The tree acknowledgement channel closed before the state transition completed.
    #[error("tree closed the backfill-pending acknowledgement channel")]
    AcknowledgementChannelClosed,
    /// Pipeline backfill is disabled by the tree configuration.
    #[error("pipeline backfill is disabled")]
    PipelineBackfillDisabled,
    /// A second pipeline run was requested while one was already active.
    #[error("pipeline backfill is already active")]
    PipelineAlreadyActive,
    /// A pipeline start was already acknowledged but has not reported Started yet.
    #[error("pipeline backfill is already pending")]
    PipelineAlreadyPending,
    /// A tree-originated Pending reservation belongs to a different pipeline action.
    #[error("pipeline backfill pending reservation belongs to a different action")]
    PendingReservationMismatch,
    /// Engine persistence currently owns database write access.
    #[error("Engine persistence is in progress")]
    PersistenceInProgress,
}

/// The type that drives the chain forward.
///
/// A state machine that orchestrates the components responsible for advancing the chain
///
///
/// ## Control flow
///
/// The [`ChainOrchestrator`] is responsible for controlling the backfill sync and additional hooks.
/// It polls the given `handler`, which is responsible for advancing the chain, how is up to the
/// handler. However, due to database restrictions (e.g. exclusive write access), following
/// invariants apply:
///  - If the handler requests a backfill run (e.g. [`BackfillAction::Start`]), the handler must
///    ensure that while the backfill sync is running, no other write access is granted.
///  - At any time the [`ChainOrchestrator`] can request exclusive write access to the database
///    (e.g. if pruning is required), but will not do so until the handler has acknowledged the
///    request for write access.
///
/// The [`ChainOrchestrator`] polls the [`ChainHandler`] to advance the chain and handles the
/// emitted events. Requests and events are passed to the [`ChainHandler`] via
/// [`ChainHandler::on_event`].
#[must_use = "Stream does nothing unless polled"]
#[derive(Debug)]
pub struct ChainOrchestrator<T, P>
where
    T: ChainHandler,
    P: BackfillSync,
{
    /// The handler for advancing the chain.
    handler: T,
    /// Controls backfill sync.
    backfill_sync: P,
}

impl<T, P> ChainOrchestrator<T, P>
where
    T: ChainHandler + Unpin,
    P: BackfillSync + Unpin,
{
    /// Creates a new [`ChainOrchestrator`] with the given handler and backfill sync.
    pub const fn new(handler: T, backfill_sync: P) -> Self {
        Self { handler, backfill_sync }
    }

    /// Returns the handler
    pub const fn handler(&self) -> &T {
        &self.handler
    }

    /// Returns a mutable reference to the handler
    pub const fn handler_mut(&mut self) -> &mut T {
        &mut self.handler
    }

    /// Triggers a backfill sync for the __valid__ given target.
    ///
    /// CAUTION: This function should be used with care and with a valid target.
    pub fn start_backfill_sync(
        &mut self,
        target: impl Into<PipelineTarget>,
    ) -> std::result::Result<(), BackfillPendingError> {
        let action = BackfillAction::Start(target.into());
        self.mark_backfill_pending(&action)?;
        self.backfill_sync.on_action(action);
        Ok(())
    }

    /// Establishes and acknowledges the tree-side Pending state before any pipeline action is
    /// queued. This is a synchronous ownership handoff: a pipeline task may not be scheduled until
    /// the tree has stopped admitting Engine persistence.
    fn mark_backfill_pending(
        &mut self,
        action: &BackfillAction,
    ) -> std::result::Result<(), BackfillPendingError> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.handler.on_event(FromOrchestrator::BackfillSyncPending { action: action.clone(), tx });
        rx.recv().map_err(|_| BackfillPendingError::AcknowledgementChannelClosed)?
    }

    /// Internal function used to advance the chain.
    ///
    /// Polls the `ChainOrchestrator` for the next event.
    #[tracing::instrument(level = "debug", target = "engine::tree::chain_orchestrator", skip_all)]
    fn poll_next_event(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<ChainEvent<T::Event>> {
        let this = self.get_mut();

        // This loop polls the components
        //
        // 1. Polls the backfill sync to completion, if active.
        // 2. Advances the chain by polling the handler.
        'outer: loop {
            // try to poll the backfill sync to completion, if active
            match this.backfill_sync.poll(cx) {
                Poll::Ready(backfill_sync_event) => match backfill_sync_event {
                    BackfillEvent::Started(_) => {
                        // notify handler that backfill sync started
                        this.handler.on_event(FromOrchestrator::BackfillSyncStarted);
                        return Poll::Ready(ChainEvent::BackfillSyncStarted);
                    }
                    BackfillEvent::Finished(res) => {
                        return match res {
                            Ok(ctrl) => {
                                tracing::debug!(?ctrl, "backfill sync finished");
                                // notify handler that backfill sync finished
                                this.handler.on_event(FromOrchestrator::BackfillSyncFinished(ctrl));
                                Poll::Ready(ChainEvent::BackfillSyncFinished)
                            }
                            Err(err) => {
                                tracing::error!( %err, "backfill sync failed");
                                Poll::Ready(ChainEvent::FatalError)
                            }
                        }
                    }
                    BackfillEvent::TaskDropped(err) => {
                        tracing::error!( %err, "backfill sync task dropped");
                        return Poll::Ready(ChainEvent::FatalError);
                    }
                },
                Poll::Pending => {}
            }

            // poll the handler for the next event
            match this.handler.poll(cx) {
                Poll::Ready(handler_event) => {
                    match handler_event {
                        HandlerEvent::BackfillAction(action) => {
                            if let Err(err) = this.mark_backfill_pending(&action) {
                                error!(target: "engine::tree", %err, "Failed to establish pending backfill ownership");
                                return Poll::Ready(ChainEvent::FatalError)
                            }
                            // Forward only after the tree has synchronously acknowledged Pending.
                            this.backfill_sync.on_action(action);
                        }
                        HandlerEvent::Event(ev) => {
                            // bubble up the event
                            return Poll::Ready(ChainEvent::Handler(ev));
                        }
                        HandlerEvent::FatalError => {
                            error!(target: "engine::tree", "Fatal error");
                            return Poll::Ready(ChainEvent::FatalError)
                        }
                    }
                }
                Poll::Pending => {
                    // no more events to process
                    break 'outer
                }
            }
        }

        Poll::Pending
    }
}

impl<T, P> Stream for ChainOrchestrator<T, P>
where
    T: ChainHandler + Unpin,
    P: BackfillSync + Unpin,
{
    type Item = ChainEvent<T::Event>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.as_mut().poll_next_event(cx).map(Some)
    }
}

/// Event emitted by the [`ChainOrchestrator`]
///
/// These are meant to be used for observability and debugging purposes.
#[derive(Debug)]
pub enum ChainEvent<T> {
    /// Backfill sync started
    BackfillSyncStarted,
    /// Backfill sync finished
    BackfillSyncFinished,
    /// Fatal error
    FatalError,
    /// Event emitted by the handler
    Handler(T),
}

impl<T: Display> Display for ChainEvent<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            Self::BackfillSyncStarted => {
                write!(f, "BackfillSyncStarted")
            }
            Self::BackfillSyncFinished => {
                write!(f, "BackfillSyncFinished")
            }
            Self::FatalError => {
                write!(f, "FatalError")
            }
            Self::Handler(event) => {
                write!(f, "Handler({event})")
            }
        }
    }
}

/// A trait that advances the chain by handling actions.
///
/// This is intended to be implement the chain consensus logic, for example `engine` API.
///
/// ## Control flow
///
/// The [`ChainOrchestrator`] is responsible for advancing this handler through
/// [`ChainHandler::poll`] and handling the emitted events, for example
/// [`HandlerEvent::BackfillAction`] to start a backfill sync. Events from the [`ChainOrchestrator`]
/// are passed to the handler via [`ChainHandler::on_event`], e.g.
/// [`FromOrchestrator::BackfillSyncStarted`] once the backfill sync started or finished.
pub trait ChainHandler: Send + Sync {
    /// Event generated by this handler that orchestrator can bubble up;
    type Event: Send;

    /// Informs the handler about an event from the [`ChainOrchestrator`].
    fn on_event(&mut self, event: FromOrchestrator);

    /// Polls for actions that [`ChainOrchestrator`] should handle.
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<HandlerEvent<Self::Event>>;
}

/// Events/Requests that the [`ChainHandler`] can emit to the [`ChainOrchestrator`].
#[derive(Clone, Debug)]
pub enum HandlerEvent<T> {
    /// Request an action to backfill sync
    BackfillAction(BackfillAction),
    /// Other event emitted by the handler
    Event(T),
    /// Fatal error
    FatalError,
}

/// Internal events issued by the [`ChainOrchestrator`].
#[derive(Debug)]
pub enum FromOrchestrator {
    /// Establish pipeline ownership before a backfill task is scheduled.
    BackfillSyncPending {
        /// Exact action whose one-shot tree reservation is being consumed.
        action: BackfillAction,
        /// Acknowledges that the tree has applied Pending, or explains why it refused.
        tx: mpsc::SyncSender<std::result::Result<(), BackfillPendingError>>,
    },
    /// Invoked when backfill sync finished
    BackfillSyncFinished(ControlFlow),
    /// Invoked when backfill sync started
    BackfillSyncStarted,
    /// Gracefully terminate the engine service.
    ///
    /// When this variant is received, the engine will persist all remaining in-memory blocks
    /// to disk before shutting down. Once persistence is complete, a signal is sent through
    /// the oneshot channel to notify the caller.
    Terminate {
        /// Channel carrying the exact termination-persistence outcome.
        tx: tokio::sync::oneshot::Sender<std::result::Result<(), AdvancePersistenceError>>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    #[derive(Debug)]
    struct RecordingHandler {
        events: Arc<Mutex<Vec<&'static str>>>,
        pending_results: VecDeque<std::result::Result<(), BackfillPendingError>>,
    }

    impl ChainHandler for RecordingHandler {
        type Event = ();

        fn on_event(&mut self, event: FromOrchestrator) {
            if let FromOrchestrator::BackfillSyncPending { tx, .. } = event {
                self.events.lock().unwrap().push("tree_pending_applied");
                tx.send(self.pending_results.pop_front().expect("missing pending result")).unwrap();
            }
        }

        fn poll(&mut self, _cx: &mut Context<'_>) -> Poll<HandlerEvent<Self::Event>> {
            Poll::Pending
        }
    }

    #[derive(Debug)]
    struct RecordingBackfill {
        events: Arc<Mutex<Vec<&'static str>>>,
    }

    impl BackfillSync for RecordingBackfill {
        fn on_action(&mut self, _action: BackfillAction) {
            self.events.lock().unwrap().push("pipeline_scheduled");
        }

        fn poll(&mut self, _cx: &mut Context<'_>) -> Poll<BackfillEvent> {
            Poll::Pending
        }
    }

    #[test]
    fn pending_acknowledgement_precedes_pipeline_scheduling() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let handler =
            RecordingHandler { events: events.clone(), pending_results: VecDeque::from([Ok(())]) };
        let backfill = RecordingBackfill { events: events.clone() };
        let mut orchestrator = ChainOrchestrator::new(handler, backfill);

        orchestrator.start_backfill_sync(B256::repeat_byte(0x11)).unwrap();

        assert_eq!(*events.lock().unwrap(), ["tree_pending_applied", "pipeline_scheduled"]);
    }

    #[test]
    fn rejected_pending_handoff_never_schedules_pipeline() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let handler = RecordingHandler {
            events: events.clone(),
            pending_results: VecDeque::from([Err(BackfillPendingError::PipelineBackfillDisabled)]),
        };
        let backfill = RecordingBackfill { events: events.clone() };
        let mut orchestrator = ChainOrchestrator::new(handler, backfill);

        assert_eq!(
            orchestrator.start_backfill_sync(B256::repeat_byte(0x22)),
            Err(BackfillPendingError::PipelineBackfillDisabled)
        );
        assert_eq!(*events.lock().unwrap(), ["tree_pending_applied"]);
    }

    #[test]
    fn duplicate_pending_handoff_schedules_only_once() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let handler = RecordingHandler {
            events: events.clone(),
            pending_results: VecDeque::from([
                Ok(()),
                Err(BackfillPendingError::PipelineAlreadyPending),
            ]),
        };
        let backfill = RecordingBackfill { events: events.clone() };
        let mut orchestrator = ChainOrchestrator::new(handler, backfill);
        let target = B256::repeat_byte(0x33);

        orchestrator.start_backfill_sync(target).unwrap();
        assert_eq!(
            orchestrator.start_backfill_sync(target),
            Err(BackfillPendingError::PipelineAlreadyPending)
        );
        assert_eq!(
            *events.lock().unwrap(),
            ["tree_pending_applied", "pipeline_scheduled", "tree_pending_applied"]
        );
    }
}
