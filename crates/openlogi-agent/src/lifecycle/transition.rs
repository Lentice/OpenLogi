//! Ownership of the watcher fleet during process replacement and terminal exit.
//!
//! [`WatcherFleet`] is the one owner of a replacement's outcome: the teardown
//! result, the OS relaunch result (through the [`Relaunch`] seam), restoring
//! the fleet, and the binary watcher's single retry ack. The lifecycle only
//! acts on the returned [`ReplacementOutcome`] — it remains the authority for
//! process exit and handover.

use std::path::PathBuf;
use std::time::Duration;

use futures::FutureExt as _;
use futures::future::BoxFuture;
use tokio::sync::oneshot;
use tracing::{info, warn};

use crate::binary_watch::{OsRelaunch, Relaunch};
use crate::startup::HidppWatcherHandles;

const STOP_TIMEOUT: Duration = Duration::from_secs(3);

/// A running fleet that can be told to stop. The future resolves `true` only
/// once every manager confirmed firmware teardown.
pub(super) trait FleetHandles: Send + 'static {
    fn stop_and_wait(self) -> BoxFuture<'static, bool>;
}

impl FleetHandles for HidppWatcherHandles {
    fn stop_and_wait(self) -> BoxFuture<'static, bool> {
        HidppWatcherHandles::stop_and_wait(self).boxed()
    }
}

/// The binary watcher's request to become the executable at `path`.
pub(super) struct Replacement {
    path: PathBuf,
    retry: Retry,
}

impl Replacement {
    pub(super) fn new(path: PathBuf, retry: oneshot::Sender<()>) -> Self {
        Self {
            path,
            retry: Retry(retry),
        }
    }
}

/// The binary watcher's one "keep watching" answer. Private to this module, so
/// [`WatcherFleet`] is its only sender.
struct Retry(oneshot::Sender<()>);

impl Retry {
    fn ack(self) {
        let _ = self.0.send(());
    }
}

/// Holds the binary watcher's request open, unanswered, until the process
/// ends: a leaving process never asks the watcher to resume.
pub(super) struct Handover {
    _retry: Retry,
}

/// What became of a replacement request.
#[must_use = "Leaving obliges the lifecycle to end the process"]
pub(super) enum ReplacementOutcome {
    /// The successor is arranged (on Windows: left to the GUI) and the fleet
    /// is fully drained. The lifecycle must now hand over and exit.
    Leaving(Handover),
    /// The current image keeps running: its fleet is restored (if one had been
    /// drained) and only then was the binary watcher told to retry.
    Resumed,
}

/// A replacement whose teardown finished; produced by
/// [`WatcherFleet::drained`] and consumed by [`WatcherFleet::complete`].
pub(super) struct Drained {
    request: Replacement,
    stopped: bool,
}

enum FleetState<H> {
    Inactive,
    Running(H),
    Replacing {
        request: Replacement,
        teardown: BoxFuture<'static, bool>,
    },
}

/// The HID++ watcher fleet and the replacement of the process that hosts it.
pub(super) struct WatcherFleet<R = OsRelaunch, H = HidppWatcherHandles> {
    state: FleetState<H>,
    relaunch: R,
}

impl WatcherFleet {
    /// An inactive fleet using this OS's real relaunch.
    pub(super) fn new() -> Self {
        Self::with_relaunch(OsRelaunch)
    }
}

impl<R: Relaunch, H: FleetHandles> WatcherFleet<R, H> {
    fn with_relaunch(relaunch: R) -> Self {
        Self {
            state: FleetState::Inactive,
            relaunch,
        }
    }

    /// Start serving with `handles`. Arming calls this exactly once, before
    /// any replacement can begin.
    pub(super) fn start(&mut self, handles: H) {
        debug_assert!(matches!(self.state, FleetState::Inactive));
        self.state = FleetState::Running(handles);
    }

    /// Begin teardown without blocking the lifecycle's other event sources.
    pub(super) fn begin(&mut self, request: Replacement) {
        match std::mem::replace(&mut self.state, FleetState::Inactive) {
            FleetState::Running(handles) => {
                info!(path = %request.path.display(), "executable changed — draining HID++ sessions before replacement");
                self.state = FleetState::Replacing {
                    request,
                    teardown: handles.stop_and_wait(),
                };
            }
            state => {
                // The binary watcher normally has only one outstanding
                // request. Do not discard an existing drain if one overlaps;
                // nothing was drained for this request, so nothing to restore.
                self.state = state;
                request.retry.ack();
            }
        }
    }

    /// Poll the owned teardown in the main select loop. Losing a select race
    /// drops only this borrow, not the managers' completion receivers.
    /// Replacement deliberately has no deadline.
    pub(super) async fn drained(&mut self) -> Drained {
        let FleetState::Replacing { teardown, .. } = &mut self.state else {
            return std::future::pending().await;
        };
        let stopped = teardown.await;
        match std::mem::replace(&mut self.state, FleetState::Inactive) {
            FleetState::Replacing { request, .. } => Drained { request, stopped },
            _ => unreachable!("exclusive borrow preserves the replacement until completion"),
        }
    }

    /// Settle a drained replacement. An unclean teardown or a failed relaunch
    /// restores the fleet with `restore` and only then acks the retry.
    pub(super) fn complete(
        &mut self,
        Drained { request, stopped }: Drained,
        restore: impl FnOnce() -> H,
    ) -> ReplacementOutcome {
        if !stopped {
            warn!("HID++ teardown was unclean — refusing replacement and retrying");
            return self.resume(request.retry, restore());
        }
        match self.relaunch.relaunch(&request.path) {
            Ok(()) => ReplacementOutcome::Leaving(Handover {
                _retry: request.retry,
            }),
            Err(error) => {
                warn!(%error, path = %request.path.display(), "could not relaunch the updated agent — restoring the current image and retrying");
                self.resume(request.retry, restore())
            }
        }
    }

    /// Replace a process that never armed: there is no fleet to drain, so a
    /// failed relaunch has nothing to restore before the retry.
    #[cfg(any(target_os = "macos", test))]
    pub(super) fn relaunch_unarmed(&mut self, request: Replacement) -> ReplacementOutcome {
        debug_assert!(matches!(self.state, FleetState::Inactive));
        match self.relaunch.relaunch(&request.path) {
            Ok(()) => ReplacementOutcome::Leaving(Handover {
                _retry: request.retry,
            }),
            Err(error) => {
                warn!(%error, path = %request.path.display(), "could not relaunch the updated agent — keeping the current image and retrying");
                request.retry.ack();
                ReplacementOutcome::Resumed
            }
        }
    }

    /// The only resumed-after-drain ack: taking the restored handles makes
    /// "restore before ack" a property of the signature.
    fn resume(&mut self, retry: Retry, restored: H) -> ReplacementOutcome {
        self.state = FleetState::Running(restored);
        retry.ack();
        ReplacementOutcome::Resumed
    }

    /// A terminal exit keeps polling the same drain, but no longer waits
    /// indefinitely for disconnected hardware. Never use this for replacement.
    pub(super) async fn stop_for_exit(&mut self) {
        let teardown = match std::mem::replace(&mut self.state, FleetState::Inactive) {
            FleetState::Inactive => return,
            FleetState::Running(handles) => handles.stop_and_wait(),
            FleetState::Replacing { teardown, .. } => teardown,
        };
        if tokio::time::timeout(STOP_TIMEOUT, teardown).await.is_err() {
            warn!(timeout = ?STOP_TIMEOUT, "watcher teardown exceeded the process-exit deadline");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::path::Path;
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::shutdown::{ShutdownRequest, request_channel};

    /// Records every observable step, so tests assert ordering, not just
    /// end states.
    type Log = Arc<Mutex<Vec<Step>>>;

    #[derive(Debug, PartialEq)]
    enum Step {
        Drain,
        Relaunch,
        Restore,
    }

    struct FakeHandles {
        log: Log,
        teardown: BoxFuture<'static, bool>,
    }

    impl FleetHandles for FakeHandles {
        fn stop_and_wait(self) -> BoxFuture<'static, bool> {
            self.log.lock().unwrap().push(Step::Drain);
            self.teardown
        }
    }

    /// A relaunch that succeeds, or fails with `NotFound`.
    struct ScriptedRelaunch {
        log: Log,
        succeeds: bool,
    }

    impl Relaunch for ScriptedRelaunch {
        fn relaunch(&mut self, path: &Path) -> io::Result<()> {
            assert_eq!(path, Path::new("replacement-agent"));
            self.log.lock().unwrap().push(Step::Relaunch);
            if self.succeeds {
                Ok(())
            } else {
                Err(io::ErrorKind::NotFound.into())
            }
        }
    }

    type TestFleet = WatcherFleet<ScriptedRelaunch, FakeHandles>;

    struct Harness {
        fleet: TestFleet,
        log: Log,
        completed: oneshot::Sender<bool>,
        retried: oneshot::Receiver<()>,
    }

    fn new_request() -> (Replacement, oneshot::Receiver<()>) {
        let (retry, retried) = oneshot::channel();
        (
            Replacement::new(PathBuf::from("replacement-agent"), retry),
            retried,
        )
    }

    fn idle_handles(log: &Log) -> FakeHandles {
        FakeHandles {
            log: Arc::clone(log),
            teardown: std::future::pending().boxed(),
        }
    }

    fn running(relaunch_succeeds: bool) -> (TestFleet, Log, oneshot::Sender<bool>) {
        let log = Log::default();
        let (completed, completion) = oneshot::channel();
        let mut fleet = WatcherFleet::with_relaunch(ScriptedRelaunch {
            log: Arc::clone(&log),
            succeeds: relaunch_succeeds,
        });
        fleet.start(FakeHandles {
            log: Arc::clone(&log),
            teardown: async move { completion.await.expect("test controls completion") }.boxed(),
        });
        (fleet, log, completed)
    }

    /// A fleet mid-replacement, reached through the real `begin` path.
    fn replacing(relaunch_succeeds: bool) -> Harness {
        let (mut fleet, log, completed) = running(relaunch_succeeds);
        let (request, retried) = new_request();
        fleet.begin(request);
        assert!(matches!(fleet.state, FleetState::Replacing { .. }));
        Harness {
            fleet,
            log,
            completed,
            retried,
        }
    }

    fn not_acked(retried: &mut oneshot::Receiver<()>) -> bool {
        matches!(retried.try_recv(), Err(oneshot::error::TryRecvError::Empty))
    }

    /// Drain with `stopped`, then complete with a `restore` that proves the
    /// ack had not yet been sent when it ran.
    async fn resume_observing_ack(stopped: bool, relaunch_succeeds: bool) -> (TestFleet, Log) {
        let Harness {
            mut fleet,
            log,
            completed,
            mut retried,
        } = replacing(relaunch_succeeds);
        completed.send(stopped).unwrap();
        let drained = fleet.drained().await;
        let outcome = fleet.complete(drained, || {
            assert!(
                not_acked(&mut retried),
                "retry acked before the fleet was restored"
            );
            log.lock().unwrap().push(Step::Restore);
            idle_handles(&log)
        });
        assert!(matches!(outcome, ReplacementOutcome::Resumed));
        assert!(matches!(fleet.state, FleetState::Running(_)));
        retried.try_recv().expect("retry acked after restore");
        (fleet, log)
    }

    #[tokio::test]
    async fn unclean_teardown_restores_the_fleet_then_acks_without_relaunching() {
        let (_fleet, log) = resume_observing_ack(false, true).await;
        assert_eq!(*log.lock().unwrap(), [Step::Drain, Step::Restore]);
    }

    #[tokio::test]
    async fn failed_relaunch_restores_the_fleet_then_acks() {
        let (_fleet, log) = resume_observing_ack(true, false).await;
        assert_eq!(
            *log.lock().unwrap(),
            [Step::Drain, Step::Relaunch, Step::Restore]
        );
    }

    #[tokio::test]
    async fn successful_relaunch_leaves_without_restoring_or_acking() {
        let mut h = replacing(true);
        h.completed.send(true).unwrap();
        let drained = h.fleet.drained().await;
        let outcome = h
            .fleet
            .complete(drained, || panic!("a leaving process must not restore"));

        let ReplacementOutcome::Leaving(handover) = outcome else {
            panic!("a relaunched successor must make the process leave");
        };
        assert!(matches!(h.fleet.state, FleetState::Inactive));
        assert!(
            not_acked(&mut h.retried),
            "the watcher stays parked while the process leaves"
        );
        assert_eq!(*h.log.lock().unwrap(), [Step::Drain, Step::Relaunch]);
        drop(handover);
        assert!(matches!(
            h.retried.try_recv(),
            Err(oneshot::error::TryRecvError::Closed)
        ));
    }

    #[tokio::test]
    async fn requests_that_cannot_drain_are_acked_at_once_and_keep_the_state() {
        // Not running yet: nothing to drain.
        let log = Log::default();
        let mut inactive: TestFleet = WatcherFleet::with_relaunch(ScriptedRelaunch {
            log: Arc::clone(&log),
            succeeds: true,
        });
        let (request, mut retried) = new_request();
        inactive.begin(request);
        retried.try_recv().expect("inactive fleet acks immediately");
        assert!(matches!(inactive.state, FleetState::Inactive));

        // Overlapping: the existing drain and its request survive.
        let mut h = replacing(true);
        let (overlap, mut overlap_retried) = new_request();
        h.fleet.begin(overlap);
        overlap_retried
            .try_recv()
            .expect("overlapping request acks immediately");
        assert!(matches!(h.fleet.state, FleetState::Replacing { .. }));
        assert!(not_acked(&mut h.retried));
        assert!(!h.completed.is_closed(), "the existing drain is retained");
        assert!(log.lock().unwrap().is_empty());
        assert_eq!(*h.log.lock().unwrap(), [Step::Drain]);
    }

    #[tokio::test]
    async fn losing_select_polls_retains_completion_and_its_failure_status() {
        for stopped in [false, true] {
            let mut h = replacing(false);
            for _ in 0..3 {
                assert!(h.fleet.drained().now_or_never().is_none());
            }
            h.completed.send(stopped).unwrap();
            let drained = h.fleet.drained().await;
            assert_eq!(drained.stopped, stopped);
            assert_eq!(drained.request.path, PathBuf::from("replacement-agent"));
            assert!(matches!(h.fleet.state, FleetState::Inactive));
            assert!(not_acked(&mut h.retried));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn quit_during_replacement_bounds_the_same_drain_and_never_relaunches() {
        let mut h = replacing(true);
        let (requests, mut incoming) = request_channel();
        let quit = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            requests.send(ShutdownRequest::Uninstalled).unwrap();
        });

        tokio::select! {
            _ = h.fleet.drained() => panic!("replacement cannot proceed without teardown"),
            request = incoming.recv() => assert!(matches!(request, Some(ShutdownRequest::Uninstalled))),
        }
        quit.await.unwrap();
        assert!(
            !h.completed.is_closed(),
            "accepting Quit must retain the existing cleanup"
        );

        let started = tokio::time::Instant::now();
        h.fleet.stop_for_exit().await;
        assert_eq!(started.elapsed(), STOP_TIMEOUT);
        assert!(
            h.completed.is_closed(),
            "terminal timeout may abandon completion before exit"
        );
        assert!(matches!(h.fleet.state, FleetState::Inactive));
        // The request went with the drain, unanswered: the process is leaving.
        assert!(matches!(
            h.retried.try_recv(),
            Err(oneshot::error::TryRecvError::Closed)
        ));
        assert_eq!(
            *h.log.lock().unwrap(),
            [Step::Drain],
            "one drain, no relaunch"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn terminal_exit_accepts_existing_cleanup_before_its_deadline() {
        let Harness {
            mut fleet,
            completed,
            ..
        } = replacing(true);
        let started = tokio::time::Instant::now();
        let completion = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(1200)).await;
            completed.send(true).unwrap();
        });
        fleet.stop_for_exit().await;
        completion.await.unwrap();
        assert_eq!(started.elapsed(), Duration::from_millis(1200));
    }

    #[test]
    fn unarmed_relaunch_leaves_on_success_and_acks_on_failure() {
        for succeeds in [true, false] {
            let log = Log::default();
            let mut fleet: TestFleet = WatcherFleet::with_relaunch(ScriptedRelaunch {
                log: Arc::clone(&log),
                succeeds,
            });
            let (request, mut retried) = new_request();
            let outcome = fleet.relaunch_unarmed(request);

            assert_eq!(*log.lock().unwrap(), [Step::Relaunch]);
            assert!(matches!(fleet.state, FleetState::Inactive));
            match outcome {
                ReplacementOutcome::Leaving(_handover) => {
                    assert!(succeeds);
                    assert!(not_acked(&mut retried));
                }
                ReplacementOutcome::Resumed => {
                    assert!(!succeeds);
                    retried.try_recv().expect("failed unarmed relaunch acks");
                }
            }
        }
    }
}
