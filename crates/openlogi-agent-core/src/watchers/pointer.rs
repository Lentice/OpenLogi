//! Off-tap pointer-window discovery with a coalescing publication.

use std::time::Duration;

use openlogi_core::config::MouseProfileTarget;
use openlogi_hook::{PointerContext, PointerTarget};
use tokio::sync::watch;
use tracing::warn;

/// Pause between the end of one pointer-mode query and the next.
const SAMPLE_PERIOD: Duration = Duration::from_millis(50);

/// Config-owned identity of a pointer discovery session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DiscoveryDemand {
    /// Selection policy for mouse application profiles.
    pub mode: MouseProfileTarget,
    /// Changes whenever the policy changes, including rapid round trips.
    pub generation: u64,
}

/// A sample carries the discovery session that requested it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Observation {
    /// Session identity used to reject delayed publications.
    pub demand: DiscoveryDemand,
    /// Capability and current hovered application/window.
    pub context: PointerContext,
}

/// Observe pointer windows only while pointer-selected profiles are enabled.
/// Queries stay off input callbacks; suspended discovery keeps its channel live.
#[must_use]
pub fn spawn(demand: watch::Receiver<DiscoveryDemand>) -> watch::Receiver<Observation> {
    spawn_with_query(
        demand,
        openlogi_hook::pointer_context_supported(),
        openlogi_hook::pointer_context,
    )
}

fn spawn_with_query(
    demand: watch::Receiver<DiscoveryDemand>,
    supported: bool,
    query: impl FnMut() -> PointerContext + Send + 'static,
) -> watch::Receiver<Observation> {
    spawn_with_wait_observer(demand, supported, query, |_| {})
}

fn spawn_with_wait_observer(
    mut demand: watch::Receiver<DiscoveryDemand>,
    supported: bool,
    mut query: impl FnMut() -> PointerContext + Send + 'static,
    mut waiting: impl FnMut(DiscoveryDemand) + Send + 'static,
) -> watch::Receiver<Observation> {
    let (tx, rx) = watch::channel(Observation {
        demand: *demand.borrow(),
        context: PointerContext {
            app: None,
            target: if supported {
                PointerTarget::Unavailable
            } else {
                PointerTarget::Unsupported
            },
        },
    });
    if !supported {
        return rx;
    }
    let spawned = openlogi_core::worker::spawn("openlogi-pointer-context", move |runtime| {
        loop {
            let session = *demand.borrow_and_update();
            if session.mode == MouseProfileTarget::Pointer {
                let context = query();
                // The orchestrator checks this identity again under its lock:
                // a reload can happen after this check or before consumption.
                if *demand.borrow() == session {
                    let current = Observation {
                        demand: session,
                        context,
                    };
                    tx.send_if_modified(|previous| {
                        if *previous == current {
                            return false;
                        }
                        *previous = current;
                        true
                    });
                }
            }
            waiting(session);
            let finished = if session.mode == MouseProfileTarget::Pointer {
                // A Tokio timer on this current-thread runtime wakes Windows
                // about twice per period; a plain sleep wakes once. Demand
                // is reread before the next query, so a transition or
                // shutdown waits out at most the current sleep.
                std::thread::sleep(SAMPLE_PERIOD);
                tx.is_closed() || demand.has_changed().is_err()
            } else {
                runtime.block_on(async {
                    tokio::select! {
                        () = tx.closed() => true,
                        changed = demand.changed() => changed.is_err(),
                    }
                })
            };
            if finished {
                break;
            }
        }
    });
    if let Err(error) = spawned {
        warn!(%error, "could not start pointer context watcher — pointer remaps remain unavailable");
    }
    rx
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    #[tokio::test]
    async fn focused_discovery_skips_queries_and_round_trip_discards_inflight_sample() {
        let focused = DiscoveryDemand {
            mode: MouseProfileTarget::Focused,
            generation: 0,
        };
        let (demand_tx, demand_rx) = watch::channel(focused);
        let (queried_tx, queried_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (waiting_tx, waiting_rx) = mpsc::channel();
        let mut calls = 0;
        let mut observations = spawn_with_wait_observer(
            demand_rx,
            true,
            move || {
                calls += 1;
                queried_tx.send(calls).unwrap();
                if release_rx.recv_timeout(Duration::from_secs(5)).is_err() {
                    return PointerContext {
                        app: None,
                        target: PointerTarget::Unavailable,
                    };
                }
                PointerContext {
                    app: None,
                    target: PointerTarget::Window {
                        process_id: 1,
                        window_id: calls,
                    },
                }
            },
            move |session| {
                let _ = waiting_tx.send(session);
            },
        );
        assert_eq!(
            waiting_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
            focused
        );
        assert_eq!(queried_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        assert_eq!(
            observations.borrow().context.target,
            PointerTarget::Unavailable
        );
        let first = DiscoveryDemand {
            mode: MouseProfileTarget::Pointer,
            generation: 1,
        };
        demand_tx.send_replace(first);
        assert_eq!(queried_rx.recv_timeout(Duration::from_secs(5)).unwrap(), 1);
        // The first native query is still blocked while both transitions occur.
        demand_tx.send_replace(DiscoveryDemand {
            mode: MouseProfileTarget::Focused,
            generation: 2,
        });
        let resumed = DiscoveryDemand {
            mode: MouseProfileTarget::Pointer,
            generation: 3,
        };
        demand_tx.send_replace(resumed);
        release_tx.send(()).unwrap();
        assert_eq!(queried_rx.recv_timeout(Duration::from_secs(5)).unwrap(), 2);
        assert_eq!(
            observations.borrow().context.target,
            PointerTarget::Unavailable
        );
        release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), observations.changed())
            .await
            .unwrap()
            .unwrap();
        let fresh = observations.borrow_and_update().clone();
        assert_eq!(fresh.demand, resumed);
        assert_eq!(
            fresh.context.target,
            PointerTarget::Window {
                process_id: 1,
                window_id: 2
            }
        );
        // Drop the output and demand so the dedicated worker shuts down.
        drop(observations);
        drop(demand_tx);
    }

    #[test]
    fn pointer_discovery_stops_when_output_or_demand_closes() {
        for drop_output in [true, false] {
            let (demand_tx, demand_rx) = watch::channel(DiscoveryDemand {
                mode: MouseProfileTarget::Pointer,
                generation: 0,
            });
            let (queried_tx, queried_rx) = mpsc::channel();
            let observations = spawn_with_query(demand_rx, true, move || {
                let _ = queried_tx.send(());
                PointerContext {
                    app: None,
                    target: PointerTarget::Desktop,
                }
            });
            queried_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            if drop_output {
                drop(observations);
            } else {
                drop(demand_tx);
            }
            // The query closure owns the sender, so disconnection proves the
            // sleeping worker noticed the closure and exited.
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                match queried_rx
                    .recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
                {
                    Ok(()) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        panic!("pointer worker kept running (drop_output = {drop_output})")
                    }
                }
            }
        }
    }

    #[test]
    fn unsupported_discovery_needs_no_ambient_runtime_or_native_queries() {
        let (_, demand) = watch::channel(DiscoveryDemand {
            mode: MouseProfileTarget::Focused,
            generation: 0,
        });
        let observed = spawn_with_query(demand, false, || panic!("unsupported query must not run"));
        assert_eq!(observed.borrow().context.target, PointerTarget::Unsupported);
    }
}
