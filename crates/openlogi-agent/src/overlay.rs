//! Supervision of the warm Actions Ring overlay helper.
//!
//! The helper owns no device state and exits harmlessly when its binary is not
//! packaged. Usable ring bindings keep it warm; otherwise an actual invocation
//! starts it on demand, and an unused helper exits without polling config files.
//!
//! Exactly one overlay may exist, and it belongs to one agent run. Both halves
//! are enforced by the `succession` crate: this supervisor waits while the role
//! is filled by its own child, and evicts a tenant left behind by a previous
//! agent — which is what stops an orphaned overlay from wedging its
//! replacement out of the lock forever (#621, #644). The run token travels to
//! the child in the environment, so a helper started by a previous agent is
//! recognizable on sight rather than after a timeout.

use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use openlogi_core::brand;
use openlogi_ipc::RUN_ENV;
use succession::eviction::{self, AnonymousOutcome, Policy};
use succession::supervision::Restart;
use succession::{Occupancy, Record, Role, Run, Verdict, verdict};
use tokio::sync::watch;
use tracing::{info, warn};

// Matches succession's private Supervisor default for pre-record helper migration.
const ANONYMOUS_GRACE: Duration = Duration::from_secs(15);

struct OwnedChild(Child);

impl Drop for OwnedChild {
    fn drop(&mut self) {
        if !matches!(self.0.try_wait(), Ok(Some(_))) {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}

/// The armed lifecycle owns completion of helper teardown before process exit.
pub(crate) struct Session {
    shutdown: watch::Sender<bool>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl Session {
    pub(crate) async fn stop(mut self) {
        self.shutdown.send_replace(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.shutdown.send_replace(true);
    }
}

/// Start supervision after arming; a disabled ring creates no helper process.
pub(crate) fn spawn(
    required: watch::Receiver<bool>,
    ring: watch::Receiver<openlogi_ipc::RingObservation>,
) -> Option<Session> {
    let Some(binary) = overlay_binary_path() else {
        warn!("Actions Ring overlay binary not found — overlay disabled");
        return None;
    };
    let Ok(directory) = openlogi_core::paths::config_dir() else {
        warn!("could not resolve the config directory — overlay disabled");
        return None;
    };
    let mine = Run::mint();
    let (shutdown, stopping) = watch::channel(false);
    let task = tokio::spawn(supervise(
        required,
        ring,
        stopping,
        Role::new(directory, "overlay"),
        mine,
        move || {
            Command::new(&binary)
                .env(RUN_ENV, mine.get().to_string())
                .spawn()
        },
        |pid| info!(pid, "Actions Ring overlay stopped"),
    ));
    Some(Session {
        shutdown,
        task: Some(task),
    })
}

#[derive(PartialEq, Eq)]
enum Activity {
    Required,
    Unused,
    Stopped,
}

/// One authority for config demand, an open snapshot, and terminal shutdown.
struct Demand {
    required: watch::Receiver<bool>,
    ring: watch::Receiver<openlogi_ipc::RingObservation>,
    shutdown: watch::Receiver<bool>,
}

impl Demand {
    fn activity(&mut self) -> Activity {
        let stopping = *self.shutdown.borrow_and_update();
        let configured = *self.required.borrow_and_update();
        let showing = self.ring.borrow_and_update().invocation.is_some();
        if stopping {
            Activity::Stopped
        } else if configured || showing {
            Activity::Required
        } else {
            Activity::Unused
        }
    }

    async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        tokio::select! {
            result = self.required.changed() => result,
            result = self.ring.changed() => result,
            result = self.shutdown.changed() => result,
        }
    }
}

async fn supervise(
    required: watch::Receiver<bool>,
    ring: watch::Receiver<openlogi_ipc::RingObservation>,
    shutdown: watch::Receiver<bool>,
    role: Role,
    mine: Run,
    mut spawn: impl FnMut() -> std::io::Result<Child> + Send,
    mut on_stopped: impl FnMut(u32) + Send,
) {
    let mut demand = Demand {
        required,
        ring,
        shutdown,
    };
    let restart = Restart::default();
    let mut delay = restart.base;
    let mut retry_at = Instant::now();
    let mut child: Option<(OwnedChild, Instant)> = None;
    let mut waiting_since = None;
    let mut pressed_anonymous = false;
    loop {
        let activity = demand.activity();
        if activity != Activity::Required {
            if let Some((child, _)) = child.take() {
                let pid = child.0.id();
                stop_child(child, role.clone()).await;
                on_stopped(pid);
            }
            // Also retire an orphan from the preceding run when starting disabled.
            let role = role.clone();
            let _ = tokio::task::spawn_blocking(move || retire_role(&role)).await;
            if activity == Activity::Stopped || demand.changed().await.is_err() {
                return;
            }
            retry_at = Instant::now();
            delay = restart.base;
            waiting_since = None;
            continue;
        }
        if let Some((running, started)) = child.as_mut() {
            match running.0.try_wait() {
                Ok(Some(status)) => {
                    let ran_for = started.elapsed();
                    info!(%status, ?ran_for, "Actions Ring overlay exited");
                    delay = restart.next_delay(delay, ran_for);
                    retry_at = Instant::now() + delay;
                    child = None;
                }
                Ok(None) => {}
                Err(error) => {
                    warn!(%error, "could not wait for the Actions Ring overlay");
                    if let Some((child, _)) = child.take() {
                        stop_child(child, role.clone()).await;
                    }
                    retry_at = Instant::now() + delay;
                }
            }
        } else if Instant::now() >= retry_at {
            match role.occupancy() {
                Ok(occupancy) => {
                    let waited = waiting_since.map_or(Duration::ZERO, |at: Instant| at.elapsed());
                    match verdict(&occupancy, mine, waited, ANONYMOUS_GRACE) {
                        Verdict::Start => {
                            waiting_since = None;
                            // Recheck after role I/O; a disabled helper must not respawn.
                            if demand.activity() == Activity::Required {
                                match spawn() {
                                    Ok(running) => {
                                        child = Some((OwnedChild(running), Instant::now()));
                                    }
                                    Err(error) => {
                                        warn!(%error, "could not start the Actions Ring overlay");
                                        delay = restart.next_delay(delay, Duration::ZERO);
                                        retry_at = Instant::now() + delay;
                                    }
                                }
                            }
                        }
                        Verdict::Wait => {
                            waiting_since.get_or_insert_with(Instant::now);
                        }
                        Verdict::Evict(record) => retire_superseded(record).await,
                        Verdict::EvictAnonymous => {
                            if !pressed_anonymous {
                                pressed_anonymous = true;
                                let _ =
                                    tokio::task::spawn_blocking(evict_unidentified_overlay).await;
                            }
                        }
                    }
                    if !matches!(occupancy, Occupancy::HeldAnonymously) {
                        pressed_anonymous = false;
                    }
                }
                Err(error) => warn!(%error, "could not read the Actions Ring overlay role"),
            }
        }
        tokio::select! {
            result = demand.changed() => {
                if result.is_err() {
                    if let Some((child, _)) = child.take() { stop_child(child, role.clone()).await; }
                    return;
                }
            }
            () = tokio::time::sleep(Duration::from_millis(500)) => {}
        }
    }
}

async fn stop_child(child: OwnedChild, role: Role) {
    let _ = tokio::task::spawn_blocking(move || {
        if let Ok(Occupancy::HeldBy(record)) = role.occupancy()
            && record.tenant.pid == child.0.id()
        {
            let _ = eviction::evict(&record, &quit_policy());
        }
        // The owned handle covers a child that has not published its role yet.
        drop(child);
    })
    .await;
}

fn quit_policy() -> Policy {
    Policy {
        escalate_after: Some(Duration::from_millis(150)),
        deadline: Duration::from_millis(750),
        ..Policy::default()
    }
}

fn retire_role(role: &Role) {
    match role.occupancy() {
        Ok(Occupancy::HeldBy(record)) => {
            let _ = eviction::evict(&record, &quit_policy());
        }
        Ok(Occupancy::HeldAnonymously) => evict_unidentified_overlay(),
        Ok(Occupancy::Free) | Err(_) => {}
    }
}

/// Best-effort helper teardown when the tray's lifecycle owner is unavailable.
/// Normal shutdown first disables supervision and waits for its owned child.
#[cfg(any(target_os = "macos", target_os = "windows"))]
pub fn evict_on_quit() {
    let Ok(directory) = openlogi_core::paths::config_dir() else {
        return;
    };
    let Ok(Occupancy::HeldBy(record)) = Role::new(directory, "overlay").occupancy() else {
        return;
    };
    let outcome = eviction::evict(&record, &quit_policy());
    info!(?outcome, "asked the overlay to leave before exiting");
}

/// Retire an identified obsolete tenant without blocking the async lifecycle.
/// Succession verifies its process identity before signalling the recorded PID.
async fn retire_superseded(record: Record) {
    let _ =
        tokio::task::spawn_blocking(move || match eviction::evict(&record, &Policy::default()) {
            eviction::Outcome::Refused(sameness) => {
                warn!(
                    ?sameness,
                    "left the overlay alone — its pid no longer matches"
                );
            }
            outcome => info!(?outcome, "asked the superseded overlay to leave"),
        })
        .await;
}

/// Ask an unidentified tenant to leave, trying every image our overlay could
/// be running from.
///
/// One path is not enough. A tenant started before an update runs the image
/// that install shipped, and macOS keeps reporting that path for the life of
/// the process even after the file is renamed or deleted — so the tenant most
/// in need of evicting is precisely the one whose image is not the one we
/// would launch today (#842).
fn evict_unidentified_overlay() {
    for image in overlay_images() {
        match eviction::evict_anonymous(&image, &Policy::default()) {
            // Not this image. The tenant may be running another of ours.
            AnonymousOutcome::NoCandidate => {}
            AnonymousOutcome::Ambiguous { running } => {
                warn!(
                    running,
                    image = %image.display(),
                    "several processes share this overlay image — left them alone rather \
                     than guess which one holds the role"
                );
                return;
            }
            outcome => {
                info!(
                    ?outcome,
                    image = %image.display(),
                    "asked the unidentified overlay to leave"
                );
                return;
            }
        }
    }
    warn!("no process is running any overlay image of ours — the role is held by something else");
}

/// Every path our overlay could be running from, in the order the launcher
/// prefers them.
///
/// Existence is deliberately not a filter: a process outlives the file it was
/// started from, and evicting one means recognizing the path it still reports.
/// [`overlay_binary_path`] applies the filter, because launching does need a
/// file that is there.
fn overlay_images() -> Vec<PathBuf> {
    let Ok(executable) = std::env::current_exe() else {
        return Vec::new();
    };
    let mut images = overlay_images_beside(&executable);
    images.extend(find_on_path(brand::Helper::Overlay.executable()));
    images
}

/// The layout-derived half of [`overlay_images`], taking the agent's own path
/// so the derivation can be tested against a bundle that is not this one.
fn overlay_images_beside(executable: &Path) -> Vec<PathBuf> {
    let sibling = executable.parent().map(|directory| {
        directory.join(format!(
            "{}{}",
            brand::Helper::Overlay.executable(),
            std::env::consts::EXE_SUFFIX
        ))
    });
    sibling
        .into_iter()
        .chain(bundled_overlay_images(executable))
        .collect()
}

/// Every helper bundle an agent at `executable` could have shipped with, on
/// the platform that bundles its helpers.
#[cfg(target_os = "macos")]
fn bundled_overlay_images(executable: &Path) -> Vec<PathBuf> {
    executable
        .ancestors()
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("app"))
        })
        .flat_map(|app| {
            brand::Helper::Overlay
                .executable_candidates()
                .into_iter()
                .map(move |relative| app.join(relative))
        })
        .collect()
}

#[cfg(not(target_os = "macos"))]
fn bundled_overlay_images(_executable: &Path) -> Vec<PathBuf> {
    Vec::new()
}

fn overlay_binary_path() -> Option<PathBuf> {
    overlay_images().into_iter().find(|image| image.is_file())
}

fn find_on_path(name: &str) -> Option<PathBuf> {
    std::env::var_os("PATH").and_then(|paths| {
        std::env::split_paths(&paths)
            .map(|directory| directory.join(name))
            .find(|candidate| candidate.is_file())
    })
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::process::Stdio;

    use super::*;

    // Spawned only by the lifecycle test, never by a normal test invocation.
    #[test]
    #[ignore = "harmless subprocess for the overlay lifecycle test"]
    fn helper_child() {
        loop {
            std::thread::park();
        }
    }

    #[tokio::test]
    async fn helper_lifecycle_follows_ring_demand() {
        let directory = tempfile::tempdir().expect("private test role directory");
        let (required, demand) = watch::channel(false);
        let manager = openlogi_agent_core::action_ring::ActionRingManager::default();
        let (shutdown, stopping) = watch::channel(false);
        let (started, mut starts) = tokio::sync::mpsc::unbounded_channel();
        let (stopped, mut stops) = tokio::sync::mpsc::unbounded_channel();
        let task = tokio::spawn(supervise(
            demand,
            manager.subscribe(),
            stopping,
            Role::new(directory.path(), "test-overlay"),
            Run::mint(),
            move || {
                let child = Command::new(std::env::current_exe()?)
                    .args(["--exact", "overlay::tests::helper_child", "--ignored"])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()?;
                started.send(child.id()).expect("test receiver is alive");
                Ok(child)
            },
            move |pid| {
                let _ = stopped.send(pid);
            },
        ));
        let session = Session {
            shutdown,
            task: Some(task),
        };
        assert!(
            tokio::time::timeout(Duration::from_millis(100), starts.recv())
                .await
                .is_err(),
            "disabled startup must not spawn a helper"
        );
        let showing = manager.begin(openlogi_agent_core::action_ring::ActionRingSessionSpec {
            device_key: "mouse".into(),
            haptic_route: None,
            layout: openlogi_core::binding::ActionRingConfig::default().default,
            language: None,
        });
        let first = tokio::time::timeout(Duration::from_secs(5), starts.recv())
            .await
            .expect("a real invocation starts even without a capability proxy")
            .expect("spawn channel alive");
        required.send_replace(true);
        required.send_replace(false);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), starts.recv())
                .await
                .is_err(),
            "disable must not respawn the helper"
        );
        assert!(
            succession::Tenant::look_up(first).is_some(),
            "a showing snapshot outlives a profile change"
        );
        manager.cancel(showing.session_id);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), stops.recv())
                .await
                .expect("dismissal stops the unused helper")
                .expect("stop channel alive"),
            first
        );
        required.send_replace(true);
        let second = tokio::time::timeout(Duration::from_secs(5), starts.recv())
            .await
            .expect("reenable starts promptly")
            .expect("spawn channel alive");
        assert_ne!(first, second);
        assert!(
            succession::Tenant::look_up(first).is_none(),
            "reenable waits for old child teardown"
        );
        session.stop().await;
        assert!(
            succession::Tenant::look_up(second).is_none(),
            "shutdown reaps the owned child"
        );
        required.send_replace(true);
        assert!(
            starts.recv().await.is_none(),
            "shutdown closes the spawning loop"
        );
    }

    #[tokio::test]
    async fn an_enabled_helper_restarts_after_exit() {
        let directory = tempfile::tempdir().expect("private test role directory");
        let (_required, demand) = watch::channel(true);
        let manager = openlogi_agent_core::action_ring::ActionRingManager::default();
        let (shutdown, stopping) = watch::channel(false);
        let (started, mut starts) = tokio::sync::mpsc::unbounded_channel();
        let mut first = true;
        let task = tokio::spawn(supervise(
            demand,
            manager.subscribe(),
            stopping,
            Role::new(directory.path(), "test-overlay"),
            Run::mint(),
            move || {
                // An unmatched test exits immediately, modelling a failed helper start.
                let test = if first {
                    "overlay::tests::absent_helper"
                } else {
                    "overlay::tests::helper_child"
                };
                first = false;
                let child = Command::new(std::env::current_exe()?)
                    .args(["--exact", test, "--ignored"])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()?;
                started.send(child.id()).expect("test receiver is alive");
                Ok(child)
            },
            |_| {},
        ));
        let session = Session {
            shutdown,
            task: Some(task),
        };
        let first = tokio::time::timeout(Duration::from_secs(5), starts.recv())
            .await
            .expect("initial child starts")
            .expect("channel alive");
        let replacement = tokio::time::timeout(Duration::from_secs(8), starts.recv())
            .await
            .expect("enabled child restarts after backoff")
            .expect("channel alive");
        assert_ne!(first, replacement);
        assert!(
            succession::Tenant::look_up(first).is_none(),
            "exited child is reaped"
        );
        session.stop().await;
        assert!(
            succession::Tenant::look_up(replacement).is_none(),
            "replacement is reaped on shutdown"
        );
        assert!(starts.recv().await.is_none());
    }

    /// The tenant that wedges the role is one started before an update, and
    /// after the helpers were renamed its image is a path that no longer
    /// exists. macOS keeps reporting that path for the life of the process, so
    /// dropping absent paths here would leave exactly that tenant
    /// unrecognizable (#842).
    #[test]
    #[cfg(target_os = "macos")]
    fn eviction_candidates_include_images_that_are_no_longer_installed() {
        let agent = Path::new(
            "/Applications/OpenLogi.app/Contents/Library/LoginItems/OpenLogi Agent.app/Contents/MacOS/openlogi-agent",
        );
        let legacy = Path::new(
            "/Applications/OpenLogi.app/Contents/Library/LoginItems/OpenLogiOverlay.app/Contents/MacOS/openlogi-overlay",
        );
        assert!(
            !legacy.exists(),
            "this test only means something while that path is absent"
        );

        let images = overlay_images_beside(agent);
        assert!(
            images.iter().any(|image| image == legacy),
            "the pre-rename image must stay a candidate: {images:?}"
        );
    }

    #[test]
    fn path_search_returns_none_for_an_impossible_name() {
        assert_eq!(
            find_on_path("openlogi-overlay-this-file-does-not-exist"),
            None
        );
    }

    #[test]
    fn nested_overlay_path_has_expected_layout() {
        let outer = Path::new("/Applications/OpenLogi.app");
        assert_eq!(
            outer.join(
                "Contents/Library/LoginItems/OpenLogi Overlay.app/Contents/MacOS/openlogi-overlay"
            ),
            Path::new(
                "/Applications/OpenLogi.app/Contents/Library/LoginItems/OpenLogi Overlay.app/Contents/MacOS/openlogi-overlay"
            )
        );
    }
}
