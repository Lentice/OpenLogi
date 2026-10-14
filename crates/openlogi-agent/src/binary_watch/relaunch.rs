//! How a restart is actually carried out, per OS.
//!
//! [`super`] decides *that* the agent should become the binary now on disk;
//! this module knows what that costs on each platform, and the three answers
//! have nothing in common:
//!
//! - **Linux and the other unixes** `exec` the new image in place, which keeps
//!   the pid, the singleton lock, and the IPC socket.
//! - **macOS** cannot: after `exec` the new program continues on the calling
//!   thread, while AppKit insists the status item be created from the process
//!   main thread. So it schedules a detached successor that waits for this
//!   process to exit. Going through LaunchServices (`open`) rather than
//!   spawning the binary directly preserves the helper's TCC identity — see
//!   `.claude/rules/objc-ffi.md` and the permissions skill.
//! - **Windows** has no `exec`; the lifecycle exits after teardown and lets
//!   the GUI's socket-down spawn (or the next login) start the replacement.
//!
//! The macOS path also serves the Input Monitoring grant, which needs the same
//! "leave and come back" move for an unrelated reason.

use std::io;
use std::path::Path;

#[cfg(all(unix, not(target_os = "macos")))]
use tracing::info;

/// The process-replacement seam: arrange for the binary at `path` to take
/// over from this process.
///
/// The lifecycle's watcher fleet calls this only after every HID++ manager
/// confirmed firmware teardown, and owns everything that follows: `Ok` means
/// this process must now leave; `Err` means the current image keeps running,
/// so the fleet restores itself before the binary watcher may retry.
pub(crate) trait Relaunch {
    /// Arrange the successor, or report why the current image must stay.
    fn relaunch(&mut self, path: &Path) -> io::Result<()>;
}

/// This OS's real relaunch; the module docs say what each one costs.
pub(crate) struct OsRelaunch;

#[cfg(all(unix, not(target_os = "macos")))]
impl Relaunch for OsRelaunch {
    /// `exec` returns only on failure.
    fn relaunch(&mut self, path: &Path) -> io::Result<()> {
        Err(replace_process(path))
    }
}

#[cfg(target_os = "macos")]
impl Relaunch for OsRelaunch {
    fn relaunch(&mut self, path: &Path) -> io::Result<()> {
        schedule(path)
    }
}

#[cfg(not(unix))]
impl Relaunch for OsRelaunch {
    /// Windows arranges nothing: the lifecycle exits after teardown and the
    /// GUI's socket-down spawn (or the next login) starts the replacement.
    fn relaunch(&mut self, _path: &Path) -> io::Result<()> {
        Ok(())
    }
}

/// Restart this process as the new binary at `path`; returns only when `exec`
/// failed.
#[cfg(all(unix, not(target_os = "macos")))]
fn replace_process(path: &Path) -> io::Error {
    use std::os::unix::process::CommandExt as _;
    info!(
        path = %path.display(),
        "executable changed on disk — restarting as the new binary"
    );
    // Forward our argv (none today) so a future flag survives the restart.
    std::process::Command::new(path)
        .args(std::env::args_os().skip(1))
        .exec()
}

/// Schedule a macOS successor without ending the current process.
///
/// The packaged helper goes through LaunchServices to preserve its TCC
/// identity; a bare dev binary starts directly. The successor waits for this
/// exact PID rather than a fixed delay, so graceful firmware teardown cannot
/// make it lose the singleton-lock race and disappear.
#[cfg(target_os = "macos")]
fn schedule(path: &Path) -> io::Result<()> {
    let mut command = std::process::Command::new("/bin/sh");
    let pid = std::process::id().to_string();
    if let Some(bundle) = openlogi_core::brand::helper_bundle_root(path) {
        command
            .arg("-c")
            .arg(
                "pid=$1; bundle=$2; while /bin/kill -0 \"$pid\" 2>/dev/null; do /bin/sleep 0.1; done; exec /usr/bin/open -g -n \"$bundle\"",
            )
            .arg("openlogi-relaunch")
            .arg(&pid)
            .arg(bundle);
    } else {
        command
            .arg("-c")
            .arg(
                "pid=$1; path=$2; shift 2; while /bin/kill -0 \"$pid\" 2>/dev/null; do /bin/sleep 0.1; done; exec \"$path\" \"$@\"",
            )
            .arg("openlogi-relaunch")
            .arg(&pid)
            .arg(path)
            .args(std::env::args_os().skip(1));
    }
    command.spawn().map(|_| ())
}

/// Schedule the macOS agent's required relaunch after Input Monitoring is
/// granted. Returns whether scheduling succeeded; the lifecycle performs the
/// actual graceful exit.
///
/// macOS does not apply a new Input Monitoring grant to the running process.
#[cfg(target_os = "macos")]
pub(crate) fn schedule_after_input_monitoring_grant() -> bool {
    use tracing::{info, warn};

    let path = match std::env::current_exe() {
        Ok(path) => path,
        Err(e) => {
            warn!(error = %e, "could not resolve own executable after Input Monitoring was granted — restart the agent manually");
            return false;
        }
    };
    info!("Input Monitoring granted — relaunching the macOS agent");
    match schedule(&path) {
        Ok(()) => true,
        Err(e) => {
            warn!(error = %e, "could not schedule agent relaunch after Input Monitoring was granted — restart the agent manually");
            false
        }
    }
}
