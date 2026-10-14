//! The Actions Ring's single-flight haptic worker: latest-wins coalescing,
//! per-interaction time budgets, the HID++-loss cool-down, and firmware
//! arming sequenced before a session's first buzz.

use std::sync::Arc;
use std::time::Duration;

use openlogi_agent_core::orchestrator::SharedHandles;
use openlogi_hid::{DeviceRoute, HapticWaveform, HidppOperation, WriteError};
// Tokio's clock, not std's: identical in production, and it lets a paused
// test runtime drive the budgets and the cool-down without sleeping.
use tokio::time::Instant;
use tracing::{info, warn};

/// The device side of ring feedback: one HID++ call per method, no retries.
/// Retrying, budgeting, and coalescing belong to the worker.
pub(crate) trait HapticOutput: Send + Sync + 'static {
    /// Make sure the firmware haptic engine is enabled. `Ok(true)` means it
    /// had been disarmed and was re-enabled.
    fn ensure_armed(
        &self,
        route: &DeviceRoute,
    ) -> impl Future<Output = Result<bool, WriteError>> + Send;

    /// Play one waveform on `route`.
    fn play(
        &self,
        route: &DeviceRoute,
        waveform: HapticWaveform,
    ) -> impl Future<Output = Result<(), WriteError>> + Send;
}

/// Production feedback goes through the agent's device-operation path.
impl HapticOutput for SharedHandles {
    fn ensure_armed(
        &self,
        route: &DeviceRoute,
    ) -> impl Future<Output = Result<bool, WriteError>> + Send {
        self.device(route)
            .run(HidppOperation::PlayHaptic, |c| async move {
                openlogi_hid::ensure_haptics_armed_on(&c).await
            })
    }

    fn play(
        &self,
        route: &DeviceRoute,
        waveform: HapticWaveform,
    ) -> impl Future<Output = Result<(), WriteError>> + Send {
        self.device(route)
            .run(HidppOperation::PlayHaptic, move |c| async move {
                openlogi_hid::play_haptic_on(&c, waveform).await
            })
    }
}

type Request = Option<(DeviceRoute, HapticWaveform, &'static str)>;

/// Coalescing Actions Ring haptic player: at most one waveform is in flight,
/// and while it plays only the newest queued request survives (latest wins).
///
/// Spawning one task per hover let a fast pointer queue HID++ plays faster
/// than the receiver drains them; the backlog then times out every
/// transaction on the channel for seconds at a time — dead haptics, and
/// collateral timeouts for unrelated writes (DPI, SmartShift) on the same
/// receiver. A stale hover buzz has no value, so dropping superseded requests
/// is strictly better than queueing them.
#[derive(Clone)]
pub(super) struct RingHapticPlayer {
    tx: tokio::sync::watch::Sender<Request>,
    pending_arm: Arc<std::sync::Mutex<Option<DeviceRoute>>>,
}

/// How long every attempt at one buzz may take together.
///
/// The write path bounds a single call at its own 5 s, which is sized for a
/// user-visible setting write, not for feedback: three attempts of it outlast
/// the ring's own 15 s lifetime, so one hover on a lossy channel could consume
/// a whole session while every later interaction — the activation buzz
/// included — was dropped by this worker's latest-wins channel. Past a couple
/// of seconds a buzz is not feedback any more anyway.
const PLAY_BUDGET: Duration = Duration::from_secs(2);

/// How long the firmware arming check may take before the first buzz.
///
/// Arming is a repair: a play works without it on any device that never lost
/// the state, so it must not be the thing that makes a session feel dead. Two
/// attempts at the write path's budget put 10 s in front of the first buzz.
const ARM_BUDGET: Duration = Duration::from_secs(1);

/// Consecutive failed buzzes that open the cool-down.
const BREAKER_THRESHOLD: u32 = 3;

/// How long haptics stay silent once the breaker opens.
const COOLDOWN: Duration = Duration::from_secs(4);

/// Re-assert the firmware haptic engine before the session's first buzz,
/// within [`ARM_BUDGET`].
///
/// Best-effort by design: a device that never lost the state plays fine
/// without this, so a silent channel must cost the session a bounded delay
/// rather than its whole haptic budget.
async fn arm_firmware_haptics(output: &impl HapticOutput, route: &DeviceRoute) {
    let budget = Budget::starting_at(Instant::now(), ARM_BUDGET);
    for attempt in 1..=2u8 {
        let Some(remaining) = budget.remaining(Instant::now()) else {
            warn!("firmware haptic check ran out of time — leaving it to the buzz");
            return;
        };
        match tokio::time::timeout(remaining, output.ensure_armed(route)).await {
            Ok(Ok(true)) => {
                info!("firmware haptics were disarmed — re-enabled");
                return;
            }
            Ok(Ok(false)) => return,
            Ok(Err(error)) => {
                if attempt == 2 {
                    warn!(%error, "could not verify firmware haptic state");
                }
            }
            // Out of budget mid-attempt; the loop head reports it and returns.
            Err(_elapsed) => {}
        }
    }
}

/// Play one waveform, retrying within [`PLAY_BUDGET`]. Returns whether it played.
///
/// A busy receiver drops HID++ replies under concurrent pointer traffic, so a
/// single attempt fails exactly when the user is actively hovering. Retrying
/// is worth it — but only until either the budget is gone or a newer request
/// supersedes this one, since a stale buzz has no value and this worker is
/// single-flight.
async fn play_within_budget(
    output: &impl HapticOutput,
    rx: &tokio::sync::watch::Receiver<Request>,
    route: &DeviceRoute,
    waveform: HapticWaveform,
    interaction: &'static str,
) -> bool {
    let budget = Budget::starting_at(Instant::now(), PLAY_BUDGET);
    for attempt in 1..=3u8 {
        let Some(remaining) = budget.remaining(Instant::now()) else {
            warn!(
                interaction,
                "ring haptic gave up — out of time to be feedback"
            );
            return false;
        };
        match tokio::time::timeout(remaining, output.play(route, waveform)).await {
            Ok(Ok(())) => return true,
            Ok(Err(error)) => {
                let superseded = rx.has_changed().unwrap_or(true);
                if attempt == 3 || superseded {
                    warn!(%error, interaction, attempt, superseded, "Actions Ring haptic failed");
                    return false;
                }
            }
            // Out of budget mid-attempt; the loop head reports it and stops,
            // so a lost reply and a slow one stay distinguishable in the log.
            Err(_elapsed) => {}
        }
    }
    false
}

/// A wall-clock allowance shared by every attempt at one interaction.
///
/// The worker is single-flight, so time one buzz spends retrying is time the
/// next interaction spends waiting. Attempts therefore share a budget rather
/// than each getting the write path's full one.
#[derive(Clone, Copy)]
struct Budget {
    deadline: Instant,
}

impl Budget {
    fn starting_at(now: Instant, total: Duration) -> Self {
        Self {
            deadline: now + total,
        }
    }

    /// How long the next attempt may take, or `None` when nothing is left.
    ///
    /// A spent budget is deliberately `None` rather than `Some(ZERO)`:
    /// `timeout(ZERO, …)` elapses before the call it wraps can run, which
    /// would log a device failure that never happened.
    fn remaining(self, now: Instant) -> Option<Duration> {
        let left = self.deadline.saturating_duration_since(now);
        (!left.is_zero()).then_some(left)
    }
}

impl RingHapticPlayer {
    /// Spawn the single-flight worker over `output`. Must be called from a
    /// tokio runtime.
    pub(super) fn spawn(output: impl HapticOutput) -> Self {
        let (tx, mut rx) = tokio::sync::watch::channel::<Request>(None);
        let pending_arm = Arc::new(std::sync::Mutex::new(None::<DeviceRoute>));
        let worker_arm = Arc::clone(&pending_arm);
        tokio::spawn(async move {
            let mut consecutive_failures = 0u32;
            let mut cooldown_until: Option<Instant> = None;
            while rx.changed().await.is_ok() {
                // Firmware arming is sequenced through this worker so it is
                // guaranteed to complete before the session's first buzz —
                // spawning it separately let the first hover race (and lose
                // to) a disarmed haptic engine.
                let arm_route = worker_arm
                    .lock()
                    .ok()
                    .and_then(|mut pending| pending.take());
                if let Some(route) = arm_route {
                    // A new session starts the breaker over. Both counters are
                    // worker-lifetime state, so without this a session that
                    // ended two failures deep makes the next session's first
                    // failure trip the cool-down, and a cool-down opened at the
                    // tail of one ring silences the opening of the next.
                    consecutive_failures = 0;
                    cooldown_until = None;
                    arm_firmware_haptics(&output, &route).await;
                }
                let request = rx.borrow_and_update().clone();
                let Some((route, waveform, interaction)) = request else {
                    continue;
                };
                // Circuit breaker: when replies keep getting lost, further
                // attempts only keep the channel saturated (and starve
                // unrelated writes). Skip haptics for a short cool-down so
                // the pipe drains; feedback resumes on the next buzz after.
                if let Some(until) = cooldown_until {
                    if Instant::now() < until {
                        continue;
                    }
                    cooldown_until = None;
                }
                if play_within_budget(&output, &rx, &route, waveform, interaction).await {
                    consecutive_failures = 0;
                } else {
                    consecutive_failures += 1;
                    if consecutive_failures >= BREAKER_THRESHOLD {
                        cooldown_until = Some(Instant::now() + COOLDOWN);
                        consecutive_failures = 0;
                        info!(
                            interaction,
                            "ring haptics cooling down after repeated HID++ loss"
                        );
                    }
                }
            }
        });
        Self { tx, pending_arm }
    }

    /// Queue a firmware arming check to run before the session's first buzz.
    /// The wake-up send also clears any stale not-yet-played waveform from a
    /// previous session so it cannot replay.
    pub(super) fn arm(&self, route: Option<DeviceRoute>) {
        let Some(route) = route else {
            return;
        };
        if let Ok(mut pending) = self.pending_arm.lock() {
            *pending = Some(route);
        }
        let _ = self.tx.send(None);
    }

    /// Queue `waveform`, replacing any not-yet-played request.
    pub(super) fn play(
        &self,
        route: Option<DeviceRoute>,
        waveform: HapticWaveform,
        interaction: &'static str,
    ) {
        let Some(route) = route else {
            return;
        };
        let _ = self.tx.send(Some((route, waveform, interaction)));
    }
}

#[cfg(test)]
mod tests {
    use super::{ARM_BUDGET, Budget, PLAY_BUDGET};
    use std::time::Duration;
    use tokio::time::Instant;

    /// Attempts share one allowance, so the second gets what the first left —
    /// not another full one, which is how three retries came to outlast the
    /// ring they were giving feedback for.
    #[test]
    fn a_partly_spent_budget_offers_only_the_remainder() {
        let start = Instant::now();
        let budget = Budget::starting_at(start, Duration::from_secs(2));

        let left = budget
            .remaining(start + Duration::from_millis(1500))
            .unwrap();

        assert_eq!(left, Duration::from_millis(500));
    }

    /// `timeout(ZERO, …)` elapses before the call it wraps can run, so a spent
    /// budget has to read as "stop", not as "you have no time".
    #[test]
    fn a_spent_budget_offers_nothing_rather_than_zero() {
        let start = Instant::now();
        let budget = Budget::starting_at(start, Duration::from_secs(2));

        assert_eq!(budget.remaining(start + Duration::from_secs(2)), None);
        assert_eq!(budget.remaining(start + Duration::from_secs(5)), None);
    }

    /// Both budgets exist to keep one interaction from eating the 15 s session
    /// the ring is open for, and arming must not outlast the buzz it precedes.
    #[test]
    fn the_budgets_stay_within_a_ring_session() {
        assert!(ARM_BUDGET < PLAY_BUDGET);
        assert!(ARM_BUDGET + PLAY_BUDGET < Duration::from_secs(15));
    }
}
