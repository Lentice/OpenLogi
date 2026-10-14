//! Actions Ring execution: the one owner of what a ring interaction *does*.
//!
//! The trigger, the overlay's hover/activate/cancel commands, and config
//! reloads arrive here as intents. Session validation stays with
//! [`ActionRingManager`]; this module sequences it with the two side effects
//! a validated interaction has — dispatching the snapshotted action and
//! buzzing the triggering device — so no caller knows that arming precedes
//! the session, or that dispatch precedes the activation buzz.
//!
//! Overlay process supervision only reads [`ActionRing::subscribe`] and
//! stays outside.

mod haptics;

use std::sync::Arc;

use openlogi_agent_core::action_ring::{ActionRingManager, ActionRingSessionSpec};
use openlogi_agent_core::runtime::ActionDispatcher;
use openlogi_core::binding::{Action, ActionRingSlot};
use openlogi_hid::HapticWaveform;
use openlogi_ipc::{ActionRingCommandError, Generation, RingObservation};
use tokio::sync::watch;

pub(crate) use self::haptics::HapticOutput;
use self::haptics::RingHapticPlayer;

/// Where a validated activation's action goes.
pub(crate) trait RingDispatch: Send + Sync + 'static {
    /// Run `action` on behalf of the device whose control opened the ring.
    fn dispatch(&self, action: &Action, device_key: &str);
}

impl RingDispatch for ActionDispatcher {
    fn dispatch(&self, action: &Action, device_key: &str) {
        Self::dispatch(self, action, Some(device_key));
    }
}

/// The agent's Actions Ring: session state plus the effects of interacting
/// with it. Cheap to clone; clones share one session and one haptic worker.
#[derive(Clone)]
pub(crate) struct ActionRing {
    sessions: Arc<ActionRingManager>,
    dispatch: Arc<dyn RingDispatch>,
    haptics: RingHapticPlayer,
}

/// A trigger press that found no ring showing, so it may open one.
///
/// Only [`ActionRing::press_trigger`] produces it: the toggle-close check runs
/// before the caller looks up the session it would open.
#[must_use = "a trigger press opens nothing until a session is supplied"]
pub(crate) struct RingOpening<'a> {
    ring: &'a ActionRing,
}

impl ActionRing {
    /// Start the haptic worker and return the ring. Must be called from a
    /// tokio runtime.
    pub(crate) fn spawn(dispatch: impl RingDispatch, haptics: impl HapticOutput) -> Self {
        Self {
            sessions: Arc::new(ActionRingManager::default()),
            dispatch: Arc::new(dispatch),
            haptics: RingHapticPlayer::spawn(haptics),
        }
    }

    /// Read-only session state for the overlay process supervisor.
    pub(crate) fn subscribe(&self) -> watch::Receiver<RingObservation> {
        self.sessions.subscribe()
    }

    /// Serve one overlay long-poll.
    pub(crate) async fn observe(&self, since: Generation) -> RingObservation {
        self.sessions.observe(since).await
    }

    /// The ring's trigger was pressed. A showing ring closes and `None` comes
    /// back; otherwise the press may open a session.
    pub(crate) fn press_trigger(&self) -> Option<RingOpening<'_>> {
        (!self.sessions.dismiss_active()).then_some(RingOpening { ring: self })
    }

    /// The pointer moved onto `slot`. A changed highlight buzzes the device.
    pub(crate) fn hover(
        &self,
        session_id: u64,
        slot: ActionRingSlot,
    ) -> Result<(), ActionRingCommandError> {
        if let Some(hover) = self.sessions.hover(session_id, slot)? {
            self.haptics
                .play(hover.haptic_route, HapticWaveform::SubtleCollision, "hover");
        }
        Ok(())
    }

    /// `slot` was chosen: consume the session, run its action, then confirm
    /// with a buzz.
    pub(crate) fn activate(
        &self,
        session_id: u64,
        slot: ActionRingSlot,
    ) -> Result<(), ActionRingCommandError> {
        let activation = self.sessions.activate(session_id, slot)?;
        self.dispatch
            .dispatch(&activation.action, &activation.device_key);
        self.haptics.play(
            activation.haptic_route,
            HapticWaveform::DampStateChange,
            "activation",
        );
        Ok(())
    }

    /// The overlay closed `session_id` without choosing.
    pub(crate) fn cancel(&self, session_id: u64) {
        self.sessions.cancel(session_id);
    }

    /// Close a showing ring whose device's ring `config` disables.
    pub(crate) fn dismiss_disabled(&self, config: &openlogi_core::config::Config) {
        self.sessions.dismiss_disabled(config);
    }
}

impl RingOpening<'_> {
    /// Open `session`. The firmware haptic engine is re-armed first: power
    /// transitions can clear it, after which plays are accepted without
    /// feedback.
    pub(crate) fn open(self, session: ActionRingSessionSpec) {
        self.ring.haptics.arm(session.haptic_route.clone());
        self.ring.sessions.begin(session);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use openlogi_agent_core::action_ring::ActionRingSessionSpec;
    use openlogi_core::binding::{Action, ActionRingConfig, ActionRingSlot};
    use openlogi_hid::{DeviceRoute, HapticWaveform, WriteError};
    use openlogi_ipc::ActionRingCommandError;
    use tokio::sync::{mpsc, oneshot};

    use super::{ActionRing, HapticOutput, RingDispatch};

    /// One externally visible effect of a ring interaction, in the order it
    /// reached the device or the dispatcher.
    #[derive(Debug, PartialEq)]
    enum Effect {
        Arm,
        Play(HapticWaveform),
        Dispatch(Action, String),
    }

    /// How the next play call ends. An empty script means success.
    enum Step {
        Fail,
        /// Succeed once the test releases it.
        Held(oneshot::Receiver<()>),
    }

    /// Records both seams into one ordered stream and scripts play outcomes.
    #[derive(Clone)]
    struct Recorder {
        effects: mpsc::UnboundedSender<Effect>,
        script: Arc<Mutex<VecDeque<Step>>>,
    }

    impl RingDispatch for Recorder {
        fn dispatch(&self, action: &Action, device_key: &str) {
            let _ = self
                .effects
                .send(Effect::Dispatch(action.clone(), device_key.to_owned()));
        }
    }

    impl HapticOutput for Recorder {
        fn ensure_armed(
            &self,
            _route: &DeviceRoute,
        ) -> impl Future<Output = Result<bool, WriteError>> + Send {
            let _ = self.effects.send(Effect::Arm);
            std::future::ready(Ok(false))
        }

        fn play(
            &self,
            _route: &DeviceRoute,
            waveform: HapticWaveform,
        ) -> impl Future<Output = Result<(), WriteError>> + Send {
            let _ = self.effects.send(Effect::Play(waveform));
            let step = self.script.lock().unwrap().pop_front();
            async move {
                match step {
                    None => Ok(()),
                    Some(Step::Fail) => Err(WriteError::DeviceNotFound),
                    Some(Step::Held(release)) => {
                        let _ = release.await;
                        Ok(())
                    }
                }
            }
        }
    }

    struct Harness {
        ring: ActionRing,
        script: Arc<Mutex<VecDeque<Step>>>,
        effects: mpsc::UnboundedReceiver<Effect>,
    }

    impl Harness {
        fn new() -> Self {
            let (tx, effects) = mpsc::unbounded_channel();
            let recorder = Recorder {
                effects: tx,
                script: Arc::default(),
            };
            let script = Arc::clone(&recorder.script);
            Self {
                ring: ActionRing::spawn(recorder.clone(), recorder),
                script,
                effects,
            }
        }

        fn fail_next_plays(&self, count: usize) {
            let mut script = self.script.lock().unwrap();
            script.extend(std::iter::repeat_with(|| Step::Fail).take(count));
        }

        /// Press the trigger with nothing showing and open a session.
        fn open(&self) -> u64 {
            self.ring
                .press_trigger()
                .expect("no ring is showing")
                .open(spec());
            self.ring
                .subscribe()
                .borrow()
                .invocation
                .as_ref()
                .expect("the opened ring is published")
                .session_id
        }

        async fn next(&mut self) -> Effect {
            tokio::time::timeout(Duration::from_secs(30), self.effects.recv())
                .await
                .expect("an effect arrives")
                .expect("the recorder is alive")
        }

        /// Assert that nothing reaches the device or dispatcher for `window`.
        async fn quiet_for(&mut self, window: Duration) {
            if let Ok(effect) = tokio::time::timeout(window, self.effects.recv()).await {
                panic!("expected no effect, got {effect:?}");
            }
        }

        /// A request that exhausts all three attempts.
        async fn expect_failed_buzz(&mut self) {
            for _ in 0..3 {
                assert_eq!(
                    self.next().await,
                    Effect::Play(HapticWaveform::SubtleCollision)
                );
            }
        }
    }

    fn spec() -> ActionRingSessionSpec {
        ActionRingSessionSpec {
            device_key: "mouse-a".to_string(),
            haptic_route: Some(DeviceRoute::Bolt {
                receiver_uid: "receiver".to_string(),
                slot: 1,
            }),
            layout: ActionRingConfig::default().default,
            language: None,
        }
    }

    /// The whole interaction, issued back to back the way a fast user does:
    /// the arming check lands before the first buzz even though the hover was
    /// queued before the worker ran, and the action runs before its buzz.
    #[tokio::test(start_paused = true)]
    async fn trigger_hover_activate_runs_in_order() {
        let mut h = Harness::new();
        let session = h.open();
        h.ring.hover(session, ActionRingSlot::Top).unwrap();

        assert_eq!(h.next().await, Effect::Arm);
        assert_eq!(
            h.next().await,
            Effect::Play(HapticWaveform::SubtleCollision)
        );

        h.ring.activate(session, ActionRingSlot::Top).unwrap();
        assert_eq!(
            h.next().await,
            Effect::Dispatch(Action::Cut, "mouse-a".to_string())
        );
        assert_eq!(
            h.next().await,
            Effect::Play(HapticWaveform::DampStateChange)
        );
        h.quiet_for(Duration::from_secs(10)).await;
    }

    /// A rejected command reaches neither the dispatcher nor the device, and
    /// the IPC caller hears why.
    #[tokio::test(start_paused = true)]
    async fn rejected_interactions_have_no_effect() {
        let mut h = Harness::new();
        let session = h.open();
        assert_eq!(h.next().await, Effect::Arm);

        // Wrong session.
        assert_eq!(
            h.ring.hover(session + 1, ActionRingSlot::Top),
            Err(ActionRingCommandError::SessionNotFound)
        );
        assert_eq!(
            h.ring.activate(session + 1, ActionRingSlot::Top),
            Err(ActionRingCommandError::SessionNotFound)
        );
        h.quiet_for(Duration::from_secs(1)).await;

        // Cancelled session.
        h.ring.cancel(session);
        assert_eq!(
            h.ring.activate(session, ActionRingSlot::Top),
            Err(ActionRingCommandError::SessionNotFound)
        );

        // Expired session: well past any ring lifetime.
        let session = h.open();
        assert_eq!(h.next().await, Effect::Arm);
        tokio::time::advance(Duration::from_secs(60)).await;
        assert_eq!(
            h.ring.hover(session, ActionRingSlot::Top),
            Err(ActionRingCommandError::SessionNotFound)
        );
        assert_eq!(
            h.ring.activate(session, ActionRingSlot::Top),
            Err(ActionRingCommandError::SessionNotFound)
        );
        h.quiet_for(Duration::from_secs(10)).await;
    }

    /// A second press closes the showing ring without re-arming, and a
    /// consumed session cannot run its action twice.
    #[tokio::test(start_paused = true)]
    async fn toggle_close_and_consumed_sessions_have_no_effect() {
        let mut h = Harness::new();
        let session = h.open();
        assert_eq!(h.next().await, Effect::Arm);
        h.ring.activate(session, ActionRingSlot::Top).unwrap();
        assert_eq!(
            h.next().await,
            Effect::Dispatch(Action::Cut, "mouse-a".to_string())
        );
        assert_eq!(
            h.next().await,
            Effect::Play(HapticWaveform::DampStateChange)
        );
        assert_eq!(
            h.ring.activate(session, ActionRingSlot::Top),
            Err(ActionRingCommandError::SessionNotFound)
        );

        h.open();
        assert_eq!(h.next().await, Effect::Arm);
        assert!(h.ring.press_trigger().is_none(), "second press closes");
        assert_eq!(h.ring.subscribe().borrow().invocation, None);
        h.quiet_for(Duration::from_secs(10)).await;
    }

    /// While one buzz plays, only the newest request survives: a hover
    /// superseded by an activation never plays.
    #[tokio::test(start_paused = true)]
    async fn only_the_latest_queued_buzz_plays() {
        let mut h = Harness::new();
        let (release, held) = oneshot::channel();
        h.script.lock().unwrap().push_back(Step::Held(held));
        let session = h.open();
        h.ring.hover(session, ActionRingSlot::Top).unwrap();
        assert_eq!(h.next().await, Effect::Arm);
        assert_eq!(
            h.next().await,
            Effect::Play(HapticWaveform::SubtleCollision)
        );

        // The first buzz is still in flight.
        h.ring.hover(session, ActionRingSlot::Right).unwrap();
        h.ring.activate(session, ActionRingSlot::Bottom).unwrap();
        release.send(()).unwrap();

        assert!(matches!(h.next().await, Effect::Dispatch(..)));
        assert_eq!(
            h.next().await,
            Effect::Play(HapticWaveform::DampStateChange)
        );
        h.quiet_for(Duration::from_secs(10)).await;
    }

    /// Three lost buzzes in a row silence haptics for the cool-down; the next
    /// buzz after it plays again.
    #[tokio::test(start_paused = true)]
    async fn repeated_loss_opens_a_cooldown_that_lapses() {
        let mut h = Harness::new();
        h.fail_next_plays(9);
        let session = h.open();
        assert_eq!(h.next().await, Effect::Arm);
        for slot in [
            ActionRingSlot::Top,
            ActionRingSlot::Right,
            ActionRingSlot::Bottom,
        ] {
            h.ring.hover(session, slot).unwrap();
            h.expect_failed_buzz().await;
        }

        h.ring.hover(session, ActionRingSlot::Left).unwrap();
        h.quiet_for(Duration::from_secs(1)).await;

        tokio::time::advance(Duration::from_secs(4)).await;
        h.ring.hover(session, ActionRingSlot::Top).unwrap();
        assert_eq!(
            h.next().await,
            Effect::Play(HapticWaveform::SubtleCollision)
        );
    }

    /// A new session starts the breaker over: failures left by the previous
    /// ring do not count toward the next one's cool-down.
    #[tokio::test(start_paused = true)]
    async fn a_new_session_resets_the_failure_count() {
        let mut h = Harness::new();
        h.fail_next_plays(6);
        let session = h.open();
        assert_eq!(h.next().await, Effect::Arm);
        h.ring.hover(session, ActionRingSlot::Top).unwrap();
        h.expect_failed_buzz().await;
        h.ring.hover(session, ActionRingSlot::Right).unwrap();
        h.expect_failed_buzz().await;
        h.ring.cancel(session);

        let session = h.open();
        assert_eq!(h.next().await, Effect::Arm);
        h.fail_next_plays(3);
        h.ring.hover(session, ActionRingSlot::Top).unwrap();
        h.expect_failed_buzz().await;
        // Third failure overall, but only the first of this session.
        h.ring.hover(session, ActionRingSlot::Right).unwrap();
        assert_eq!(
            h.next().await,
            Effect::Play(HapticWaveform::SubtleCollision)
        );
    }

    /// A new session also lifts a cool-down the previous ring opened.
    #[tokio::test(start_paused = true)]
    async fn a_new_session_lifts_an_open_cooldown() {
        let mut h = Harness::new();
        h.fail_next_plays(9);
        let session = h.open();
        assert_eq!(h.next().await, Effect::Arm);
        for slot in [
            ActionRingSlot::Top,
            ActionRingSlot::Right,
            ActionRingSlot::Bottom,
        ] {
            h.ring.hover(session, slot).unwrap();
            h.expect_failed_buzz().await;
        }
        h.ring.cancel(session);

        let session = h.open();
        h.ring.hover(session, ActionRingSlot::Top).unwrap();
        assert_eq!(h.next().await, Effect::Arm);
        assert_eq!(
            h.next().await,
            Effect::Play(HapticWaveform::SubtleCollision)
        );
    }
}
