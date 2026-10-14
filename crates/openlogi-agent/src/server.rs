//! tarpc IPC server: backs the [`Agent`] service with the orchestrator and the
//! agent-core device-I/O helpers, listening on the agent's local IPC socket
//! (a Unix-domain socket on Unix, a named pipe on Windows).
//!
//! The agent owns all device I/O, so the GUI never opens a device — it routes
//! "apply now" / "read" commands here, and polls snapshots.

use std::sync::Arc;

use futures::StreamExt as _;
use openlogi_agent_core::event_monitor::SharedEventMonitor;
use openlogi_agent_core::observable::ObservableState;
use openlogi_agent_core::orchestrator::{Orchestrator, SharedHandles};
use openlogi_agent_core::runtime::ActionDispatcher;
use openlogi_core::binding::ActionRingSlot;
use openlogi_core::config::{Config, Lighting};
use openlogi_core::device::DeviceInventory;
use openlogi_hid::{
    BacklightState, DeviceRoute, Dpi, DpiInfo, FnLockState, HidppOperation, LightCommand,
    ReceiverSelector, ScrollWheelMode, SmartShiftStatus, WriteError,
};
use openlogi_ipc::transport;
use openlogi_ipc::{
    ActionRingCommandError, ActionRingInvocation, Agent, AgentSnapshot, AgentStatus, ClientKind,
    ConfigReloadError, Generation, Identity, MonitorEvent, Observation, PROTOCOL_VERSION,
    PairingCommandError, PairingFailure, PairingUpdate, RingObservation,
};
use succession::Compat;

use crate::action_ring::ActionRing;
use crate::pairing::PairingManager;
// Brings `Listener::accept` into scope for the concrete listener `transport::bind`
// returns; `as _` keeps it anonymous (method resolution only).
use interprocess::local_socket::traits::tokio::Listener as _;
use openlogi_hook::Hook;
use tarpc::context::Context;
use tarpc::server::{BaseChannel, Channel as _};
use tokio::sync::Mutex;
use tracing::{info, warn};

/// Shared handle to the agent's state, cloned per connection (and per request).
#[derive(Clone)]
pub struct AgentServer {
    pub orchestrator: Arc<Mutex<Orchestrator>>,
    pub shared: SharedHandles,
    /// Everything the GUI observes, answered from here rather than recomposed
    /// per request: reads take no orchestrator lock and no permission syscall.
    pub observable: Arc<ObservableState>,
    pub pairing: Arc<PairingManager>,
    pub event_monitor: SharedEventMonitor,
    pub action_ring: ActionRing,
    pub dispatcher: ActionDispatcher,
    /// Forwards each connection's [`ClientKind`] declaration to the
    /// dormancy gate.
    pub demand: tokio::sync::mpsc::UnboundedSender<ClientKind>,
}

impl AgentServer {
    /// Build a server. The second return is the demand channel the dormancy gate drains.
    pub fn new(
        orchestrator: Arc<Mutex<Orchestrator>>,
        shared: SharedHandles,
        observable: Arc<ObservableState>,
        pairing: Arc<PairingManager>,
        event_monitor: SharedEventMonitor,
        action_ring: ActionRing,
        dispatcher: ActionDispatcher,
    ) -> (Self, tokio::sync::mpsc::UnboundedReceiver<ClientKind>) {
        let (demand, declarations) = tokio::sync::mpsc::unbounded_channel();
        (
            Self {
                orchestrator,
                shared,
                observable,
                pairing,
                event_monitor,
                action_ring,
                dispatcher,
                demand,
            },
            declarations,
        )
    }
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the handlers that only read cached state need no await, but the RPC \
              surface reads as one thing when every method in the impl keeps the \
              `async fn` the tarpc trait declares — worth more than shaving a \
              future off a dozen of them with `std::future::ready`"
)]
impl Agent for AgentServer {
    async fn read_pointer_speed(
        self,
        _: Context,
        route: DeviceRoute,
    ) -> Result<openlogi_core::hid::PointerSpeed, WriteError> {
        self.shared
            .device(&route)
            .run(HidppOperation::ReadPointerSpeed, |c| async move {
                openlogi_hid::get_pointer_speed_on(&c).await
            })
            .await
    }

    async fn set_pointer_speed(
        self,
        _: Context,
        route: DeviceRoute,
        speed: openlogi_core::hid::PointerSpeed,
    ) -> Result<(), WriteError> {
        self.shared
            .device(&route)
            .run(HidppOperation::WritePointerSpeed, |c| async move {
                openlogi_hid::set_pointer_speed_on(&c, speed).await
            })
            .await
    }
    async fn protocol_version(self, _: Context) -> u32 {
        PROTOCOL_VERSION
    }

    /// `Run::mint` is process-global, so the overlay supervisor hands its
    /// child the very same run this answers with.
    async fn identity(self, _: Context) -> Identity {
        Identity::mine(Compat::from(PROTOCOL_VERSION))
    }

    async fn status(self, _: Context) -> AgentStatus {
        self.observable.read(|state| state.status.clone())
    }

    async fn inventory(self, _: Context) -> Vec<DeviceInventory> {
        self.observable.read(|state| state.inventory.clone())
    }

    async fn reload_config(self, _: Context) -> Result<(), ConfigReloadError> {
        match Config::load_or_default() {
            Ok(config) => {
                let launch_at_login = config.app_settings.launch_at_login;
                #[cfg(target_os = "macos")]
                let app_icon = config.app_settings.app_icon;
                let language = config.app_settings.language.clone();
                let mut orchestrator = self.orchestrator.lock().await;
                self.action_ring.dismiss_disabled(&config);
                orchestrator.reload_config(config);
                drop(orchestrator);
                self.dispatcher.cancel_all_buttons();
                // The GUI's launch-at-login toggle reaches us through this
                // reload, so re-reconcile the autostart from the new config.
                crate::autostart::reconcile(launch_at_login);
                // So does the app icon, and the menu-bar item is ours to
                // restyle — the GUI can only reach the Dock and the bundle.
                #[cfg(target_os = "macos")]
                crate::tray::set_icon(app_icon);
                // And the interface language: re-resolve the process locale
                // (the Windows popup rebuilds per show and picks it up alone)
                // and rebuild the macOS menu, whose titles were stamped at
                // install.
                openlogi_core::locale::activate(language.as_deref());
                #[cfg(target_os = "macos")]
                crate::tray::relocalize();
                Ok(())
            }
            Err(error) => {
                warn!(error = %error, "reload_config: parse failed; keeping current config");
                Err(ConfigReloadError {
                    message: error.to_string(),
                })
            }
        }
    }

    async fn set_dpi(self, _: Context, route: DeviceRoute, dpi: Dpi) -> Result<(), WriteError> {
        self.shared
            .device(&route)
            .run(HidppOperation::WriteDpi, |c| async move {
                openlogi_hid::set_dpi_on(&c, dpi).await
            })
            .await
    }

    async fn set_lighting(
        self,
        _: Context,
        route: DeviceRoute,
        lighting: Lighting,
    ) -> Result<(), WriteError> {
        self.shared.device(&route).lighting(&lighting)?.wait().await
    }

    async fn set_smartshift(
        self,
        _: Context,
        route: DeviceRoute,
        status: SmartShiftStatus,
    ) -> Result<(), WriteError> {
        self.shared
            .device(&route)
            .run(HidppOperation::WriteSmartShift, |c| async move {
                openlogi_hid::set_smartshift_on(&c, status).await
            })
            .await
    }

    async fn read_dpi(self, _: Context, route: DeviceRoute) -> Result<DpiInfo, WriteError> {
        self.shared
            .device(&route)
            .run(HidppOperation::ReadDpiCapabilities, |c| async move {
                openlogi_hid::get_dpi_info_on(&c).await
            })
            .await
    }

    async fn read_smartshift(
        self,
        _: Context,
        route: DeviceRoute,
    ) -> Result<SmartShiftStatus, WriteError> {
        self.shared
            .device(&route)
            .run(HidppOperation::ReadSmartShift, |c| async move {
                openlogi_hid::get_smartshift_status_on(&c).await
            })
            .await
    }

    async fn read_wheel(
        self,
        _: Context,
        route: DeviceRoute,
    ) -> Result<ScrollWheelMode, WriteError> {
        self.shared
            .device(&route)
            .run(HidppOperation::ReadWheelMode, |c| async move {
                openlogi_hid::get_scroll_wheel_mode_on(&c).await
            })
            .await
    }

    async fn read_backlight(
        self,
        _: Context,
        route: DeviceRoute,
    ) -> Result<BacklightState, WriteError> {
        self.shared
            .device(&route)
            .run(HidppOperation::ReadBacklight, |c| async move {
                openlogi_hid::get_backlight_on(&c).await
            })
            .await
    }

    async fn read_fn_lock(self, _: Context, route: DeviceRoute) -> Result<FnLockState, WriteError> {
        self.shared
            .keyboard_device(&route)
            .run(HidppOperation::ReadFnLock, |c| async move {
                openlogi_hid::get_fn_lock_on(&c).await
            })
            .await
    }

    async fn set_fn_lock(
        self,
        _: Context,
        route: DeviceRoute,
        fn_lock: bool,
    ) -> Result<FnLockState, WriteError> {
        self.shared.set_fn_lock(&route, fn_lock).await
    }

    async fn request_accessibility_prompt(self, _: Context) {
        Hook::prompt_accessibility();
    }

    async fn start_pairing(
        self,
        _: Context,
        selector: ReceiverSelector,
    ) -> Result<(), PairingCommandError> {
        self.pairing.start(selector).await
    }

    async fn pair_device(self, _: Context, address: [u8; 6]) -> Result<(), PairingCommandError> {
        self.pairing.pair(address)
    }

    async fn cancel_pairing(self, _: Context) -> Result<(), PairingCommandError> {
        self.pairing.cancel()
    }

    async fn unpair_device(self, _: Context, route: DeviceRoute) -> Result<(), PairingFailure> {
        self.pairing.unpair(&route).await
    }

    async fn next_pairing(self, _: Context) -> Option<PairingUpdate> {
        self.pairing.next_update().await
    }

    async fn snapshot(self, _: Context) -> AgentSnapshot {
        self.observable.snapshot()
    }

    async fn observe(self, _: Context, since: Generation) -> Observation {
        self.observable.observe(since).await
    }

    async fn poll_event_monitor(self, _: Context) -> Vec<MonitorEvent> {
        self.event_monitor.poll()
    }

    async fn set_light(
        self,
        _: Context,
        route: DeviceRoute,
        command: LightCommand,
    ) -> Result<(), WriteError> {
        self.shared.hardware().apply_light(&route, command).await
    }

    async fn set_light_manual_power(
        self,
        _: Context,
        route: DeviceRoute,
        enabled: bool,
    ) -> Result<(), WriteError> {
        self.shared
            .hardware()
            .apply_light(&route, LightCommand::Power(enabled))
            .await?;
        if !self
            .orchestrator
            .lock()
            .await
            .set_manual_light_power(&route, enabled)
        {
            warn!(?route, "manual light power applied without camera override");
        }
        Ok(())
    }

    async fn next_action_ring(self, _: Context) -> Option<ActionRingInvocation> {
        // Superseded by `observe_action_ring`; nothing calls this, and a
        // matching-version overlay never will (see the trait's note).
        None
    }

    async fn observe_action_ring(self, _: Context, since: Generation) -> RingObservation {
        self.action_ring.observe(since).await
    }

    async fn declare_client(self, _: Context, kind: ClientKind) {
        // A failed send is the designed steady state: the gate drops its
        // receiver at arming, and an armed agent no longer cares.
        let _ = self.demand.send(kind);
    }

    async fn action_ring_hover(
        self,
        _: Context,
        session_id: u64,
        slot: ActionRingSlot,
    ) -> Result<(), ActionRingCommandError> {
        self.action_ring.hover(session_id, slot)
    }

    async fn action_ring_activate(
        self,
        _: Context,
        session_id: u64,
        slot: ActionRingSlot,
    ) -> Result<(), ActionRingCommandError> {
        self.action_ring.activate(session_id, slot)
    }

    async fn action_ring_cancel(self, _: Context, session_id: u64) {
        self.action_ring.cancel(session_id);
    }
}

/// Bind the agent's IPC socket and serve [`Agent`] requests until the process
/// exits. A stale socket left by a prior crash is reclaimed by the listener —
/// `main` holds the single-instance lock (`agent.lock`), so no other live agent
/// owns this socket and any leftover is from a dead instance.
pub async fn run(server: AgentServer) {
    let listener = match transport::bind() {
        Ok(listener) => listener,
        Err(e) => {
            warn!(error = %e, "could not bind IPC socket; IPC disabled");
            return;
        }
    };
    info!("IPC server listening");

    loop {
        let stream = match listener.accept().await {
            Ok(stream) => stream,
            Err(e) => {
                warn!(error = %e, "IPC accept failed");
                continue;
            }
        };
        let server = server.clone();
        let channel = BaseChannel::with_defaults(transport::wrap(stream));
        tokio::spawn(
            channel
                .execute(server.serve())
                .for_each(|response| async move {
                    tokio::spawn(response);
                }),
        );
    }
}
