//! Orchestrator tests: the shared fixtures, and one module per area.

use super::{
    AgentDevice, InventoryHealth, Orchestrator, VOLATILE_REAPPLY_CONFIRM_RETRIES,
    any_device_needs_capture_rearm, build_devices, configured_wheel_mode, host_switch_links,
    pick_current, plan_reapply, pointer_policy, reapply_targets, stable_id,
};
use crate::hardware::WheelModeChange;
use crate::watchers::pointer::{PointerPolicy, PointerScope};
use openlogi_core::app::ForegroundApp;
use openlogi_core::binding::{Action, Binding, ButtonId};
use openlogi_core::config::{
    Config, DeviceConfig, LightSettings, LinkConfig, ScrollResolution, VerticalScrollSensitivity,
};
use openlogi_core::device::{
    Capabilities, DeviceInventory, DeviceKind, DeviceModelInfo, DeviceTransports,
    LightCapabilities, PairedDevice, RawDeviceAddress, ReceiverInfo, StandaloneDevice,
};
use openlogi_core::device_order::{DeviceIdentity, DeviceStableId};
use openlogi_core::hid::Dpi;
use openlogi_hid::{DIRECT_DEVICE_INDEX, DeviceRoute};
use std::sync::Arc;

use crate::observable::ObservableState;

mod camera;
mod capture_plans;
mod device_list;
mod device_settings;
mod publication;
mod reapply;

/// An orchestrator wired to a state cell nobody subscribes to. The publishing
/// paths still run, so a mutator that stops republishing shows up here rather
/// than only in the running agent.
fn orchestrator(config: Config) -> Orchestrator {
    Orchestrator::new(config, Arc::new(ObservableState::new("test".to_string())))
}

fn dev(key: &str, slot: u8, online: bool) -> AgentDevice {
    AgentDevice {
        config_key: key.to_string(),
        model_key: key.to_string(),
        route: Some(DeviceRoute::Bolt {
            receiver_uid: "AA00".to_string(),
            slot,
        }),
        slot,
        serial: None,
        unit_id: [0; 4],
        capabilities: None,
        kind: openlogi_core::device::DeviceKind::Mouse,
        light_capabilities: None,
        online,
    }
}

fn direct_inventory(serial_number: Option<&str>, unit_id: [u8; 4]) -> DeviceInventory {
    DeviceInventory {
        receiver: ReceiverInfo {
            name: "MX Master 3S".to_string(),
            vendor_id: 0x046d,
            product_id: 0xb023,
            unique_id: None,
        },
        paired: vec![PairedDevice {
            slot: DIRECT_DEVICE_INDEX,
            codename: Some("MX Master 3S".to_string()),
            wpid: None,
            kind: DeviceKind::Mouse,
            online: true,
            battery: None,
            model_info: Some(DeviceModelInfo {
                entity_count: 1,
                serial_number: serial_number.map(str::to_string),
                unit_id,
                transports: DeviceTransports::default(),
                model_ids: [0xb034, 0, 0],
                extended_model_id: 2,
            }),
            capabilities: Some(Capabilities::presumed_from_kind(DeviceKind::Mouse)),
        }],
    }
}

#[test]
fn pointer_discovery_round_trip_rejects_old_samples_and_restarts_unavailable() {
    use crate::watchers::pointer::Observation;
    use openlogi_core::config::MouseProfileTarget;
    use openlogi_hook::{PointerContext, PointerTarget};
    // A per-app override keeps pointer sampling continuous on every platform.
    let mut config = Config::default();
    config.set_per_app_binding("a", "browser", ButtonId::Back, Some(Action::BrowserBack));
    let mut orch = orchestrator(config);
    let initial = *orch.shared.pointer_discovery.borrow();
    let context = PointerContext {
        app: None,
        target: PointerTarget::Desktop,
    };
    assert!(orch.set_pointer_observation(Observation {
        demand: initial,
        context: context.clone()
    }));
    let mut config = orch.config.clone();
    config.app_settings.mouse_profile_target = MouseProfileTarget::Focused;
    orch.reload_config(config.clone());
    config.app_settings.mouse_profile_target = MouseProfileTarget::Pointer;
    orch.reload_config(config);
    assert_eq!(
        orch.mouse_context().1,
        PointerScope::Sampled(PointerTarget::Unavailable)
    );
    assert!(!orch.set_pointer_observation(Observation {
        demand: initial,
        context: context.clone()
    }));
    assert_eq!(
        orch.mouse_context().1,
        PointerScope::Sampled(PointerTarget::Unavailable)
    );
    let fresh = *orch.shared.pointer_discovery.borrow();
    assert_ne!(fresh, initial);
    assert!(orch.set_pointer_observation(Observation {
        demand: fresh,
        context
    }));
    assert_eq!(
        orch.mouse_context().1,
        PointerScope::Sampled(PointerTarget::Desktop)
    );
}

#[test]
fn unsupported_pointer_capability_survives_config_reload_before_consumption() {
    use crate::watchers::pointer::Observation;
    use openlogi_core::config::MouseProfileTarget;
    use openlogi_hook::{PointerContext, PointerTarget};
    let mut orch = orchestrator(Config::default());
    let initial = *orch.shared.pointer_discovery.borrow();
    let mut config = orch.config.clone();
    config.app_settings.mouse_profile_target = MouseProfileTarget::Focused;
    orch.reload_config(config.clone());
    orch.set_pointer_observation(Observation {
        demand: initial,
        context: PointerContext {
            app: None,
            target: PointerTarget::Unsupported,
        },
    });
    config.app_settings.mouse_profile_target = MouseProfileTarget::Pointer;
    orch.reload_config(config);
    assert_eq!(orch.pointer_context.target, PointerTarget::Unsupported);
    assert_eq!(orch.mouse_context().1, PointerScope::Focused);
}

/// The policy pointer-selected profiles get when nothing needs a continuous
/// sample: on-demand where the platform can query cheaply, else continuous.
fn app_independent_pointer_policy() -> PointerPolicy {
    if cfg!(target_os = "windows") {
        PointerPolicy::OnDemand
    } else {
        PointerPolicy::Continuous
    }
}

#[test]
fn pointer_sampling_is_continuous_only_when_a_sample_can_change_an_outcome() {
    use openlogi_core::config::{MouseProfileTarget, ThumbwheelSensitivity};
    let config = Config::default();
    assert_eq!(pointer_policy(&config), app_independent_pointer_policy());

    let mut focused = config.clone();
    focused.app_settings.mouse_profile_target = MouseProfileTarget::Focused;
    assert_eq!(pointer_policy(&focused), PointerPolicy::Focused);

    let mut per_app = config.clone();
    per_app.set_per_app_binding("a", "browser", ButtonId::Back, Some(Action::BrowserBack));
    assert_eq!(pointer_policy(&per_app), PointerPolicy::Continuous);
    per_app.set_device_enabled("a", false);
    assert_eq!(pointer_policy(&per_app), app_independent_pointer_policy());

    let mut rebound_wheel = config.clone();
    rebound_wheel.set_binding(
        "a",
        ButtonId::ThumbwheelScrollUp,
        Binding::Single(Action::VolumeUp),
    );
    assert_eq!(pointer_policy(&rebound_wheel), PointerPolicy::Continuous);

    let mut device_sensitivity = config.clone();
    device_sensitivity.set_device_thumbwheel_sensitivity("a", Some(ThumbwheelSensitivity::MAX));
    assert_eq!(
        pointer_policy(&device_sensitivity),
        PointerPolicy::Continuous
    );

    let mut app_sensitivity = config;
    app_sensitivity.app_settings.thumbwheel_sensitivity = ThumbwheelSensitivity::MAX;
    assert_eq!(pointer_policy(&app_sensitivity), PointerPolicy::Continuous);
}

#[test]
#[cfg(target_os = "windows")]
fn on_demand_bindings_ignore_samples_and_policy_changes_restart_discovery() {
    use crate::watchers::pointer::Observation;
    use openlogi_hook::{PointerContext, PointerTarget};
    let mut orch = orchestrator(Config::default());
    orch.devices = vec![dev("a", 1, true)];
    orch.set_current_app(Some(ForegroundApp::unnamed("browser".into())));
    let initial = *orch.shared.pointer_discovery.borrow();
    assert_eq!(initial.policy, PointerPolicy::OnDemand);
    assert_eq!(orch.mouse_context(), (None, PointerScope::OnDemand));
    let plans = orch.shared.capture_plans.borrow().clone();
    // A late sample changes neither the published bindings nor any press.
    assert!(!orch.set_pointer_observation(Observation {
        demand: initial,
        context: PointerContext {
            app: Some(ForegroundApp::unnamed("browser".into())),
            target: PointerTarget::Desktop,
        },
    }));
    assert_eq!(*orch.shared.capture_plans.borrow(), plans);
    assert_eq!(
        orch.shared.hook_maps.read().expect("maps").pointer_scope,
        PointerScope::OnDemand
    );

    // The first per-app override restarts discovery with a fresh session
    // whose first sample is still pending.
    let mut config = orch.config.clone();
    config.set_per_app_binding("a", "browser", ButtonId::Back, Some(Action::BrowserBack));
    orch.reload_config(config.clone());
    let continuous = *orch.shared.pointer_discovery.borrow();
    assert_eq!(continuous.policy, PointerPolicy::Continuous);
    assert_ne!(continuous.generation, initial.generation);
    assert_eq!(
        orch.mouse_context().1,
        PointerScope::Sampled(PointerTarget::Unavailable)
    );

    // An unrelated edit keeps the session.
    config.app_settings.launch_at_login = !config.app_settings.launch_at_login;
    orch.reload_config(config);
    assert_eq!(*orch.shared.pointer_discovery.borrow(), continuous);
}
