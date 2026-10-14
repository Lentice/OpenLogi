//! Held OS-hook presses across orchestrator profile transitions, driven
//! through the real orchestrator, hook callback, and button worker.

use std::sync::Arc;

use openlogi_core::app::ForegroundApp;
use openlogi_core::config::{Config, MouseProfileTarget};
use openlogi_core::device::{
    Capabilities, DeviceInventory, DeviceKind, DeviceModelInfo, DeviceTransports, PairedDevice,
    ReceiverInfo,
};
use openlogi_hid::DIRECT_DEVICE_INDEX;
use openlogi_hook::{PointerContext, PointerTarget};

use super::super::super::button::{ButtonRuntimeEvent, EndReason};
use super::*;
use crate::observable::ObservableState;
use crate::orchestrator::Orchestrator;
use crate::watchers::pointer::Observation;

const SAVED: &str = "unit:01000000";
const FALLBACK: &str = "unit:02000000";

/// The window an on-demand press admits against.
fn hovered_window() -> PointerTarget {
    PointerTarget::Window {
        process_id: 7,
        window_id: 9,
    }
}

fn mouse() -> EventDevice {
    EventDevice {
        vendor_id: Some(openlogi_hook::LOGITECH_VENDOR_ID),
        product_name: Some("Logitech MX Master 3S".into()),
        ..EventDevice::default()
    }
}

/// One directly connected mouse whose config key is `unit:0N000000`.
fn direct_mouse(unit: u8, online: bool) -> DeviceInventory {
    DeviceInventory {
        receiver: ReceiverInfo {
            name: "MX Master 3S".to_string(),
            vendor_id: 0x046d,
            product_id: 0xb030 + u16::from(unit),
            unique_id: None,
        },
        paired: vec![PairedDevice {
            slot: DIRECT_DEVICE_INDEX,
            codename: Some("MX Master 3S".to_string()),
            wpid: None,
            kind: DeviceKind::Mouse,
            online,
            battery: None,
            model_info: Some(DeviceModelInfo {
                entity_count: 1,
                serial_number: None,
                unit_id: [unit, 0, 0, 0],
                transports: DeviceTransports::default(),
                model_ids: [0xb030 + u16::from(unit), 0, 0],
                extended_model_id: 2,
            }),
            capabilities: Some(Capabilities::presumed_from_kind(DeviceKind::Mouse)),
        }],
    }
}

/// Back as an OS-hook gesture button whose plain click runs `click`. HID++
/// side-button capture is off so the gesture stays in the OS-hook map on
/// every platform.
fn gesture_back(config: &mut Config, key: &str, click: Action) {
    config.app_settings.capture_mouse_events = false;
    config.set_binding(
        key,
        ButtonId::Back,
        Binding::Gesture(BTreeMap::from([(GestureDirection::Click, click)])),
    );
}

fn orchestrator(config: Config) -> Orchestrator {
    Orchestrator::new(config, Arc::new(ObservableState::new("test".to_string())))
}

fn press_back(hooks: &SharedHookMaps, dispatcher: &ActionDispatcher, pressed: bool) {
    assert_eq!(
        handle_button(
            ButtonId::Back,
            pressed,
            Some(&mouse()),
            hooks,
            dispatcher,
            || ActionDispatchTarget::Keyboard,
        ),
        EventDisposition::Suppress,
        "a gesture button's edges are owned by its lifecycle"
    );
}

fn started(events: &mpsc::Receiver<ButtonRuntimeEvent>) -> ActionDispatchTarget {
    match events.recv_timeout(Duration::from_secs(1)).expect("down") {
        ButtonRuntimeEvent::Started(press) => press.target(),
        _ => panic!("the press should start first"),
    }
}

/// Every action the press triggered, then how its lifecycle ended.
fn outcome(events: &mpsc::Receiver<ButtonRuntimeEvent>) -> (Vec<Action>, EndReason) {
    let mut triggered = Vec::new();
    loop {
        match events.recv_timeout(Duration::from_secs(1)).expect("end") {
            ButtonRuntimeEvent::Triggered { action, .. } => triggered.push(action),
            ButtonRuntimeEvent::Ended { reason, .. } => return (triggered, reason),
            ButtonRuntimeEvent::Started(_) => panic!("no second press was made"),
        }
    }
}

/// The agent's foreground-app transition (`lifecycle.rs` `apply_foreground`).
fn apply_foreground(orch: &mut Orchestrator, dispatcher: &ActionDispatcher, app: &str) {
    dispatcher.invalidate(orch.set_current_app(Some(ForegroundApp::unnamed(app.into()))));
}

/// The agent's inventory transition (`lifecycle.rs` `apply_inventory`).
fn apply_inventory(
    orch: &mut Orchestrator,
    dispatcher: &ActionDispatcher,
    inventories: &[DeviceInventory],
) {
    dispatcher.invalidate(orch.refresh_inventory(inventories, &[], false));
}

#[test]
fn a_selection_change_mid_hold_never_runs_the_new_devices_gesture_click() {
    let mut config = Config::default();
    config.set_selected_device(Some(SAVED.to_string()));
    gesture_back(&mut config, SAVED, Action::Copy);
    gesture_back(&mut config, FALLBACK, Action::Paste);
    let mut orch = orchestrator(config);
    let hooks = orch.shared().hook_maps;
    let (dispatcher, mut owner, events) = test_dispatcher_with_pointer(hovered_window);
    // The saved mouse sleeps, so the hook follows the fallback mouse.
    apply_inventory(
        &mut orch,
        &dispatcher,
        &[direct_mouse(1, false), direct_mouse(2, true)],
    );

    press_back(&hooks, &dispatcher, true);
    started(&events);
    // The saved mouse wakes mid-hold and takes the selection back.
    apply_inventory(
        &mut orch,
        &dispatcher,
        &[direct_mouse(1, true), direct_mouse(2, true)],
    );
    assert_eq!(
        hooks.read().expect("maps").selected_device.as_deref(),
        Some(SAVED)
    );
    press_back(&hooks, &dispatcher, false);

    let (triggered, reason) = outcome(&events);
    assert_eq!(
        triggered,
        Vec::<Action>::new(),
        "a hold begun under one device's bindings must not finish under another's"
    );
    assert!(matches!(reason, EndReason::Canceled(_)), "{reason:?}");
    assert!(owner.shutdown());
}

#[test]
fn a_steady_inventory_refresh_keeps_a_held_gesture() {
    let mut config = Config::default();
    gesture_back(&mut config, SAVED, Action::Copy);
    let mut orch = orchestrator(config);
    let hooks = orch.shared().hook_maps;
    let (dispatcher, mut owner, events) = test_dispatcher_with_pointer(hovered_window);
    apply_inventory(&mut orch, &dispatcher, &[direct_mouse(1, true)]);

    press_back(&hooks, &dispatcher, true);
    started(&events);
    // A second device appearing leaves the selection where it was.
    apply_inventory(
        &mut orch,
        &dispatcher,
        &[direct_mouse(1, true), direct_mouse(2, true)],
    );
    press_back(&hooks, &dispatcher, false);

    assert_eq!(outcome(&events), (vec![Action::Copy], EndReason::Released));
    assert!(owner.shutdown());
}

#[test]
fn a_foreground_change_keeps_a_held_pointer_scoped_gesture() {
    let mut config = Config::default();
    gesture_back(&mut config, SAVED, Action::Copy);
    // A per-app override keeps pointer sampling continuous on every platform.
    config.set_per_app_binding(SAVED, "editor", ButtonId::Back, Some(Action::Paste));
    let mut orch = orchestrator(config);
    let hooks = orch.shared().hook_maps;
    let (dispatcher, mut owner, events) = test_dispatcher();
    apply_inventory(&mut orch, &dispatcher, &[direct_mouse(1, true)]);
    let hovered = hovered_window();
    let demand = *orch.shared().pointer_discovery.borrow();
    orch.set_pointer_observation(Observation {
        demand,
        context: PointerContext {
            app: Some(ForegroundApp::unnamed("browser".into())),
            target: hovered,
        },
    });
    apply_foreground(&mut orch, &dispatcher, "terminal");

    press_back(&hooks, &dispatcher, true);
    assert_eq!(started(&events), ActionDispatchTarget::Pointer(hovered));
    // A window steals focus while the pointer stays over the browser, whose
    // bindings this press was resolved against.
    apply_foreground(&mut orch, &dispatcher, "editor");
    press_back(&hooks, &dispatcher, false);

    assert_eq!(outcome(&events), (vec![Action::Copy], EndReason::Released));
    assert!(owner.shutdown());
}

#[test]
#[cfg(target_os = "windows")]
fn a_foreground_change_keeps_a_held_on_demand_gesture() {
    let mut config = Config::default();
    gesture_back(&mut config, SAVED, Action::Copy);
    let mut orch = orchestrator(config);
    let hooks = orch.shared().hook_maps;
    let hovered = hovered_window();
    let (dispatcher, mut owner, events) = test_dispatcher_with_pointer(move || hovered);
    apply_inventory(&mut orch, &dispatcher, &[direct_mouse(1, true)]);
    apply_foreground(&mut orch, &dispatcher, "terminal");

    press_back(&hooks, &dispatcher, true);
    assert_eq!(
        started(&events),
        ActionDispatchTarget::AdmittedPointer(hovered)
    );
    apply_foreground(&mut orch, &dispatcher, "editor");
    press_back(&hooks, &dispatcher, false);

    assert_eq!(outcome(&events), (vec![Action::Copy], EndReason::Released));
    assert!(owner.shutdown());
}

#[test]
fn a_foreground_change_still_cancels_a_focus_scoped_hold() {
    let mut config = Config::default();
    config.app_settings.mouse_profile_target = MouseProfileTarget::Focused;
    gesture_back(&mut config, SAVED, Action::Copy);
    let mut orch = orchestrator(config);
    let hooks = orch.shared().hook_maps;
    let (dispatcher, mut owner, events) = test_dispatcher();
    apply_inventory(&mut orch, &dispatcher, &[direct_mouse(1, true)]);
    apply_foreground(&mut orch, &dispatcher, "terminal");

    press_back(&hooks, &dispatcher, true);
    assert_eq!(started(&events), ActionDispatchTarget::Keyboard);
    apply_foreground(&mut orch, &dispatcher, "editor");
    press_back(&hooks, &dispatcher, false);

    let (triggered, reason) = outcome(&events);
    assert_eq!(triggered, Vec::<Action>::new());
    assert!(matches!(reason, EndReason::Canceled(_)), "{reason:?}");
    assert!(owner.shutdown());
}
