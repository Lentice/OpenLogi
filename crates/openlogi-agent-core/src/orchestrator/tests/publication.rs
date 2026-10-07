//! Inventory readiness and what every mutator republishes.

use super::*;
use std::collections::BTreeMap;

#[test]
fn action_ring_demand_tracks_usable_bindings_inventory_and_profiles() {
    let inventory = direct_inventory(Some("ring-mouse"), [1, 2, 3, 4]);
    let mut orch = orchestrator(Config::ephemeral());
    let mut demand = orch.shared().action_ring_demand;
    assert!(!*demand.borrow());
    orch.refresh_inventory(std::slice::from_ref(&inventory), &[], false);
    let key = orch.devices[0].config_key.clone();
    let mut config = Config::ephemeral();
    config.app_settings.mouse_profile_target = openlogi_core::config::MouseProfileTarget::Focused;
    config.set_pointer_speed(&key, openlogi_core::hid::PointerSpeed::NORMAL);
    orch.reload_config(config.clone());
    assert!(
        !*demand.borrow(),
        "ordinary M720 settings cannot imply a haptic panel"
    );
    config.set_binding(
        &key,
        ButtonId::GestureButton,
        Action::ShowActionsRing.into(),
    );
    orch.reload_config(config.clone());
    assert!(*demand.borrow(), "a usable thumb trigger warms the overlay");
    let _ = demand.borrow_and_update();
    orch.refresh_inventory(std::slice::from_ref(&inventory), &[], false);
    assert!(
        !demand.has_changed().expect("projection open"),
        "identical inventory must not wake the helper"
    );
    config.set_action_ring_enabled(&key, false);
    orch.reload_config(config.clone());
    assert!(!*demand.borrow(), "a trigger cannot bypass the ring switch");
    config.set_action_ring_enabled(&key, true);
    config.set_per_app_binding(&key, "editor", ButtonId::GestureButton, Some(Action::Copy));
    orch.reload_config(config.clone());
    assert!(*demand.borrow());
    orch.set_current_app(Some(ForegroundApp::unnamed("editor".into())));
    assert!(
        !*demand.borrow(),
        "foreground override changes usable demand"
    );
    orch.set_current_app(None);
    assert!(*demand.borrow());
    orch.refresh_inventory(&[], &[], false);
    assert!(!*demand.borrow(), "no device means no configured demand");
    orch.refresh_inventory(std::slice::from_ref(&inventory), &[], false);
    assert!(*demand.borrow(), "reconnect restores demand");
}

#[test]
fn action_ring_demand_includes_haptic_defaults_long_press_gestures_and_keyboard() {
    let mut inventory = direct_inventory(Some("ring-mouse"), [1, 2, 3, 4]);
    let mut orch = orchestrator(Config::ephemeral());
    orch.refresh_inventory(std::slice::from_ref(&inventory), &[], false);
    let key = orch.devices[0].config_key.clone();
    let demand = orch.shared().action_ring_demand;
    assert!(!*demand.borrow());
    inventory.paired[0]
        .capabilities
        .as_mut()
        .expect("capabilities")
        .haptic_feedback = true;
    orch.refresh_inventory(std::slice::from_ref(&inventory), &[], false);
    assert!(
        *demand.borrow(),
        "haptic panel default can warm without saved config"
    );
    inventory.paired[0]
        .capabilities
        .as_mut()
        .expect("capabilities")
        .haptic_feedback = false;
    orch.refresh_inventory(std::slice::from_ref(&inventory), &[], false);
    assert!(!*demand.borrow());
    let mut config = Config::ephemeral();
    config.set_binding(
        &key,
        ButtonId::GestureButton,
        Binding::LongPress(openlogi_core::binding::LongPressBinding::new(
            Action::Copy,
            Action::ShowActionsRing,
        )),
    );
    orch.reload_config(config.clone());
    assert!(*demand.borrow(), "long action is a ring trigger");
    config.set_binding(
        &key,
        ButtonId::GestureButton,
        Binding::Gesture(BTreeMap::from([(
            openlogi_core::binding::GestureDirection::Right,
            Action::ShowActionsRing,
        )])),
    );
    orch.reload_config(config.clone());
    assert!(*demand.borrow(), "directional action is a ring trigger");
    config.set_binding(&key, ButtonId::GestureButton, Action::Copy.into());
    config.keyboard.bindings.insert(
        "f1".parse().expect("valid trigger"),
        Action::ShowActionsRing,
    );
    orch.reload_config(config);
    assert!(*demand.borrow(), "global keyboard trigger remains usable");
}

/// An *empty* snapshot still flips the health to `Ready`: the watcher only
/// forwards completed enumerations, so "checked and found nothing" must not
/// be reported as "still scanning" — that's the whole distinction the
/// health exists to carry.
#[test]
fn empty_refresh_marks_inventory_ready() {
    let mut orch = orchestrator(Config::default());
    assert_eq!(orch.inventory_health(), InventoryHealth::Scanning);
    orch.refresh_inventory(&[], &[], false);
    assert_eq!(orch.inventory_health(), InventoryHealth::Ready);
}

/// `Unavailable` is a startup-only downgrade: it reports "enumeration has
/// never worked", recovers when a snapshot finally lands, and never
/// clobbers a live device set on a mid-session failure (mirroring the
/// watcher's keep-last-snapshot policy).
#[test]
fn unavailable_only_downgrades_a_pending_inventory() {
    let mut orch = orchestrator(Config::default());
    orch.mark_inventory_unavailable();
    assert_eq!(orch.inventory_health(), InventoryHealth::Unavailable);
    orch.refresh_inventory(&[], &[], false);
    assert_eq!(orch.inventory_health(), InventoryHealth::Ready);
    orch.mark_inventory_unavailable();
    assert_eq!(orch.inventory_health(), InventoryHealth::Ready);
}

#[test]
fn every_inventory_mutator_republishes_what_the_ipc_server_answers() {
    let observable = Arc::new(ObservableState::new("test".to_string()));
    let mut orch = Orchestrator::new(Config::default(), Arc::clone(&observable));
    assert_eq!(
        observable.snapshot().status.inventory,
        InventoryHealth::Scanning,
        "a fresh agent has not enumerated yet"
    );

    orch.mark_inventory_unavailable();
    assert_eq!(
        observable.snapshot().status.inventory,
        InventoryHealth::Unavailable
    );

    orch.refresh_inventory(
        &[direct_inventory(Some("serial-1"), [1, 2, 3, 4])],
        &[],
        false,
    );
    let published = observable.snapshot();
    assert_eq!(published.status.inventory, InventoryHealth::Ready);
    assert_eq!(published.inventory, orch.inventory());
    assert_eq!(published.standalone, orch.standalone());
    assert_eq!(published.inventory.len(), 1);

    // A camera sample and a config reload are the other two facts the cell
    // carries; both must reach it from inside the mutator.
    orch.set_camera_active(true);
    assert!(observable.snapshot().camera_active);

    let mut config = Config::default();
    config.app_settings.launch_at_login = true;
    orch.reload_config(config);
    assert!(observable.snapshot().status.launch_at_login);
}

#[test]
fn equal_runtime_projection_does_not_wake_managers() {
    let mut orch = orchestrator(Config::default());
    orch.devices = vec![dev("a", 1, true)];
    orch.rebuild();
    let mut capture_plans = orch.shared.capture_plans.clone();
    let mut keyboard_spec = orch.shared.keyboard_spec.clone();
    let mut host_switch_links = orch.shared.host_switch_links.clone();
    let _ = capture_plans.borrow_and_update();
    let _ = keyboard_spec.borrow_and_update();
    let _ = host_switch_links.borrow_and_update();

    orch.publish_device_runtime();

    assert!(
        !capture_plans
            .has_changed()
            .expect("publication remains open")
    );
    assert!(
        !keyboard_spec
            .has_changed()
            .expect("publication remains open")
    );
    assert!(
        !host_switch_links
            .has_changed()
            .expect("publication remains open")
    );
}
