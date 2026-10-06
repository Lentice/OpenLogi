use super::*;
use crate::services::ipc::{Command, SetPointerSpeed};
use openlogi_core::hid::PointerSpeed;
use swr_core::SwrClient;
use swr_gpui::GpuiRuntime;

fn m720_state(
    cx: &mut gpui::TestAppContext,
) -> (
    gpui::Entity<AppState>,
    tokio::sync::mpsc::UnboundedReceiver<Command>,
) {
    let resolver = AssetResolver::new();
    let (commands, receiver) = tokio::sync::mpsc::unbounded_channel();
    let mut inventory = direct_inventory([0x4f, 0x4c, 0x44, 0x03]);
    inventory.paired[0].capabilities = Some(Capabilities::from_feature_ids(&[0x1b04, 0x2205]));
    let state = AppState::new(Sources {
        inventories: &[inventory],
        ..Sources::in_memory(Config::ephemeral(), &resolver, commands)
    });
    let state = cx.update(|cx| {
        gpui_component::init(cx);
        crate::ui::theme::register_builtin_themes(cx);
        let runtime = Arc::new(GpuiRuntime::new(cx));
        let cache = SwrClient::builder().build(runtime.clone());
        let state = cx.new(|cx| {
            let mut state = state;
            state.connect_device_reads(cache, runtime);
            state.load_current_pointer_speed(cx);
            state
        });
        state.update(cx, |state, _| {
            let key = state.current_record().expect("mouse").device_key();
            state
                .pointer
                .reads
                .set_pointer_speed_ready(&key, PointerSpeed::NORMAL);
        });
        AppState::set_global(state.clone(), cx);
        state
    });
    (state, receiver)
}

#[gpui::test]
fn pointer_speed_persists_verified_writes_and_fences_removed_routes(cx: &mut gpui::TestAppContext) {
    let (state, mut receiver) = m720_state(cx);
    let (_panel, window_cx) =
        cx.add_window_view(|_, cx| crate::features::pointer::speed::SpeedPanel::new(cx));
    window_cx.update(|window, cx| window.draw(cx).clear(cx));
    window_cx.update(|_, cx| {
        state.update(cx, |state, _| {
            let key = state.current_record().expect("mouse").device_key();
            state
                .pointer
                .reads
                .set_pointer_speed_ready(&key, PointerSpeed::NORMAL);
            while receiver.try_recv().is_ok() {}
            let speed = PointerSpeed::try_new(384).expect("1.5×");
            let events = state.commit_pointer_speed(speed);
            assert_eq!(events, [StateEvent::PointerSpeedChanged(key.clone())]);
            assert_eq!(state.config.pointer_speed(key.as_str()), None);
            assert!(matches!(
                state.current_pointer_speed_load(),
                super::super::PointerSpeedLoad::Loading
            ));
            let commands: Vec<_> = std::iter::from_fn(|| receiver.try_recv().ok()).collect();
            let [
                Command::SetPointerSpeed(SetPointerSpeed {
                    flight,
                    route,
                    speed: sent,
                    ..
                }),
            ] = commands.as_slice()
            else {
                panic!("write before persisting");
            };
            assert_eq!(*sent, speed);
            assert_eq!(
                Some(route),
                state.current_record().expect("mouse").route.as_ref()
            );
            assert!(
                state
                    .apply_pointer_speed_written(&key, *flight + 1, PointerSpeed::NORMAL, Ok(()))
                    .is_empty()
            );
            let _ = state.apply_pointer_speed_written(&key, *flight, speed, Ok(()));
            assert_eq!(state.config.pointer_speed(key.as_str()), Some(speed));
            assert_eq!(
                state.current_pointer_speed_load(),
                super::super::PointerSpeedLoad::Ready(Arc::new(speed))
            );
            let _ = state.commit_pointer_speed(PointerSpeed::NORMAL);
            let commands: Vec<_> = std::iter::from_fn(|| receiver.try_recv().ok()).collect();
            let flight = commands
                .iter()
                .find_map(|command| match command {
                    Command::SetPointerSpeed(write) => Some(write.flight),
                    _ => None,
                })
                .expect("next write");
            let _ = state.apply_pointer_speed_written(
                &key,
                flight,
                PointerSpeed::NORMAL,
                Err(WriteError::AgentUnavailable),
            );
            assert!(matches!(
                state.current_pointer_speed_load(),
                super::super::PointerSpeedLoad::Failed(_)
            ));
            assert_eq!(state.config.pointer_speed(key.as_str()), Some(speed));
            state.pointer.reads.remove(&key);
            assert!(
                state
                    .apply_pointer_speed_written(&key, flight, PointerSpeed::NORMAL, Ok(()))
                    .is_empty()
            );
        });
    });
    window_cx.update(|window, cx| {
        gpui_component::Theme::change(gpui_component::ThemeMode::Dark, Some(window), cx);
        window.draw(cx).clear(cx);
    });
}

#[test]
fn pointer_speed_does_not_write_when_unread_or_unsupported() {
    let resolver = AssetResolver::new();
    let (commands, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let mut state = AppState::new(Sources {
        inventories: &[direct_inventory([0x4f, 0x4c, 0x44, 0x03])],
        ..Sources::in_memory(Config::ephemeral(), &resolver, commands)
    });
    while receiver.try_recv().is_ok() {}
    assert!(state.commit_pointer_speed(PointerSpeed::NORMAL).is_empty());
    assert!(receiver.try_recv().is_err());
}

#[gpui::test]
async fn offline_device_reads_wait_for_reconnect(cx: &mut gpui::TestAppContext) {
    let (state, mut receiver) = m720_state(cx);
    let initial = hold_feature_reads(&mut receiver, 1).await;
    let resolver = AssetResolver::new();
    let mut inventory = pointer_receiver_inventory();
    cx.update(|cx| {
        state.update(cx, |state, _| {
            let _ = state.refresh_inventories(&[inventory.clone()], &[], &resolver, &[]);
        });
        AppState::load_current_device_reads(cx);
    });
    let mut outstanding = hold_feature_reads(&mut receiver, 4).await;
    outstanding.extend(initial);
    inventory.paired[0].online = false;
    cx.update(|cx| {
        state.update(cx, |state, _| {
            let _ = state.refresh_inventories(&[inventory.clone()], &[], &resolver, &[]);
            assert!(!state.current_record().expect("offline mouse").online);
        });
        AppState::load_current_device_reads(cx);
    });
    cx.run_until_parked();
    let offline: Vec<_> = std::iter::from_fn(|| receiver.try_recv().ok()).collect();
    assert!(
        !offline.iter().any(|command| matches!(
            command,
            Command::ReadDpi(_)
                | Command::ReadSmartShift(_)
                | Command::ReadFnLock(_)
                | Command::ReadPointerSpeed(_)
        )),
        "offline device must not restart exhausted feature queries"
    );
    inventory.paired[0].online = true;
    cx.update(|cx| {
        state.update(cx, |state, _| {
            let _ = state.refresh_inventories(&[inventory], &[], &resolver, &[]);
        });
        AppState::load_current_device_reads(cx);
    });
    cx.run_until_parked();
    let online: Vec<_> = std::iter::from_fn(|| receiver.try_recv().ok()).collect();
    assert_eq!(
        [
            online
                .iter()
                .any(|command| matches!(command, Command::ReadDpi(_))),
            online
                .iter()
                .any(|command| matches!(command, Command::ReadSmartShift(_))),
            online
                .iter()
                .any(|command| matches!(command, Command::ReadFnLock(_))),
            online
                .iter()
                .any(|command| matches!(command, Command::ReadPointerSpeed(_))),
        ],
        [true; 4]
    );
    // A reply from before sleep must never repopulate the replacement query.
    for command in outstanding {
        if let Command::ReadPointerSpeed(read) = command {
            let _ = read
                .reply
                .send(Ok(PointerSpeed::try_new(384).expect("1.5×")));
        }
    }
    cx.run_until_parked();
    for command in online {
        if let Command::ReadPointerSpeed(read) = command {
            let _ = read.reply.send(Ok(PointerSpeed::NORMAL));
        }
    }
    cx.run_until_parked();
    state.read_with(cx, |state, _| {
        assert_eq!(
            state.current_pointer_speed_load(),
            super::super::PointerSpeedLoad::Ready(Arc::new(PointerSpeed::NORMAL))
        );
    });
}

fn pointer_receiver_inventory() -> DeviceInventory {
    let mut inventory = direct_inventory([0x4f, 0x4c, 0x44, 0x03]);
    inventory.receiver.unique_id = Some("synthetic-receiver".into());
    inventory.receiver.product_id = 0xc52b;
    inventory.paired[0].slot = 1;
    inventory.paired[0].capabilities = Some(Capabilities::from_feature_ids(&[
        0x1b04, 0x2201, 0x2110, 0x40a2, 0x2205,
    ]));
    inventory
}

async fn hold_feature_reads(
    receiver: &mut tokio::sync::mpsc::UnboundedReceiver<Command>,
    count: usize,
) -> Vec<Command> {
    let mut reads = Vec::new();
    while reads.len() < count {
        let command = receiver.recv().await.expect("online feature read");
        if matches!(
            command,
            Command::ReadDpi(_)
                | Command::ReadSmartShift(_)
                | Command::ReadFnLock(_)
                | Command::ReadPointerSpeed(_)
        ) {
            reads.push(command);
        }
    }
    reads
}
