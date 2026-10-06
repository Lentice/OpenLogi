//! Replay-backed vertical coverage for the agent hardware boundary.

use std::sync::Arc;
use std::time::Duration;

use openlogi_core::config::Config;
use openlogi_fixture::{
    CassetteExchange, FIXTURE_SCHEMA_VERSION, HidCassette, ReportSupport, RequestMatch,
};
use openlogi_hid::replay::{
    ChannelConnection, NodePresence, OpenOutcome, RawWriterAvailability, ReplayBackend,
    ReplayChannel, ReplayNode, ReplayTopology,
};
use openlogi_hid::{
    DeviceRoute, Dpi, HidppOperation, NodeId, NodeInfo, device_io_channel, get_dpi_info_on,
};

use crate::hardware::HardwareContext;
use crate::observable::ObservableState;
use crate::orchestrator::Orchestrator;

use super::{InventoryEvent, spawn_with_hardware};

const CHANNEL: &str = "agent-direct-mouse";
const PRODUCT_ID: u16 = 0xb35b;

#[tokio::test]
async fn replay_inventory_and_authoritative_read_share_one_injected_backend() {
    let node_id = NodeId::from("agent-direct-node".to_string());
    let route = DeviceRoute::Direct {
        vendor_id: 0x046d,
        product_id: PRODUCT_ID,
    };
    let backend = Arc::new(
        ReplayBackend::new(
            direct_topology(node_id.clone()),
            vec![direct_inventory_and_dpi_cassette()],
        )
        .expect("valid agent replay fixture"),
    );
    let (_device_io_signal, device_io) = device_io_channel();
    let hardware = HardwareContext::injected(backend.clone(), device_io);
    let observable = Arc::new(ObservableState::new("test".to_string()));
    let mut orchestrator = Orchestrator::with_hardware(Config::default(), observable, hardware);
    let shared = orchestrator.shared();
    let mut watcher = spawn_with_hardware(shared.hardware(), shared.channel_registry.clone());

    let event = tokio::time::timeout(Duration::from_secs(2), watcher.events.recv())
        .await
        .expect("initial replay inventory must be bounded")
        .expect("inventory watcher must publish its initial snapshot");
    let (inventories, standalone, hid_open_failures) = match event {
        InventoryEvent::Snapshot {
            inventories,
            standalone,
            hid_open_failures,
        } => (inventories, standalone, hid_open_failures),
        InventoryEvent::Unavailable | InventoryEvent::SystemWake => {
            panic!("initial replay reconciliation must publish a snapshot")
        }
    };
    assert_eq!(inventories.len(), 1);
    assert!(standalone.is_empty());
    assert!(!hid_open_failures);
    assert!(
        shared
            .channel_registry
            .lookup(&route)
            .is_some_and(|channel| channel.matches(&route)),
        "the watcher must publish the exact direct-device route"
    );
    assert!(
        shared
            .channel_registry
            .lookup(&DeviceRoute::Direct {
                vendor_id: 0x046d,
                product_id: PRODUCT_ID + 1,
            })
            .is_none(),
        "registry lookup must remain exact"
    );

    orchestrator.refresh_inventory(&inventories, &standalone, hid_open_failures);
    assert_eq!(orchestrator.inventory(), inventories);

    let dpi = shared
        .device(&route)
        .run(HidppOperation::ReadDpiCapabilities, |channel| async move {
            get_dpi_info_on(&channel).await
        })
        .await
        .expect("authoritative replay DPI read succeeds");
    assert_eq!(dpi.current, Dpi::new(800));
    assert_eq!(
        dpi.capabilities.values(),
        [Dpi::new(400), Dpi::new(800), Dpi::new(1600)]
    );
    assert_eq!(backend.open_count(&node_id).expect("known replay node"), 1);
    let completion = backend
        .channel_completion(CHANNEL)
        .expect("known replay channel");
    assert_eq!(completion.channel_open_count, 1);
    assert!(completion.unmatched_requests.is_empty());
    backend
        .require_complete()
        .expect("inventory and DPI exchanges fully consumed");
}

fn direct_topology(node_id: NodeId) -> ReplayTopology {
    ReplayTopology {
        nodes: vec![ReplayNode {
            info: NodeInfo {
                id: node_id,
                vendor_id: 0x046d,
                product_id: PRODUCT_ID,
                usage_page: 0xff00,
                usage_id: 0x0002,
                name: "Agent Replay Mouse".to_string(),
                manufacturer: Some("Logitech".to_string()),
                serial_number: None,
            },
            presence: NodePresence::Present,
            open_outcome: OpenOutcome::Hidpp,
            channel: Some(CHANNEL.to_string()),
            raw_writer: RawWriterAvailability::Unavailable,
            receiver_slots: Vec::new(),
        }],
        channels: vec![ReplayChannel {
            id: CHANNEL.to_string(),
            connection: ChannelConnection::Connected,
            report_support: ReportSupport::ShortAndLong,
        }],
    }
}

#[tokio::test]
async fn replay_first_speed_read_follows_the_published_reconnect_restoration() {
    use crate::receiver_access::ExclusiveAccessReason;
    use openlogi_core::hid::PointerSpeed;

    let node = NodeId::from("agent-scaling-node".to_string());
    let route = DeviceRoute::Direct {
        vendor_id: 0x046d,
        product_id: PRODUCT_ID,
    };
    let backend = Arc::new(
        ReplayBackend::new(
            direct_topology(node.clone()),
            vec![scaling_restore_cassette()],
        )
        .expect("valid scaling replay"),
    );
    let speed = PointerSpeed::try_new(384).expect("1.5×");
    let mut config = Config::ephemeral();
    config.set_pointer_speed("unit:4f4c4404", speed);
    let hardware = HardwareContext::injected(backend.clone(), device_io_channel().1);
    let observable = Arc::new(ObservableState::new("test".into()));
    let mut orchestrator = Orchestrator::with_hardware(config, observable.clone(), hardware);
    let shared = orchestrator.shared();
    let inventories = shared
        .hardware()
        .enumerator()
        .with_registry(shared.channel_registry.clone())
        .enumerate()
        .await
        .expect("scaling device inventory");
    assert!(
        !inventories.is_empty(),
        "inventory failed: {:?}",
        backend.channel_completion(CHANNEL)
    );
    assert!(
        inventories[0].paired[0]
            .capabilities
            .expect("measured capabilities")
            .pointer_speed
    );

    // Hold restoration before its worker can touch HID. Publishing inventory
    // must already register that work, so the first RPC read waits for it.
    let exclusive = shared
        .receiver_access
        .acquire_exclusive(ExclusiveAccessReason::Pairing)
        .await;
    orchestrator.refresh_inventory(&inventories, &[], false);
    assert!(observable.snapshot().inventory[0].paired[0].online);
    let read = shared
        .device(&route)
        .run(HidppOperation::ReadPointerSpeed, |channel| async move {
            openlogi_hid::get_pointer_speed_on(&channel).await
        });
    tokio::pin!(read);
    assert!(futures_lite::future::poll_once(&mut read).await.is_none());
    drop(exclusive);
    let actual = tokio::time::timeout(Duration::from_secs(2), read)
        .await
        .expect("restoration and read are bounded")
        .expect("verified restored speed");
    assert_eq!(actual, speed);
    assert_eq!(backend.open_count(&node).expect("known node"), 1);
    backend
        .require_complete()
        .expect("restore, verification and client read consumed in order");
}

fn scaling_restore_cassette() -> HidCassette {
    let mut cassette = direct_inventory_and_dpi_cassette();
    cassette.name = "agent pointer scaling restoration then read".into();
    cassette.exchanges.truncate(5);
    cassette.exchanges[2] = h20(short(0xff, 1, 0, [0; 3]), short(0xff, 1, 0, [3, 0, 0]));
    cassette.exchanges[4] = h20(
        short(0xff, 1, 0x10, [2, 0, 0]),
        short(0xff, 1, 0x10, [0x22, 0x05, 0]),
    );
    cassette.exchanges.extend([
        h20(
            short(0xff, 1, 0x10, [3, 0, 0]),
            short(0xff, 1, 0x10, [0, 3, 0]),
        ),
        h20(
            short(0xff, 3, 0, [0; 3]),
            long(
                0xff,
                3,
                0,
                [
                    1, 0x4f, 0x4c, 0x44, 4, 0, 1, 0xb3, 0x5b, 0, 0, 0, 0, 0, 0, 0,
                ],
            ),
        ),
    ]);
    for write in [true, false] {
        cassette.exchanges.extend([
            h20(
                short(0xff, 0, 0x10, [0; 3]),
                short(0xff, 0, 0x10, [4, 0, 0]),
            ),
            h20(
                short(0xff, 0, 0, [0x22, 0x05, 0]),
                short(0xff, 0, 0, [2, 0, 0]),
            ),
        ]);
        if write {
            cassette.exchanges.push(h20(
                short(0xff, 2, 0x10, [1, 0x80, 0]),
                short(0xff, 2, 0x10, [0; 3]),
            ));
        }
        cassette.exchanges.push(h20(
            short(0xff, 2, 0, [0; 3]),
            short(0xff, 2, 0, [1, 0x80, 0]),
        ));
    }
    cassette
}

fn direct_inventory_and_dpi_cassette() -> HidCassette {
    let exchanges = vec![
        h20(
            short(0xff, 0x00, 0x10, [0, 0, 0]),
            short(0xff, 0x00, 0x10, [4, 0, 0]),
        ),
        h20(
            short(0xff, 0x00, 0x00, [0x00, 0x01, 0]),
            short(0xff, 0x00, 0x00, [0x01, 0, 0]),
        ),
        h20(
            short(0xff, 0x01, 0x00, [0, 0, 0]),
            short(0xff, 0x01, 0x00, [2, 0, 0]),
        ),
        h20(
            short(0xff, 0x01, 0x10, [1, 0, 0]),
            short(0xff, 0x01, 0x10, [0x00, 0x01, 0]),
        ),
        h20(
            short(0xff, 0x01, 0x10, [2, 0, 0]),
            short(0xff, 0x01, 0x10, [0x22, 0x01, 0]),
        ),
        h20(
            short(0xff, 0x00, 0x10, [0, 0, 0]),
            short(0xff, 0x00, 0x10, [4, 0, 0]),
        ),
        h20(
            short(0xff, 0x00, 0x00, [0x22, 0x01, 0]),
            short(0xff, 0x00, 0x00, [0x02, 0, 0]),
        ),
        h20(
            short(0xff, 0x02, 0x00, [0, 0, 0]),
            short(0xff, 0x02, 0x00, [1, 0, 0]),
        ),
        h20(
            short(0xff, 0x02, 0x20, [0, 0, 0]),
            short(0xff, 0x02, 0x20, [0, 0x03, 0x20]),
        ),
        h20(
            short(0xff, 0x02, 0x10, [0, 0, 0]),
            long(
                0xff,
                0x02,
                0x10,
                [
                    0, 0x01, 0x90, 0x03, 0x20, 0x06, 0x40, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                ],
            ),
        ),
    ];
    HidCassette {
        schema_version: FIXTURE_SCHEMA_VERSION,
        name: "agent inventory and DPI read".to_string(),
        channel: CHANNEL.to_string(),
        report_support: ReportSupport::ShortAndLong,
        exchanges,
    }
}

fn h20(request: Vec<u8>, response: Vec<u8>) -> CassetteExchange {
    CassetteExchange {
        request_match: RequestMatch::Hidpp20,
        request,
        response: Some(response),
        required: true,
    }
}

fn short(device: u8, feature: u8, function: u8, payload: [u8; 3]) -> Vec<u8> {
    vec![
        0x10, device, feature, function, payload[0], payload[1], payload[2],
    ]
}

fn long(device: u8, feature: u8, function: u8, payload: [u8; 16]) -> Vec<u8> {
    let mut report = vec![0x11, device, feature, function];
    report.extend_from_slice(&payload);
    report
}
