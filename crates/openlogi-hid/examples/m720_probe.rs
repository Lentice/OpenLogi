//! Capture an M720 thumb button through the production session, restoring it afterwards.

use std::{error::Error, sync::Arc, time::Duration};

use openlogi_core::binding::ButtonId;
use openlogi_hid::reprog_controls::control_ids;
use openlogi_hid::session::gesture::{
    CaptureHost, CaptureSessionOutcome, CaptureSpec, run_capture_session,
};
use openlogi_hid::{ChannelRegistry, DeviceRoute, host, inventory::Enumerator};
use tokio::sync::{mpsc, oneshot};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let seconds = std::env::args()
        .nth(1)
        .map_or(Ok(60), |s| s.parse::<u64>())?;
    let registry = ChannelRegistry::default();
    let mut enumerator = Enumerator::with_backend(host::backend()).with_registry(registry.clone());
    let inventory = enumerator.enumerate().await?;
    let routes: Vec<_> = inventory
        .iter()
        .flat_map(|inv| {
            inv.paired
                .iter()
                .filter(|p| p.online && p.codename.as_deref().is_some_and(|n| n.contains("M720")))
                .filter_map(|p| DeviceRoute::for_slot(inv, p.slot))
        })
        .collect();
    let [route] = routes.as_slice() else {
        return Err("Expected exactly one online M720; quit OpenLogi and Options+ first".into());
    };
    if std::env::args().nth(2).as_deref() == Some("speed") {
        let shared = registry.lookup(route).ok_or("M720 channel unavailable")?;
        let original = openlogi_hid::get_pointer_speed_on(&shared).await?;
        println!(
            "Original pointer speed: {:.2}x (raw {})",
            original.multiplier(),
            original.into_inner()
        );
        let trial = async {
            for raw in [384., 128.] {
                let speed = openlogi_hid::PointerSpeed::from_rounded(raw);
                openlogi_hid::set_pointer_speed_on(&shared, speed).await?;
                println!(
                    "Pointer speed: {:.2}x for {seconds}s; move the mouse to compare.",
                    speed.multiplier()
                );
                tokio::time::sleep(Duration::from_secs(seconds)).await;
            }
            Ok::<_, openlogi_hid::WriteError>(())
        }
        .await;
        // Restore even if a test write or read-back failed.
        openlogi_hid::set_pointer_speed_on(&shared, original).await?;
        println!("Original pointer speed restored and verified.");
        trial?;
        return Ok(());
    }
    let (sink, mut inputs) = mpsc::unbounded_channel();
    let (stop, shutdown) = oneshot::channel();
    let spec = CaptureSpec {
        divert_buttons: vec![(
            control_ids::MULTIPLATFORM_GESTURE_BUTTON.0,
            ButtonId::GestureButton,
        )],
        ..Default::default()
    };
    let capture = run_capture_session(
        route.clone(),
        spec,
        CaptureHost {
            sink,
            shutdown,
            channel_slot: Arc::default(),
            registry: &registry,
            device_io: host::device_io_gate(),
        },
    );
    tokio::pin!(capture);
    let timer = tokio::time::sleep(Duration::from_secs(seconds));
    tokio::pin!(timer);
    println!(
        "Press/release the thumb-rest button. Capturing for {seconds}s; native reporting is restored at the end."
    );
    loop {
        tokio::select! {
            result = &mut capture => { return finish(&result?); }
            Some(input) = inputs.recv() => println!("{input:?}"),
            () = &mut timer => break,
        }
    }
    let _ = stop.send(());
    let outcome = capture.await?;
    while let Ok(input) = inputs.try_recv() {
        println!("{input:?}");
    }
    finish(&outcome)
}

fn finish(outcome: &CaptureSessionOutcome) -> Result<(), Box<dyn Error>> {
    match outcome {
        CaptureSessionOutcome::Restored => {
            println!("Original reporting restored.");
            Ok(())
        }
        CaptureSessionOutcome::RestorePending(_) => Err(
            "Restoration incomplete; power-cycle the mouse to release temporary diversion".into(),
        ),
    }
}
