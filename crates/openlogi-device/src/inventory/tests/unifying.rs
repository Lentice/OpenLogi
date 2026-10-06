//! The Unifying receiver: arrival broadcasts, the arrival trigger and its retries, and codename reads.

use super::*;

#[tokio::test]
async fn changed_unit_with_a_failed_serial_read_cannot_borrow_the_previous_identity() {
    let fail_serial = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let responder_failure = Arc::clone(&fail_serial);
    let (raw, handle) = ScriptedRawHidChannel::with_dynamic_responder(move |request| {
        if request[2] == 99
            || (request[2], request[3] >> 4) == (2, 2)
                && responder_failure.load(std::sync::atomic::Ordering::Relaxed)
        {
            return Some(crate::channel::scripted::feature_error(request, 6));
        }
        let mut response = vec![0; 20];
        response[..4].copy_from_slice(&request[..4]);
        response[0] = 0x11;
        match (request[2], request[3] >> 4) {
            (0, 1) | (1, 0) => response[4] = 4,
            (0, 0) => response[4] = 1,
            (1, 1) => response[4..6].copy_from_slice(
                &[0x0001_u16, 0x0003, 0x0005, 0x2201][usize::from(request[4]) - 1].to_be_bytes(),
            ),
            (2, 0) => {
                response[4..9].copy_from_slice(&[1, 2, 2, 2, 2]);
                response[18] = 1;
            }
            (2, 2) => response[4..14].copy_from_slice(b"NEW-SERIAL"),
            (3, 2) => response[4] = 3,
            (3, 0) => response[4] = 5,
            (3, 1) => response[4..9].copy_from_slice(b"Unit2"),
            _ => panic!("unexpected request {request:02x?}"),
        }
        Some(response)
    });
    let channel = scripted_channel(raw).await;
    let event = online_slot_event();
    let key = CacheKey::UnifyingSlot {
        receiver_uid: "SERIAL".into(),
        slot: 1,
        wpid: 0x4069,
    };
    let mut entry = cache_entry();
    entry.probe.model_info = Some(model([1; 4], Some("OLD-SERIAL")));
    entry.probe.identity_feature = Some(99);
    let mut enumerator = Enumerator::with_backend(ScriptedBackend::new(Vec::new()));
    enumerator.cache.insert(key.clone(), entry);
    for pass in 0..3 {
        fail_serial.store(pass == 0, std::sync::atomic::Ordering::Relaxed);
        let before = handle.written_reports().len();
        let (device, outcome) = probe_unifying_slot(
            &channel,
            &event,
            "SERIAL",
            PassContext {
                cache: &enumerator.cache,
                now: Instant::now(),
                subscriptions: None,
                timeouts: &ProbeTimeouts::DEFAULT,
            },
        )
        .await
        .unwrap();
        let info = device.model_info.unwrap();
        assert_eq!(info.unit_id, [2; 4]);
        if pass == 0 {
            assert!(
                info.serial_number.is_none(),
                "a new unit cannot borrow the old serial"
            );
        } else {
            assert_eq!(info.serial_number.as_deref(), Some("NEW-SERIAL"));
        }
        enumerator.apply_outcomes(vec![outcome]);
        if pass == 0 {
            assert!(
                !enumerator.cache.contains_key(&key),
                "the old unit must not survive a partial replacement probe"
            );
        } else if pass == 2 {
            assert_eq!(
                handle.written_reports().len(),
                before + 1,
                "a complete repair returns to a single identity check"
            );
        }
    }
}

#[tokio::test]
async fn rejected_identity_index_keeps_repair_pending_after_a_failed_probe() {
    let key = CacheKey::UnifyingSlot {
        receiver_uid: "SERIAL".into(),
        slot: 1,
        wpid: 0x4069,
    };
    let mut entry = cache_entry();
    entry.probe.model_info = Some(model([1, 2, 3, 4], None));
    entry.probe.identity_feature = Some(2);
    let mut enumerator = Enumerator::with_backend(ScriptedBackend::new(Vec::new()));
    enumerator.cache.insert(key.clone(), entry);
    let (raw, _) = ScriptedRawHidChannel::with_responder(|request| {
        Some(crate::channel::scripted::feature_error(request, 6))
    });
    let channel = scripted_channel(raw).await;
    let event = online_slot_event();
    let (device, outcome) = probe_unifying_slot(
        &channel,
        &event,
        "SERIAL",
        PassContext {
            cache: &enumerator.cache,
            now: Instant::now(),
            subscriptions: None,
            timeouts: &ProbeTimeouts::DEFAULT,
        },
    )
    .await
    .unwrap();
    assert_eq!(device.model_info.unwrap().unit_id, [1, 2, 3, 4]);
    enumerator.apply_outcomes(vec![outcome]);
    assert!(
        enumerator.cache[&key].probed_at.is_none(),
        "a failed repair must not restore the rejected index's validation"
    );
}

#[tokio::test]
async fn same_wpid_slot_replacement_is_detected_by_a_cheap_own_unit_read() {
    let unit = Arc::new(std::sync::atomic::AtomicU8::new(1));
    let responder_unit = Arc::clone(&unit);
    let fail_probe = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let responder_failure = Arc::clone(&fail_probe);
    let (raw, handle) = ScriptedRawHidChannel::with_dynamic_responder(move |request| {
        let unit = responder_unit.load(std::sync::atomic::Ordering::Relaxed);
        let mut response = vec![0; 20];
        response[..4].copy_from_slice(&request[..4]);
        response[0] = 0x11;
        if responder_failure.load(std::sync::atomic::Ordering::Relaxed)
            && (request[2], request[3] >> 4) == (0, 1)
        {
            response[2..6].copy_from_slice(&[0xff, request[2], request[3], 6]);
            return Some(response);
        }
        match (request[2], request[3] >> 4) {
            (0, 1) | (1, 0) => response[4] = 4,
            (0, 0) => response[4] = 1,
            (1, 1) => response[4..6].copy_from_slice(
                &[0x0001_u16, 0x0003, 0x0005, 0x2201][usize::from(request[4]) - 1].to_be_bytes(),
            ),
            (2, 0) => response[4..9].copy_from_slice(&[1, unit, unit, unit, unit]),
            (3, 2) => response[4] = 3,
            (3, 0) => response[4] = 5,
            (3, 1) => response[4..9].copy_from_slice(format!("Unit{unit}").as_bytes()),
            _ => panic!("unexpected request {request:02x?}"),
        }
        Some(response)
    });
    let channel = scripted_channel(raw).await;
    let event = online_slot_event();
    let mut enumerator = Enumerator::with_backend(ScriptedBackend::new(Vec::new()));
    for (expected_unit, failed) in [(1, false), (2, true), (2, false), (2, false)] {
        unit.store(expected_unit, std::sync::atomic::Ordering::Relaxed);
        fail_probe.store(failed, std::sync::atomic::Ordering::Relaxed);
        let was_cached = enumerator.cache.values().any(|entry| {
            entry
                .probe
                .model_info
                .as_ref()
                .is_some_and(|model| model.unit_id == [expected_unit; 4])
        });
        let before = handle.written_reports().len();
        let (device, outcome) = probe_unifying_slot(
            &channel,
            &event,
            "SERIAL",
            PassContext {
                cache: &enumerator.cache,
                now: Instant::now(),
                subscriptions: None,
                timeouts: &ProbeTimeouts::DEFAULT,
            },
        )
        .await
        .unwrap();
        if failed {
            assert!(
                device.model_info.is_none(),
                "failed replacement probe must not restore the predecessor"
            );
            enumerator.apply_outcomes(vec![outcome]);
            assert!(
                enumerator.cache.is_empty(),
                "replacement identity ends the old cache lifetime even when its probe fails"
            );
            continue;
        }
        assert_eq!(
            device.model_info.unwrap().unit_id,
            [expected_unit; 4],
            "the same model in the same slot must not inherit its predecessor's identity"
        );
        assert_eq!(
            device.codename.as_deref(),
            Some(format!("Unit{expected_unit}").as_str())
        );
        enumerator.apply_outcomes(vec![outcome]);
        if was_cached {
            let writes = handle.written_reports();
            assert_eq!(
                writes.len(),
                before + 1,
                "a known unit only needs its own-ID check"
            );
            assert_eq!(
                writes.last().unwrap()[2],
                2,
                "no model-name or feature-table request"
            );
        }
    }
}

#[tokio::test]
async fn a_changed_arrival_wpid_cannot_inherit_the_slots_cached_identity() {
    let message = Message::Short(
        MessageHeader {
            device_index: 1,
            sub_id: 0x41,
        },
        [0x04, 0x62, 0x69, 0x40],
    );
    let Some(UnifyingEvent::DeviceConnection(event)) = decode_notification(&message) else {
        panic!("device connection expected");
    };
    let old_key = CacheKey::UnifyingSlot {
        receiver_uid: "SERIAL".into(),
        slot: 1,
        wpid: 0x40b8,
    };
    let mut entry = cache_entry();
    entry.probe = probed(Some(model([1, 2, 3, 4], Some("OLD"))), false);
    let cache = HashMap::from([(old_key, entry)]);
    let (raw, handle) = ScriptedRawHidChannel::with_responder(|_| None);
    let channel = scripted_channel(raw).await;
    let before = handle.written_reports().len();
    let (device, outcome) = probe_unifying_slot(
        &channel,
        &event,
        "SERIAL",
        PassContext {
            cache: &cache,
            now: Instant::now(),
            subscriptions: None,
            timeouts: &ProbeTimeouts::DEFAULT,
        },
    )
    .await
    .unwrap();
    assert!(
        device.model_info.is_none(),
        "an offline replacement must never get the old unit ID"
    );
    assert_eq!(
        outcome.key(),
        Some(&CacheKey::UnifyingSlot {
            receiver_uid: "SERIAL".into(),
            slot: 1,
            wpid: 0x4069,
        })
    );
    assert_eq!(
        handle.written_reports().len(),
        before,
        "offline identity remains arrival-authoritative"
    );
}

#[test]
fn a_complete_unpaired_snapshot_ends_the_same_model_slots_cache_lifetime() {
    let mut enumerator = Enumerator::with_backend(ScriptedBackend::new(Vec::new()));
    let node = NodeId::from("unpaired-receiver".to_string());
    let key = CacheKey::UnifyingSlot {
        receiver_uid: "SERIAL".into(),
        slot: 1,
        wpid: 0x4069,
    };
    let known = NodeProbe {
        inventory: Some(inventory(&[1]).remove(0)),
        verdict: ProbeVerdict::Healthy { complete: true },
        outcomes: vec![CacheOutcome::Fresh(key.clone(), cache_entry())],
    };
    enumerator.hold_or_note_cache_keys(&node, &known, &mut HashSet::new());
    enumerator.apply_outcomes(known.outcomes);
    let incomplete = NodeProbe {
        inventory: None,
        verdict: ProbeVerdict::Healthy { complete: false },
        outcomes: vec![],
    };
    enumerator.hold_or_note_cache_keys(&node, &incomplete, &mut HashSet::new());
    assert!(
        enumerator.cache.contains_key(&key),
        "a missing offline arrival is not proof of unpairing"
    );
    let unpaired = NodeProbe {
        inventory: Some(inventory(&[]).remove(0)),
        verdict: ProbeVerdict::Healthy { complete: true },
        outcomes: vec![],
    };
    enumerator.hold_or_note_cache_keys(&node, &unpaired, &mut HashSet::new());
    assert!(
        !enumerator.cache.contains_key(&key),
        "same-WPID re-pair must start without the old probe"
    );
}

#[tokio::test]
async fn offline_arrival_rebroadcasts_surface_without_probing_the_device() {
    // The exact wire bytes once misread as proof that the online bit is
    // stuck: `04 62 69 40` is an encrypted MX Master 2S (wpid 0x4069) slot
    // re-broadcast with bit 6 *set* — link not established, device offline.
    let message = Message::Short(
        MessageHeader {
            device_index: 1,
            sub_id: 0x41,
        },
        [0x04, 0x62, 0x69, 0x40],
    );
    let Some(UnifyingEvent::DeviceConnection(event)) = decode_notification(&message) else {
        panic!("expected a device-connection event");
    };
    assert!(!event.online, "bit 6 set must decode as offline");

    let (raw, handle) = ScriptedRawHidChannel::with_responder(|_| None);
    let channel = scripted_channel(raw).await;
    let writes_before = handle.written_reports().len();

    let cache = HashMap::new();
    let pass = PassContext {
        cache: &cache,
        now: Instant::now(),
        subscriptions: None,
        timeouts: &ProbeTimeouts::DEFAULT,
    };
    let (device, _) = probe_unifying_slot(&channel, &event, "SERIAL", pass)
        .await
        .expect("an offline slot still surfaces from its re-broadcast");

    assert!(!device.online);
    assert_eq!(device.wpid, Some(0x4069));
    assert_eq!(
        handle.written_reports().len(),
        writes_before,
        "an offline slot must not be probed for features, battery, or codename"
    );
}

#[test]
fn unifying_arrival_liveness_survives_missing_feature_data() {
    let device = assemble_unifying_device(
        1,
        None,
        0x40b8,
        DeviceKind::Mouse,
        ProbedFeatures::default(),
        true,
    );
    assert!(device.online);
    assert_eq!(device.wpid, Some(0x40b8));
    assert_eq!(device.kind, DeviceKind::Mouse);
}

#[tokio::test]
async fn unifying_arrival_trigger_retries_one_transient_failure() {
    let mut attempts = 0;

    let result = retry_arrival_trigger(
        || {
            attempts += 1;
            std::future::ready((attempts > 1).then_some(()).ok_or("transient"))
        },
        std::time::Duration::from_secs(1),
        std::time::Duration::ZERO,
    )
    .await;

    assert_eq!(result, Some(()));
    assert_eq!(attempts, 2);
}

#[tokio::test]
async fn unifying_arrival_trigger_surfaces_a_persistent_failure() {
    let mut attempts = 0;

    let result = retry_arrival_trigger(
        || {
            attempts += 1;
            std::future::ready(Err::<(), _>("persistent"))
        },
        std::time::Duration::from_secs(1),
        std::time::Duration::ZERO,
    )
    .await;

    assert_eq!(result, None);
    assert_eq!(attempts, 2);
}

#[tokio::test]
async fn unifying_arrival_trigger_bounds_two_stalled_attempts() {
    let attempt_timeout = std::time::Duration::from_millis(1);
    let retry_delay = std::time::Duration::from_millis(1);
    let mut attempts = 0;

    let result = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        retry_arrival_trigger(
            || {
                attempts += 1;
                std::future::pending::<Result<(), &str>>()
            },
            attempt_timeout,
            retry_delay,
        ),
    )
    .await
    .expect("the trigger retry must finish inside its caller's budget");

    assert_eq!(result, None);
    assert_eq!(attempts, 2);
}

#[test]
fn codename_reads_len_prefixed_name() {
    // wire-verified MX Master 2S reply: `40 0c "MX Master 2S"` then padding.
    let mut buf = vec![0x40, 0x0c];
    buf.extend_from_slice(b"MX Master 2S");
    buf.extend_from_slice(&[0u8; 2]); // trailing bytes of the 16-byte register
    assert_eq!(parse_codename(&buf).as_deref(), Some("MX Master 2S"));
}

#[test]
fn codename_clamps_overlong_len() {
    // a bogus length byte must not over-read past the buffer.
    let buf = [0x40, 0xff, b'h', b'i'];
    assert_eq!(parse_codename(&buf).as_deref(), Some("hi"));
}

#[test]
fn codename_rejects_short_response() {
    assert_eq!(parse_codename(&[0x40]), None);
}

fn online_slot_event() -> hidpp::receiver::unifying::DeviceConnection {
    let message = Message::Short(
        MessageHeader {
            device_index: 1,
            sub_id: 0x41,
        },
        [0x04, 0x22, 0x69, 0x40],
    );
    let Some(UnifyingEvent::DeviceConnection(event)) = decode_notification(&message) else {
        panic!("device connection expected");
    };
    assert!(event.online);
    event
}
