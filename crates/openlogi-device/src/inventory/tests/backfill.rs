//! Backfilling an incomplete probe from the cache, field by field.

use super::*;

#[tokio::test]
async fn failed_initial_identity_name_and_capability_queries_are_retried_then_cached() {
    for (failed_feature, failed_function) in [(2, 0), (3, 0), (4, 1)] {
        let walks = std::sync::atomic::AtomicUsize::new(0);
        let (raw, handle) = ScriptedRawHidChannel::with_dynamic_responder(move |request| {
            let feature = request[2];
            let function = request[3] >> 4;
            if (feature, function) == (0, 1) {
                walks.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            if walks.load(std::sync::atomic::Ordering::Relaxed) == 1
                && (feature, function) == (failed_feature, failed_function)
            {
                return Some(crate::channel::scripted::feature_error(request, 8));
            }
            let mut response = vec![0; 20];
            response[..4].copy_from_slice(&request[..4]);
            response[0] = 0x11;
            match (feature, function) {
                (0, 1) | (1, 0) => response[4] = 4,
                (0 | 4, 0) => response[4] = 1,
                (1, 1) => response[4..6].copy_from_slice(
                    &[0x0001_u16, 0x0003, 0x0005, 0x1b04][usize::from(request[4]) - 1]
                        .to_be_bytes(),
                ),
                (2, 0) => response[4..9].copy_from_slice(&[1, 1, 2, 3, 4]),
                (3, 2) => response[4] = 3,
                (3, 0) => response[4] = 5,
                (3, 1) => response[4..9].copy_from_slice(b"Mouse"),
                (4, 1) => {
                    response[4..6].copy_from_slice(&0x00ed_u16.to_be_bytes());
                    response[8] = 0x20;
                    response[12] = 1;
                }
                _ => panic!("unexpected request {request:02x?}"),
            }
            Some(response)
        });
        let channel = scripted_channel(raw).await;
        let mut enumerator = Enumerator::with_backend(ScriptedBackend::new(Vec::new()));
        let key = CacheKey::Bolt {
            unit_id: [1, 2, 3, 4],
        };
        for pass in 0..3 {
            let before = handle.written_reports().len();
            let (probe, outcome) = super::super::cache::probe_or_reuse(
                &channel,
                1,
                Some(key.clone()),
                enumerator.cache.get(&key),
                true,
                Instant::now(),
                None,
            )
            .await;
            enumerator.apply_outcomes(vec![outcome]);
            if pass == 0 {
                assert!(
                    !enumerator.cache.contains_key(&key),
                    "a failed initial query must not be pinned"
                );
            } else {
                assert!(enumerator.cache.contains_key(&key));
                assert_eq!(probe.marketing_name.as_deref(), Some("Mouse"));
                assert_eq!(probe.model_info.unwrap().unit_id, [1, 2, 3, 4]);
                assert!(probe.capabilities.unwrap().dpi_gestures);
                if pass == 2 {
                    assert_eq!(
                        handle.written_reports().len(),
                        before,
                        "successful repair ends full probes"
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn a_restored_bolt_probe_serves_sleeping_identity_then_validates_online_once() {
    let key = CacheKey::Bolt {
        unit_id: [1, 2, 3, 4],
    };
    let mut entry = cache_entry();
    entry.probe = probed(Some(model([1, 2, 3, 4], Some("KNOWN"))), false);
    entry.probe.capabilities = Some(Capabilities {
        pointer: true,
        ..Capabilities::default()
    });
    let cache =
        super::super::persist::ProbeCacheSnapshot::of(&HashMap::from([(key.clone(), entry)]))
            .into_entries();
    let mut enumerator = Enumerator::with_backend(ScriptedBackend::new(Vec::new()));
    enumerator.cache = cache;
    let (raw, handle) = ScriptedRawHidChannel::with_responder(|request| {
        let mut response = vec![0; 20];
        response[..4].copy_from_slice(&request[..4]);
        response[0] = 0x11;
        match (request[2], request[3] >> 4) {
            (0, 1) => response[4] = 4,
            (0, 0) => response[4] = 1,
            (1, 0) => response[4] = 3,
            (1, 1) => response[4..6].copy_from_slice(
                &[0x0001_u16, 0x2201, 0x0003][usize::from(request[4]) - 1].to_be_bytes(),
            ),
            (3, 0) => response[4..9].copy_from_slice(&[1, 1, 2, 3, 4]),
            _ => panic!("unexpected request {request:02x?}"),
        }
        Some(response)
    });
    let channel = scripted_channel(raw).await;
    let before = handle.written_reports().len();
    for online in [false, true, true] {
        let (probe, outcome) = super::super::cache::probe_or_reuse(
            &channel,
            1,
            Some(key.clone()),
            enumerator.cache.get(&key),
            online,
            Instant::now(),
            None,
        )
        .await;
        let model = probe.model_info.unwrap();
        assert_eq!(model.unit_id, [1, 2, 3, 4]);
        if !online {
            assert_eq!(model.serial_number.as_deref(), Some("KNOWN"));
        }
        enumerator.apply_outcomes(vec![outcome]);
        if online {
            assert!(enumerator.cache[&key].probed_at.is_some());
            assert_eq!(
                handle.written_reports().len(),
                before + 7,
                "the restored entry is validated once"
            );
        } else {
            assert_eq!(
                handle.written_reports().len(),
                before,
                "restored offline metadata sends no device requests"
            );
            assert!(enumerator.cache[&key].probed_at.is_none());
        }
    }
}

/// A control-table read that fails half way reads exactly like "no haptic
/// panel". Caching that incomplete answer would make the Actions Ring binding
/// vanish indefinitely on a device that has it.
#[test]
fn an_incomplete_capability_walk_keeps_the_last_complete_answer() {
    let mut fresh = probed(None, false);
    fresh.capabilities_incomplete = true;
    fresh.capabilities = Some(Capabilities::default());
    let mut cached = probed(None, false);
    cached.capabilities = Some(Capabilities {
        haptic_panel: true,
        dpi_gestures: true,
        ..Capabilities::default()
    });

    keep_known_capabilities(&mut fresh, &cached);

    assert_eq!(
        fresh.capabilities, cached.capabilities,
        "the last complete control walk must survive a lost reply"
    );
    assert!(
        fresh.capabilities_incomplete,
        "the failed probe still needs repair"
    );
}

/// A device that genuinely lost a capability must still be able to say so.
#[test]
fn a_complete_capability_walk_is_left_alone() {
    let mut fresh = probed(None, false);
    fresh.capabilities = Some(Capabilities::default());
    let mut cached = probed(None, false);
    cached.capabilities = Some(Capabilities {
        haptic_panel: true,
        dpi_gestures: true,
        ..Capabilities::default()
    });

    keep_known_capabilities(&mut fresh, &cached);

    assert_eq!(fresh.capabilities, Some(Capabilities::default()));
}

#[test]
fn failed_device_info_read_backfills_from_cache() {
    let mut fresh = probed(None, true);
    let cached = probed(Some(model([0x46, 0, 0x2e, 0], None)), false);

    backfill_identity(&mut fresh, &cached);

    assert_eq!(fresh.model_info, cached.model_info);
    assert!(
        !fresh.identity_incomplete,
        "a backfilled identity is complete and may be cached"
    );
}

#[test]
fn failed_serial_read_backfills_only_the_serial() {
    let mut fresh = probed(Some(model([1, 2, 3, 4], None)), true);
    let cached = probed(Some(model([1, 2, 3, 4], Some("abc123"))), false);

    backfill_identity(&mut fresh, &cached);

    let Some(info) = fresh.model_info else {
        panic!("model info kept");
    };
    assert_eq!(info.serial_number.as_deref(), Some("abc123"));
    assert_eq!(info.unit_id, [1, 2, 3, 4], "fresh unit id wins");
    assert!(!fresh.identity_incomplete);
}

#[test]
fn complete_probe_is_never_overwritten_by_cache() {
    let mut fresh = probed(Some(model([1, 2, 3, 4], None)), false);
    let cached = probed(Some(model([9, 9, 9, 9], Some("stale"))), false);

    backfill_identity(&mut fresh, &cached);

    let Some(info) = fresh.model_info else {
        panic!("model info kept");
    };
    assert_eq!(info.unit_id, [1, 2, 3, 4]);
    assert!(
        info.serial_number.is_none(),
        "no serial was read, none faked"
    );
}

#[test]
fn incomplete_probe_without_cached_identity_stays_incomplete() {
    let mut fresh = probed(None, true);
    let cached = probed(None, false);

    backfill_identity(&mut fresh, &cached);

    assert!(
        fresh.identity_incomplete,
        "nothing to backfill from — the caller must not memoize this probe"
    );
}

#[test]
fn failed_kind_read_is_carried_forward() {
    let mut fresh = ProbedFeatures::default();
    let cached = probed(None, false);

    backfill_identity(&mut fresh, &cached);

    assert_eq!(fresh.kind, Some(DeviceKind::Mouse));
}
