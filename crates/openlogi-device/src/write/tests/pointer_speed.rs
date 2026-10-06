use super::*;

/// A fixed-DPI mouse with scaling at feature index 9. The read deliberately
/// returns 1.5×, independently of the bytes a caller requested to write.
fn m720_response(request: &[u8]) -> Option<Vec<u8>> {
    if request.len() < 7 {
        return None;
    }
    let mut response = vec![0u8; 7];
    response[0] = 0x10;
    response[1..4].copy_from_slice(&request[1..4]);
    match (request[2], request[3] >> 4) {
        (0, 1) => response[4] = 4,
        (0, 0) if request[4..6] == [0x22, 0x05] => response[4] = 9,
        (0, 0) | (9, 1) => {}
        (9, 0) => response[4..6].copy_from_slice(&[0x01, 0x80]),
        _ => return None,
    }
    Some(response)
}

#[tokio::test]
async fn pointer_speed_reads_and_verifies_big_endian_q8_8() {
    let (raw, handle) = ScriptedRawHidChannel::with_responder(m720_response);
    let shared = SharedChannel::new(
        scripted_channel(raw).await,
        DeviceRoute::Direct {
            vendor_id: 0x046d,
            product_id: 0xb015,
        },
    );
    let speed = PointerSpeed::try_new(384).expect("1.5×");
    assert_eq!(get_pointer_speed_on(&shared).await.expect("read"), speed);
    set_pointer_speed_on(&shared, speed)
        .await
        .expect("verified write");
    let reports = handle.written_reports();
    let write = reports
        .iter()
        .position(|report| report[2] == 9 && report[3] >> 4 == 1)
        .expect("setScale");
    assert_eq!(&reports[write][4..7], &[0x01, 0x80, 0]);
    assert!(
        reports[write + 1..]
            .iter()
            .any(|report| report[2] == 9 && report[3] >> 4 == 0),
        "success requires an independent readback"
    );
}

#[tokio::test]
async fn pointer_speed_rejects_a_write_the_mouse_did_not_take() {
    let (raw, _) = ScriptedRawHidChannel::with_responder(m720_response);
    let shared = SharedChannel::new(
        scripted_channel(raw).await,
        DeviceRoute::Direct {
            vendor_id: 0x046d,
            product_id: 0xb015,
        },
    );
    assert!(matches!(
        set_pointer_speed_on(&shared, PointerSpeed::NORMAL).await,
        Err(WriteError::UnsupportedResponse {
            operation: HidppOperation::WritePointerSpeed,
            feature_hex: 0x2205
        })
    ));
}

#[tokio::test]
async fn pointer_speed_reports_unsupported_without_writing() {
    fn without_scaling(request: &[u8]) -> Option<Vec<u8>> {
        let mut response = m720_response(request)?;
        if request[2] == 0 && request[3] >> 4 == 0 {
            response[4] = 0;
        }
        Some(response)
    }
    let (raw, handle) = ScriptedRawHidChannel::with_responder(without_scaling);
    let shared = SharedChannel::new(
        scripted_channel(raw).await,
        DeviceRoute::Direct {
            vendor_id: 0x046d,
            product_id: 0xb015,
        },
    );
    assert!(matches!(
        set_pointer_speed_on(&shared, PointerSpeed::NORMAL).await,
        Err(WriteError::FeatureUnsupported {
            feature_hex: 0x2205
        })
    ));
    assert!(!handle.written_reports().iter().any(|report| report[2] == 9));
}

#[tokio::test]
async fn pointer_speed_rejects_an_invalid_device_reading_instead_of_clamping() {
    fn invalid_scaling(request: &[u8]) -> Option<Vec<u8>> {
        let mut response = m720_response(request)?;
        if request[2] == 9 && request[3] >> 4 == 0 {
            response[4..6].copy_from_slice(&[0, 0]);
        }
        Some(response)
    }
    let (raw, _) = ScriptedRawHidChannel::with_responder(invalid_scaling);
    let shared = SharedChannel::new(
        scripted_channel(raw).await,
        DeviceRoute::Direct {
            vendor_id: 0x046d,
            product_id: 0xb015,
        },
    );
    assert!(matches!(
        get_pointer_speed_on(&shared).await,
        Err(WriteError::UnsupportedResponse {
            operation: HidppOperation::ReadPointerSpeed,
            feature_hex: 0x2205
        })
    ));
}
