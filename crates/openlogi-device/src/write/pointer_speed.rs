use super::{HidppOperation, WriteError, classify_hidpp_error, open_feature, with_route};
use crate::{DeviceRoute, SharedChannel, backend::HidBackend};
use hidpp::{
    channel::HidppChannel,
    device::Device,
    feature::{CreatableFeature, pointer_motion_scaling::PointerMotionScalingFeature},
};
use std::sync::Arc;

pub use openlogi_core::hid::PointerSpeed;

async fn feature(
    channel: &Arc<HidppChannel>,
    index: u8,
) -> Result<Arc<PointerMotionScalingFeature>, WriteError> {
    let mut device = Device::new(Arc::clone(channel), index)
        .await
        .map_err(|_| WriteError::DeviceUnreachable { index })?;
    open_feature::<PointerMotionScalingFeature>(&mut device).await
}

async fn read(feature: &PointerMotionScalingFeature) -> Result<PointerSpeed, WriteError> {
    let raw = feature.get_scale().await.map_err(|e| {
        classify_hidpp_error(
            e,
            HidppOperation::ReadPointerSpeed,
            PointerMotionScalingFeature::ID,
        )
    })?;
    PointerSpeed::try_new(raw).map_err(|_| WriteError::UnsupportedResponse {
        operation: HidppOperation::ReadPointerSpeed,
        feature_hex: PointerMotionScalingFeature::ID,
    })
}

/// Read pointer scaling through an inventory-owned channel.
pub async fn get_pointer_speed_on(shared: &SharedChannel) -> Result<PointerSpeed, WriteError> {
    read(&*feature(shared.channel(), shared.device_index()).await?).await
}

/// Read the device-side pointer multiplier for a hardware route.
pub async fn get_pointer_speed(
    backend: &dyn HidBackend,
    route: &DeviceRoute,
) -> Result<PointerSpeed, WriteError> {
    let index = route.device_index();
    with_route(backend, route, |channel| async move {
        read(&*feature(&channel, index).await?).await
    })
    .await
}

async fn write(
    feature: &PointerMotionScalingFeature,
    speed: PointerSpeed,
) -> Result<(), WriteError> {
    feature.set_scale(speed.into_inner()).await.map_err(|e| {
        classify_hidpp_error(
            e,
            HidppOperation::WritePointerSpeed,
            PointerMotionScalingFeature::ID,
        )
    })?;
    let actual = read(feature).await?;
    if actual != speed {
        return Err(WriteError::UnsupportedResponse {
            operation: HidppOperation::WritePointerSpeed,
            feature_hex: PointerMotionScalingFeature::ID,
        });
    }
    Ok(())
}

/// Write and verify pointer scaling through an inventory-owned channel.
pub async fn set_pointer_speed_on(
    shared: &SharedChannel,
    speed: PointerSpeed,
) -> Result<(), WriteError> {
    write(
        &*feature(shared.channel(), shared.device_index()).await?,
        speed,
    )
    .await
}

/// Write and verify the device-side multiplier for a hardware route.
pub async fn set_pointer_speed(
    backend: &dyn HidBackend,
    route: &DeviceRoute,
    speed: PointerSpeed,
) -> Result<(), WriteError> {
    let index = route.device_index();
    with_route(backend, route, |channel| async move {
        write(&*feature(&channel, index).await?, speed).await
    })
    .await
}
