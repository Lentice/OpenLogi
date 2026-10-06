//! HID++ `0x2205` device-side pointer scaling.
//!
//! Reverse-engineered: read function 0, write function 1, and the big-endian
//! Q8.8 value are cross-checked against Solaar's `get_pointer_speed_info` and
//! `PointerSpeed` setting (`hidpp20.py`, `settings_templates.py`). This feature
//! has no document in Logitech's public HID++ feature-spec collection.

use crate::{feature::FeatureEndpoint, protocol::v20::Hidpp20Error};
use openlogi_hidpp_derive::Feature;

/// Implements the device-side pointer multiplier, not adjustable sensor DPI.
#[derive(Clone, Feature)]
#[creatable(id = 0x2205, version = 0)]
pub struct PointerMotionScalingFeature {
    endpoint: FeatureEndpoint,
}

impl PointerMotionScalingFeature {
    /// Read the current Q8.8 multiplier (256 means unscaled movement).
    pub async fn get_scale(&self) -> Result<u16, Hidpp20Error> {
        let payload = self.endpoint.call(0, [0; 3]).await?.extend_payload();
        Ok(u16::from_be_bytes([payload[0], payload[1]]))
    }

    /// Write a Q8.8 multiplier. Callers validate their selectable range.
    pub async fn set_scale(&self, value: u16) -> Result<(), Hidpp20Error> {
        let [hi, lo] = value.to_be_bytes();
        self.endpoint.call(1, [hi, lo, 0]).await?;
        Ok(())
    }
}
