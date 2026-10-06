//! Device-side pointer scaling, separate from sensor DPI and OS-wide pointer speed.

use az::SaturatingAs;
use nutype::nutype;

/// HID++ `0x2205` pointer multiplier in Q8.8 units (256 = 1×).
///
/// The selectable range follows Solaar's reverse-engineered PointerSpeed setting;
/// Logitech's public feature-spec collection does not include `0x2205`.
#[nutype(
    const_fn,
    validate(greater_or_equal = 46, less_or_equal = 511),
    derive(
        Debug,
        Clone,
        Copy,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        TryFrom,
        Into,
        Display,
        Serialize,
        Deserialize
    )
)]
pub struct PointerSpeed(u16);

impl PointerSpeed {
    /// Minimum supported setting (approximately 0.18×).
    pub const MIN: Self = match Self::try_new(46) {
        Ok(v) => v,
        Err(_) => panic!("valid minimum pointer speed"),
    };
    /// Maximum supported setting (approximately 2×).
    pub const MAX: Self = match Self::try_new(511) {
        Ok(v) => v,
        Err(_) => panic!("valid maximum pointer speed"),
    };
    /// Unscaled pointer movement.
    pub const NORMAL: Self = match Self::try_new(256) {
        Ok(v) => v,
        Err(_) => panic!("valid normal pointer speed"),
    };

    /// Round a slider value in Q8.8 units into the supported range.
    #[must_use]
    pub fn from_rounded(value: f32) -> Self {
        let value = if value.is_nan() {
            f32::from(Self::NORMAL)
        } else {
            value
        };
        let raw = value
            .clamp(f32::from(Self::MIN), f32::from(Self::MAX))
            .round()
            .saturating_as::<u16>();
        let Ok(speed) = Self::try_new(raw) else {
            unreachable!("clamped pointer speed is valid")
        };
        speed
    }

    /// Movement multiplier relative to the device's unscaled reports.
    #[must_use]
    pub fn multiplier(self) -> f32 {
        f32::from(self.into_inner()) / f32::from(Self::NORMAL.into_inner())
    }
}

impl From<PointerSpeed> for f32 {
    fn from(value: PointerSpeed) -> Self {
        Self::from(value.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pointer_speed_rejects_out_of_range_settings() {
        PointerSpeed::try_new(45).expect_err("below the supported range");
        PointerSpeed::try_new(512).expect_err("above the supported range");
        assert_eq!(PointerSpeed::from_rounded(f32::NAN), PointerSpeed::NORMAL);
        assert_eq!(PointerSpeed::from_rounded(-1.), PointerSpeed::MIN);
        assert_eq!(PointerSpeed::from_rounded(f32::INFINITY), PointerSpeed::MAX);
        assert_eq!(
            PointerSpeed::try_new(384).unwrap().multiplier().to_bits(),
            1.5_f32.to_bits()
        );
    }
}
