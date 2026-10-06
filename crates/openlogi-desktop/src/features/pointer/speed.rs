//! Movement speed for fixed-DPI mice with HID++ PointerMotionScaling.

use gpui::{Context, IntoElement, ParentElement, Render, Styled, Subscription, Window, div};
use gpui_component::{h_flex, slider::Slider, v_flex};
use openlogi_core::hid::PointerSpeed;

use crate::state::{AppState, DeviceKey, DeviceRecord, PointerSpeedLoad, StateEvent};
use crate::ui::commit_slider::{CommitSlider, SliderRange};
use crate::ui::status::{retry_line, status_line};
use crate::ui::theme::{self, Typography as _};

pub struct SpeedPanel {
    slider: Option<(DeviceKey, CommitSlider<PointerSpeed>)>,
    _state_obs: Subscription,
}

impl SpeedPanel {
    pub fn new(cx: &mut Context<Self>) -> Self {
        Self {
            slider: None,
            _state_obs: AppState::repaint_on(cx, |event| {
                matches!(event, StateEvent::PointerSpeedChanged(_))
            }),
        }
    }
}

impl Render for SpeedPanel {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let pal = theme::palette(cx);
        let (key, reachable, load) =
            AppState::try_read(cx).map_or((None, false, PointerSpeedLoad::Unknown), |state| {
                (
                    state.current_record().map(DeviceRecord::device_key),
                    state
                        .current_record()
                        .is_some_and(|record| record.online && record.route.is_some()),
                    state.current_pointer_speed_load(),
                )
            });
        let mut content = v_flex().gap_3().w_full();
        match (&key, &load) {
            (Some(key), PointerSpeedLoad::Ready(speed)) => {
                if let Some((_, slider)) =
                    self.slider.as_ref().filter(|(current, _)| current == key)
                {
                    slider.sync(**speed, window, cx);
                } else {
                    self.slider = Some((
                        key.clone(),
                        CommitSlider::new(
                            SliderRange::new(PointerSpeed::MIN, PointerSpeed::MAX),
                            **speed,
                            cx,
                            |_, speed, cx| {
                                AppState::apply(cx, |state| state.commit_pointer_speed(speed));
                            },
                        ),
                    ));
                }
                if let Some((_, slider)) = &self.slider {
                    content =
                        content
                            .child(
                                h_flex()
                                    .justify_between()
                                    .child(
                                        div()
                                            .text_body()
                                            .text_color(pal.text_muted)
                                            .child(tr!("pointer.speed")),
                                    )
                                    .child(div().text_body().text_color(pal.text_primary).child(
                                        format!("{:.2}×", slider.shown(**speed).multiplier()),
                                    )),
                            )
                            .child(Slider::new(slider.slider()).horizontal());
                }
            }
            (_, PointerSpeedLoad::Failed(_)) => {
                self.slider = None;
                content = content.child(retry_line(
                    "pointer-speed-retry",
                    tr!("pointer.speed_retry"),
                    pal,
                    |cx| {
                        AppState::update(cx, AppState::retry_current_pointer_speed);
                    },
                ));
            }
            (_, PointerSpeedLoad::Unsupported(error)) => {
                self.slider = None;
                content = content.child(status_line(error.clone(), pal));
            }
            _ => {
                self.slider = None;
                content = content.child(status_line(
                    if reachable {
                        tr!("pointer.reading_speed")
                    } else {
                        tr!("pointer.speed_offline")
                    },
                    pal,
                ));
            }
        }
        content.child(
            div()
                .text_caption()
                .text_color(pal.text_muted)
                .child(tr!("pointer.speed_description")),
        )
    }
}
