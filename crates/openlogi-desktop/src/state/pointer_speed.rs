//! Persisted device motion scaling and the device's verified live reading.

use openlogi_core::hid::{PointerSpeed, WriteError};

use super::events::StateEvents;
use super::{AppState, DeviceKey, PointerSpeedLoad, StateEvent};

impl AppState {
    pub fn current_pointer_speed_load(&self) -> PointerSpeedLoad {
        self.current_record()
            .and_then(|record| self.pointer.reads.pointer_speed_load(&record.device_key()))
            .cloned()
            .unwrap_or_default()
    }

    pub fn commit_pointer_speed(&mut self, speed: PointerSpeed) -> StateEvents {
        let Some(record) = self.current_record().filter(|record| {
            record.online && record.capabilities.is_some_and(|caps| caps.pointer_speed)
        }) else {
            return StateEvents::none();
        };
        let key = record.device_key();
        let Some(route) = record.route.clone() else {
            return StateEvents::none();
        };
        if !matches!(
            self.current_pointer_speed_load(),
            PointerSpeedLoad::Ready(_)
        ) {
            return StateEvents::none();
        }
        if let Some(flight) = self.pointer.reads.begin_pointer_speed_write(&key) {
            self.send_ipc(crate::services::ipc::SetPointerSpeed {
                route,
                speed,
                key: key.clone(),
                flight,
            });
        }
        StateEvent::PointerSpeedChanged(key).into()
    }

    pub fn apply_pointer_speed_written(
        &mut self,
        key: &DeviceKey,
        flight: u64,
        speed: PointerSpeed,
        result: Result<(), WriteError>,
    ) -> StateEvents {
        let verified = result.is_ok();
        if self
            .pointer
            .reads
            .finish_pointer_speed_write(key, flight, speed, result)
        {
            if verified
                && self
                    .devices
                    .records
                    .iter()
                    .any(|record| record.persistent_config_key() == Some(key.as_str()))
            {
                self.config
                    .edit(|config| config.set_pointer_speed(key.as_str(), speed));
                self.persist_and_reload("pointer-speed");
            }
            StateEvent::PointerSpeedChanged(key.clone()).into()
        } else {
            StateEvents::none()
        }
    }

    pub fn retry_current_pointer_speed(&mut self, cx: &mut gpui::Context<Self>) {
        if let Some(key) = self
            .current_record()
            .map(super::devices::DeviceRecord::device_key)
        {
            self.pointer.reads.remove_pointer_speed(&key);
        }
        self.load_current_pointer_speed(cx);
        cx.notify();
    }

    pub(super) fn load_current_pointer_speed(&mut self, cx: &mut gpui::Context<Self>) {
        let Some((key, route)) = self
            .current_record()
            .filter(|record| {
                record.online && record.capabilities.is_some_and(|caps| caps.pointer_speed)
            })
            .and_then(|record| Some((record.device_key(), record.route.clone()?)))
        else {
            return;
        };
        self.pointer
            .reads
            .ensure_pointer_speed(key, route, self.ipc_sender(), cx);
    }
}
