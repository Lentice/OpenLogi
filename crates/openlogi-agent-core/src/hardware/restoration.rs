//! Pending volatile restoration, registered before publishing an online device.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError, Weak};

use openlogi_hid::DeviceRoute;
use tokio::sync::watch;

/// Shared per-route completion barriers for reconnect restoration.
#[derive(Clone, Default)]
pub struct DeviceRestoration(Arc<Mutex<HashMap<String, Weak<watch::Sender<usize>>>>>);

/// Keeps a restoration pending until its worker exits, including early failures.
pub(crate) struct Restoration(Arc<watch::Sender<usize>>);

impl DeviceRestoration {
    fn pending(&self, route: &DeviceRoute) -> Arc<watch::Sender<usize>> {
        let mut routes = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        routes.retain(|_, pending| pending.strong_count() != 0);
        let entry = routes.entry(route.to_string()).or_default();
        if let Some(pending) = entry.upgrade() {
            return pending;
        }
        let (pending, _) = watch::channel(0);
        let pending = Arc::new(pending);
        *entry = Arc::downgrade(&pending);
        pending
    }

    pub(crate) fn begin(&self, route: &DeviceRoute) -> Restoration {
        let pending = self.pending(route);
        pending.send_modify(|count| *count += 1);
        Restoration(pending)
    }

    pub(crate) async fn wait(&self, route: &DeviceRoute) {
        let pending = self.pending(route);
        let mut changes = pending.subscribe();
        while *changes.borrow_and_update() != 0 {
            if changes.changed().await.is_err() {
                break;
            }
        }
    }
}

impl Drop for Restoration {
    fn drop(&mut self) {
        self.0.send_modify(|count| *count -= 1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route(slot: u8) -> DeviceRoute {
        DeviceRoute::Unifying {
            receiver_uid: "synthetic-receiver".into(),
            slot,
        }
    }

    #[tokio::test]
    async fn reads_wait_for_all_restorations_but_other_devices_do_not() {
        let barriers = DeviceRestoration::default();
        let first = barriers.begin(&route(1));
        let second = barriers.begin(&route(1));
        let mouse = route(1);
        let waiting = barriers.wait(&mouse);
        tokio::pin!(waiting);
        assert!(
            futures_lite::future::poll_once(&mut waiting)
                .await
                .is_none()
        );
        barriers.wait(&route(2)).await;
        drop(first);
        assert!(
            futures_lite::future::poll_once(&mut waiting)
                .await
                .is_none()
        );
        drop(second);
        waiting.await;
    }
}
