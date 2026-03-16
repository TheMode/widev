use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use futures::channel::oneshot;
use futures::{FutureExt, StreamExt};

const WATCHER_BASELINE_GRACE: Duration = Duration::from_secs(2);
const MIGRATION_DEBOUNCE: Duration = Duration::from_millis(750);

pub(super) struct NetworkMigrationCoordinator {
    has_resumed_once: bool,
    started_at: Instant,
    pending_change: bool,
    last_migration_at: Option<Instant>,
    watcher: InterfaceWatcher,
}

impl NetworkMigrationCoordinator {
    pub(super) fn new() -> Self {
        Self {
            has_resumed_once: false,
            started_at: Instant::now(),
            pending_change: false,
            last_migration_at: None,
            watcher: InterfaceWatcher::new(),
        }
    }

    pub(super) fn on_resumed(&mut self, now: Instant, connected: bool) -> bool {
        let should_migrate = self.has_resumed_once && connected && self.should_migrate_now(now);
        self.has_resumed_once = true;
        if should_migrate {
            self.last_migration_at = Some(now);
        }
        should_migrate
    }

    pub(super) fn poll_network_change(&mut self, now: Instant, connected: bool) -> bool {
        if self.watcher.poll_network_change() {
            if now.duration_since(self.started_at) >= WATCHER_BASELINE_GRACE {
                self.pending_change = true;
            } else {
                log::debug!("ignoring initial interface watcher baseline event");
            }
        }

        if !connected || !self.pending_change {
            return false;
        }

        if !self.should_migrate_now(now) {
            return false;
        }

        self.pending_change = false;
        self.last_migration_at = Some(now);
        true
    }

    fn should_migrate_now(&self, now: Instant) -> bool {
        !self.last_migration_at.is_some_and(|last_migration_at| {
            now.duration_since(last_migration_at) < MIGRATION_DEBOUNCE
        })
    }
}

struct InterfaceWatcher {
    rx: mpsc::Receiver<()>,
    stop_tx: Option<oneshot::Sender<()>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl InterfaceWatcher {
    fn new() -> Self {
        let (change_tx, change_rx) = mpsc::channel();
        let (stop_tx, stop_rx) = oneshot::channel();
        let thread = thread::spawn(move || run_interface_watcher(change_tx, stop_rx));
        Self { rx: change_rx, stop_tx: Some(stop_tx), thread: Some(thread) }
    }

    fn poll_network_change(&mut self) -> bool {
        let mut changed = false;
        while self.rx.try_recv().is_ok() {
            changed = true;
        }
        changed
    }
}

impl Drop for InterfaceWatcher {
    fn drop(&mut self) {
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn run_interface_watcher(change_tx: mpsc::Sender<()>, stop_rx: oneshot::Receiver<()>) {
    smol::block_on(async move {
        let mut watcher = match if_watch::smol::IfWatcher::new() {
            Ok(watcher) => watcher,
            Err(err) => {
                log::warn!("failed to start interface watcher: {err:#}");
                return;
            },
        };
        let mut stop_rx = stop_rx.fuse();

        loop {
            let next_event = watcher.next().fuse();
            futures::pin_mut!(next_event);

            let event = match futures::future::select(stop_rx, next_event).await {
                futures::future::Either::Left((_, _)) => break,
                futures::future::Either::Right((event, next_stop_rx)) => {
                    stop_rx = next_stop_rx;
                    event
                },
            };

            let Some(event) = event else {
                break;
            };

            match event {
                Ok(if_watch::IfEvent::Up(_)) | Ok(if_watch::IfEvent::Down(_)) => {
                    if change_tx.send(()).is_err() {
                        break;
                    }
                },
                Err(err) => {
                    log::warn!("interface watcher error: {err:#}");
                    break;
                },
            }
        }
    });
}
