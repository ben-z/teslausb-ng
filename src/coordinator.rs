use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use crate::archive::{ArchiveManager, ArchiveState};
use crate::error::Result;
use crate::filesystem::FileSystem;
use crate::gadget::{GadgetDisableGuard, UsbGadget};
use crate::idle::{default_timeout, ProcIdleDetector};
use crate::led::{LedPattern, SysfsLedController};
use crate::mount::{fsck_image, mount_image};
use crate::snapshot::SnapshotManager;

static STOP_REQUESTED: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Clone)]
pub struct Coordinator<F: FileSystem> {
    snapshot_manager: SnapshotManager<F>,
    archive_manager: ArchiveManager<F>,
    gadget: Option<UsbGadget>,
    led: Option<SysfsLedController>,
    idle_detector: Option<ProcIdleDetector>,
    idle_timeout: Duration,
    poll_interval: Duration,
    max_idle_interval: Duration,
}

impl<F: FileSystem> Coordinator<F> {
    pub fn new(
        snapshot_manager: SnapshotManager<F>,
        archive_manager: ArchiveManager<F>,
        gadget: Option<UsbGadget>,
    ) -> Self {
        Self {
            snapshot_manager,
            archive_manager,
            gadget,
            led: None,
            idle_detector: None,
            idle_timeout: default_timeout(),
            poll_interval: Duration::from_secs(5),
            max_idle_interval: Duration::from_secs(300),
        }
    }

    pub fn with_led(mut self, led: SysfsLedController) -> Self {
        self.led = Some(led);
        self
    }

    pub fn with_idle_detector(mut self, detector: ProcIdleDetector) -> Self {
        self.idle_detector = Some(detector);
        self
    }

    pub fn run_once(&mut self) -> Result<bool> {
        if !self.archive_manager.backend().is_reachable() {
            eprintln!("error: archive backend is not reachable");
            return Ok(false);
        }
        Ok(self.do_archive_cycle()?.success)
    }

    pub fn run(&mut self) -> Result<()> {
        let result = self.run_inner();
        self.set_led(LedPattern::Off);
        result
    }

    fn run_inner(&mut self) -> Result<()> {
        STOP_REQUESTED.store(false, Ordering::SeqCst);
        install_signal_handlers();

        let mut idle_backoff = Backoff::new(self.poll_interval, self.max_idle_interval);
        while !STOP_REQUESTED.load(Ordering::SeqCst) {
            self.wait_for_archive(&STOP_REQUESTED);
            if STOP_REQUESTED.load(Ordering::SeqCst) {
                break;
            }

            let cycle = self.do_archive_cycle()?;
            let delay = if !cycle.success {
                idle_backoff.reset();
                Duration::from_secs(30)
            } else if cycle.files_transferred == 0 {
                let delay = idle_backoff.next();
                eprintln!(
                    "no files archived; waiting {}s before next cycle",
                    delay.as_secs()
                );
                delay
            } else {
                idle_backoff.reset();
                self.poll_interval
            };
            wait_interruptible(delay, &STOP_REQUESTED);
        }
        Ok(())
    }

    fn wait_for_archive(&self, stop: &AtomicBool) {
        self.set_led(LedPattern::SlowBlink);
        let mut backoff = Backoff::new(self.poll_interval, self.max_idle_interval);
        while !stop.load(Ordering::SeqCst) {
            if self.archive_manager.backend().is_reachable() {
                return;
            }
            let delay = backoff.next();
            eprintln!(
                "archive backend not reachable; retrying in {}s",
                delay.as_secs()
            );
            wait_interruptible(delay, stop);
        }
    }

    fn do_archive_cycle(&mut self) -> Result<ArchiveCycle> {
        self.set_led(LedPattern::FastBlink);
        let mut stale = 0;
        while self.snapshot_manager.delete_oldest_if_deletable()? {
            stale += 1;
        }
        if stale == 1 {
            eprintln!("warning: deleted 1 stale snapshot, likely from an unclean stop");
        } else if stale > 1 {
            eprintln!(
                "error: deleted {} stale snapshots; expected at most 1 under eager clean up",
                stale
            );
        }

        self.wait_for_usb_idle();

        let result = self.archive_manager.archive_new_snapshot(false)?;
        if result.state == ArchiveState::Completed {
            eprintln!(
                "archive complete: {} files, {} bytes",
                result.files_transferred, result.bytes_transferred
            );
        } else {
            eprintln!(
                "warning: archive finished with issues: {}",
                result
                    .error
                    .clone()
                    .unwrap_or_else(|| "unknown error".into())
            );
        }

        if !result.archived_files.is_empty() {
            self.delete_archived_files(&result)?;
        }

        if let Err(err) = self.snapshot_manager.delete_snapshot(result.snapshot_id) {
            eprintln!(
                "warning: failed to delete snapshot {}: {}",
                result.snapshot_id, err
            );
        }

        let cycle = ArchiveCycle {
            success: result.state == ArchiveState::Completed,
            files_transferred: result.files_transferred,
        };
        if cycle.success {
            self.set_led(LedPattern::Heartbeat);
        } else {
            self.set_led(LedPattern::SlowBlink);
        }
        Ok(cycle)
    }

    fn delete_archived_files(&self, result: &crate::archive::ArchiveResult) -> Result<()> {
        let _guard = if let Some(gadget) = &self.gadget {
            Some(GadgetDisableGuard::disable_if_needed(gadget.clone())?)
        } else {
            None
        };

        let cam_disk: PathBuf = self.archive_manager.cam_disk_path().to_path_buf();
        if !fsck_image(&cam_disk)? {
            eprintln!("warning: fsck reported unresolved errors; proceeding with mount");
        }
        let mounted = mount_image(&cam_disk, false)?;
        let (deleted, skipped) = self
            .archive_manager
            .delete_archived_files(result, mounted.path())?;
        eprintln!(
            "clean up complete: deleted {}, skipped {}",
            deleted, skipped
        );
        Ok(())
    }

    fn set_led(&self, pattern: LedPattern) {
        if let Some(led) = &self.led {
            led.set_pattern(pattern);
        }
    }

    fn wait_for_usb_idle(&mut self) {
        if let Some(detector) = &mut self.idle_detector {
            eprintln!(
                "waiting up to {}s for USB writes to become idle",
                self.idle_timeout.as_secs()
            );
            detector.wait_for_idle(self.idle_timeout);
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ArchiveCycle {
    success: bool,
    files_transferred: u64,
}

#[derive(Debug, Clone)]
struct Backoff {
    base: Duration,
    max: Duration,
    current: Duration,
}

impl Backoff {
    fn new(base: Duration, max: Duration) -> Self {
        Self {
            base,
            max,
            current: base.min(max),
        }
    }

    fn next(&mut self) -> Duration {
        let value = self.current;
        self.current = (self.current * 2).min(self.max);
        value
    }

    fn reset(&mut self) {
        self.current = self.base.min(self.max);
    }
}

fn wait_interruptible(delay: Duration, stop: &AtomicBool) {
    let mut waited = Duration::ZERO;
    while waited < delay && !stop.load(Ordering::SeqCst) {
        let step = Duration::from_millis(250).min(delay - waited);
        thread::sleep(step);
        waited += step;
    }
}

#[cfg(unix)]
fn install_signal_handlers() {
    use std::os::raw::c_int;

    const SIGINT: c_int = 2;
    const SIGTERM: c_int = 15;

    unsafe extern "C" {
        fn signal(signum: c_int, handler: extern "C" fn(c_int)) -> usize;
    }

    extern "C" fn handle_signal(_signum: c_int) {
        STOP_REQUESTED.store(true, Ordering::SeqCst);
    }

    unsafe {
        let _ = signal(SIGINT, handle_signal);
        let _ = signal(SIGTERM, handle_signal);
    }
}

#[cfg(not(unix))]
fn install_signal_handlers() {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_yields_exponential_sequence_capped_at_max() {
        let mut backoff = Backoff::new(Duration::from_secs(5), Duration::from_secs(300));
        let values = (0..9).map(|_| backoff.next().as_secs()).collect::<Vec<_>>();
        assert_eq!(values, vec![5, 10, 20, 40, 80, 160, 300, 300, 300]);
    }

    #[test]
    fn backoff_starts_capped_when_base_exceeds_max() {
        let mut backoff = Backoff::new(Duration::from_secs(100), Duration::from_secs(50));
        assert_eq!(backoff.next(), Duration::from_secs(50));
        assert_eq!(backoff.next(), Duration::from_secs(50));
    }

    #[test]
    fn backoff_reset_returns_to_base() {
        let mut backoff = Backoff::new(Duration::from_secs(5), Duration::from_secs(60));
        assert_eq!(backoff.next(), Duration::from_secs(5));
        assert_eq!(backoff.next(), Duration::from_secs(10));
        backoff.reset();
        assert_eq!(backoff.next(), Duration::from_secs(5));
    }

    #[test]
    fn wait_interruptible_returns_promptly_when_stop_is_set() {
        let stop = AtomicBool::new(true);
        let started = std::time::Instant::now();
        wait_interruptible(Duration::from_secs(10), &stop);
        assert!(started.elapsed() < Duration::from_millis(50));
    }
}
