use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use crate::archive::{format_size, ArchiveManager};
use crate::config::RuntimeConfig;
use crate::error::Result;
use crate::filesystem::FileSystem;
use crate::gadget::{GadgetDisableGuard, UsbGadget};
use crate::idle::ProcIdleDetector;
use crate::led::{LedPattern, SysfsLedController};
use crate::mount::{fsck_image, mount_image};

static STOP_REQUESTED: AtomicBool = AtomicBool::new(false);

pub fn stop_requested() -> bool {
    STOP_REQUESTED.load(Ordering::SeqCst)
}

pub fn request_stop() {
    STOP_REQUESTED.store(true, Ordering::SeqCst);
}

#[derive(Debug, Clone)]
pub struct Coordinator<F: FileSystem> {
    archive_manager: ArchiveManager<F>,
    gadget: Option<UsbGadget>,
    led: Option<SysfsLedController>,
    idle_detector: Option<ProcIdleDetector>,
    idle_timeout: Duration,
    poll_interval: Duration,
    max_idle_interval: Duration,
    retry_interval: Duration,
}

impl<F: FileSystem> Coordinator<F> {
    pub fn new(
        archive_manager: ArchiveManager<F>,
        gadget: Option<UsbGadget>,
        config: &RuntimeConfig,
    ) -> Self {
        Self {
            archive_manager,
            gadget,
            led: None,
            idle_detector: None,
            idle_timeout: config.idle_timeout,
            poll_interval: config.poll_interval,
            max_idle_interval: config.max_poll_interval,
            retry_interval: config.retry_interval,
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
        install_signal_handlers();
        if !self.archive_manager.backend().is_reachable() {
            eprintln!("error: archive backend is not reachable");
            return Ok(false);
        }
        Ok(self.do_archive_cycle()?.success)
    }

    pub fn run(&mut self) -> Result<()> {
        let result = self.run_inner();
        let led_result = self.set_led(LedPattern::Off);
        if let Err(error) = &led_result {
            eprintln!("error: failed to turn off status LED: {error}");
        }
        result.and(led_result)
    }

    fn run_inner(&mut self) -> Result<()> {
        install_signal_handlers();

        let mut idle_backoff = Backoff::new(self.poll_interval, self.max_idle_interval);
        while !STOP_REQUESTED.load(Ordering::SeqCst) {
            self.wait_for_archive(&STOP_REQUESTED)?;
            if STOP_REQUESTED.load(Ordering::SeqCst) {
                break;
            }

            let cycle = self.do_archive_cycle()?;
            let delay = if !cycle.success {
                idle_backoff.reset();
                self.retry_interval
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

    fn wait_for_archive(&self, stop: &AtomicBool) -> Result<()> {
        self.set_led(LedPattern::SlowBlink)?;
        let mut backoff = Backoff::new(self.poll_interval, self.max_idle_interval);
        while !stop.load(Ordering::SeqCst) {
            if self.archive_manager.backend().is_reachable() {
                return Ok(());
            }
            let delay = backoff.next();
            eprintln!(
                "archive backend not reachable; retrying in {}s",
                delay.as_secs()
            );
            wait_interruptible(delay, stop);
        }
        Ok(())
    }

    fn do_archive_cycle(&mut self) -> Result<ArchiveCycle> {
        self.set_led(LedPattern::FastBlink)?;
        if !self.archive_manager.has_pending_snapshot()? && !self.wait_for_usb_idle() {
            return Ok(ArchiveCycle {
                success: false,
                files_transferred: 0,
            });
        }

        let result = self.archive_manager.archive_pending_or_new_snapshot()?;
        if result.success() {
            eprintln!(
                "archive complete: {} files transferred, {}",
                result.files_transferred,
                format_size(result.bytes_transferred)
            );
        } else {
            eprintln!(
                "warning: archive finished with issues: {} files transferred, {}: {}",
                result.files_transferred,
                format_size(result.bytes_transferred),
                result
                    .error
                    .clone()
                    .unwrap_or_else(|| "unknown error".into())
            );
        }

        let needs_cleanup = result
            .archived_files
            .iter()
            .any(|(name, files)| name != "RecentClips" && !files.is_empty());
        if needs_cleanup && !stop_requested() && self.wait_for_usb_idle() && !stop_requested() {
            self.delete_archived_files(&result)?;
        }

        if result.success() {
            self.archive_manager.retire_snapshot(&result)?;
        } else {
            eprintln!(
                "retained pending archive snapshot {} for retry",
                result.snapshot_id
            );
        }

        let cycle = ArchiveCycle {
            success: result.success(),
            files_transferred: result.files_transferred,
        };
        if cycle.success {
            self.set_led(LedPattern::Heartbeat)?;
        } else {
            self.set_led(LedPattern::SlowBlink)?;
        }
        Ok(cycle)
    }

    fn delete_archived_files(&self, result: &crate::archive::ArchiveResult) -> Result<()> {
        self.set_led(LedPattern::Heartbeat)?;
        let guard = if let Some(gadget) = &self.gadget {
            Some(GadgetDisableGuard::disable_if_needed(gadget.clone())?)
        } else {
            None
        };

        let cam_disk: PathBuf = self.archive_manager.cam_disk_path().to_path_buf();
        fsck_image(&cam_disk)?;
        let mounted = mount_image(&cam_disk)?;
        let (deleted, skipped) = self
            .archive_manager
            .delete_archived_files(result, mounted.path())?;
        mounted.unmount()?;
        if let Some(guard) = guard {
            guard.restore()?;
        }
        eprintln!(
            "clean up complete: deleted {}, skipped {}",
            deleted, skipped
        );
        Ok(())
    }

    fn set_led(&self, pattern: LedPattern) -> Result<()> {
        if let Some(led) = &self.led {
            led.set_pattern(pattern)?;
        }
        Ok(())
    }

    fn wait_for_usb_idle(&mut self) -> bool {
        if let Some(detector) = &mut self.idle_detector {
            eprintln!(
                "waiting up to {}s for USB writes to become idle",
                self.idle_timeout.as_secs()
            );
            return detector.wait_for_idle(self.idle_timeout);
        }
        true
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
        request_stop();
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
