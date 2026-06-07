use std::fs;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

pub const WRITE_THRESHOLD: u64 = 500_000;
pub const IDLE_CONFIRM_SECONDS: u64 = 5;
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(90);
pub const PROC_PATH_ENV: &str = "TESLAUSB_PROC_PATH";
pub const PROCESS_NAME_ENV: &str = "TESLAUSB_IDLE_PROCESS";
pub const IDLE_TIMEOUT_ENV: &str = "TESLAUSB_IDLE_TIMEOUT_SECS";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdleState {
    Undetermined,
    Writing,
    Idle,
}

impl IdleState {
    fn as_str(self) -> &'static str {
        match self {
            IdleState::Undetermined => "undetermined",
            IdleState::Writing => "writing",
            IdleState::Idle => "idle",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdleStatus {
    pub state: IdleState,
    pub bytes_written: u64,
    pub burst_size: u64,
    pub idle_seconds: u64,
}

impl IdleStatus {
    #[cfg(test)]
    pub fn new(state: IdleState) -> Self {
        Self {
            state,
            bytes_written: 0,
            burst_size: 0,
            idle_seconds: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProcIdleDetector {
    proc_path: PathBuf,
    process_name: String,
    state: IdleState,
    prev_written: Option<u64>,
    burst_size: u64,
    idle_count: u64,
    sample_interval: Duration,
}

impl ProcIdleDetector {
    pub fn new(proc_path: PathBuf, process_name: impl Into<String>) -> Self {
        Self {
            proc_path,
            process_name: process_name.into(),
            state: IdleState::Undetermined,
            prev_written: None,
            burst_size: 0,
            idle_count: 0,
            sample_interval: Duration::from_secs(1),
        }
    }

    pub fn default_proc() -> Self {
        let proc_path = std::env::var_os(PROC_PATH_ENV)
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/proc"));
        let process_name =
            std::env::var(PROCESS_NAME_ENV).unwrap_or_else(|_| "file-storage".to_string());
        Self::new(proc_path, process_name)
    }

    #[cfg(test)]
    pub fn with_sample_interval(mut self, interval: Duration) -> Self {
        self.sample_interval = interval;
        self
    }

    pub fn find_process_pid(&self) -> Option<u32> {
        let entries = fs::read_dir(&self.proc_path).ok()?;
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if !name.chars().all(|ch| ch.is_ascii_digit()) {
                continue;
            }

            let Ok(comm) = fs::read_to_string(entry.path().join("comm")) else {
                continue;
            };
            if comm.trim() == self.process_name {
                if let Ok(pid) = name.parse::<u32>() {
                    return Some(pid);
                }
            }
        }
        None
    }

    pub fn write_bytes_for_pid(&self, pid: u32) -> Option<u64> {
        parse_write_bytes(
            &fs::read_to_string(self.proc_path.join(pid.to_string()).join("io")).ok()?,
        )
    }

    pub fn wait_for_idle(&mut self, timeout: Duration) -> bool {
        self.state = IdleState::Undetermined;
        self.prev_written = None;
        self.burst_size = 0;
        self.idle_count = 0;

        let started = Instant::now();
        while started.elapsed() < timeout {
            if !self.sample_interval.is_zero() {
                thread::sleep(self.sample_interval);
            }

            let Some(pid) = self.find_process_pid() else {
                self.state = IdleState::Idle;
                return true;
            };
            let Some(written) = self.write_bytes_for_pid(pid) else {
                continue;
            };
            let Some(previous) = self.prev_written.replace(written) else {
                continue;
            };
            let delta = written.saturating_sub(previous);
            self.update_state(delta);

            if self.state == IdleState::Idle && self.idle_count >= IDLE_CONFIRM_SECONDS {
                return true;
            }
        }
        let status = self.status();
        eprintln!(
            "warning: timed out waiting for USB writes to become idle; proceeding \
             (state={}, bytes_written={}, burst_size={}, idle_seconds={})",
            status.state.as_str(),
            status.bytes_written,
            status.burst_size,
            status.idle_seconds
        );
        false
    }

    fn update_state(&mut self, delta: u64) {
        match self.state {
            IdleState::Undetermined => {
                if delta > WRITE_THRESHOLD {
                    self.state = IdleState::Writing;
                    self.burst_size = delta;
                } else {
                    self.state = IdleState::Idle;
                    self.idle_count = 1;
                }
            }
            IdleState::Writing => {
                if delta < WRITE_THRESHOLD {
                    self.state = IdleState::Idle;
                    self.burst_size = 0;
                    self.idle_count = 0;
                } else {
                    self.burst_size += delta;
                }
            }
            IdleState::Idle => {
                if delta > WRITE_THRESHOLD {
                    self.state = IdleState::Writing;
                    self.burst_size = delta;
                    self.idle_count = 0;
                } else {
                    self.idle_count += 1;
                }
            }
        }
    }

    pub fn status(&self) -> IdleStatus {
        IdleStatus {
            state: self.state,
            bytes_written: self.prev_written.unwrap_or(0),
            burst_size: self.burst_size,
            idle_seconds: self.idle_count,
        }
    }
}

impl Default for ProcIdleDetector {
    fn default() -> Self {
        Self::default_proc()
    }
}

fn parse_write_bytes(content: &str) -> Option<u64> {
    for line in content.lines() {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        if key.trim() == "write_bytes" {
            return value.trim().parse::<u64>().ok();
        }
    }
    None
}

pub fn default_timeout() -> Duration {
    std::env::var(IDLE_TIMEOUT_ENV)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_TIMEOUT)
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub struct MockIdleDetector {
    always_idle: bool,
    state: IdleState,
    pub wait_count: u64,
}

#[cfg(test)]
impl MockIdleDetector {
    pub fn new(always_idle: bool) -> Self {
        Self {
            always_idle,
            state: if always_idle {
                IdleState::Idle
            } else {
                IdleState::Writing
            },
            wait_count: 0,
        }
    }

    pub fn wait_for_idle(&mut self, _timeout: Duration) -> bool {
        self.wait_count += 1;
        self.state = if self.always_idle {
            IdleState::Idle
        } else {
            IdleState::Writing
        };
        self.always_idle
    }

    pub fn status(&self) -> IdleStatus {
        IdleStatus::new(self.state)
    }
}

#[cfg(test)]
impl Default for MockIdleDetector {
    fn default() -> Self {
        Self::new(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn temp_dir(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "teslausb-idle-{name}-{}-{}",
            std::process::id(),
            unique_suffix()
        ));
        fs::create_dir_all(&root).unwrap();
        root
    }

    fn unique_suffix() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }

    fn write_proc(root: &Path, pid: u32, comm: &str, write_bytes: u64) {
        let proc_dir = root.join(pid.to_string());
        fs::create_dir_all(&proc_dir).unwrap();
        fs::write(proc_dir.join("comm"), format!("{comm}\n")).unwrap();
        fs::write(
            proc_dir.join("io"),
            format!("read_bytes: 1\nwrite_bytes: {write_bytes}\n"),
        )
        .unwrap();
    }

    #[test]
    fn idle_status_defaults_are_stable() {
        let status = IdleStatus::new(IdleState::Undetermined);
        assert_eq!(status.state, IdleState::Undetermined);
        assert_eq!(status.bytes_written, 0);
        assert_eq!(status.burst_size, 0);
        assert_eq!(status.idle_seconds, 0);
    }

    #[test]
    fn mock_idle_detector_tracks_waits() {
        let mut detector = MockIdleDetector::default();
        assert!(detector.wait_for_idle(Duration::from_secs(1)));
        assert!(detector.wait_for_idle(Duration::from_secs(1)));
        assert_eq!(detector.wait_count, 2);
        assert_eq!(detector.status().state, IdleState::Idle);

        let mut busy = MockIdleDetector::new(false);
        assert!(!busy.wait_for_idle(Duration::from_secs(1)));
        assert_eq!(busy.status().state, IdleState::Writing);
    }

    #[test]
    fn proc_detector_finds_process_and_write_bytes() {
        let root = temp_dir("proc");
        write_proc(&root, 1234, "file-storage", 2000);

        let detector = ProcIdleDetector::new(root.clone(), "file-storage");
        assert_eq!(detector.find_process_pid(), Some(1234));
        assert_eq!(detector.write_bytes_for_pid(1234), Some(2000));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn proc_detector_treats_missing_process_as_idle() {
        let root = temp_dir("no-process");
        let mut detector = ProcIdleDetector::new(root.clone(), "file-storage")
            .with_sample_interval(Duration::ZERO);

        assert!(detector.wait_for_idle(Duration::from_millis(10)));
        assert_eq!(detector.status().state, IdleState::Idle);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn proc_detector_skips_unreadable_process_entries() {
        let root = temp_dir("skip");
        fs::create_dir_all(root.join("1")).unwrap();
        write_proc(&root, 2, "file-storage", 1234);

        let detector = ProcIdleDetector::new(root.clone(), "file-storage");

        assert_eq!(detector.find_process_pid(), Some(2));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn low_write_delta_becomes_idle_without_first_large_burst() {
        let root = temp_dir("quiet");
        write_proc(&root, 42, "file-storage", 1000);
        let mut detector = ProcIdleDetector::new(root.clone(), "file-storage")
            .with_sample_interval(Duration::from_millis(1));

        let updater_root = root.clone();
        let updater = thread::spawn(move || {
            for value in [1000_u64, 1001, 1002, 1003, 1004, 1005, 1006] {
                write_proc(&updater_root, 42, "file-storage", value);
                thread::sleep(Duration::from_millis(2));
            }
        });

        assert!(detector.wait_for_idle(Duration::from_secs(1)));
        assert_eq!(detector.status().state, IdleState::Idle);
        updater.join().unwrap();

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn parse_write_bytes_reads_expected_field() {
        assert_eq!(
            parse_write_bytes("read_bytes: 5\nwrite_bytes: 123\n"),
            Some(123)
        );
        assert_eq!(parse_write_bytes("read_bytes: 5\n"), None);
    }
}
