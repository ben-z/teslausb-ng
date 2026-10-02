use crate::config::RuntimeConfig;
use crate::error::{Error, Result};
use std::fs;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

pub const PROC_PATH_ENV: &str = "TESLAUSB_PROC_PATH";
pub const PROCESS_NAME_ENV: &str = "TESLAUSB_IDLE_PROCESS";

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
    confirm_samples: u64,
}

impl ProcIdleDetector {
    pub fn new(
        proc_path: PathBuf,
        process_name: impl Into<String>,
        config: &RuntimeConfig,
    ) -> Self {
        Self {
            proc_path,
            process_name: process_name.into(),
            state: IdleState::Undetermined,
            prev_written: None,
            burst_size: 0,
            idle_count: 0,
            sample_interval: config.idle_sample_interval,
            confirm_samples: config.idle_confirm_samples,
        }
    }

    pub fn default_proc(config: &RuntimeConfig) -> Self {
        let proc_path = std::env::var_os(PROC_PATH_ENV)
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/proc"));
        let process_name =
            std::env::var(PROCESS_NAME_ENV).unwrap_or_else(|_| "file-storage".to_string());
        Self::new(proc_path, process_name, config)
    }

    #[cfg(test)]
    pub fn with_sample_interval(mut self, interval: Duration) -> Self {
        self.sample_interval = interval;
        self
    }

    pub fn find_process_pid(&self) -> Result<Option<u32>> {
        let entries = fs::read_dir(&self.proc_path)?;
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if !name.chars().all(|ch| ch.is_ascii_digit()) {
                continue;
            }

            let comm = match fs::read_to_string(entry.path().join("comm")) {
                Ok(comm) => comm,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            if comm.trim() == self.process_name {
                if let Ok(pid) = name.parse::<u32>() {
                    return Ok(Some(pid));
                }
            }
        }
        Ok(None)
    }

    pub fn write_bytes_for_pid(&self, pid: u32) -> Result<u64> {
        let content = fs::read_to_string(self.proc_path.join(pid.to_string()).join("io"))?;
        parse_write_bytes(&content).ok_or_else(|| {
            Error::new(format!(
                "missing or invalid wchar counter for USB writer {pid}"
            ))
        })
    }

    pub fn wait_for_idle(&mut self, timeout: Duration) -> bool {
        self.state = IdleState::Undetermined;
        self.prev_written = None;
        self.burst_size = 0;
        self.idle_count = 0;

        let started = Instant::now();
        while started.elapsed() < timeout {
            if crate::coordinator::stop_requested() {
                return false;
            }
            if !self.sample_interval.is_zero() {
                thread::sleep(self.sample_interval);
            }

            let pid = match self.find_process_pid() {
                Ok(Some(pid)) => pid,
                Ok(None) => {
                    self.state = IdleState::Idle;
                    return true;
                }
                Err(error) => {
                    eprintln!("error: cannot observe USB writes: {error}");
                    return false;
                }
            };
            let written = match self.write_bytes_for_pid(pid) {
                Ok(written) => written,
                Err(error) => {
                    eprintln!("error: cannot observe USB writes: {error}");
                    return false;
                }
            };
            let Some(previous) = self.prev_written.replace(written) else {
                continue;
            };
            let delta = written.saturating_sub(previous);
            self.update_state(delta);

            if self.state == IdleState::Idle && self.idle_count >= self.confirm_samples {
                return true;
            }
        }
        let status = self.status();
        eprintln!(
            "warning: timed out waiting for USB writes to become idle; skipping archive cycle \
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
                if delta > 0 {
                    self.state = IdleState::Writing;
                    self.burst_size = delta;
                } else {
                    self.state = IdleState::Idle;
                    self.idle_count = 1;
                }
            }
            IdleState::Writing => {
                if delta == 0 {
                    self.state = IdleState::Idle;
                    self.burst_size = 0;
                    self.idle_count = 0;
                } else {
                    self.burst_size += delta;
                }
            }
            IdleState::Idle => {
                if delta > 0 {
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

fn parse_write_bytes(content: &str) -> Option<u64> {
    for line in content.lines() {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        if key.trim() == "wchar" {
            return value.trim().parse::<u64>().ok();
        }
    }
    None
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
        if !proc_dir.join("comm").exists() {
            fs::write(proc_dir.join("comm"), format!("{comm}\n")).unwrap();
        }
        fs::write(
            proc_dir.join("io.tmp"),
            format!("read_bytes: 1\nwchar: {write_bytes}\n"),
        )
        .unwrap();
        fs::rename(proc_dir.join("io.tmp"), proc_dir.join("io")).unwrap();
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

        let detector =
            ProcIdleDetector::new(root.clone(), "file-storage", &RuntimeConfig::default());
        assert_eq!(detector.find_process_pid().unwrap(), Some(1234));
        assert_eq!(detector.write_bytes_for_pid(1234).unwrap(), 2000);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn proc_detector_treats_missing_process_as_idle() {
        let root = temp_dir("no-process");
        let mut detector =
            ProcIdleDetector::new(root.clone(), "file-storage", &RuntimeConfig::default())
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

        let detector =
            ProcIdleDetector::new(root.clone(), "file-storage", &RuntimeConfig::default());

        assert_eq!(detector.find_process_pid().unwrap(), Some(2));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn low_write_delta_becomes_idle_without_first_large_burst() {
        let root = temp_dir("quiet");
        write_proc(&root, 42, "file-storage", 1000);
        let mut detector =
            ProcIdleDetector::new(root.clone(), "file-storage", &RuntimeConfig::default())
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
        assert_eq!(parse_write_bytes("read_bytes: 5\nwchar: 123\n"), Some(123));
        assert_eq!(parse_write_bytes("read_bytes: 5\n"), None);
    }
}
