use std::fmt;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

pub const THERMAL_ZONE_PATH: &str = "/sys/class/thermal/thermal_zone0/temp";
pub const THERMAL_PATH_ENV: &str = "TESLAUSB_THERMAL_PATH";
pub const HYSTERESIS_MILLIDEGREES: i64 = 5000;

#[derive(Debug, Clone, Copy)]
pub struct TemperatureReading {
    pub millidegrees: i64,
}

impl TemperatureReading {
    pub fn new(millidegrees: i64) -> Self {
        Self { millidegrees }
    }

    pub fn celsius(&self) -> f64 {
        self.millidegrees as f64 / 1000.0
    }

    #[cfg(test)]
    fn fahrenheit(&self) -> f64 {
        self.celsius() * 9.0 / 5.0 + 32.0
    }
}

impl fmt::Display for TemperatureReading {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:.1} C", self.celsius())
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, Default)]
pub struct TemperatureStatus {
    pub current: Option<TemperatureReading>,
    pub peak: Option<TemperatureReading>,
    pub warning_triggered: bool,
    pub caution_triggered: bool,
}

#[derive(Debug, Clone)]
pub struct TemperatureConfig {
    pub warning_threshold: Option<i64>,
    pub caution_threshold: Option<i64>,
    pub poll_interval: Duration,
}

impl Default for TemperatureConfig {
    fn default() -> Self {
        Self {
            warning_threshold: None,
            caution_threshold: None,
            poll_interval: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Default)]
struct TemperatureState {
    current: Option<TemperatureReading>,
    peak: Option<TemperatureReading>,
    warning_triggered: bool,
    caution_triggered: bool,
}

#[derive(Debug, Clone)]
pub struct SysfsTemperatureMonitor {
    thermal_path: PathBuf,
    config: TemperatureConfig,
    state: Arc<Mutex<TemperatureState>>,
    stop_requested: Arc<AtomicBool>,
}

impl SysfsTemperatureMonitor {
    pub fn new(thermal_path: PathBuf, config: TemperatureConfig) -> Self {
        Self {
            thermal_path,
            config,
            state: Arc::new(Mutex::new(TemperatureState::default())),
            stop_requested: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn default_sysfs(config: TemperatureConfig) -> Self {
        let thermal_path = std::env::var_os(THERMAL_PATH_ENV)
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(THERMAL_ZONE_PATH));
        Self::new(thermal_path, config)
    }

    pub fn is_available(&self) -> bool {
        self.thermal_path.exists()
    }

    pub fn get_temperature(&self) -> Option<TemperatureReading> {
        let content = fs::read_to_string(&self.thermal_path).ok()?;
        let millidegrees = content.trim().parse::<i64>().ok()?;
        Some(TemperatureReading::new(millidegrees))
    }

    pub fn update(&self) -> Option<TemperatureReading> {
        let reading = self.get_temperature()?;
        let mut state = self.state.lock().unwrap();
        state.current = Some(reading);
        if state
            .peak
            .map(|peak| reading.millidegrees > peak.millidegrees)
            .unwrap_or(true)
        {
            state.peak = Some(reading);
        }
        update_threshold(
            reading,
            self.config.warning_threshold,
            &mut state.warning_triggered,
            "warning",
        );
        update_threshold(
            reading,
            self.config.caution_threshold,
            &mut state.caution_triggered,
            "caution",
        );
        Some(reading)
    }

    #[cfg(test)]
    pub fn status(&self) -> TemperatureStatus {
        let state = self.state.lock().unwrap();
        TemperatureStatus {
            current: state.current,
            peak: state.peak,
            warning_triggered: state.warning_triggered,
            caution_triggered: state.caution_triggered,
        }
    }

    #[cfg(test)]
    pub fn reset_peak(&self) {
        let mut state = self.state.lock().unwrap();
        state.peak = None;
    }

    pub fn start(&self) -> Option<TemperatureMonitorGuard> {
        if !self.is_available() {
            return None;
        }
        self.stop_requested.store(false, Ordering::SeqCst);
        let monitor = self.clone();
        let handle = thread::spawn(move || {
            while !monitor.stop_requested.load(Ordering::SeqCst) {
                monitor.update();
                sleep_interruptible(monitor.config.poll_interval, &monitor.stop_requested);
            }
        });
        Some(TemperatureMonitorGuard {
            stop_requested: self.stop_requested.clone(),
            handle: Some(handle),
        })
    }
}

pub struct TemperatureMonitorGuard {
    stop_requested: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl Drop for TemperatureMonitorGuard {
    fn drop(&mut self) {
        self.stop_requested.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn update_threshold(
    reading: TemperatureReading,
    threshold: Option<i64>,
    triggered: &mut bool,
    name: &str,
) {
    let Some(threshold) = threshold else {
        return;
    };
    let clear_threshold = threshold - HYSTERESIS_MILLIDEGREES;
    if reading.millidegrees < clear_threshold {
        *triggered = false;
    } else if reading.millidegrees > threshold && !*triggered {
        *triggered = true;
        eprintln!("temperature {name}: {reading}");
    }
}

fn sleep_interruptible(duration: Duration, stop_requested: &AtomicBool) {
    let mut waited = Duration::ZERO;
    while waited < duration && !stop_requested.load(Ordering::SeqCst) {
        let step = Duration::from_millis(250).min(duration - waited);
        thread::sleep(step);
        waited += step;
    }
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub struct MockTemperatureMonitor {
    temperature: i64,
    peak: i64,
    pub reading_count: u64,
}

#[cfg(test)]
impl MockTemperatureMonitor {
    pub fn new(temperature: i64) -> Self {
        Self {
            temperature,
            peak: temperature,
            reading_count: 0,
        }
    }

    pub fn set_temperature(&mut self, millidegrees: i64) {
        self.temperature = millidegrees;
        self.peak = self.peak.max(millidegrees);
    }

    pub fn get_temperature(&mut self) -> TemperatureReading {
        self.reading_count += 1;
        TemperatureReading::new(self.temperature)
    }

    pub fn status(&self) -> TemperatureStatus {
        TemperatureStatus {
            current: Some(TemperatureReading::new(self.temperature)),
            peak: Some(TemperatureReading::new(self.peak)),
            warning_triggered: false,
            caution_triggered: false,
        }
    }

    pub fn reset_peak(&mut self) {
        self.peak = self.temperature;
    }
}

#[cfg(test)]
impl Default for MockTemperatureMonitor {
    fn default() -> Self {
        Self::new(45_000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

    fn temp_file(name: &str, value: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "teslausb-temp-{name}-{}-{}",
            std::process::id(),
            unique_suffix()
        ));
        fs::create_dir_all(&root).unwrap();
        let path = root.join("temp");
        fs::write(&path, value).unwrap();
        path
    }

    fn unique_suffix() -> u128 {
        SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }

    #[test]
    fn temperature_reading_converts_units() {
        let reading = TemperatureReading::new(37_000);
        assert_eq!(reading.celsius(), 37.0);
        assert!((reading.fahrenheit() - 98.6).abs() < 0.1);
        assert_eq!(reading.to_string(), "37.0 C");
    }

    #[test]
    fn sysfs_monitor_reads_temperature_and_tracks_peak() {
        let path = temp_file("read", "45_000");
        fs::write(&path, "45000").unwrap();
        let monitor = SysfsTemperatureMonitor::new(path.clone(), TemperatureConfig::default());

        assert!(monitor.is_available());
        assert_eq!(monitor.get_temperature().unwrap().millidegrees, 45_000);
        monitor.update();
        fs::write(&path, "70000").unwrap();
        monitor.update();
        fs::write(&path, "60000").unwrap();
        monitor.update();

        let status = monitor.status();
        assert_eq!(status.current.unwrap().millidegrees, 60_000);
        assert_eq!(status.peak.unwrap().millidegrees, 70_000);
        monitor.reset_peak();
        assert!(monitor.status().peak.is_none());

        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn threshold_hysteresis_prevents_flapping() {
        let path = temp_file("threshold", "85000");
        let monitor = SysfsTemperatureMonitor::new(
            path.clone(),
            TemperatureConfig {
                warning_threshold: Some(80_000),
                caution_threshold: Some(70_000),
                poll_interval: Duration::from_secs(60),
            },
        );

        monitor.update();
        assert!(monitor.status().warning_triggered);
        assert!(monitor.status().caution_triggered);

        fs::write(&path, "76000").unwrap();
        monitor.update();
        assert!(monitor.status().warning_triggered);
        assert!(monitor.status().caution_triggered);

        fs::write(&path, "64000").unwrap();
        monitor.update();
        assert!(!monitor.status().warning_triggered);
        assert!(!monitor.status().caution_triggered);

        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn mock_temperature_tracks_peak_and_count() {
        let mut monitor = MockTemperatureMonitor::new(40_000);
        monitor.set_temperature(60_000);
        monitor.set_temperature(50_000);
        assert_eq!(monitor.get_temperature().millidegrees, 50_000);
        assert_eq!(monitor.reading_count, 1);
        assert_eq!(monitor.status().peak.unwrap().millidegrees, 60_000);
        monitor.reset_peak();
        assert_eq!(monitor.status().peak.unwrap().millidegrees, 50_000);
    }
}
