use std::collections::HashSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::error::{Error, Result};

pub const LED_PATH_ENV: &str = "TESLAUSB_LED_PATH";

const LED_PATHS: &[&str] = &[
    "/sys/class/leds/led0",
    "/sys/class/leds/ACT",
    "/sys/class/leds/status",
    "/sys/class/leds/user-led2",
    "/sys/class/leds/radxa-zero:green",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LedPattern {
    Off,
    SlowBlink,
    FastBlink,
    Heartbeat,
}

impl LedPattern {
    #[cfg(test)]
    pub fn as_str(self) -> &'static str {
        match self {
            LedPattern::Off => "off",
            LedPattern::SlowBlink => "slow_blink",
            LedPattern::FastBlink => "fast_blink",
            LedPattern::Heartbeat => "heartbeat",
        }
    }
}

#[derive(Debug)]
struct LedInner {
    led_path: PathBuf,
    pattern: LedPattern,
}

#[derive(Debug, Clone)]
pub struct SysfsLedController {
    inner: Arc<Mutex<LedInner>>,
}

impl SysfsLedController {
    pub fn auto_detect() -> Result<Self> {
        Self::new(None)
    }

    pub fn new(led_path: Option<PathBuf>) -> Result<Self> {
        let led_path = match led_path {
            Some(path) => path,
            None => find_led()?,
        };
        let available_triggers = load_triggers(&led_path)?;
        for trigger in ["none", "timer", "heartbeat"] {
            if !available_triggers.contains(trigger) {
                return Err(Error::new(format!(
                    "status LED {} does not support the {} trigger",
                    led_path.display(),
                    trigger
                )));
            }
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(LedInner {
                led_path,
                pattern: LedPattern::Off,
            })),
        })
    }

    pub fn set_pattern(&self, pattern: LedPattern) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let writes: &[(&str, &str)] = match pattern {
            LedPattern::Off => &[("trigger", "none"), ("brightness", "0")],
            LedPattern::SlowBlink => &[
                ("trigger", "timer"),
                ("delay_off", "900"),
                ("delay_on", "100"),
            ],
            LedPattern::FastBlink => &[
                ("trigger", "timer"),
                ("delay_off", "150"),
                ("delay_on", "50"),
            ],
            LedPattern::Heartbeat => &[("trigger", "heartbeat"), ("invert", "0")],
        };
        for (name, value) in writes {
            write_led_file(&inner.led_path, name, value)?;
        }
        inner.pattern = pattern;
        Ok(())
    }

    #[cfg(test)]
    pub fn pattern(&self) -> LedPattern {
        self.inner.lock().unwrap().pattern
    }

    #[cfg(test)]
    fn led_path(&self) -> PathBuf {
        self.inner.lock().unwrap().led_path.clone()
    }
}

fn find_led() -> Result<PathBuf> {
    if let Some(path) = env::var_os(LED_PATH_ENV) {
        return Ok(PathBuf::from(path));
    }
    LED_PATHS
        .iter()
        .map(PathBuf::from)
        .find(|path| path.exists())
        .ok_or_else(|| {
            Error::new(
                "no supported status LED found; set TESLAUSB_LED_PATH to the board's LED directory",
            )
        })
}

fn load_triggers(led_path: &Path) -> Result<HashSet<String>> {
    let path = led_path.join("trigger");
    let content = fs::read_to_string(&path).map_err(|error| {
        Error::new(format!(
            "cannot read LED triggers from {}: {}",
            path.display(),
            error
        ))
    })?;
    Ok(content
        .replace(['[', ']'], "")
        .split_whitespace()
        .map(str::to_string)
        .collect())
}

fn write_led_file(led_path: &Path, name: &str, value: &str) -> Result<()> {
    let path = led_path.join(name);
    fs::write(&path, value).map_err(|error| {
        Error::new(format!(
            "cannot set status LED {}: {}",
            path.display(),
            error
        ))
    })?;
    Ok(())
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub struct MockLedController {
    pattern: LedPattern,
    pub pattern_history: Vec<LedPattern>,
}

#[cfg(test)]
impl MockLedController {
    pub fn new() -> Self {
        Self {
            pattern: LedPattern::Off,
            pattern_history: Vec::new(),
        }
    }

    pub fn set_pattern(&mut self, pattern: LedPattern) {
        self.pattern = pattern;
        self.pattern_history.push(pattern);
    }

    pub fn pattern(&self) -> LedPattern {
        self.pattern
    }
}

#[cfg(test)]
impl Default for MockLedController {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn led_fixture(name: &str, triggers: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "teslausb-led-{name}-{}-{}",
            std::process::id(),
            unique_suffix()
        ));
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("trigger"), triggers).unwrap();
        fs::write(root.join("brightness"), "1").unwrap();
        fs::write(root.join("delay_off"), "").unwrap();
        fs::write(root.join("delay_on"), "").unwrap();
        fs::write(root.join("invert"), "").unwrap();
        root
    }

    fn unique_suffix() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }

    #[test]
    fn led_pattern_names_match_python_values() {
        assert_eq!(LedPattern::Off.as_str(), "off");
        assert_eq!(LedPattern::SlowBlink.as_str(), "slow_blink");
        assert_eq!(LedPattern::FastBlink.as_str(), "fast_blink");
        assert_eq!(LedPattern::Heartbeat.as_str(), "heartbeat");
    }

    #[test]
    fn mock_led_records_history() {
        let mut led = MockLedController::new();
        led.set_pattern(LedPattern::SlowBlink);
        led.set_pattern(LedPattern::FastBlink);
        led.set_pattern(LedPattern::Heartbeat);
        led.set_pattern(LedPattern::Off);

        assert_eq!(led.pattern(), LedPattern::Off);
        assert_eq!(
            led.pattern_history,
            [
                LedPattern::SlowBlink,
                LedPattern::FastBlink,
                LedPattern::Heartbeat,
                LedPattern::Off
            ]
        );
    }

    #[test]
    fn sysfs_led_sets_timer_patterns() {
        let led_path = led_fixture("timer", "[none] timer heartbeat");
        let led = SysfsLedController::new(Some(led_path.clone())).unwrap();
        assert_eq!(led.led_path(), led_path.clone());

        led.set_pattern(LedPattern::SlowBlink).unwrap();
        assert_eq!(
            fs::read_to_string(led_path.join("trigger")).unwrap(),
            "timer"
        );
        assert_eq!(
            fs::read_to_string(led_path.join("delay_off")).unwrap(),
            "900"
        );
        assert_eq!(
            fs::read_to_string(led_path.join("delay_on")).unwrap(),
            "100"
        );

        led.set_pattern(LedPattern::FastBlink).unwrap();
        assert_eq!(
            fs::read_to_string(led_path.join("delay_off")).unwrap(),
            "150"
        );
        assert_eq!(fs::read_to_string(led_path.join("delay_on")).unwrap(), "50");

        let _ = fs::remove_dir_all(led_path);
    }

    #[test]
    fn sysfs_led_sets_heartbeat_and_off() {
        let led_path = led_fixture("heartbeat", "[none] timer heartbeat");
        let led = SysfsLedController::new(Some(led_path.clone())).unwrap();

        led.set_pattern(LedPattern::Heartbeat).unwrap();
        assert_eq!(
            fs::read_to_string(led_path.join("trigger")).unwrap(),
            "heartbeat"
        );
        assert_eq!(fs::read_to_string(led_path.join("invert")).unwrap(), "0");

        led.set_pattern(LedPattern::Off).unwrap();
        assert_eq!(
            fs::read_to_string(led_path.join("trigger")).unwrap(),
            "none"
        );
        assert_eq!(
            fs::read_to_string(led_path.join("brightness")).unwrap(),
            "0"
        );

        let _ = fs::remove_dir_all(led_path);
    }

    #[test]
    fn sysfs_led_requires_supported_triggers() {
        let led_path = led_fixture("none", "[none]");
        let error = SysfsLedController::new(Some(led_path.clone())).unwrap_err();
        assert!(error.to_string().contains("timer trigger"));
        fs::remove_dir_all(led_path).unwrap();
    }

    #[test]
    fn sysfs_led_reports_missing_trigger_file() {
        let led_path = led_fixture("missing", "[none] timer heartbeat");
        fs::remove_file(led_path.join("trigger")).unwrap();
        assert!(SysfsLedController::new(Some(led_path.clone())).is_err());
        fs::remove_dir_all(led_path).unwrap();
    }

    #[test]
    fn sysfs_led_reports_write_failure_without_recording_success() {
        let led_path = led_fixture("write-failure", "[none] timer heartbeat");
        let led = SysfsLedController::new(Some(led_path.clone())).unwrap();
        fs::remove_file(led_path.join("delay_on")).unwrap();
        fs::create_dir(led_path.join("delay_on")).unwrap();
        let error = led.set_pattern(LedPattern::SlowBlink).unwrap_err();
        assert!(error.to_string().contains("delay_on"));
        assert_eq!(led.pattern(), LedPattern::Off);
        fs::remove_dir_all(led_path).unwrap();
    }
}
