use std::collections::HashSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

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
    led_path: Option<PathBuf>,
    pattern: LedPattern,
    available_triggers: HashSet<String>,
}

#[derive(Debug, Clone)]
pub struct SysfsLedController {
    inner: Arc<Mutex<LedInner>>,
}

impl SysfsLedController {
    pub fn auto_detect() -> Self {
        Self::new(None)
    }

    pub fn new(led_path: Option<PathBuf>) -> Self {
        let led_path = led_path.or_else(find_led);
        let available_triggers = led_path
            .as_ref()
            .map(|path| load_triggers(path.as_path()))
            .unwrap_or_default();
        Self {
            inner: Arc::new(Mutex::new(LedInner {
                led_path,
                pattern: LedPattern::Off,
                available_triggers,
            })),
        }
    }

    pub fn set_pattern(&self, pattern: LedPattern) {
        let mut inner = self.inner.lock().unwrap();
        inner.pattern = pattern;
        let Some(led_path) = inner.led_path.clone() else {
            return;
        };

        match pattern {
            LedPattern::Off => {
                let _ = write_led_file(&led_path, "trigger", "none");
                let _ = write_led_file(&led_path, "brightness", "0");
            }
            LedPattern::SlowBlink => {
                if inner.available_triggers.contains("timer") {
                    let _ = write_led_file(&led_path, "trigger", "timer");
                    let _ = write_led_file(&led_path, "delay_off", "900");
                    let _ = write_led_file(&led_path, "delay_on", "100");
                }
            }
            LedPattern::FastBlink => {
                if inner.available_triggers.contains("timer") {
                    let _ = write_led_file(&led_path, "trigger", "timer");
                    let _ = write_led_file(&led_path, "delay_off", "150");
                    let _ = write_led_file(&led_path, "delay_on", "50");
                }
            }
            LedPattern::Heartbeat => {
                if inner.available_triggers.contains("heartbeat") {
                    let _ = write_led_file(&led_path, "trigger", "heartbeat");
                    let _ = write_led_file(&led_path, "invert", "0");
                }
            }
        }
    }

    #[cfg(test)]
    pub fn pattern(&self) -> LedPattern {
        self.inner.lock().unwrap().pattern
    }

    #[cfg(test)]
    fn led_path(&self) -> Option<PathBuf> {
        self.inner.lock().unwrap().led_path.clone()
    }
}

fn find_led() -> Option<PathBuf> {
    if let Some(path) = env::var_os(LED_PATH_ENV).map(PathBuf::from) {
        return path.exists().then_some(path);
    }
    LED_PATHS
        .iter()
        .map(PathBuf::from)
        .find(|path| path.exists())
}

fn load_triggers(led_path: &Path) -> HashSet<String> {
    fs::read_to_string(led_path.join("trigger"))
        .map(|content| {
            content
                .replace(['[', ']'], "")
                .split_whitespace()
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn write_led_file(led_path: &Path, name: &str, value: &str) -> std::io::Result<()> {
    fs::write(led_path.join(name), value)
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
        let led = SysfsLedController::new(Some(led_path.clone()));
        assert_eq!(led.led_path(), Some(led_path.clone()));

        led.set_pattern(LedPattern::SlowBlink);
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

        led.set_pattern(LedPattern::FastBlink);
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
        let led = SysfsLedController::new(Some(led_path.clone()));

        led.set_pattern(LedPattern::Heartbeat);
        assert_eq!(
            fs::read_to_string(led_path.join("trigger")).unwrap(),
            "heartbeat"
        );
        assert_eq!(fs::read_to_string(led_path.join("invert")).unwrap(), "0");

        led.set_pattern(LedPattern::Off);
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
    fn sysfs_led_without_triggers_only_records_pattern() {
        let led_path = led_fixture("none", "[none]");
        let led = SysfsLedController::new(Some(led_path.clone()));

        led.set_pattern(LedPattern::SlowBlink);
        assert_eq!(led.pattern(), LedPattern::SlowBlink);
        assert_eq!(
            fs::read_to_string(led_path.join("trigger")).unwrap(),
            "[none]"
        );

        let _ = fs::remove_dir_all(led_path);
    }
}
