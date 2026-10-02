use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::error::{Error, Result};

pub const GB: u64 = 1024 * 1024 * 1024;
const MB: u64 = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CameraFilesystem {
    Fat32,
    Ext4,
}

impl CameraFilesystem {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "fat32" => Ok(Self::Fat32),
            "ext4" => Ok(Self::Ext4),
            _ => Err(Error::new(format!(
                "CAM_FILESYSTEM must be fat32 or ext4, got {value:?}"
            ))),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Fat32 => "fat32",
            Self::Ext4 => "ext4",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveConfig {
    pub system: String,
    pub rclone_drive: String,
    pub rclone_path: String,
    pub rclone_flags: Vec<String>,
    pub archive_recent: bool,
    pub archive_saved: bool,
    pub archive_sentry: bool,
    pub archive_track: bool,
    pub archive_photobooth: bool,
    pub event_stability: Duration,
    pub copy_timeout: Duration,
}

impl Default for ArchiveConfig {
    fn default() -> Self {
        Self {
            system: "none".to_string(),
            rclone_drive: String::new(),
            rclone_path: String::new(),
            rclone_flags: Vec::new(),
            archive_recent: false,
            archive_saved: true,
            archive_sentry: true,
            archive_track: true,
            archive_photobooth: true,
            event_stability: Duration::from_secs(600),
            copy_timeout: Duration::from_secs(3600),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConfig {
    pub idle_timeout: Duration,
    pub idle_confirm_samples: u64,
    pub idle_sample_interval: Duration,
    pub poll_interval: Duration,
    pub max_poll_interval: Duration,
    pub retry_interval: Duration,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            idle_timeout: Duration::from_secs(90),
            idle_confirm_samples: 5,
            idle_sample_interval: Duration::from_secs(1),
            poll_interval: Duration::from_secs(5),
            max_poll_interval: Duration::from_secs(300),
            retry_interval: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub cam_filesystem: CameraFilesystem,
    pub backingfiles_path: PathBuf,
    pub mutable_path: PathBuf,
    pub archive: ArchiveConfig,
    pub runtime: RuntimeConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            cam_filesystem: CameraFilesystem::Fat32,
            backingfiles_path: PathBuf::from("/backingfiles"),
            mutable_path: PathBuf::from("/mutable"),
            archive: ArchiveConfig::default(),
            runtime: RuntimeConfig::default(),
        }
    }
}

impl Config {
    pub fn cam_disk_path(&self) -> PathBuf {
        self.backingfiles_path.join("cam_disk.bin")
    }

    pub fn snapshots_path(&self) -> PathBuf {
        self.backingfiles_path.join("snapshots")
    }

    pub fn backingfiles_image_path(&self) -> PathBuf {
        self.mutable_path.join("backingfiles.img")
    }

    pub fn warnings(&self) -> Vec<String> {
        let mut warnings = Vec::new();
        if !matches!(self.archive.system.as_str(), "rclone" | "none") {
            warnings.push(format!("Unknown archive system: {}", self.archive.system));
        }
        if self.archive.system == "rclone" && self.archive.rclone_drive.trim().is_empty() {
            warnings.push("ARCHIVE_SYSTEM=rclone requires RCLONE_DRIVE".to_string());
        }
        warnings
    }
}

pub fn parse_size(input: &str) -> Result<u64> {
    let value = input.trim().to_ascii_uppercase();
    if value.is_empty() {
        return Err(Error::new("size is empty"));
    }
    if value.ends_with('%') {
        return Err(Error::new(format!(
            "percentage sizes are not supported: {}",
            input
        )));
    }

    let mut number = String::new();
    let mut suffix = String::new();
    for ch in value.chars() {
        if ch.is_ascii_digit() || ch == '.' {
            if !suffix.is_empty() {
                return Err(Error::new(format!("invalid size string: {}", input)));
            }
            number.push(ch);
        } else if ch.is_ascii_whitespace() {
            continue;
        } else {
            suffix.push(ch);
        }
    }

    if number.is_empty() {
        return Err(Error::new(format!("invalid size string: {}", input)));
    }

    if suffix.ends_with('B') {
        suffix.pop();
    }

    let multiplier = match suffix.as_str() {
        "" => 1,
        "K" => 1024,
        "M" => MB,
        "G" => GB,
        "T" => 1024 * GB,
        _ => return Err(Error::new(format!("invalid size suffix: {}", suffix))),
    };

    let numeric: f64 = number.parse()?;
    Ok((numeric * multiplier as f64) as u64)
}

pub fn load_config(path: Option<&Path>) -> Result<Config> {
    let file_values = match path {
        Some(path) => parse_config_file(path)?,
        None if Path::new("/etc/teslausb.conf").exists() => {
            parse_config_file(Path::new("/etc/teslausb.conf"))?
        }
        None => HashMap::new(),
    };
    let config = load_from_sources(&file_values)?;
    let errors = config.warnings();
    if !errors.is_empty() {
        return Err(Error::new(errors.join("; ")));
    }
    Ok(config)
}

fn load_from_sources(file_values: &HashMap<String, String>) -> Result<Config> {
    let mut config = Config::default();
    let mut archive = ArchiveConfig::default();

    if let Some(value) = get_var(file_values, "CAM_FILESYSTEM") {
        config.cam_filesystem = CameraFilesystem::parse(&value)?;
    }

    if let Some(value) = get_var(file_values, "MUTABLE_PATH") {
        config.mutable_path = PathBuf::from(value);
    }
    if let Some(value) = get_var(file_values, "BACKINGFILES_PATH") {
        config.backingfiles_path = PathBuf::from(value);
    }

    if let Some(value) = get_var(file_values, "ARCHIVE_SYSTEM") {
        archive.system = value.to_ascii_lowercase();
    }
    if let Some(value) = get_var(file_values, "RCLONE_DRIVE") {
        archive.rclone_drive = value;
    }
    if let Some(value) = get_var(file_values, "RCLONE_PATH") {
        archive.rclone_path = value;
    }
    if let Some(value) = get_var(file_values, "RCLONE_FLAGS") {
        archive.rclone_flags = value.split_whitespace().map(str::to_string).collect();
    }

    archive.archive_recent = get_bool(file_values, "ARCHIVE_RECENTCLIPS", archive.archive_recent)?;
    archive.archive_saved = get_bool(file_values, "ARCHIVE_SAVEDCLIPS", archive.archive_saved)?;
    archive.archive_sentry = get_bool(file_values, "ARCHIVE_SENTRYCLIPS", archive.archive_sentry)?;
    archive.archive_track = get_bool(file_values, "ARCHIVE_TRACKMODECLIPS", archive.archive_track)?;
    archive.archive_photobooth = get_bool(
        file_values,
        "ARCHIVE_PHOTOBOOTH",
        archive.archive_photobooth,
    )?;

    if let Some(value) = get_var(file_values, "EVENT_STABILITY_SECONDS") {
        archive.event_stability =
            Duration::from_secs(value.parse().map_err(|error| {
                Error::new(format!("invalid EVENT_STABILITY_SECONDS: {error}"))
            })?);
    }

    if let Some(value) = get_var(file_values, "TESLAUSB_IDLE_TIMEOUT_SECS") {
        let seconds: u64 = value
            .parse()
            .map_err(|error| Error::new(format!("invalid TESLAUSB_IDLE_TIMEOUT_SECS: {error}")))?;
        if seconds == 0 {
            return Err(Error::new("TESLAUSB_IDLE_TIMEOUT_SECS must be positive"));
        }
        config.runtime.idle_timeout = Duration::from_secs(seconds);
    }

    config.archive = archive;
    Ok(config)
}

fn get_var(file_values: &HashMap<String, String>, key: &str) -> Option<String> {
    file_values.get(key).cloned().or_else(|| env::var(key).ok())
}

fn get_bool(file_values: &HashMap<String, String>, key: &str, default: bool) -> Result<bool> {
    match get_var(file_values, key) {
        Some(value) if value.eq_ignore_ascii_case("true") => Ok(true),
        Some(value) if value.eq_ignore_ascii_case("false") => Ok(false),
        Some(value) => Err(Error::new(format!(
            "{key} must be true or false, got {value:?}"
        ))),
        None => Ok(default),
    }
}

fn parse_config_file(path: &Path) -> Result<HashMap<String, String>> {
    let content = fs::read_to_string(path).map_err(|e| {
        Error::new(format!(
            "config file not found or unreadable {}: {}",
            path.display(),
            e
        ))
    })?;
    let mut values = HashMap::new();

    for (index, raw_line) in content.lines().enumerate() {
        let mut line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix("export ") {
            line = rest.trim_start();
        }
        let Some((key, value)) = line.split_once('=') else {
            return Err(Error::new(format!(
                "{}:{}: expected KEY=value",
                path.display(),
                index + 1
            )));
        };
        let key = key.trim();
        if key.is_empty()
            || !key
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        {
            return Err(Error::new(format!(
                "{}:{}: invalid configuration key",
                path.display(),
                index + 1
            )));
        }
        let mut value = value.trim();
        if ((value.starts_with('"') && value.ends_with('"'))
            || (value.starts_with('\'') && value.ends_with('\'')))
            && value.len() >= 2
        {
            value = &value[1..value.len() - 1];
        }
        values.insert(key.to_string(), value.to_string());
    }

    Ok(values)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    static ENV_LOCK: Mutex<()> = Mutex::new(());
    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_config(content: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
        let path = std::env::temp_dir().join(format!(
            "teslausb-test-{}-{counter}-{suffix}.conf",
            std::process::id()
        ));
        fs::write(&path, content).unwrap();
        path
    }

    #[test]
    fn parse_size_accepts_suffixes() {
        assert_eq!(parse_size("1000").unwrap(), 1000);
        assert_eq!(parse_size("0").unwrap(), 0);
        assert_eq!(parse_size("1K").unwrap(), 1024);
        assert_eq!(parse_size("1KB").unwrap(), 1024);
        assert_eq!(parse_size("10k").unwrap(), 10 * 1024);
        assert_eq!(parse_size("1M").unwrap(), 1024 * 1024);
        assert_eq!(parse_size("500m").unwrap(), 500 * 1024 * 1024);
        assert_eq!(parse_size("10G").unwrap(), 10 * GB);
        assert_eq!(parse_size("40 g").unwrap(), 40 * GB);
        assert_eq!(parse_size("1T").unwrap(), 1024 * GB);
        assert_eq!(parse_size("1.5M").unwrap(), 1536 * 1024);
        assert_eq!(parse_size("0.5M").unwrap(), 512 * 1024);
        assert_eq!(parse_size("512").unwrap(), 512);
    }

    #[test]
    fn parse_size_rejects_bad_values() {
        assert!(parse_size("50%").is_err());
        assert!(parse_size("").is_err());
        assert!(parse_size("invalid").is_err());
        assert!(parse_size("40X").is_err());
        assert!(parse_size("10G1").is_err());
    }

    #[test]
    fn defaults_and_derived_paths_match_expected_layout() {
        let config = Config::default();
        assert_eq!(config.cam_filesystem, CameraFilesystem::Fat32);
        assert_eq!(config.backingfiles_path, PathBuf::from("/backingfiles"));
        assert_eq!(config.mutable_path, PathBuf::from("/mutable"));
        assert_eq!(
            config.cam_disk_path(),
            PathBuf::from("/backingfiles/cam_disk.bin")
        );
        assert_eq!(
            config.snapshots_path(),
            PathBuf::from("/backingfiles/snapshots")
        );
        assert_eq!(
            config.backingfiles_image_path(),
            PathBuf::from("/mutable/backingfiles.img")
        );
        assert!(config.warnings().is_empty());
        assert!(config.archive.archive_saved);
        assert!(config.archive.archive_sentry);
        assert!(!config.archive.archive_recent);
    }

    #[test]
    fn camera_filesystem_is_explicit_and_rejects_unknown_formats() {
        for (value, expected) in [
            ("fat32", CameraFilesystem::Fat32),
            ("ext4", CameraFilesystem::Ext4),
        ] {
            let values = HashMap::from([("CAM_FILESYSTEM".to_string(), value.to_string())]);
            assert_eq!(load_from_sources(&values).unwrap().cam_filesystem, expected);
            assert_eq!(expected.name(), value);
        }
        for value in ["", "exfat", "vfat", "EXT4"] {
            let values = HashMap::from([("CAM_FILESYSTEM".to_string(), value.to_string())]);
            assert!(load_from_sources(&values)
                .unwrap_err()
                .to_string()
                .contains("CAM_FILESYSTEM must be fat32 or ext4"));
        }
    }

    #[test]
    fn warnings_catch_invalid_archive_settings() {
        let mut config = Config::default();
        config.archive.system = "bogus".into();
        assert!(config
            .warnings()
            .iter()
            .any(|warning| warning.contains("Unknown archive system")));

        config.archive.system = "rclone".into();
        assert!(config
            .warnings()
            .iter()
            .any(|warning| warning.contains("RCLONE_DRIVE")));
    }

    #[test]
    fn load_config_reads_shell_style_files() {
        let path = temp_config(
            r#"
# comment
export ARCHIVE_SYSTEM=rclone
RCLONE_DRIVE='gdrive'
RCLONE_PATH="/My Drive/TeslaCam"
RCLONE_FLAGS=--fast-list --checkers 4
MUTABLE_PATH=/var/lib/teslausb
BACKINGFILES_PATH=/mnt/backing
"#,
        );

        let config = load_config(Some(&path)).unwrap();
        let _ = fs::remove_file(&path);

        assert_eq!(config.archive.system, "rclone");
        assert_eq!(config.archive.rclone_drive, "gdrive");
        assert_eq!(config.archive.rclone_path, "/My Drive/TeslaCam");
        assert_eq!(
            config.archive.rclone_flags,
            ["--fast-list", "--checkers", "4"]
        );
        assert_eq!(config.mutable_path, PathBuf::from("/var/lib/teslausb"));
        assert_eq!(config.backingfiles_path, PathBuf::from("/mnt/backing"));
    }

    #[test]
    fn load_config_reads_archive_flags() {
        let path = temp_config(
            r#"
ARCHIVE_RECENTCLIPS=true
ARCHIVE_SAVEDCLIPS=false
ARCHIVE_SENTRYCLIPS=true
ARCHIVE_TRACKMODECLIPS=false
ARCHIVE_PHOTOBOOTH=false
"#,
        );

        let config = load_config(Some(&path)).unwrap();
        let _ = fs::remove_file(&path);

        assert!(config.archive.archive_recent);
        assert!(!config.archive.archive_saved);
        assert!(config.archive.archive_sentry);
        assert!(!config.archive.archive_track);
        assert!(!config.archive.archive_photobooth);
    }

    #[test]
    fn malformed_configuration_and_invalid_timeouts_fail() {
        for content in [
            "ARCHIVE_SYSTEM rclone",
            "=value",
            "TESLAUSB_IDLE_TIMEOUT_SECS=wrong",
            "TESLAUSB_IDLE_TIMEOUT_SECS=0",
            "ARCHIVE_RECENTCLIPS=maybe",
        ] {
            let path = temp_config(content);
            assert!(load_config(Some(&path)).is_err(), "{content}");
            fs::remove_file(path).unwrap();
        }
    }

    #[test]
    fn load_config_rejects_missing_file() {
        let path = std::env::temp_dir().join("teslausb-definitely-missing.conf");
        let _ = fs::remove_file(&path);
        assert!(load_config(Some(&path)).is_err());
    }

    #[test]
    fn environment_values_are_used_when_file_values_are_absent() {
        let _guard = ENV_LOCK.lock().unwrap();
        let keys = [
            "MUTABLE_PATH",
            "BACKINGFILES_PATH",
            "ARCHIVE_SYSTEM",
            "RCLONE_DRIVE",
            "RCLONE_PATH",
            "RCLONE_FLAGS",
            "ARCHIVE_RECENTCLIPS",
            "ARCHIVE_SAVEDCLIPS",
        ];
        for key in keys {
            std::env::remove_var(key);
        }

        std::env::set_var("MUTABLE_PATH", "/env/mutable");
        std::env::set_var("BACKINGFILES_PATH", "/env/backingfiles");
        std::env::set_var("ARCHIVE_SYSTEM", "RCLONE");
        std::env::set_var("RCLONE_DRIVE", "remote:");
        std::env::set_var("RCLONE_PATH", "Tesla");
        std::env::set_var("RCLONE_FLAGS", "--fast-list --transfers 2");
        std::env::set_var("ARCHIVE_RECENTCLIPS", "true");
        std::env::set_var("ARCHIVE_SAVEDCLIPS", "false");

        let config = load_from_sources(&std::collections::HashMap::new()).unwrap();

        assert_eq!(config.mutable_path, PathBuf::from("/env/mutable"));
        assert_eq!(config.backingfiles_path, PathBuf::from("/env/backingfiles"));
        assert_eq!(config.archive.system, "rclone");
        assert_eq!(config.archive.rclone_drive, "remote:");
        assert_eq!(config.archive.rclone_path, "Tesla");
        assert_eq!(
            config.archive.rclone_flags,
            ["--fast-list", "--transfers", "2"]
        );
        assert!(config.archive.archive_recent);
        assert!(!config.archive.archive_saved);

        for key in keys {
            std::env::remove_var(key);
        }
    }
}
