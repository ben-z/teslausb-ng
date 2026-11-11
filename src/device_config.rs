use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::io::Write;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::{load_from_file_values, parse_config_content, validate_rclone_environment};
use crate::error::{Error, Result};

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);
const PRIVATE_KEYS: [&str; 4] = [
    "ARCHIVE_PAUSED_SSIDS",
    "RCLONE_DRIVE",
    "RCLONE_PATH",
    "RCLONE_FLAGS",
];

pub fn install(base: &Path, private: &Path, output: &Path) -> Result<()> {
    validate_rclone_environment()?;
    let base_text = fs::read_to_string(base)
        .map_err(|error| Error::from(error).context("failed to read shared configuration"))?;
    let private_text = read_private_text(private)?;
    let mut values = parse_config_content(&base_text, Path::new("shared configuration"))?;
    let private_values = parse_config_content(&private_text, Path::new("private configuration"))?;
    for key in PRIVATE_KEYS {
        if values.contains_key(key) {
            return Err(Error::new(format!(
                "{key} belongs in the private configuration, not the shared configuration"
            )));
        }
    }
    for key in private_values.keys() {
        if values.contains_key(key) {
            return Err(Error::new(format!(
                "configuration key {key} occurs in both shared and private configuration"
            )));
        }
    }
    values.extend(private_values);
    let config = load_from_file_values(&values)?;
    let warnings = config.warnings();
    if !warnings.is_empty() {
        return Err(Error::new(warnings.join("; ")));
    }

    match fs::symlink_metadata(output) {
        Ok(destination) => {
            for source in [base, private] {
                let source = fs::metadata(source)?;
                if destination.dev() == source.dev() && destination.ino() == source.ino() {
                    return Err(Error::new(
                        "combined output must not replace a shared or private input file",
                    ));
                }
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(Error::from(error).context("failed to inspect configuration output"))
        }
    }

    let mut merged = base_text;
    if !merged.ends_with('\n') {
        merged.push('\n');
    }
    merged.push_str(&private_text);
    write_private_atomic(output, &merged)
}

pub(crate) fn read_private_text(path: &Path) -> Result<String> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| Error::from(error).context("failed to inspect private configuration"))?;
    require_private_input(&metadata)?;
    let mut file = File::open(path)
        .map_err(|error| Error::from(error).context("failed to open private configuration"))?;
    let opened = file.metadata()?;
    require_private_input(&opened)?;
    if metadata.dev() != opened.dev() || metadata.ino() != opened.ino() {
        return Err(Error::new(
            "private configuration changed while being opened",
        ));
    }
    let mut text = String::new();
    file.read_to_string(&mut text)
        .map_err(|error| Error::from(error).context("failed to read private configuration"))?;
    Ok(text)
}

fn effective_uid() -> u32 {
    unsafe extern "C" {
        fn geteuid() -> u32;
    }
    unsafe { geteuid() }
}

fn require_private_input(metadata: &fs::Metadata) -> Result<()> {
    if !metadata.file_type().is_file() {
        return Err(Error::new(
            "private configuration must be a regular file, not a symbolic link or directory",
        ));
    }
    if metadata.uid() != effective_uid() {
        return Err(Error::new(
            "private configuration must be owned by the effective user",
        ));
    }
    if metadata.mode() & 0o077 != 0 {
        return Err(Error::new(
            "private configuration must have no group or other permissions; use chmod 600",
        ));
    }
    Ok(())
}

fn check_destination(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.file_type().is_file() {
                return Err(Error::new(
                    "private output must be a regular file, not a symbolic link or directory",
                ));
            }
            if metadata.uid() != effective_uid() {
                return Err(Error::new(
                    "private output must be owned by the effective user",
                ));
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Error::from(error).context("failed to inspect private output")),
    }
}

pub(crate) fn write_private_atomic(path: &Path, content: &str) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| Error::new("private output must have an explicit parent directory"))?;
    let directory = File::open(parent)
        .map_err(|error| Error::from(error).context("failed to open output parent directory"))?;
    if !directory.metadata()?.is_dir() {
        return Err(Error::new("private output parent must be a directory"));
    }
    check_destination(path)?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            Error::new(format!(
                "failed to create private temporary file timestamp: {error}"
            ))
        })?
        .as_nanos();
    let temp_path = parent.join(format!(
        ".teslausb-private-{}-{}-{nonce}",
        std::process::id(),
        NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
    ));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temp_path)
        .map_err(|error| Error::from(error).context("failed to create private temporary file"))?;
    let mut temporary = TemporaryFile {
        path: temp_path,
        remove_on_drop: true,
    };
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    file.write_all(content.as_bytes())?;
    file.sync_all()?;
    check_destination(path)?;
    fs::rename(&temporary.path, path)?;
    temporary.remove_on_drop = false;
    directory.sync_all()?;
    Ok(())
}

struct TemporaryFile {
    path: PathBuf,
    remove_on_drop: bool,
}

impl Drop for TemporaryFile {
    fn drop(&mut self) {
        if self.remove_on_drop {
            if let Err(error) = fs::remove_file(&self.path) {
                eprintln!("error: failed to remove private temporary file: {error}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::symlink;

    use super::*;

    struct Fixture {
        root: PathBuf,
        base: PathBuf,
        private: PathBuf,
        output: PathBuf,
    }

    impl Fixture {
        fn new(base: &str, private: &str) -> Self {
            let root = std::env::temp_dir().join(format!(
                "teslausb-device-config-test-{}-{}",
                std::process::id(),
                NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&root).unwrap();
            let fixture = Self {
                base: root.join("shared.conf"),
                private: root.join("private.conf"),
                output: root.join("installed.conf"),
                root,
            };
            fs::write(&fixture.base, base).unwrap();
            fs::write(&fixture.private, private).unwrap();
            fs::set_permissions(&fixture.private, fs::Permissions::from_mode(0o600)).unwrap();
            fixture
        }

        fn install(&self) -> Result<()> {
            install(&self.base, &self.private, &self.output)
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.root).unwrap();
        }
    }

    #[test]
    fn combined_output_cannot_replace_input_files_or_their_aliases() {
        let fixture = Fixture::new("ARCHIVE_SYSTEM=none\n", "ARCHIVE_PAUSED_SSIDS=[]\n");
        let base = fs::read(&fixture.base).unwrap();
        let private = fs::read(&fixture.private).unwrap();
        let nested = fixture.root.join("nested");
        fs::create_dir(&nested).unwrap();
        let hard_link = fixture.root.join("alias.conf");
        fs::hard_link(&fixture.base, &hard_link).unwrap();
        for output in [
            &fixture.base,
            &hard_link,
            &fixture.private,
            &nested.join("../shared.conf"),
        ] {
            assert!(install(&fixture.base, &fixture.private, output)
                .unwrap_err()
                .to_string()
                .contains("must not replace"));
            assert_eq!(fs::read(&fixture.base).unwrap(), base);
            assert_eq!(fs::read(&fixture.private).unwrap(), private);
        }
    }

    #[test]
    fn merges_disjoint_settings_and_preserves_comments_and_shell_syntax() {
        let base = "# Common appliance policy\nexport ARCHIVE_SYSTEM='rclone'\nARCHIVE_RECENTCLIPS=\"true\"";
        let private = "# Fictional device details\nRCLONE_DRIVE='example-remote'\nRCLONE_PATH=\"Example Archive\"\nARCHIVE_PAUSED_SSIDS='[\"Example Mobile\"]'\n";
        let fixture = Fixture::new(base, private);
        fixture.install().unwrap();
        assert_eq!(
            fs::read_to_string(&fixture.output).unwrap(),
            format!("{base}\n{private}")
        );
        let content = fs::read_to_string(&fixture.output).unwrap();
        let values = parse_config_content(&content, &fixture.output).unwrap();
        let config = load_from_file_values(&values).unwrap();
        assert!(config.archive.archive_recent);
        assert_eq!(config.archive.rclone_drive, "example-remote");
        assert_eq!(config.archive.rclone_path, "Example Archive");
        assert_eq!(config.archive.network.paused_ssids, ["Example Mobile"]);
        assert_eq!(fs::metadata(&fixture.output).unwrap().mode() & 0o777, 0o600);
    }

    #[test]
    fn rejects_private_fields_in_shared_configuration_without_echoing_values() {
        for key in PRIVATE_KEYS {
            let fixture = Fixture::new(&format!("{key}=fictional-sensitive-value\n"), "");
            fs::write(&fixture.output, "previous output").unwrap();
            let error = fixture.install().unwrap_err().to_string();
            assert!(error.contains(key));
            assert!(!error.contains("fictional-sensitive-value"));
            assert_eq!(
                fs::read_to_string(&fixture.output).unwrap(),
                "previous output"
            );
        }
    }

    #[test]
    fn refuses_overlapping_keys_without_revealing_either_value() {
        let fixture = Fixture::new("EXAMPLE_KEY=fictional-one\n", "EXAMPLE_KEY=fictional-two\n");
        fs::write(&fixture.output, "previous output").unwrap();
        let error = fixture.install().unwrap_err().to_string();
        assert!(error.contains("EXAMPLE_KEY"));
        assert!(!error.contains("fictional-one"));
        assert!(!error.contains("fictional-two"));
        assert_eq!(
            fs::read_to_string(&fixture.output).unwrap(),
            "previous output"
        );
    }

    #[test]
    fn missing_or_invalid_sources_leave_installed_output_unchanged() {
        for private in [
            "ARCHIVE_RECENTCLIPS=fictional-sensitive-value\n",
            "ARCHIVE_PAUSED_SSIDS='[\"\"]'\n",
            "ARCHIVE_SYSTEM=fictional-sensitive-value\n",
            "ARCHIVE_SYSTEM=rclone\nRCLONE_DRIVE=\n",
            "RCLONE_FLAGS=--checksum=fictional-sensitive-value\n",
            "not a configuration line\n",
            "WIFI_DEFAULT_PRIORITY=fictional-sensitive-value\n",
            "WIFI_DEFAULT_PRIORITY=0\nWIFI_PAUSED_PRIORITY=1\n",
        ] {
            let fixture = Fixture::new("# Common policy\n", private);
            fs::write(&fixture.output, "previous output").unwrap();
            let error = fixture.install().unwrap_err().to_string();
            assert!(!error.contains("fictional-sensitive-value"), "{error}");
            assert_eq!(
                fs::read_to_string(&fixture.output).unwrap(),
                "previous output"
            );
        }
        let fixture = Fixture::new("ARCHIVE_SYSTEM=none\n", "");
        fs::write(&fixture.output, "previous output").unwrap();
        fs::remove_file(&fixture.base).unwrap();
        assert!(fixture.install().is_err());
        assert_eq!(
            fs::read_to_string(&fixture.output).unwrap(),
            "previous output"
        );
        fs::write(&fixture.base, "ARCHIVE_SYSTEM=none\n").unwrap();
        fs::remove_file(&fixture.private).unwrap();
        assert!(fixture.install().is_err());
        assert_eq!(
            fs::read_to_string(&fixture.output).unwrap(),
            "previous output"
        );
    }

    #[test]
    fn private_input_requires_restricted_permissions_and_a_regular_file() {
        for mode in [0o644, 0o640, 0o601] {
            let fixture = Fixture::new("ARCHIVE_SYSTEM=none\n", "");
            fs::set_permissions(&fixture.private, fs::Permissions::from_mode(mode)).unwrap();
            let error = fixture.install().unwrap_err().to_string();
            assert!(error.contains("chmod 600"));
            assert!(!fixture.output.exists());
        }
        let fixture = Fixture::new("ARCHIVE_SYSTEM=none\n", "");
        fs::remove_file(&fixture.private).unwrap();
        fs::create_dir(&fixture.private).unwrap();
        assert!(fixture
            .install()
            .unwrap_err()
            .to_string()
            .contains("regular file"));
    }

    #[test]
    fn rejects_input_and_output_symlinks_without_touching_targets() {
        let fixture = Fixture::new("ARCHIVE_SYSTEM=none\n", "");
        let target = fixture.root.join("target.conf");
        fs::rename(&fixture.private, &target).unwrap();
        symlink(&target, &fixture.private).unwrap();
        assert!(fixture
            .install()
            .unwrap_err()
            .to_string()
            .contains("regular file"));
        assert_eq!(fs::read_to_string(&target).unwrap(), "");
        fs::remove_file(&fixture.private).unwrap();
        fs::rename(&target, &fixture.private).unwrap();
        fs::write(&target, "previous target").unwrap();
        symlink(&target, &fixture.output).unwrap();
        assert!(fixture
            .install()
            .unwrap_err()
            .to_string()
            .contains("regular file"));
        assert!(fs::symlink_metadata(&fixture.output)
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(fs::read_to_string(&target).unwrap(), "previous target");
    }

    #[test]
    fn temporary_file_guard_removes_uncommitted_output() {
        let fixture = Fixture::new("", "");
        let path = fixture.root.join("uncommitted");
        fs::write(&path, "fictional private content").unwrap();
        {
            let _temporary = TemporaryFile {
                path: path.clone(),
                remove_on_drop: true,
            };
        }
        assert!(!path.exists());
    }

    #[test]
    fn atomic_output_replaces_content_with_mode_600_and_leaves_no_temporary_files() {
        let fixture = Fixture::new("", "");
        fs::write(&fixture.output, "previous output").unwrap();
        fs::set_permissions(&fixture.output, fs::Permissions::from_mode(0o644)).unwrap();
        write_private_atomic(&fixture.output, "replacement output").unwrap();
        assert_eq!(
            fs::read_to_string(&fixture.output).unwrap(),
            "replacement output"
        );
        assert_eq!(fs::metadata(&fixture.output).unwrap().mode() & 0o777, 0o600);
        assert!(fs::read_dir(&fixture.root).unwrap().all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".teslausb-private-")));
        assert!(
            write_private_atomic(&fixture.root.join("missing/output.conf"), "new output").is_err()
        );
        assert_eq!(
            fs::read_to_string(&fixture.output).unwrap(),
            "replacement output"
        );
        assert!(write_private_atomic(&fixture.root, "new output")
            .unwrap_err()
            .to_string()
            .contains("regular file"));
    }
}
