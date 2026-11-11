#![cfg(unix)]

use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    root: PathBuf,
    base: PathBuf,
    private: PathBuf,
    output: PathBuf,
    supplicant: PathBuf,
    tools: PathBuf,
    tool_log: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "teslausb-device-config-cli-{}-{}",
            std::process::id(),
            NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        let fixture = Self {
            base: root.join("shared.conf"),
            private: root.join("private.conf"),
            output: root.join("installed.conf"),
            supplicant: root.join("netplan-example.conf"),
            tools: root.join("bin"),
            tool_log: root.join("external-tools.log"),
            root,
        };
        fs::create_dir(&fixture.tools).unwrap();
        for tool in [
            "iw",
            "rclone",
            "nmcli",
            "wpa_cli",
            "systemctl",
            "netplan",
            "ip",
            "mount",
            "losetup",
            "sudo",
        ] {
            let path = fixture.tools.join(tool);
            fs::write(
                &path,
                format!("#!/bin/sh\nprintf '{tool}\\n' >> \"$TESLAUSB_TEST_TOOL_LOG\"\nexit 99\n"),
            )
            .unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
        }
        fs::write(&fixture.base, "ARCHIVE_SYSTEM=none\n").unwrap();
        write_private(&fixture.private, "");
        fixture
    }

    fn run(&self, args: &[&str]) -> Output {
        self.run_with_env(args, &[])
    }

    fn run_with_env(&self, args: &[&str], values: &[(&str, &str)]) -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_teslausb"));
        command
            .args(args)
            .env_clear()
            .env("PATH", &self.tools)
            .env("TESLAUSB_TEST_TOOL_LOG", &self.tool_log);
        if let Some(profile) = std::env::var_os("LLVM_PROFILE_FILE") {
            command.env("LLVM_PROFILE_FILE", profile);
        }
        for (key, value) in values {
            command.env(key, value);
        }
        let output = command.output().unwrap();
        assert!(
            !self.tool_log.exists(),
            "CLI invoked an external tool: {}",
            fs::read_to_string(&self.tool_log).unwrap()
        );
        output
    }

    fn install(&self) -> Output {
        self.run(&[
            "config",
            "install",
            "--base",
            path(&self.base),
            "--private",
            path(&self.private),
            "--output",
            path(&self.output),
        ])
    }

    fn apply_priorities(&self) -> Output {
        self.run(&[
            "--config",
            path(&self.private),
            "wifi-priorities",
            "--supplicant-config",
            path(&self.supplicant),
        ])
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}

fn path(path: &Path) -> &str {
    path.to_str().unwrap()
}

fn write_private(path: &Path, content: &str) {
    fs::write(path, content).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}

fn describe(output: &Output) -> String {
    format!(
        "status: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn assert_success(output: &Output) {
    assert!(output.status.success(), "{}", describe(output));
}

fn assert_private_error(output: &Output, private_values: &[&str]) {
    assert!(!output.status.success(), "{}", describe(output));
    let diagnostic = describe(output).to_lowercase();
    for value in private_values {
        assert!(
            !diagnostic.contains(&value.to_lowercase()),
            "private value appeared in CLI output"
        );
    }
}

#[test]
fn config_install_merges_sources_with_private_permissions_without_external_tools() {
    let fixture = Fixture::new();
    let base = "# Common policy\nexport ARCHIVE_SYSTEM='rclone'\nARCHIVE_RECENTCLIPS=true\n";
    let private = "# Fictional device settings\nRCLONE_DRIVE=example-remote\nRCLONE_PATH='Example Archive'\nARCHIVE_PAUSED_SSIDS='[\"Example Mobile\"]'\n";
    fs::write(&fixture.base, base).unwrap();
    write_private(&fixture.private, private);
    fs::write(&fixture.output, "previous output").unwrap();
    fs::set_permissions(&fixture.output, fs::Permissions::from_mode(0o644)).unwrap();
    let output = fixture.install();
    assert_success(&output);
    assert_eq!(
        fs::read_to_string(&fixture.output).unwrap(),
        format!("{base}{private}")
    );
    assert_eq!(fs::metadata(&fixture.output).unwrap().mode() & 0o777, 0o600);
    let diagnostic = describe(&output);
    for value in ["example-remote", "Example Archive", "Example Mobile"] {
        assert!(!diagnostic.contains(value));
    }
}

#[test]
fn config_install_rejects_public_identities_and_preserves_previous_output() {
    let fixture = Fixture::new();
    fs::write(
        &fixture.base,
        "ARCHIVE_SYSTEM=none\nARCHIVE_PAUSED_SSIDS='[\"Example Private Mobile\"]'\n",
    )
    .unwrap();
    fs::write(&fixture.output, "previous output").unwrap();
    let output = fixture.install();
    assert_private_error(&output, &["Example Private Mobile"]);
    assert!(String::from_utf8_lossy(&output.stderr).contains("ARCHIVE_PAUSED_SSIDS"));
    assert_eq!(
        fs::read_to_string(&fixture.output).unwrap(),
        "previous output"
    );
}

#[test]
fn config_install_rejects_missing_invalid_and_overlapping_private_sources() {
    let fixture = Fixture::new();
    fs::write(&fixture.output, "previous output").unwrap();
    for private in [
        "ARCHIVE_RECENTCLIPS=fictional-private-value\n",
        "ARCHIVE_SYSTEM=fictional-private-value\n",
        "ARCHIVE_PAUSED_SSIDS='[42, \"fictional-private-value\"]'\n",
    ] {
        write_private(&fixture.private, private);
        let output = fixture.install();
        assert_private_error(&output, &["fictional-private-value"]);
        assert_eq!(
            fs::read_to_string(&fixture.output).unwrap(),
            "previous output"
        );
    }
    write_private(&fixture.private, "");
    fs::remove_file(&fixture.private).unwrap();
    let output = fixture.install();
    assert!(!output.status.success());
    assert_eq!(
        fs::read_to_string(&fixture.output).unwrap(),
        "previous output"
    );
}

#[test]
fn config_install_does_not_use_the_callers_environment_for_missing_file_settings() {
    let fixture = Fixture::new();
    fs::write(&fixture.base, "ARCHIVE_SYSTEM=rclone\n").unwrap();
    write_private(&fixture.private, "RCLONE_PATH='Example Archive'\n");
    fs::write(&fixture.output, "previous output").unwrap();
    let output = fixture.run_with_env(
        &[
            "config",
            "install",
            "--base",
            path(&fixture.base),
            "--private",
            path(&fixture.private),
            "--output",
            path(&fixture.output),
        ],
        &[("RCLONE_DRIVE", "fictional-environment-remote")],
    );
    assert_private_error(
        &output,
        &["fictional-environment-remote", "Example Archive"],
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("RCLONE_DRIVE"));
    assert_eq!(
        fs::read_to_string(&fixture.output).unwrap(),
        "previous output"
    );
}

#[test]
fn wifi_priorities_preserve_authentication_and_are_idempotent() {
    let fixture = Fixture::new();
    write_private(
        &fixture.private,
        "ARCHIVE_SYSTEM=none\nARCHIVE_PAUSED_SSIDS='[\"Example Mobile\"]'\nWIFI_DEFAULT_PRIORITY=100\nWIFI_PAUSED_PRIORITY=0\n",
    );
    let original = r#"ctrl_interface=/run/wpa_supplicant
update_config=0

network={
    ssid="Example Trusted"
    psk="fictional-passphrase-}#{"
    key_mgmt=WPA-PSK
    priority=-50
}
network={
    ssid="Example Mobile"
    psk=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
    scan_ssid=1
    priority=999
}
network={
    ssid=P"Example Future"
    key_mgmt=WPA-EAP
    identity="fictional-identity"
    password="fictional-authentication-value"
}
"#;
    write_private(&fixture.supplicant, original);
    let output = fixture.apply_priorities();
    assert_success(&output);
    let updated = fs::read_to_string(&fixture.supplicant).unwrap();
    let expected = original
        .replace("    priority=-50\n", "    priority=100\n")
        .replace("    priority=999\n", "    priority=0\n");
    // The third profile has no earlier priority and uses the normal preference.
    let last_close = expected.rfind("}\n").unwrap();
    let expected = format!(
        "{}    priority=100\n{}",
        &expected[..last_close],
        &expected[last_close..]
    );
    assert_eq!(updated, expected);
    let without_priorities = |text: &str| {
        text.split_inclusive('\n')
            .filter(|line| !line.trim_start().starts_with("priority="))
            .collect::<String>()
    };
    assert_eq!(without_priorities(&updated), without_priorities(original));
    assert_eq!(
        fs::metadata(&fixture.supplicant).unwrap().mode() & 0o777,
        0o600
    );
    let inode = fs::metadata(&fixture.supplicant).unwrap().ino();
    assert_success(&fixture.apply_priorities());
    assert_eq!(fs::read_to_string(&fixture.supplicant).unwrap(), updated);
    assert_eq!(fs::metadata(&fixture.supplicant).unwrap().ino(), inode);
    let diagnostic = describe(&output);
    for value in [
        "Example Trusted",
        "Example Mobile",
        "fictional-identity",
        "fictional-authentication-value",
    ] {
        assert!(!diagnostic.contains(value));
    }
}

#[test]
fn wifi_priorities_reject_malformed_or_public_files_without_changing_them() {
    let fixture = Fixture::new();
    write_private(&fixture.private, "ARCHIVE_SYSTEM=none\n");
    for malformed in [
        "network={\nssid=fictional-private-network\npsk=\"fictional-authentication-value\"\n}\n",
        "network={\nssid=P\"fictional-private-network\"\npriority=fictional-private-value\n}\n",
        "network={\nssid=P\"fictional-private-network\"\npsk=\"fictional-authentication-value\"\n",
    ] {
        write_private(&fixture.supplicant, malformed);
        let output = fixture.apply_priorities();
        assert_private_error(
            &output,
            &[
                "fictional-private-network",
                "fictional-private-value",
                "fictional-authentication-value",
            ],
        );
        assert_eq!(fs::read_to_string(&fixture.supplicant).unwrap(), malformed);
    }
    let original =
        "network={\nssid=P\"Example Trusted\"\npsk=\"fictional-authentication-value\"\n}\n";
    write_private(&fixture.supplicant, original);
    fs::set_permissions(&fixture.supplicant, fs::Permissions::from_mode(0o644)).unwrap();
    let output = fixture.apply_priorities();
    assert_private_error(
        &output,
        &["Example Trusted", "fictional-authentication-value"],
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("chmod 600"));
    assert_eq!(fs::read_to_string(&fixture.supplicant).unwrap(), original);
}

#[test]
fn invalid_private_settings_are_redacted_before_any_cli_work() {
    let fixture = Fixture::new();
    let original =
        "network={\nssid=\"Example Trusted\"\npsk=\"fictional-authentication-value\"\n}\n";
    write_private(&fixture.supplicant, original);
    for (key, value) in [
        ("ARCHIVE_PAUSED_SSIDS", "\"PRIVATE-SENTINEL\""),
        ("ARCHIVE_RECENTCLIPS", "PRIVATE-SENTINEL"),
        ("RCLONE_FLAGS", "--checksum=PRIVATE-SENTINEL"),
        ("ARCHIVE_SYSTEM", "PRIVATE-SENTINEL"),
    ] {
        for in_file in [true, false] {
            let content = if in_file {
                format!("{key}='{value}'\n")
            } else {
                String::new()
            };
            write_private(&fixture.private, &content);
            let environment = if in_file { vec![] } else { vec![(key, value)] };
            for command in [
                "wifi-priorities",
                "init",
                "archive",
                "run",
                "status",
                "doctor",
            ] {
                let mut args = vec!["--config", path(&fixture.private), command];
                if command == "wifi-priorities" {
                    args.extend(["--supplicant-config", path(&fixture.supplicant)]);
                }
                let output = fixture.run_with_env(&args, &environment);
                assert_private_error(
                    &output,
                    &[
                        "PRIVATE-SENTINEL",
                        "Example Trusted",
                        "fictional-authentication-value",
                    ],
                );
                assert!(
                    String::from_utf8_lossy(&output.stderr).contains(key),
                    "{}",
                    describe(&output)
                );
                assert_eq!(fs::read_to_string(&fixture.supplicant).unwrap(), original);
            }
        }
    }
}

#[test]
fn missing_explicit_paths_fail_before_any_external_command() {
    let fixture = Fixture::new();
    for args in [
        vec!["config", "install"],
        vec!["config", "install", "--base"],
        vec!["config", "install", "--base", path(&fixture.base)],
        vec![
            "config",
            "install",
            "--base",
            path(&fixture.base),
            "--private",
            path(&fixture.private),
        ],
        vec!["--config", path(&fixture.base), "wifi-priorities"],
        vec![
            "--config",
            path(&fixture.base),
            "wifi-priorities",
            "--supplicant-config",
        ],
    ] {
        let output = fixture.run(&args);
        assert!(!output.status.success(), "{}", describe(&output));
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("requires"),
            "{}",
            describe(&output)
        );
        assert!(!fixture.output.exists());
        assert!(!fixture.supplicant.exists());
    }
}
