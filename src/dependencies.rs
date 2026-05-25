use std::fmt;
use std::time::Duration;

use crate::command::CommandRunner;
use crate::config::Config;
use crate::error::{Error, Result};

const CHECK_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DependencySet {
    Init,
    Mount,
    Runtime,
    Gadget,
    Service,
    Startup,
    Full,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version {
    major: u64,
    minor: u64,
    patch: u64,
}

impl Version {
    pub const fn new(major: u64, minor: u64, patch: u64) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DependencyReport {
    pub name: &'static str,
    pub command: &'static str,
    pub ok: bool,
    pub version: Option<Version>,
    pub min_version: Option<Version>,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FeatureProbe {
    None,
    CpReflink,
}

#[derive(Debug, Clone, Copy)]
struct DependencySpec {
    name: &'static str,
    command: &'static str,
    version_args: &'static [&'static str],
    min_version: Option<Version>,
    feature_probe: FeatureProbe,
}

pub fn check_dependencies(config: &Config, set: DependencySet) -> Vec<DependencyReport> {
    specs_for(config, set)
        .into_iter()
        .map(check_dependency)
        .collect()
}

pub fn ensure_dependencies(config: &Config, set: DependencySet) -> Result<()> {
    let reports = check_dependencies(config, set);
    let failures: Vec<_> = reports.iter().filter(|report| !report.ok).collect();
    if failures.is_empty() {
        return Ok(());
    }

    let mut message = String::from("dependency check failed:");
    for report in failures {
        message.push_str(&format!(
            "\n  - {}: {}",
            report.name,
            dependency_detail(report)
        ));
    }
    message.push_str("\nRun 'teslausb doctor' for the full dependency report.");
    Err(Error::new(message))
}

pub fn dependency_detail(report: &DependencyReport) -> String {
    let mut detail = report.detail.clone();
    if let Some(version) = report.version {
        detail.push_str(&format!(" (version {version})"));
    }
    if let Some(min_version) = report.min_version {
        detail.push_str(&format!("; requires >= {min_version}"));
    }
    detail
}

fn specs_for(config: &Config, set: DependencySet) -> Vec<DependencySpec> {
    let mut specs = Vec::new();
    match set {
        DependencySet::Init => add_init_specs(&mut specs),
        DependencySet::Mount => add_mount_specs(&mut specs),
        DependencySet::Runtime => add_runtime_specs(&mut specs, config),
        DependencySet::Gadget => add_gadget_specs(&mut specs),
        DependencySet::Service => add_service_specs(&mut specs),
        DependencySet::Startup => {
            add_init_specs(&mut specs);
            add_runtime_specs(&mut specs, config);
            add_gadget_specs(&mut specs);
        }
        DependencySet::Full => {
            add_init_specs(&mut specs);
            add_runtime_specs(&mut specs, config);
            add_gadget_specs(&mut specs);
            add_service_specs(&mut specs);
        }
    }
    specs
}

fn add_init_specs(specs: &mut Vec<DependencySpec>) {
    add(specs, spec("df", "df", &["--version"], None));
    add(specs, spec("truncate", "truncate", &["--version"], None));
    add(
        specs,
        spec("mkfs.xfs", "mkfs.xfs", &["-V"], Some(Version::new(4, 9, 0))),
    );
    add(specs, spec("parted", "parted", &["--version"], None));
    add(specs, spec("losetup", "losetup", &["--version"], None));
    add(specs, spec("blockdev", "blockdev", &["--version"], None));
    add(specs, spec("kpartx", "kpartx", &["-V"], None));
    add(specs, spec("mkfs.vfat", "mkfs.vfat", &["--version"], None));
    add_mount_specs(specs);
    add_reflink_cp(specs);
    add(specs, spec("sync", "sync", &["--version"], None));
}

fn add_mount_specs(specs: &mut Vec<DependencySpec>) {
    add(specs, spec("mount", "mount", &["--version"], None));
    add(
        specs,
        spec("mountpoint", "mountpoint", &["--version"], None),
    );
    add(specs, spec("umount", "umount", &["--version"], None));
    add(specs, spec("stat", "stat", &["--version"], None));
}

fn add_runtime_specs(specs: &mut Vec<DependencySpec>, config: &Config) {
    add_mount_specs(specs);
    add(specs, spec("df", "df", &["--version"], None));
    add(specs, spec("sync", "sync", &["--version"], None));
    add(specs, spec("fsck", "fsck", &["--version"], None));
    add(specs, spec("losetup", "losetup", &["--version"], None));
    add(specs, spec("blockdev", "blockdev", &["--version"], None));
    add(specs, spec("kpartx", "kpartx", &["-V"], None));
    add_reflink_cp(specs);
    if config.archive.system == "rclone" {
        add(
            specs,
            spec(
                "rclone",
                "rclone",
                &["version"],
                Some(Version::new(1, 50, 0)),
            ),
        );
    }
}

fn add_gadget_specs(specs: &mut Vec<DependencySpec>) {
    add(specs, spec("modprobe", "modprobe", &["--version"], None));
}

fn add_service_specs(specs: &mut Vec<DependencySpec>) {
    add(specs, spec("systemctl", "systemctl", &["--version"], None));
}

fn add_reflink_cp(specs: &mut Vec<DependencySpec>) {
    add(
        specs,
        DependencySpec {
            name: "cp",
            command: "cp",
            version_args: &["--version"],
            min_version: Some(Version::new(8, 23, 0)),
            feature_probe: FeatureProbe::CpReflink,
        },
    );
}

fn spec(
    name: &'static str,
    command: &'static str,
    version_args: &'static [&'static str],
    min_version: Option<Version>,
) -> DependencySpec {
    DependencySpec {
        name,
        command,
        version_args,
        min_version,
        feature_probe: FeatureProbe::None,
    }
}

fn add(specs: &mut Vec<DependencySpec>, spec: DependencySpec) {
    if !specs.iter().any(|existing| existing.name == spec.name) {
        specs.push(spec);
    }
}

fn check_dependency(spec: DependencySpec) -> DependencyReport {
    let runner = CommandRunner;
    let output = match runner.run(spec.command, spec.version_args, Some(CHECK_TIMEOUT)) {
        Ok(output) => output,
        Err(err) => {
            return DependencyReport {
                name: spec.name,
                command: spec.command,
                ok: false,
                version: None,
                min_version: spec.min_version,
                detail: format!("missing or not executable: {err}"),
            };
        }
    };

    let combined_output = format!("{}\n{}", output.stdout, output.stderr);
    let version = first_version(&combined_output);
    if !output.success() && version.is_none() {
        return DependencyReport {
            name: spec.name,
            command: spec.command,
            ok: false,
            version,
            min_version: spec.min_version,
            detail: format!("version check failed: {}", output.last_error_line()),
        };
    }

    if let (Some(found), Some(required)) = (version, spec.min_version) {
        if found < required {
            return DependencyReport {
                name: spec.name,
                command: spec.command,
                ok: false,
                version,
                min_version: spec.min_version,
                detail: "version is too old".to_string(),
            };
        }
    }

    if let Err(err) = run_feature_probe(spec.feature_probe) {
        return DependencyReport {
            name: spec.name,
            command: spec.command,
            ok: false,
            version,
            min_version: spec.min_version,
            detail: err.to_string(),
        };
    }

    DependencyReport {
        name: spec.name,
        command: spec.command,
        ok: true,
        version,
        min_version: spec.min_version,
        detail: match spec.feature_probe {
            FeatureProbe::None if output.success() => "ok".to_string(),
            FeatureProbe::None => "ok; version reported with nonzero status".to_string(),
            FeatureProbe::CpReflink => "ok; supports --reflink".to_string(),
        },
    }
}

fn run_feature_probe(feature: FeatureProbe) -> Result<()> {
    match feature {
        FeatureProbe::None => Ok(()),
        FeatureProbe::CpReflink => probe_cp_reflink(),
    }
}

fn probe_cp_reflink() -> Result<()> {
    let output = CommandRunner
        .run("cp", ["--help"], Some(CHECK_TIMEOUT))
        .map_err(|err| Error::new(format!("cp reflink probe failed: {err}")))?;
    let help = format!("{}\n{}", output.stdout, output.stderr);
    if output.success() && help.contains("--reflink") {
        Ok(())
    } else {
        Err(Error::new("cp does not advertise --reflink support"))
    }
}

fn first_version(text: &str) -> Option<Version> {
    for (start, ch) in text.char_indices() {
        if !ch.is_ascii_digit() {
            continue;
        }

        let candidate: String = text[start..]
            .chars()
            .take_while(|c| c.is_ascii_digit() || *c == '.')
            .collect();
        if let Some(version) = parse_version_candidate(&candidate) {
            return Some(version);
        }
    }
    None
}

fn parse_version_candidate(candidate: &str) -> Option<Version> {
    let mut parts = candidate
        .split('.')
        .filter(|part| !part.is_empty())
        .map(str::parse::<u64>);
    let major = parts.next()?.ok()?;
    let minor = parts.next().transpose().ok()?.unwrap_or(0);
    let patch = parts.next().transpose().ok()?.unwrap_or(0);
    Some(Version::new(major, minor, patch))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_first_version_from_common_outputs() {
        assert_eq!(
            first_version("rclone v1.65.2\n- os/version: debian 12"),
            Some(Version::new(1, 65, 2))
        );
        assert_eq!(
            first_version("mkfs.xfs version 6.1.0"),
            Some(Version::new(6, 1, 0))
        );
        assert_eq!(
            first_version("systemd 252 (252.22-1)"),
            Some(Version::new(252, 0, 0))
        );
        assert_eq!(first_version("no version here"), None);
    }

    #[test]
    fn compares_versions_numerically() {
        assert!(Version::new(1, 50, 0) >= Version::new(1, 9, 9));
        assert!(Version::new(4, 8, 9) < Version::new(4, 9, 0));
    }

    #[test]
    fn dependency_detail_can_report_nonzero_version_status() {
        let report = DependencyReport {
            name: "mkfs.vfat",
            command: "mkfs.vfat",
            ok: true,
            version: Some(Version::new(4, 2, 0)),
            min_version: None,
            detail: "ok; version reported with nonzero status".to_string(),
        };

        assert!(dependency_detail(&report).contains("nonzero status"));
        assert!(dependency_detail(&report).contains("version 4.2.0"));
    }
}
