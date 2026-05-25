use std::path::Path;
use std::time::Duration;

use crate::command::CommandRunner;
use crate::config::GB;
use crate::error::{Error, Result};

pub const SECTOR_SIZE: u64 = 512;
pub const XFS_OVERHEAD_PROPORTION: f64 = 0.03;
pub const MIN_CAM_SIZE: u64 = GB;
pub const DEFAULT_RESERVE: u64 = 10 * GB;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpaceInfo {
    pub total_bytes: u64,
    pub free_bytes: u64,
    pub used_bytes: u64,
}

impl SpaceInfo {
    pub fn total_gib(&self) -> f64 {
        self.total_bytes as f64 / GB as f64
    }

    pub fn free_gib(&self) -> f64 {
        self.free_bytes as f64 / GB as f64
    }

    pub fn used_gib(&self) -> f64 {
        self.used_bytes as f64 / GB as f64
    }
}

pub fn calculate_cam_size(backingfiles_size: u64) -> u64 {
    let overhead = (backingfiles_size as f64 * XFS_OVERHEAD_PROPORTION) as u64;
    let usable = backingfiles_size.saturating_sub(overhead);
    let cam_size = usable / 2;
    (cam_size / SECTOR_SIZE) * SECTOR_SIZE
}

pub fn disk_space(path: &Path) -> Result<SpaceInfo> {
    let runner = CommandRunner;
    let _ = runner.run(
        "sync",
        std::iter::empty::<&str>(),
        Some(Duration::from_secs(30)),
    );
    let output = runner.check(
        "df",
        ["-Pk", &path.display().to_string()],
        Some(Duration::from_secs(30)),
    )?;
    parse_df_pk(&output.stdout)
}

fn parse_df_pk(stdout: &str) -> Result<SpaceInfo> {
    let line = stdout
        .lines()
        .rfind(|line| !line.trim().is_empty())
        .ok_or_else(|| Error::new("df produced no output"))?;
    let fields: Vec<&str> = line.split_whitespace().collect();
    if fields.len() < 4 {
        return Err(Error::new(format!("unexpected df output: {}", line)));
    }

    let total_kib: u64 = fields[1].parse()?;
    let used_kib: u64 = fields[2].parse()?;
    let free_kib: u64 = fields[3].parse()?;

    Ok(SpaceInfo {
        total_bytes: total_kib * 1024,
        free_bytes: free_kib * 1024,
        used_bytes: used_kib * 1024,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cam_size_uses_half_minus_overhead_and_aligns() {
        let backing = 100 * GB;
        assert_eq!(calculate_cam_size(backing), 52_076_478_464);
        assert_eq!(calculate_cam_size(backing) % SECTOR_SIZE, 0);
        assert!(calculate_cam_size(backing) < backing / 2);
        assert!(calculate_cam_size(backing) > backing * 48 / 100);
    }

    #[test]
    fn cam_size_scales_and_handles_small_values() {
        assert_eq!(calculate_cam_size(0), 0);
        assert_eq!(calculate_cam_size(500), 0);
        assert_eq!(
            calculate_cam_size(100 * GB),
            calculate_cam_size(50 * GB) * 2
        );
    }

    #[test]
    fn cam_size_alignment_rounds_down_never_up() {
        for backing in [
            50 * GB,
            100 * GB,
            118 * GB,
            127 * GB,
            200 * GB,
            100 * GB + 1,
            100 * GB + 255,
            100 * GB + 511,
            100 * GB + 513,
        ] {
            let overhead = (backing as f64 * XFS_OVERHEAD_PROPORTION) as u64;
            let max_unaligned = backing.saturating_sub(overhead) / 2;
            let actual = calculate_cam_size(backing);
            assert_eq!(actual % SECTOR_SIZE, 0);
            assert!(actual <= max_unaligned);
            assert!(actual + SECTOR_SIZE > max_unaligned);
        }
    }

    #[test]
    fn constants_have_expected_values() {
        assert_eq!(SECTOR_SIZE, 512);
        assert_eq!(XFS_OVERHEAD_PROPORTION, 0.03);
        assert_eq!(MIN_CAM_SIZE, GB);
        assert_eq!(DEFAULT_RESERVE, 10 * GB);
    }

    #[test]
    fn space_info_converts_bytes_to_gib() {
        let info = SpaceInfo {
            total_bytes: 100 * GB,
            free_bytes: 50 * GB,
            used_bytes: 50 * GB,
        };
        assert_eq!(info.total_gib(), 100.0);
        assert_eq!(info.free_gib(), 50.0);
        assert_eq!(info.used_gib(), 50.0);
    }

    #[test]
    fn parse_df_output() {
        let output = "Filesystem 1024-blocks Used Available Capacity Mounted on\n/dev/x 1000 250 750 25% /x\n";
        let info = parse_df_pk(output).unwrap();
        assert_eq!(info.total_bytes, 1_024_000);
        assert_eq!(info.used_bytes, 256_000);
        assert_eq!(info.free_bytes, 768_000);
    }

    #[test]
    fn parse_df_rejects_bad_output() {
        assert!(parse_df_pk("").is_err());
        assert!(parse_df_pk("bad\n").is_err());
        assert!(parse_df_pk("Filesystem 1024-blocks Used Available\n/dev/x nope 2 3\n").is_err());
    }
}
