use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::command::CommandRunner;
use crate::config::ArchiveConfig;
use crate::error::{Error, Result};
use crate::filesystem::FileSystem;
use crate::mount::mount_image;
use crate::snapshot::{SnapshotHandle, SnapshotManager};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArchiveState {
    Pending,
    Connecting,
    Archiving,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchivedFile {
    pub relative_path: PathBuf,
    pub size: u64,
}

#[derive(Debug, Clone)]
pub struct ArchiveResult {
    pub snapshot_id: u64,
    pub state: ArchiveState,
    pub files_transferred: u64,
    pub bytes_transferred: u64,
    pub completed_secs: Option<u64>,
    pub error: Option<String>,
    pub archived_files: Vec<(String, Vec<ArchivedFile>)>,
}

impl ArchiveResult {
    pub fn new(snapshot_id: u64) -> Self {
        Self {
            snapshot_id,
            state: ArchiveState::Pending,
            files_transferred: 0,
            bytes_transferred: 0,
            completed_secs: None,
            error: None,
            archived_files: Vec::new(),
        }
    }

    pub fn success(&self) -> bool {
        self.state == ArchiveState::Completed
    }
}

#[derive(Debug, Clone)]
pub struct CopyResult {
    pub success: bool,
    pub files_transferred: u64,
    pub bytes_transferred: u64,
    pub error: Option<String>,
    pub archived_files: Vec<ArchivedFile>,
}

#[derive(Debug, Clone)]
pub enum ArchiveBackend<F: FileSystem> {
    None,
    Rclone(RcloneBackend<F>),
    #[cfg(test)]
    Mock(MockArchiveBackend),
}

impl<F: FileSystem> ArchiveBackend<F> {
    pub fn from_config(config: &ArchiveConfig, fs: F) -> Self {
        if config.system == "rclone" {
            Self::Rclone(RcloneBackend::new(
                config.rclone_drive.clone(),
                config.rclone_path.clone(),
                config.rclone_flags.clone(),
                fs,
            ))
        } else {
            Self::None
        }
    }

    pub fn is_reachable(&self) -> bool {
        match self {
            Self::None => true,
            Self::Rclone(backend) => backend.is_reachable(),
            #[cfg(test)]
            Self::Mock(backend) => backend.is_reachable(),
        }
    }

    pub fn copy_directory(&self, src: &Path, dst_name: &str) -> CopyResult {
        match self {
            Self::None => CopyResult {
                success: true,
                files_transferred: 0,
                bytes_transferred: 0,
                error: None,
                archived_files: Vec::new(),
            },
            Self::Rclone(backend) => backend.copy_directory(src, dst_name),
            #[cfg(test)]
            Self::Mock(backend) => backend.copy_directory(src, dst_name),
        }
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Default)]
pub struct MockArchiveBackend {
    reachable: bool,
    fail_dirs: std::collections::HashSet<String>,
    partial_fail_dirs: std::collections::HashSet<String>,
    copied_dirs: std::sync::Arc<std::sync::Mutex<Vec<(PathBuf, String)>>>,
}

#[cfg(test)]
impl MockArchiveBackend {
    fn reachable(reachable: bool) -> Self {
        Self {
            reachable,
            fail_dirs: std::collections::HashSet::new(),
            partial_fail_dirs: std::collections::HashSet::new(),
            copied_dirs: Default::default(),
        }
    }

    fn failing(fail_dirs: &[&str]) -> Self {
        Self {
            reachable: true,
            fail_dirs: fail_dirs.iter().map(|value| value.to_string()).collect(),
            partial_fail_dirs: std::collections::HashSet::new(),
            copied_dirs: Default::default(),
        }
    }

    fn partial_failing(fail_dirs: &[&str]) -> Self {
        Self {
            reachable: true,
            fail_dirs: std::collections::HashSet::new(),
            partial_fail_dirs: fail_dirs.iter().map(|value| value.to_string()).collect(),
            copied_dirs: Default::default(),
        }
    }

    fn copied_dirs(&self) -> Vec<(PathBuf, String)> {
        self.copied_dirs.lock().unwrap().clone()
    }

    fn is_reachable(&self) -> bool {
        self.reachable
    }

    fn copy_directory(&self, src: &Path, dst_name: &str) -> CopyResult {
        if self.fail_dirs.contains(dst_name) {
            return CopyResult {
                success: false,
                files_transferred: 0,
                bytes_transferred: 0,
                error: Some(format!("mock failure for {dst_name}")),
                archived_files: Vec::new(),
            };
        }
        if self.partial_fail_dirs.contains(dst_name) {
            return CopyResult {
                success: false,
                files_transferred: 1,
                bytes_transferred: 1000,
                error: Some(format!("mock timeout for {dst_name}")),
                archived_files: vec![ArchivedFile {
                    relative_path: PathBuf::from("event/front.mp4"),
                    size: 1000,
                }],
            };
        }

        self.copied_dirs
            .lock()
            .unwrap()
            .push((src.to_path_buf(), dst_name.to_string()));

        CopyResult {
            success: true,
            files_transferred: 10,
            bytes_transferred: 1000,
            error: None,
            archived_files: vec![ArchivedFile {
                relative_path: PathBuf::from("event/front.mp4"),
                size: 1000,
            }],
        }
    }
}

#[derive(Debug, Clone)]
pub struct RcloneBackend<F: FileSystem> {
    remote: String,
    path: String,
    flags: Vec<String>,
    timeout: Duration,
    fs: F,
}

impl<F: FileSystem> RcloneBackend<F> {
    pub fn new(remote: String, path: String, flags: Vec<String>, fs: F) -> Self {
        Self {
            remote,
            path: path.trim_matches('/').to_string(),
            flags,
            timeout: Duration::from_secs(3600),
            fs,
        }
    }

    fn remote_with_colon(&self) -> String {
        if self.remote.ends_with(':') {
            self.remote.clone()
        } else {
            format!("{}:", self.remote)
        }
    }

    fn destination(&self, subpath: &str) -> String {
        let remote = self.remote_with_colon();
        let mut parts = Vec::new();
        if !self.path.is_empty() {
            parts.push(self.path.as_str());
        }
        if !subpath.is_empty() {
            parts.push(subpath);
        }
        if parts.is_empty() {
            remote
        } else {
            format!("{}{}", remote, parts.join("/"))
        }
    }

    pub fn is_reachable(&self) -> bool {
        CommandRunner
            .run(
                "rclone",
                ["lsf", &self.remote_with_colon(), "--max-depth", "1"],
                Some(Duration::from_secs(30)),
            )
            .map(|output| output.success())
            .unwrap_or(false)
    }

    pub fn copy_directory(&self, src: &Path, dst_name: &str) -> CopyResult {
        let archived_files = match self.scan_directory(src) {
            Ok(files) => files,
            Err(err) => {
                eprintln!(
                    "warning: could not scan {} before archive: {}",
                    src.display(),
                    err
                );
                Vec::new()
            }
        };
        let mut args = vec![
            "copy".to_string(),
            src.display().to_string(),
            self.destination(dst_name),
            "--stats-one-line".to_string(),
            "-v".to_string(),
        ];
        args.extend(self.flags.clone());

        let output = CommandRunner.run(
            "rclone",
            args.iter().map(String::as_str),
            Some(self.timeout),
        );
        let output = match output {
            Ok(output) => output,
            Err(err) => {
                return CopyResult {
                    success: false,
                    files_transferred: 0,
                    bytes_transferred: 0,
                    error: Some(err.to_string()),
                    archived_files: Vec::new(),
                };
            }
        };

        let combined_output = combined_command_output(&output);
        if !output.success() {
            let confirmed_paths =
                parse_rclone_paths(&combined_output, &[": Copied (", ": Unchanged skipping"]);
            let confirmed_files = select_archived_files(&archived_files, &confirmed_paths);
            return CopyResult {
                success: false,
                files_transferred: confirmed_files.len() as u64,
                bytes_transferred: confirmed_files.iter().map(|file| file.size).sum(),
                error: Some(if output.timed_out {
                    "Timeout".to_string()
                } else {
                    output.last_error_line()
                }),
                archived_files: confirmed_files,
            };
        }

        let copied_paths = parse_rclone_paths(&combined_output, &[": Copied ("]);
        let copied_files = select_archived_files(&archived_files, &copied_paths);

        CopyResult {
            success: true,
            files_transferred: copied_files.len() as u64,
            bytes_transferred: copied_files.iter().map(|file| file.size).sum(),
            error: None,
            archived_files,
        }
    }

    fn scan_directory(&self, src: &Path) -> Result<Vec<ArchivedFile>> {
        let mut files = Vec::new();
        for file in self.fs.walk_files(src)? {
            let relative_path = file
                .strip_prefix(src)
                .map_err(|_| Error::new("failed to build relative archive path"))?
                .to_path_buf();
            files.push(ArchivedFile {
                relative_path,
                size: self.fs.file_size(&file)?,
            });
        }
        Ok(files)
    }
}

#[derive(Debug, Clone)]
pub struct ArchiveManager<F: FileSystem> {
    fs: F,
    snapshot_manager: SnapshotManager<F>,
    backend: ArchiveBackend<F>,
    cam_disk_path: PathBuf,
    archive_recent: bool,
    archive_saved: bool,
    archive_sentry: bool,
    archive_track: bool,
    archive_photobooth: bool,
}

impl<F: FileSystem> ArchiveManager<F> {
    pub fn new(
        fs: F,
        snapshot_manager: SnapshotManager<F>,
        backend: ArchiveBackend<F>,
        cam_disk_path: PathBuf,
        config: &ArchiveConfig,
    ) -> Self {
        Self {
            fs,
            snapshot_manager,
            backend,
            cam_disk_path,
            archive_recent: config.archive_recent,
            archive_saved: config.archive_saved,
            archive_sentry: config.archive_sentry,
            archive_track: config.archive_track,
            archive_photobooth: config.archive_photobooth,
        }
    }

    pub fn backend(&self) -> &ArchiveBackend<F> {
        &self.backend
    }

    pub fn cam_disk_path(&self) -> &Path {
        &self.cam_disk_path
    }

    pub fn archive_new_snapshot(&self, delete_after_archive: bool) -> Result<ArchiveResult> {
        let snapshot = self.snapshot_manager.create_snapshot()?;
        let handle = self.snapshot_manager.acquire(snapshot.id)?;
        let mounted = mount_image(&snapshot.image_path(), true)?;
        let result = self.archive_snapshot(&handle, mounted.path())?;
        drop(mounted);

        if delete_after_archive && result.success() && !result.archived_files.is_empty() {
            let mounted_cam = mount_image(&self.cam_disk_path, false)?;
            let (deleted, skipped) = self.delete_archived_files(&result, mounted_cam.path())?;
            eprintln!("clean up: deleted {}, skipped {}", deleted, skipped);
        }

        Ok(result)
    }

    pub fn archive_snapshot(
        &self,
        handle: &SnapshotHandle<F>,
        mount_path: &Path,
    ) -> Result<ArchiveResult> {
        let snapshot = handle.snapshot()?;
        let mut result = ArchiveResult::new(snapshot.id);

        result.state = ArchiveState::Connecting;
        if !self.backend.is_reachable() {
            result.state = ArchiveState::Failed;
            result.error = Some("archive backend is not reachable".to_string());
            result.completed_secs = Some(now_secs());
            return Ok(result);
        }

        result.state = ArchiveState::Archiving;
        let dirs = self.dirs_to_archive(mount_path);
        if dirs.is_empty() {
            result.state = ArchiveState::Completed;
            result.completed_secs = Some(now_secs());
            return Ok(result);
        }

        let mut errors = Vec::new();
        for (src, dst_name) in dirs {
            let copy = self.backend.copy_directory(&src, &dst_name);
            if copy.success {
                result.files_transferred += copy.files_transferred;
                result.bytes_transferred += copy.bytes_transferred;
                if !copy.archived_files.is_empty() {
                    result.archived_files.push((dst_name, copy.archived_files));
                }
            } else {
                result.files_transferred += copy.files_transferred;
                result.bytes_transferred += copy.bytes_transferred;
                if !copy.archived_files.is_empty() {
                    result
                        .archived_files
                        .push((dst_name.clone(), copy.archived_files));
                }
                errors.push(format!(
                    "{}: {}",
                    dst_name,
                    copy.error.unwrap_or_else(|| "unknown error".to_string())
                ));
            }
        }

        result.completed_secs = Some(now_secs());
        if errors.is_empty() {
            result.state = ArchiveState::Completed;
        } else {
            result.state = ArchiveState::Failed;
            result.error = Some(errors.join("; "));
        }
        Ok(result)
    }

    fn dirs_to_archive(&self, mount_path: &Path) -> Vec<(PathBuf, String)> {
        let mut dirs = Vec::new();
        self.push_dir(
            &mut dirs,
            self.archive_saved,
            mount_path,
            "TeslaCam/SavedClips",
            "SavedClips",
        );
        self.push_dir(
            &mut dirs,
            self.archive_sentry,
            mount_path,
            "TeslaCam/SentryClips",
            "SentryClips",
        );
        self.push_dir(
            &mut dirs,
            self.archive_recent,
            mount_path,
            "TeslaCam/RecentClips",
            "RecentClips",
        );
        self.push_dir(
            &mut dirs,
            self.archive_track,
            mount_path,
            "TeslaTrackMode",
            "TrackMode",
        );
        self.push_dir(
            &mut dirs,
            self.archive_photobooth,
            mount_path,
            "TeslaCam/Photobooth",
            "Photobooth",
        );
        dirs
    }

    fn push_dir(
        &self,
        dirs: &mut Vec<(PathBuf, String)>,
        enabled: bool,
        mount_path: &Path,
        relative: &str,
        name: &str,
    ) {
        let path = mount_path.join(relative);
        if enabled && self.fs.exists(&path) {
            dirs.push((path, name.to_string()));
        }
    }

    pub fn delete_archived_files(
        &self,
        result: &ArchiveResult,
        cam_disk_mount: &Path,
    ) -> Result<(u64, u64)> {
        let mut deleted = 0;
        let mut skipped = 0;

        for (dir_name, files) in &result.archived_files {
            let Some(relative_base) = cam_dir_for_archive_name(dir_name) else {
                eprintln!("warning: unknown archive directory name: {}", dir_name);
                continue;
            };
            let base_path = cam_disk_mount.join(relative_base);
            for archived_file in files {
                let file_path = base_path.join(&archived_file.relative_path);
                if !self.fs.exists(&file_path) {
                    skipped += 1;
                    continue;
                }
                match self.fs.file_size(&file_path) {
                    Ok(size) if size == archived_file.size => {}
                    Ok(size) => {
                        eprintln!(
                            "warning: file size changed for {}; archived={}, current={}, skipping",
                            file_path.display(),
                            archived_file.size,
                            size
                        );
                        skipped += 1;
                        continue;
                    }
                    Err(err) => {
                        eprintln!("warning: could not stat {}: {}", file_path.display(), err);
                        skipped += 1;
                        continue;
                    }
                }
                match self.fs.remove_file(&file_path) {
                    Ok(()) => deleted += 1,
                    Err(err) => {
                        eprintln!("warning: could not delete {}: {}", file_path.display(), err);
                        skipped += 1;
                    }
                }
            }
            self.cleanup_empty_dirs(&base_path);
        }

        Ok((deleted, skipped))
    }

    fn cleanup_empty_dirs(&self, base_path: &Path) {
        let Ok(mut files_or_dirs) = self.collect_dirs(base_path) else {
            return;
        };
        files_or_dirs.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
        for dir in files_or_dirs {
            if self
                .fs
                .list_dir_names(&dir)
                .map(|entries| entries.is_empty())
                .unwrap_or(false)
            {
                let _ = self.fs.remove_dir(&dir);
            }
        }
    }

    fn collect_dirs(&self, base_path: &Path) -> Result<Vec<PathBuf>> {
        let mut dirs = Vec::new();
        self.collect_dirs_recursive(base_path, &mut dirs)?;
        Ok(dirs)
    }

    fn collect_dirs_recursive(&self, path: &Path, dirs: &mut Vec<PathBuf>) -> Result<()> {
        if !self.fs.exists(path) {
            return Ok(());
        }
        for name in self.fs.list_dir_names(path)? {
            let child = path.join(name);
            if self.fs.is_dir(&child) {
                dirs.push(child.clone());
                self.collect_dirs_recursive(&child, dirs)?;
            }
        }
        Ok(())
    }
}

fn cam_dir_for_archive_name(name: &str) -> Option<&'static str> {
    match name {
        "SavedClips" => Some("TeslaCam/SavedClips"),
        "SentryClips" => Some("TeslaCam/SentryClips"),
        "RecentClips" => Some("TeslaCam/RecentClips"),
        "Photobooth" => Some("TeslaCam/Photobooth"),
        "TrackMode" => Some("TeslaTrackMode"),
        _ => None,
    }
}

fn combined_command_output(output: &crate::command::CommandOutput) -> String {
    [output.stdout.as_str(), output.stderr.as_str()]
        .into_iter()
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn parse_rclone_paths(output: &str, markers: &[&str]) -> std::collections::HashSet<PathBuf> {
    let mut paths = std::collections::HashSet::new();
    for raw_line in output.lines() {
        let line = raw_line.trim();
        for marker in markers {
            let Some((prefix, _)) = line.split_once(marker) else {
                continue;
            };
            let rel_path = prefix
                .rsplit_once(" : ")
                .map(|(_, path)| path)
                .unwrap_or(prefix)
                .trim()
                .trim_start_matches('/');
            if !rel_path.is_empty() {
                paths.insert(PathBuf::from(rel_path));
            }
            break;
        }
    }
    paths
}

fn select_archived_files(
    files: &[ArchivedFile],
    relative_paths: &std::collections::HashSet<PathBuf>,
) -> Vec<ArchivedFile> {
    let by_path = files
        .iter()
        .map(|file| (file.relative_path.clone(), file.clone()))
        .collect::<std::collections::HashMap<_, _>>();
    let mut selected = relative_paths
        .iter()
        .filter_map(|path| by_path.get(path).cloned())
        .collect::<Vec<_>>();
    selected.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    selected
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::config::ArchiveConfig;
    use crate::filesystem::{FileSystem, MockFileSystem};
    use crate::snapshot::SnapshotManager;

    use super::*;

    fn manager(fs: MockFileSystem) -> ArchiveManager<MockFileSystem> {
        manager_with(fs, ArchiveConfig::default(), ArchiveBackend::None)
    }

    fn manager_with(
        fs: MockFileSystem,
        config: ArchiveConfig,
        backend: ArchiveBackend<MockFileSystem>,
    ) -> ArchiveManager<MockFileSystem> {
        fs.create_dir_all(Path::new("/backingfiles/snapshots"))
            .unwrap();
        fs.write_bytes("/backingfiles/cam_disk.bin", b"cam");
        let snapshot_manager = SnapshotManager::new(
            fs.clone(),
            PathBuf::from("/backingfiles/cam_disk.bin"),
            PathBuf::from("/backingfiles/snapshots"),
        )
        .unwrap();
        ArchiveManager::new(
            fs,
            snapshot_manager,
            backend,
            PathBuf::from("/backingfiles/cam_disk.bin"),
            &config,
        )
    }

    fn result_with_file(dir_name: &str, relative_path: &str, size: u64) -> ArchiveResult {
        let mut result = ArchiveResult::new(0);
        result.archived_files.push((
            dir_name.to_string(),
            vec![ArchivedFile {
                relative_path: PathBuf::from(relative_path),
                size,
            }],
        ));
        result
    }

    #[test]
    fn archive_result_success_tracks_completed_only() {
        let mut result = ArchiveResult::new(1);
        assert!(!result.success());
        result.state = ArchiveState::Completed;
        assert!(result.success());
        result.state = ArchiveState::Failed;
        assert!(!result.success());
    }

    #[test]
    fn copy_result_carries_success_and_error_details() {
        let ok = CopyResult {
            success: true,
            files_transferred: 2,
            bytes_transferred: 42,
            error: None,
            archived_files: Vec::new(),
        };
        assert!(ok.success);
        assert_eq!(ok.files_transferred, 2);
        assert_eq!(ok.bytes_transferred, 42);

        let failed = CopyResult {
            success: false,
            files_transferred: 0,
            bytes_transferred: 0,
            error: Some("connection failed".into()),
            archived_files: Vec::new(),
        };
        assert!(!failed.success);
        assert_eq!(failed.error.as_deref(), Some("connection failed"));
    }

    #[test]
    fn rclone_destination_building_matches_expected_paths() {
        let fs = MockFileSystem::new();
        let remote_only = RcloneBackend::new("gdrive".into(), "".into(), Vec::new(), fs.clone());
        assert_eq!(remote_only.destination(""), "gdrive:");
        assert_eq!(remote_only.destination("SavedClips"), "gdrive:SavedClips");

        let with_path = RcloneBackend::new(
            "gdrive:".into(),
            "/TeslaCam/archive/".into(),
            Vec::new(),
            fs,
        );
        assert_eq!(with_path.destination(""), "gdrive:TeslaCam/archive");
        assert_eq!(
            with_path.destination("SentryClips"),
            "gdrive:TeslaCam/archive/SentryClips"
        );
    }

    #[test]
    fn rclone_scan_directory_collects_relative_paths_and_sizes() {
        let fs = MockFileSystem::new();
        fs.create_dir_all(Path::new("/clips/event1")).unwrap();
        fs.write_bytes("/clips/event1/front.mp4", &[0; 1000]);
        fs.write_bytes("/clips/event1/back.mp4", &[0; 2000]);
        fs.write_bytes("/clips/event1/event.json", b"{}");

        let backend = RcloneBackend::new("gdrive".into(), "".into(), Vec::new(), fs);
        let files = backend.scan_directory(Path::new("/clips")).unwrap();
        let by_path = files
            .iter()
            .map(|file| (file.relative_path.clone(), file.size))
            .collect::<std::collections::HashMap<_, _>>();

        assert_eq!(files.len(), 3);
        assert_eq!(by_path[&PathBuf::from("event1/front.mp4")], 1000);
        assert_eq!(by_path[&PathBuf::from("event1/back.mp4")], 2000);
        assert_eq!(by_path[&PathBuf::from("event1/event.json")], 2);
    }

    #[test]
    fn rclone_output_parsing_selects_confirmed_files() {
        let output = "\
<6>INFO  : event1/front.mp4: Copied (new)\n\
<6>INFO  : /event1/back.mp4: Unchanged skipping\n\
unrelated line\n";
        let paths = parse_rclone_paths(output, &[": Copied (", ": Unchanged skipping"]);
        let files = vec![
            ArchivedFile {
                relative_path: PathBuf::from("event1/front.mp4"),
                size: 1000,
            },
            ArchivedFile {
                relative_path: PathBuf::from("event1/back.mp4"),
                size: 2000,
            },
            ArchivedFile {
                relative_path: PathBuf::from("event1/left.mp4"),
                size: 3000,
            },
        ];

        let selected = select_archived_files(&files, &paths);

        assert_eq!(
            selected
                .iter()
                .map(|file| (&file.relative_path, file.size))
                .collect::<Vec<_>>(),
            vec![
                (&PathBuf::from("event1/back.mp4"), 2000),
                (&PathBuf::from("event1/front.mp4"), 1000),
            ]
        );
    }

    #[test]
    fn dirs_to_archive_defaults_skip_recent_and_include_enabled_dirs() {
        let fs = MockFileSystem::new();
        fs.create_dir_all(Path::new("/mnt/TeslaCam/SavedClips"))
            .unwrap();
        fs.create_dir_all(Path::new("/mnt/TeslaCam/SentryClips"))
            .unwrap();
        fs.create_dir_all(Path::new("/mnt/TeslaCam/RecentClips"))
            .unwrap();
        fs.create_dir_all(Path::new("/mnt/TeslaCam/Photobooth"))
            .unwrap();

        let manager = manager(fs);
        let dirs = manager.dirs_to_archive(Path::new("/mnt"));
        let names = dirs.into_iter().map(|(_, name)| name).collect::<Vec<_>>();

        assert_eq!(names, vec!["SavedClips", "SentryClips", "Photobooth"]);
    }

    #[test]
    fn dirs_to_archive_respects_config_flags() {
        let fs = MockFileSystem::new();
        fs.create_dir_all(Path::new("/mnt/TeslaCam/SavedClips"))
            .unwrap();
        fs.create_dir_all(Path::new("/mnt/TeslaCam/SentryClips"))
            .unwrap();
        fs.create_dir_all(Path::new("/mnt/TeslaCam/RecentClips"))
            .unwrap();
        fs.create_dir_all(Path::new("/mnt/TeslaTrackMode")).unwrap();

        let config = ArchiveConfig {
            archive_saved: false,
            archive_sentry: false,
            archive_recent: true,
            archive_track: true,
            archive_photobooth: false,
            ..ArchiveConfig::default()
        };
        let manager = manager_with(fs, config, ArchiveBackend::None);
        let names = manager
            .dirs_to_archive(Path::new("/mnt"))
            .into_iter()
            .map(|(_, name)| name)
            .collect::<Vec<_>>();

        assert_eq!(names, vec!["RecentClips", "TrackMode"]);
    }

    #[test]
    fn archive_snapshot_copies_each_enabled_directory() {
        let fs = MockFileSystem::new();
        fs.create_dir_all(Path::new("/mnt/TeslaCam/SavedClips"))
            .unwrap();
        fs.create_dir_all(Path::new("/mnt/TeslaCam/SentryClips"))
            .unwrap();
        fs.create_dir_all(Path::new("/mnt/TeslaCam/Photobooth"))
            .unwrap();
        let backend = MockArchiveBackend::reachable(true);
        let copied_backend = backend.clone();
        let manager = manager_with(
            fs.clone(),
            ArchiveConfig::default(),
            ArchiveBackend::Mock(backend),
        );
        let snapshot = manager.snapshot_manager.create_snapshot().unwrap();
        let handle = manager.snapshot_manager.acquire(snapshot.id).unwrap();

        let result = manager
            .archive_snapshot(&handle, Path::new("/mnt"))
            .unwrap();

        assert_eq!(result.state, ArchiveState::Completed);
        assert_eq!(result.files_transferred, 30);
        assert_eq!(result.archived_files.len(), 3);
        let copied_names = copied_backend
            .copied_dirs()
            .into_iter()
            .map(|(_, name)| name)
            .collect::<Vec<_>>();
        assert_eq!(
            copied_names,
            vec!["SavedClips", "SentryClips", "Photobooth"]
        );
    }

    #[test]
    fn archive_snapshot_fails_gracefully_when_backend_unreachable() {
        let fs = MockFileSystem::new();
        fs.create_dir_all(Path::new("/mnt/TeslaCam/SavedClips"))
            .unwrap();
        let manager = manager_with(
            fs,
            ArchiveConfig::default(),
            ArchiveBackend::Mock(MockArchiveBackend::reachable(false)),
        );
        let snapshot = manager.snapshot_manager.create_snapshot().unwrap();
        let handle = manager.snapshot_manager.acquire(snapshot.id).unwrap();

        let result = manager
            .archive_snapshot(&handle, Path::new("/mnt"))
            .unwrap();

        assert_eq!(result.state, ArchiveState::Failed);
        assert!(result.error.unwrap().contains("not reachable"));
    }

    #[test]
    fn archive_snapshot_reports_copy_failures_but_keeps_successful_files() {
        let fs = MockFileSystem::new();
        fs.create_dir_all(Path::new("/mnt/TeslaCam/SavedClips"))
            .unwrap();
        fs.create_dir_all(Path::new("/mnt/TeslaCam/SentryClips"))
            .unwrap();
        let manager = manager_with(
            fs,
            ArchiveConfig::default(),
            ArchiveBackend::Mock(MockArchiveBackend::failing(&["SavedClips"])),
        );
        let snapshot = manager.snapshot_manager.create_snapshot().unwrap();
        let handle = manager.snapshot_manager.acquire(snapshot.id).unwrap();

        let result = manager
            .archive_snapshot(&handle, Path::new("/mnt"))
            .unwrap();

        assert_eq!(result.state, ArchiveState::Failed);
        assert!(result.error.as_deref().unwrap().contains("SavedClips"));
        assert_eq!(result.files_transferred, 10);
        assert_eq!(result.archived_files.len(), 1);
        assert_eq!(result.archived_files[0].0, "SentryClips");
    }

    #[test]
    fn archive_snapshot_keeps_files_confirmed_before_copy_failure() {
        let fs = MockFileSystem::new();
        fs.create_dir_all(Path::new("/mnt/TeslaCam/SavedClips"))
            .unwrap();
        let manager = manager_with(
            fs,
            ArchiveConfig::default(),
            ArchiveBackend::Mock(MockArchiveBackend::partial_failing(&["SavedClips"])),
        );
        let snapshot = manager.snapshot_manager.create_snapshot().unwrap();
        let handle = manager.snapshot_manager.acquire(snapshot.id).unwrap();

        let result = manager
            .archive_snapshot(&handle, Path::new("/mnt"))
            .unwrap();

        assert_eq!(result.state, ArchiveState::Failed);
        assert_eq!(result.files_transferred, 1);
        assert_eq!(result.bytes_transferred, 1000);
        assert_eq!(result.archived_files.len(), 1);
        assert_eq!(result.archived_files[0].0, "SavedClips");
        assert_eq!(
            result.archived_files[0].1[0].relative_path,
            PathBuf::from("event/front.mp4")
        );
    }

    #[test]
    fn archive_snapshot_with_no_dirs_completes_without_copies() {
        let fs = MockFileSystem::new();
        let manager = manager_with(
            fs,
            ArchiveConfig::default(),
            ArchiveBackend::Mock(MockArchiveBackend::reachable(true)),
        );
        let snapshot = manager.snapshot_manager.create_snapshot().unwrap();
        let handle = manager.snapshot_manager.acquire(snapshot.id).unwrap();

        let result = manager
            .archive_snapshot(&handle, Path::new("/mnt"))
            .unwrap();

        assert_eq!(result.state, ArchiveState::Completed);
        assert_eq!(result.files_transferred, 0);
        assert!(result.archived_files.is_empty());
    }

    #[test]
    fn archive_none_backend_does_not_mark_files_for_deletion() {
        let fs = MockFileSystem::new();
        fs.create_dir_all(Path::new("/mnt/TeslaCam/SavedClips"))
            .unwrap();
        let manager = manager(fs);
        let snapshot = manager.snapshot_manager.create_snapshot().unwrap();
        let handle = manager.snapshot_manager.acquire(snapshot.id).unwrap();

        let result = manager
            .archive_snapshot(&handle, Path::new("/mnt"))
            .unwrap();

        assert_eq!(result.state, ArchiveState::Completed);
        assert_eq!(result.files_transferred, 0);
        assert!(result.archived_files.is_empty());
    }

    #[test]
    fn delete_archived_files_checks_size_before_delete() {
        let fs = MockFileSystem::new();
        fs.create_dir_all(Path::new("/cam/TeslaCam/SavedClips/event"))
            .unwrap();
        fs.write_bytes("/cam/TeslaCam/SavedClips/event/front.mp4", b"1234");
        let manager = manager(fs.clone());
        let mut result = ArchiveResult::new(0);
        result.archived_files.push((
            "SavedClips".into(),
            vec![ArchivedFile {
                relative_path: PathBuf::from("event/front.mp4"),
                size: 4,
            }],
        ));

        let (deleted, skipped) = manager
            .delete_archived_files(&result, Path::new("/cam"))
            .unwrap();
        assert_eq!((deleted, skipped), (1, 0));
        assert!(!fs.exists(Path::new("/cam/TeslaCam/SavedClips/event/front.mp4")));
    }

    #[test]
    fn delete_archived_files_skips_changed_file() {
        let fs = MockFileSystem::new();
        fs.create_dir_all(Path::new("/cam/TeslaCam/SavedClips/event"))
            .unwrap();
        fs.write_bytes("/cam/TeslaCam/SavedClips/event/front.mp4", b"12345");
        let manager = manager(fs.clone());
        let mut result = ArchiveResult::new(0);
        result.archived_files.push((
            "SavedClips".into(),
            vec![ArchivedFile {
                relative_path: PathBuf::from("event/front.mp4"),
                size: 4,
            }],
        ));

        let (deleted, skipped) = manager
            .delete_archived_files(&result, Path::new("/cam"))
            .unwrap();
        assert_eq!((deleted, skipped), (0, 1));
        assert!(fs.exists(Path::new("/cam/TeslaCam/SavedClips/event/front.mp4")));
    }

    #[test]
    fn delete_archived_files_skips_missing_file() {
        let fs = MockFileSystem::new();
        fs.create_dir_all(Path::new("/cam/TeslaCam/SavedClips"))
            .unwrap();
        let manager = manager(fs);
        let result = result_with_file("SavedClips", "event/front.mp4", 4);

        let (deleted, skipped) = manager
            .delete_archived_files(&result, Path::new("/cam"))
            .unwrap();

        assert_eq!((deleted, skipped), (0, 1));
    }

    #[test]
    fn delete_archived_files_removes_empty_event_dirs_only() {
        let fs = MockFileSystem::new();
        fs.create_dir_all(Path::new("/cam/TeslaCam/SavedClips/event"))
            .unwrap();
        fs.write_bytes("/cam/TeslaCam/SavedClips/event/front.mp4", b"1234");
        let manager = manager(fs.clone());
        let result = result_with_file("SavedClips", "event/front.mp4", 4);

        manager
            .delete_archived_files(&result, Path::new("/cam"))
            .unwrap();

        assert!(!fs.exists(Path::new("/cam/TeslaCam/SavedClips/event")));
        assert!(fs.exists(Path::new("/cam/TeslaCam/SavedClips")));
    }

    #[test]
    fn delete_archived_files_handles_multiple_directories() {
        let fs = MockFileSystem::new();
        fs.create_dir_all(Path::new("/cam/TeslaCam/SavedClips/event1"))
            .unwrap();
        fs.create_dir_all(Path::new("/cam/TeslaCam/SentryClips/event2"))
            .unwrap();
        fs.write_bytes("/cam/TeslaCam/SavedClips/event1/front.mp4", b"1234");
        fs.write_bytes("/cam/TeslaCam/SentryClips/event2/front.mp4", b"12345");
        let manager = manager(fs.clone());
        let mut result = ArchiveResult::new(0);
        result.archived_files.push((
            "SavedClips".into(),
            vec![ArchivedFile {
                relative_path: PathBuf::from("event1/front.mp4"),
                size: 4,
            }],
        ));
        result.archived_files.push((
            "SentryClips".into(),
            vec![ArchivedFile {
                relative_path: PathBuf::from("event2/front.mp4"),
                size: 5,
            }],
        ));

        let (deleted, skipped) = manager
            .delete_archived_files(&result, Path::new("/cam"))
            .unwrap();

        assert_eq!((deleted, skipped), (2, 0));
        assert!(!fs.exists(Path::new("/cam/TeslaCam/SavedClips/event1/front.mp4")));
        assert!(!fs.exists(Path::new("/cam/TeslaCam/SentryClips/event2/front.mp4")));
    }

    #[test]
    fn delete_archived_files_handles_trackmode_and_photobooth() {
        let fs = MockFileSystem::new();
        fs.create_dir_all(Path::new("/cam/TeslaTrackMode/event"))
            .unwrap();
        fs.create_dir_all(Path::new("/cam/TeslaCam/Photobooth"))
            .unwrap();
        fs.write_bytes("/cam/TeslaTrackMode/event/front.mp4", b"1234");
        fs.write_bytes("/cam/TeslaCam/Photobooth/selfie.png", b"123");
        let manager = manager(fs.clone());
        let mut result = ArchiveResult::new(0);
        result.archived_files.push((
            "TrackMode".into(),
            vec![ArchivedFile {
                relative_path: PathBuf::from("event/front.mp4"),
                size: 4,
            }],
        ));
        result.archived_files.push((
            "Photobooth".into(),
            vec![ArchivedFile {
                relative_path: PathBuf::from("selfie.png"),
                size: 3,
            }],
        ));

        let (deleted, skipped) = manager
            .delete_archived_files(&result, Path::new("/cam"))
            .unwrap();

        assert_eq!((deleted, skipped), (2, 0));
        assert!(!fs.exists(Path::new("/cam/TeslaTrackMode/event/front.mp4")));
        assert!(!fs.exists(Path::new("/cam/TeslaCam/Photobooth/selfie.png")));
    }

    #[test]
    fn delete_archived_files_ignores_unknown_archive_directory() {
        let fs = MockFileSystem::new();
        let manager = manager(fs);
        let result = result_with_file("UnknownDir", "event/front.mp4", 4);

        let (deleted, skipped) = manager
            .delete_archived_files(&result, Path::new("/cam"))
            .unwrap();

        assert_eq!((deleted, skipped), (0, 0));
    }
}
