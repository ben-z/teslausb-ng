use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
                config.copy_timeout,
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
    pub fn new(remote: String, path: String, flags: Vec<String>, timeout: Duration, fs: F) -> Self {
        Self {
            remote,
            path: path.trim_matches('/').to_string(),
            flags,
            timeout,
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
            .run_interruptible(
                "rclone",
                ["lsf", &self.remote_with_colon(), "--max-depth", "1"],
                Some(Duration::from_secs(30)),
            )
            .map(|output| output.success())
            .unwrap_or(false)
    }

    pub fn copy_directory(&self, src: &Path, dst_name: &str) -> CopyResult {
        match self.copy_directory_inner(src, dst_name) {
            Ok(result) => result,
            Err(error) => CopyResult {
                success: false,
                files_transferred: 0,
                bytes_transferred: 0,
                error: Some(error.to_string()),
                archived_files: Vec::new(),
            },
        }
    }

    fn copy_directory_inner(&self, src: &Path, dst_name: &str) -> Result<CopyResult> {
        let files = self.scan_directory(src)?;
        if dst_name != "RecentClips" {
            return self.copy_batch(src, dst_name, &files, None);
        }
        let mut batches = std::collections::BTreeMap::<String, Vec<ArchivedFile>>::new();
        for file in files {
            batches
                .entry(recent_archive_directory(&file.relative_path)?)
                .or_default()
                .push(file);
        }
        let mut result = CopyResult {
            success: true,
            files_transferred: 0,
            bytes_transferred: 0,
            error: None,
            archived_files: Vec::new(),
        };
        let mut errors = Vec::new();
        for (date, files) in batches {
            let list = ArchiveFileList::new(self.fs.clone(), &files)?;
            let batch = self.copy_batch(
                src,
                &format!("RecentClips/{date}"),
                &files,
                Some(&list.path),
            )?;
            result.files_transferred += batch.files_transferred;
            result.bytes_transferred += batch.bytes_transferred;
            result.archived_files.extend(batch.archived_files);
            if let Some(error) = batch.error {
                errors.push(error);
            }
        }
        result.success = errors.is_empty();
        if !errors.is_empty() {
            result.error = Some(errors.join("; "));
        }
        Ok(result)
    }

    fn copy_batch(
        &self,
        src: &Path,
        destination: &str,
        files: &[ArchivedFile],
        file_list: Option<&Path>,
    ) -> Result<CopyResult> {
        let mut args = vec![
            "copy".to_string(),
            src.display().to_string(),
            self.destination(destination),
        ];
        args.extend(self.flags.clone());
        args.extend(
            ["--use-json-log", "--log-level", "DEBUG", "--no-traverse"].map(str::to_string),
        );
        if let Some(path) = file_list {
            args.extend(["--files-from-raw".to_string(), path.display().to_string()]);
        }
        let output = CommandRunner.run_interruptible(
            "rclone",
            args.iter().map(String::as_str),
            Some(self.timeout),
        )?;
        let log = combined_command_output(&output);
        let confirmed = parse_rclone_paths(&log, &["Copied (", "Unchanged skipping"])?;
        let copied = select_archived_files(files, &parse_rclone_paths(&log, &["Copied ("])?);
        let error = if output.timed_out {
            Some(format!("rclone copy to {destination} timed out"))
        } else if !output.success() {
            Some(output.last_error_line())
        } else {
            None
        };
        Ok(CopyResult {
            success: error.is_none(),
            files_transferred: copied.len() as u64,
            bytes_transferred: copied.iter().map(|file| file.size).sum(),
            error,
            archived_files: select_archived_files(files, &confirmed),
        })
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

fn recent_archive_directory(path: &Path) -> Result<String> {
    let name = path
        .to_str()
        .ok_or_else(|| Error::new("RecentClips filename is not UTF-8"))?;
    if matches!(name, "thumb.png" | "event.json") {
        return Ok("metadata".to_string());
    }
    let date = name
        .get(..10)
        .ok_or_else(|| Error::new(format!("invalid RecentClips filename: {name}")))?;
    let parts: Vec<_> = date.split('-').collect();
    let valid_date = (|| -> Option<bool> {
        if parts.len() != 3 || parts[0].len() != 4 || parts[1].len() != 2 || parts[2].len() != 2 {
            return Some(false);
        }
        let year: u32 = parts[0].parse().ok()?;
        let month: u32 = parts[1].parse().ok()?;
        let day: u32 = parts[2].parse().ok()?;
        let days = match month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
            2 => 28,
            _ => return Some(false),
        };
        Some(year > 0 && (1..=days).contains(&day))
    })() == Some(true);
    let valid_time = name.get(11..19).is_some_and(|time| {
        let parts: Vec<_> = time.split('-').collect();
        parts.len() == 3
            && parts.iter().all(|part| part.len() == 2)
            && parts[0].parse::<u32>().is_ok_and(|hour| hour < 24)
            && parts[1].parse::<u32>().is_ok_and(|minute| minute < 60)
            && parts[2].parse::<u32>().is_ok_and(|second| second < 60)
    });
    let valid_camera = name
        .get(19..)
        .and_then(|suffix| suffix.strip_prefix('-'))
        .and_then(|suffix| suffix.strip_suffix(".mp4"))
        .is_some_and(|camera| {
            !camera.is_empty()
                && camera
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
        });
    if !valid_date
        || !valid_time
        || !valid_camera
        || !name.get(10..).is_some_and(|rest| rest.starts_with('_'))
        || !name.ends_with(".mp4")
        || path.components().count() != 1
        || name.contains(['\n', '\r'])
    {
        return Err(Error::new(format!("invalid RecentClips filename: {name}")));
    }
    Ok(date.to_string())
}

struct ArchiveFileList<F: FileSystem> {
    fs: F,
    path: PathBuf,
}

impl<F: FileSystem> ArchiveFileList<F> {
    fn new(fs: F, files: &[ArchivedFile]) -> Result<Self> {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| Error::new(error.to_string()))?
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("teslausb-files-{}-{nonce}", std::process::id()));
        let content = files
            .iter()
            .map(|file| format!("{}\n", file.relative_path.display()))
            .collect::<String>();
        fs.write_text_atomic(&path, &content)?;
        Ok(Self { fs, path })
    }
}

impl<F: FileSystem> Drop for ArchiveFileList<F> {
    fn drop(&mut self) {
        if let Err(error) = self.fs.remove_file(&self.path) {
            eprintln!(
                "error: failed to remove archive file list {}: {}",
                self.path.display(),
                error
            );
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct EventFile {
    path: PathBuf,
    size: u64,
    modified: u64,
}

#[derive(Debug)]
struct EventObservation {
    files: Vec<EventFile>,
    unchanged_since: Instant,
    warned: bool,
}

fn has_video(files: &[EventFile]) -> bool {
    files.iter().any(|file| {
        file.size > 0
            && file
                .path
                .extension()
                .is_some_and(|extension| extension == "mp4")
    })
}

fn fully_confirmed(event: &[EventFile], confirmed: &[ArchivedFile]) -> bool {
    event.iter().all(|file| {
        confirmed
            .iter()
            .any(|copy| copy.relative_path == file.path && copy.size == file.size)
    })
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
    event_stability: Duration,
    events: Arc<Mutex<HashMap<(String, PathBuf), EventObservation>>>,
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
            event_stability: config.event_stability,
            events: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn backend(&self) -> &ArchiveBackend<F> {
        &self.backend
    }

    pub fn cam_disk_path(&self) -> &Path {
        &self.cam_disk_path
    }

    pub fn archive_new_snapshot(&self) -> Result<ArchiveResult> {
        let snapshot = self.snapshot_manager.create_snapshot()?;
        let handle = self.snapshot_manager.acquire(snapshot.id)?;
        let mounted = mount_image(&snapshot.image_path(), true)?;
        let result = self.archive_snapshot(&handle, mounted.path())?;
        mounted.unmount()?;

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
            result.files_transferred += copy.files_transferred;
            result.bytes_transferred += copy.bytes_transferred;
            if !copy.success {
                errors.push(format!(
                    "{}: {}",
                    dst_name,
                    copy.error.unwrap_or_else(|| "unknown error".to_string())
                ));
            }
            let cleanup_files = self.cleanup_candidates(&src, &dst_name, copy.archived_files)?;
            if !cleanup_files.is_empty() {
                result.archived_files.push((dst_name, cleanup_files));
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

    fn cleanup_candidates(
        &self,
        source: &Path,
        directory: &str,
        files: Vec<ArchivedFile>,
    ) -> Result<Vec<ArchivedFile>> {
        if directory == "RecentClips" {
            return Ok(Vec::new());
        }
        if !matches!(directory, "SavedClips" | "SentryClips") {
            return Ok(files);
        }
        let events = self.event_files(source)?;
        let mut observations = self.events.lock().unwrap();
        observations.retain(|(name, event), _| name != directory || events.contains_key(event));
        let mut eligible = std::collections::HashSet::new();
        for (event, signature) in events {
            let observation = observations
                .entry((directory.to_string(), event.clone()))
                .or_insert_with(|| EventObservation {
                    files: signature.clone(),
                    unchanged_since: Instant::now(),
                    warned: false,
                });
            if observation.files != signature {
                *observation = EventObservation {
                    files: signature.clone(),
                    unchanged_since: Instant::now(),
                    warned: false,
                };
            }
            if observation.unchanged_since.elapsed() < self.event_stability {
                continue;
            }
            if !has_video(&signature) {
                if !observation.warned {
                    eprintln!("warning: {directory}/{} has remained without video; preserving the event on the camera disk", event.display());
                    observation.warned = true;
                }
            } else if fully_confirmed(&signature, &files) {
                eligible.insert(event);
            }
        }
        Ok(files
            .into_iter()
            .filter(|file| {
                file.relative_path
                    .parent()
                    .is_some_and(|parent| eligible.contains(parent))
            })
            .collect())
    }

    fn event_files(&self, source: &Path) -> Result<HashMap<PathBuf, Vec<EventFile>>> {
        let mut events = HashMap::<PathBuf, Vec<EventFile>>::new();
        for path in self.fs.walk_files(source)? {
            let relative = path
                .strip_prefix(source)
                .map_err(|error| Error::new(error.to_string()))?
                .to_path_buf();
            let event = relative
                .parent()
                .ok_or_else(|| Error::new("event file has no parent"))?
                .to_path_buf();
            events.entry(event).or_default().push(EventFile {
                path: relative,
                size: self.fs.file_size(&path)?,
                modified: self.fs.mtime_secs(&path)?,
            });
        }
        for files in events.values_mut() {
            files.sort();
        }
        Ok(events)
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
            if dir_name == "RecentClips" {
                // The car uses this rolling buffer when saving dashcam and Sentry events.
                skipped += files.len() as u64;
                continue;
            }
            let Some(relative_base) = cam_dir_for_archive_name(dir_name) else {
                eprintln!("warning: unknown archive directory name: {}", dir_name);
                continue;
            };
            let base_path = cam_disk_mount.join(relative_base);
            let eligible_events = if matches!(dir_name.as_str(), "SavedClips" | "SentryClips") {
                let live_events = self.event_files(&base_path)?;
                let observations = self.events.lock().unwrap();
                Some(
                    live_events
                        .into_iter()
                        .filter_map(|(event, signature)| {
                            let unchanged = observations
                                .get(&(dir_name.clone(), event.clone()))
                                .is_some_and(|observation| {
                                    observation.files == signature
                                        && observation.unchanged_since.elapsed()
                                            >= self.event_stability
                                });
                            (unchanged
                                && has_video(&signature)
                                && fully_confirmed(&signature, files))
                            .then_some(event)
                        })
                        .collect::<std::collections::HashSet<_>>(),
                )
            } else {
                None
            };
            for archived_file in files {
                if !archived_file
                    .relative_path
                    .components()
                    .all(|part| matches!(part, std::path::Component::Normal(_)))
                {
                    return Err(Error::new(
                        "archive cleanup path must be relative and stay inside its event",
                    ));
                }
                let file_path = base_path.join(&archived_file.relative_path);
                if eligible_events.as_ref().is_some_and(|events| {
                    !archived_file
                        .relative_path
                        .parent()
                        .is_some_and(|parent| events.contains(parent))
                }) {
                    skipped += 1;
                    continue;
                }
                if !self.fs.exists(&file_path) {
                    skipped += 1;
                    continue;
                }
                let size = self.fs.file_size(&file_path)?;
                if size != archived_file.size {
                    eprintln!(
                        "warning: file size changed for {}; archived={}, current={}, skipping",
                        file_path.display(),
                        archived_file.size,
                        size
                    );
                    skipped += 1;
                    continue;
                }
                self.fs.remove_file(&file_path)?;
                deleted += 1;
            }
            self.cleanup_empty_dirs(&base_path)?;
        }

        Ok((deleted, skipped))
    }

    fn cleanup_empty_dirs(&self, base_path: &Path) -> Result<()> {
        let mut dirs = self.collect_dirs(base_path)?;
        dirs.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
        for dir in dirs {
            if self.fs.list_dir_names(&dir)?.is_empty() {
                self.fs.remove_dir(&dir)?;
            }
        }
        Ok(())
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

fn parse_rclone_paths(
    output: &str,
    markers: &[&str],
) -> Result<std::collections::HashSet<PathBuf>> {
    let mut paths = std::collections::HashSet::new();
    for line in output.lines().filter(|line| !line.trim().is_empty()) {
        let record: serde_json::Value = serde_json::from_str(line)
            .map_err(|error| Error::new(format!("invalid rclone JSON log record: {error}")))?;
        let (Some(object), Some(message)) = (record["object"].as_str(), record["msg"].as_str())
        else {
            continue;
        };
        if markers.iter().any(|marker| message.starts_with(marker)) {
            let path = PathBuf::from(object);
            if !path.as_os_str().is_empty()
                && path
                    .components()
                    .all(|component| matches!(component, std::path::Component::Normal(_)))
            {
                paths.insert(path);
            }
        }
    }
    Ok(paths)
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
        mut config: ArchiveConfig,
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
        config.event_stability = Duration::ZERO;
        let manager = ArchiveManager::new(
            fs,
            snapshot_manager,
            backend,
            PathBuf::from("/backingfiles/cam_disk.bin"),
            &config,
        );
        for directory in ["SavedClips", "SentryClips"] {
            let source = PathBuf::from("/cam/TeslaCam").join(directory);
            manager
                .cleanup_candidates(&source, directory, Vec::new())
                .unwrap();
        }
        manager
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
    fn recent_archive_directory_rejects_invalid_dates_and_paths() {
        assert_eq!(
            recent_archive_directory(Path::new("2026-10-02_12-30-00-front.mp4")).unwrap(),
            "2026-10-02"
        );
        assert!(recent_archive_directory(Path::new("2024-02-29_12-30-00-front.mp4")).is_ok());
        for name in [
            "2026-02-29_12-30-00-front.mp4",
            "2026-13-01_x.mp4",
            "../2026-10-02_x.mp4",
            "unknown.mp4",
            "2026-10-02_x.png",
            "2026-10-02_x\n.mp4",
        ] {
            assert!(recent_archive_directory(Path::new(name)).is_err(), "{name}");
        }
    }

    #[test]
    fn recent_and_marker_only_events_are_never_cleanup_candidates() {
        let fs = MockFileSystem::new();
        fs.write_bytes("/clips/event/event.json", b"{}");
        fs.write_bytes("/clips/event/thumb.png", b"png");
        let manager = manager(fs.clone());
        let files = vec![ArchivedFile {
            relative_path: "event/event.json".into(),
            size: 2,
        }];
        assert!(manager
            .cleanup_candidates(Path::new("/clips"), "RecentClips", files.clone())
            .unwrap()
            .is_empty());
        assert!(manager
            .cleanup_candidates(Path::new("/clips"), "SavedClips", files.clone())
            .unwrap()
            .is_empty());
        fs.write_bytes("/clips/event/front.mp4", b"video");
        assert!(manager
            .cleanup_candidates(Path::new("/clips"), "SavedClips", files)
            .unwrap()
            .is_empty());
        let confirmed = vec![
            ArchivedFile {
                relative_path: "event/event.json".into(),
                size: 2,
            },
            ArchivedFile {
                relative_path: "event/thumb.png".into(),
                size: 3,
            },
            ArchivedFile {
                relative_path: "event/front.mp4".into(),
                size: 5,
            },
        ];
        assert_eq!(
            manager
                .cleanup_candidates(Path::new("/clips"), "SavedClips", confirmed)
                .unwrap()
                .len(),
            3
        );
    }

    #[test]
    fn events_must_remain_unchanged_for_the_observed_grace_period() {
        let fs = MockFileSystem::new();
        fs.write_bytes("/clips/event/front.mp4", b"video");
        let mut manager = manager(fs.clone());
        manager.event_stability = Duration::from_secs(600);
        let files = vec![ArchivedFile {
            relative_path: "event/front.mp4".into(),
            size: 5,
        }];
        assert!(manager
            .cleanup_candidates(Path::new("/clips"), "SavedClips", files.clone())
            .unwrap()
            .is_empty());
        manager
            .events
            .lock()
            .unwrap()
            .get_mut(&("SavedClips".into(), "event".into()))
            .unwrap()
            .unchanged_since -= Duration::from_secs(600);
        assert_eq!(
            manager
                .cleanup_candidates(Path::new("/clips"), "SavedClips", files.clone())
                .unwrap()
                .len(),
            1
        );
        fs.write_bytes("/clips/event/front.mp4", b"other");
        assert!(manager
            .cleanup_candidates(Path::new("/clips"), "SavedClips", files)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn cleanup_preserves_entire_event_changed_since_the_snapshot() {
        let fs = MockFileSystem::new();
        fs.write_bytes("/cam/TeslaCam/SavedClips/event/front.mp4", b"video");
        let manager = manager(fs.clone());
        let result = result_with_file("SavedClips", "event/front.mp4", 5);
        fs.write_bytes("/cam/TeslaCam/SavedClips/event/front.mp4", b"other");
        assert_eq!(
            manager
                .delete_archived_files(&result, Path::new("/cam"))
                .unwrap(),
            (0, 1)
        );
        assert_eq!(
            fs.read_bytes("/cam/TeslaCam/SavedClips/event/front.mp4")
                .unwrap(),
            b"other"
        );
    }

    #[test]
    fn cleanup_preserves_recent_buffer_and_marker_only_events() {
        let fs = MockFileSystem::new();
        fs.write_bytes("/cam/TeslaCam/RecentClips/front.mp4", b"video");
        fs.write_bytes("/cam/TeslaCam/SavedClips/event/event.json", b"{}");
        let manager = manager(fs.clone());
        for (directory, file, size) in [
            ("RecentClips", "front.mp4", 5),
            ("SavedClips", "event/event.json", 2),
        ] {
            assert_eq!(
                manager
                    .delete_archived_files(
                        &result_with_file(directory, file, size),
                        Path::new("/cam")
                    )
                    .unwrap(),
                (0, 1)
            );
        }
        assert!(fs.exists(Path::new("/cam/TeslaCam/RecentClips/front.mp4")));
        assert!(fs.exists(Path::new("/cam/TeslaCam/SavedClips/event/event.json")));
    }

    #[test]
    fn rclone_destination_building_matches_expected_paths() {
        let fs = MockFileSystem::new();
        let remote_only = RcloneBackend::new(
            "gdrive".into(),
            "".into(),
            Vec::new(),
            ArchiveConfig::default().copy_timeout,
            fs.clone(),
        );
        assert_eq!(remote_only.destination(""), "gdrive:");
        assert_eq!(remote_only.destination("SavedClips"), "gdrive:SavedClips");

        let with_path = RcloneBackend::new(
            "gdrive:".into(),
            "/TeslaCam/archive/".into(),
            Vec::new(),
            ArchiveConfig::default().copy_timeout,
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

        let backend = RcloneBackend::new(
            "gdrive".into(),
            "".into(),
            Vec::new(),
            ArchiveConfig::default().copy_timeout,
            fs,
        );
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
        let output = r#"{"object":"event1/front.mp4","msg":"Copied (new)"}
{"object":"event1/back.mp4","msg":"Unchanged skipping"}
{"object":"../outside.mp4","msg":"Copied (new)"}
{"object":"/outside.mp4","msg":"Copied (new)"}
{"msg":"summary"}
"#;
        let paths = parse_rclone_paths(output, &["Copied (", "Unchanged skipping"]).unwrap();
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
        fs.write_bytes("/mnt/TeslaCam/SavedClips/event/front.mp4", &[0; 1000]);
        fs.write_bytes("/mnt/TeslaCam/SentryClips/event/front.mp4", &[0; 1000]);
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
        fs.write_bytes("/mnt/TeslaCam/SentryClips/event/front.mp4", &[0; 1000]);
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
        fs.write_bytes("/mnt/TeslaCam/SavedClips/event/front.mp4", &[0; 1000]);
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
