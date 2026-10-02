use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(test)]
use std::collections::{BTreeSet, HashMap, HashSet};
#[cfg(test)]
use std::sync::{Arc, Mutex};

use crate::command::CommandRunner;
use crate::error::{Error, Result};

pub trait FileSystem: Clone + Send + Sync + 'static {
    type Lock: std::fmt::Debug + Send;

    fn exists(&self, path: &Path) -> bool;
    fn is_dir(&self, path: &Path) -> bool;
    fn list_dir_names(&self, path: &Path) -> Result<Vec<String>>;
    fn create_dir_all(&self, path: &Path) -> Result<()>;
    fn remove_file(&self, path: &Path) -> Result<()>;
    fn remove_dir_all(&self, path: &Path) -> Result<()>;
    fn remove_dir(&self, path: &Path) -> Result<()>;
    fn read_text(&self, path: &Path) -> Result<String>;
    fn create_private_file(&self, path: &Path, content: &str) -> Result<()>;
    fn write_text_atomic(&self, path: &Path, content: &str) -> Result<()>;
    fn copy_reflink(&self, src: &Path, dst: &Path) -> Result<()>;
    fn file_size(&self, path: &Path) -> Result<u64>;
    fn mtime_secs(&self, path: &Path) -> Result<u64>;
    fn walk_files(&self, path: &Path) -> Result<Vec<PathBuf>>;
    fn sync_dir(&self, path: &Path) -> Result<()>;
    fn try_lock(&self, path: &Path) -> Result<Option<Self::Lock>>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RealFileSystem;

#[derive(Debug)]
pub struct FileLock(File);

impl Drop for FileLock {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Err(error) = unlock_file(&self.0) {
            eprintln!("error: failed to release file lock: {}", error);
        }
    }
}

impl FileSystem for RealFileSystem {
    type Lock = FileLock;

    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }

    fn is_dir(&self, path: &Path) -> bool {
        path.is_dir()
    }

    fn list_dir_names(&self, path: &Path) -> Result<Vec<String>> {
        let mut names = Vec::new();
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            names.push(entry.file_name().to_string_lossy().to_string());
        }
        names.sort();
        Ok(names)
    }

    fn create_dir_all(&self, path: &Path) -> Result<()> {
        fs::create_dir_all(path)?;
        Ok(())
    }

    fn remove_file(&self, path: &Path) -> Result<()> {
        fs::remove_file(path)?;
        if let Some(parent) = path.parent() {
            self.sync_dir(parent)?;
        }
        Ok(())
    }

    fn remove_dir_all(&self, path: &Path) -> Result<()> {
        fs::remove_dir_all(path)?;
        if let Some(parent) = path.parent() {
            self.sync_dir(parent)?;
        }
        Ok(())
    }

    fn remove_dir(&self, path: &Path) -> Result<()> {
        fs::remove_dir(path)?;
        if let Some(parent) = path.parent() {
            self.sync_dir(parent)?;
        }
        Ok(())
    }

    fn create_private_file(&self, path: &Path, content: &str) -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)?;
        if let Err(error) = file.write_all(content.as_bytes()) {
            drop(file);
            fs::remove_file(path).map_err(|cleanup| {
                Error::new(format!(
                    "failed to write private file {}: {error}; failed to remove it: {cleanup}",
                    path.display()
                ))
            })?;
            return Err(error.into());
        }
        Ok(())
    }

    fn write_text_atomic(&self, path: &Path, content: &str) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp_path = temp_sibling(path);
        {
            let mut file = File::create(&tmp_path)?;
            file.write_all(content.as_bytes())?;
            file.sync_all()?;
        }
        fs::rename(&tmp_path, path)?;
        if let Some(parent) = path.parent() {
            self.sync_dir(parent)?;
        }
        Ok(())
    }

    fn read_text(&self, path: &Path) -> Result<String> {
        Ok(fs::read_to_string(path)?)
    }

    fn copy_reflink(&self, src: &Path, dst: &Path) -> Result<()> {
        if let Some(parent) = dst.parent() {
            fs::create_dir_all(parent)?;
        }
        let output = CommandRunner.check(
            "cp",
            [
                "--reflink=always",
                &src.display().to_string(),
                &dst.display().to_string(),
            ],
            Some(Duration::from_secs(300)),
        );
        output.map_err(|e| {
            Error::new(format!(
                "reflink copy failed; TeslaUSB requires XFS or btrfs: {}",
                e
            ))
        })?;
        File::open(dst)?.sync_all()?;
        Ok(())
    }

    fn file_size(&self, path: &Path) -> Result<u64> {
        Ok(fs::metadata(path)?.len())
    }

    fn mtime_secs(&self, path: &Path) -> Result<u64> {
        let modified = fs::metadata(path)?.modified()?;
        Ok(modified
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs())
    }

    fn walk_files(&self, path: &Path) -> Result<Vec<PathBuf>> {
        let mut files = Vec::new();
        walk_files_real(path, &mut files)?;
        files.sort();
        Ok(files)
    }

    fn sync_dir(&self, path: &Path) -> Result<()> {
        let dir = File::open(path)?;
        dir.sync_all()?;
        Ok(())
    }

    fn try_lock(&self, path: &Path) -> Result<Option<Self::Lock>> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        if try_lock_exclusive(&file)? {
            Ok(Some(FileLock(file)))
        } else {
            Ok(None)
        }
    }
}

#[cfg(unix)]
fn try_lock_exclusive(file: &File) -> std::io::Result<bool> {
    use std::os::fd::AsRawFd;

    const LOCK_EX: i32 = 2;
    const LOCK_NB: i32 = 4;
    extern "C" {
        fn flock(fd: i32, operation: i32) -> i32;
    }
    if unsafe { flock(file.as_raw_fd(), LOCK_EX | LOCK_NB) } == 0 {
        Ok(true)
    } else {
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::WouldBlock {
            Ok(false)
        } else {
            Err(error)
        }
    }
}

#[cfg(unix)]
fn unlock_file(file: &File) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    const LOCK_UN: i32 = 8;
    extern "C" {
        fn flock(fd: i32, operation: i32) -> i32;
    }
    if unsafe { flock(file.as_raw_fd(), LOCK_UN) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(unix))]
fn try_lock_exclusive(_file: &File) -> std::io::Result<bool> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "snapshot locking requires Unix flock support",
    ))
}

fn walk_files_real(path: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let entry_path = entry.path();
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            walk_files_real(&entry_path, files)?;
        } else if metadata.is_file() {
            files.push(entry_path);
        }
    }
    Ok(())
}

fn temp_sibling(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_else(|| "tmp".into());
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    path.with_file_name(format!(".{}.{}.tmp", name, suffix))
}

#[cfg(test)]
#[derive(Debug, Clone, Default)]
pub struct MockFileSystem {
    inner: Arc<Mutex<MockState>>,
}

#[cfg(test)]
#[derive(Debug, Default)]
struct MockState {
    files: HashMap<PathBuf, Vec<u8>>,
    dirs: HashSet<PathBuf>,
    mtimes: HashMap<PathBuf, u64>,
    tick: u64,
    locks: HashSet<PathBuf>,
    removal_failures: HashSet<PathBuf>,
}

#[cfg(test)]
#[derive(Debug)]
pub struct MockFileLock {
    inner: Arc<Mutex<MockState>>,
    path: PathBuf,
}

#[cfg(test)]
impl Drop for MockFileLock {
    fn drop(&mut self) {
        self.inner.lock().unwrap().locks.remove(&self.path);
    }
}

#[cfg(test)]
impl MockFileSystem {
    pub fn new() -> Self {
        let fs = Self::default();
        fs.inner.lock().unwrap().dirs.insert(PathBuf::from("/"));
        fs
    }

    pub fn write_bytes(&self, path: impl AsRef<Path>, content: &[u8]) {
        let path = normalize(path.as_ref());
        let mut state = self.inner.lock().unwrap();
        let parent = path.parent().unwrap_or(Path::new("/")).to_path_buf();
        state.dirs.insert(parent);
        state.tick += 1;
        let tick = state.tick;
        state.files.insert(path.clone(), content.to_vec());
        state.mtimes.insert(path, tick);
    }

    pub fn read_bytes(&self, path: impl AsRef<Path>) -> Result<Vec<u8>> {
        let path = normalize(path.as_ref());
        self.inner
            .lock()
            .unwrap()
            .files
            .get(&path)
            .cloned()
            .ok_or_else(|| Error::new(format!("file not found: {}", path.display())))
    }

    pub fn read_text(&self, path: impl AsRef<Path>) -> Result<String> {
        Ok(String::from_utf8_lossy(&self.read_bytes(path)?).into_owned())
    }

    pub fn fail_removal(&self, path: &Path) {
        self.inner
            .lock()
            .unwrap()
            .removal_failures
            .insert(normalize(path));
    }

    pub fn allow_removal(&self, path: &Path) {
        self.inner
            .lock()
            .unwrap()
            .removal_failures
            .remove(&normalize(path));
    }
}

#[cfg(test)]
impl FileSystem for MockFileSystem {
    type Lock = MockFileLock;

    fn exists(&self, path: &Path) -> bool {
        let path = normalize(path);
        let state = self.inner.lock().unwrap();
        state.files.contains_key(&path) || state.dirs.contains(&path)
    }

    fn read_text(&self, path: &Path) -> Result<String> {
        MockFileSystem::read_text(self, path)
    }

    fn is_dir(&self, path: &Path) -> bool {
        self.inner.lock().unwrap().dirs.contains(&normalize(path))
    }

    fn list_dir_names(&self, path: &Path) -> Result<Vec<String>> {
        let path = normalize(path);
        let state = self.inner.lock().unwrap();
        if !state.dirs.contains(&path) {
            return Err(Error::new(format!(
                "directory not found: {}",
                path.display()
            )));
        }
        let mut names = BTreeSet::new();
        for child in state.files.keys().chain(state.dirs.iter()) {
            if child != &path && child.parent() == Some(path.as_path()) {
                if let Some(name) = child.file_name() {
                    names.insert(name.to_string_lossy().to_string());
                }
            }
        }
        Ok(names.into_iter().collect())
    }

    fn create_dir_all(&self, path: &Path) -> Result<()> {
        let mut current = PathBuf::new();
        let mut state = self.inner.lock().unwrap();
        for component in normalize(path).components() {
            current.push(component);
            state.dirs.insert(current.clone());
        }
        Ok(())
    }

    fn remove_file(&self, path: &Path) -> Result<()> {
        let path = normalize(path);
        let mut state = self.inner.lock().unwrap();
        if state.removal_failures.contains(&path) {
            return Err(Error::new(format!("cannot remove {}", path.display())));
        }
        state
            .files
            .remove(&path)
            .ok_or_else(|| Error::new(format!("file not found: {}", path.display())))?;
        state.mtimes.remove(&path);
        Ok(())
    }

    fn remove_dir_all(&self, path: &Path) -> Result<()> {
        let path = normalize(path);
        let mut state = self.inner.lock().unwrap();
        if state.removal_failures.contains(&path) {
            return Err(Error::new(format!("cannot remove {}", path.display())));
        }
        if !state.dirs.contains(&path) {
            return Err(Error::new(format!(
                "directory not found: {}",
                path.display()
            )));
        }
        state
            .files
            .retain(|candidate, _| !is_under(candidate, &path));
        state.dirs.retain(|candidate| !is_under(candidate, &path));
        state
            .mtimes
            .retain(|candidate, _| !is_under(candidate, &path));
        Ok(())
    }

    fn remove_dir(&self, path: &Path) -> Result<()> {
        let path = normalize(path);
        let mut state = self.inner.lock().unwrap();
        let has_children = state
            .files
            .keys()
            .chain(state.dirs.iter())
            .any(|candidate| candidate != &path && candidate.parent() == Some(path.as_path()));
        if has_children {
            return Err(Error::new(format!(
                "directory not empty: {}",
                path.display()
            )));
        }
        state.dirs.remove(&path);
        Ok(())
    }

    fn create_private_file(&self, path: &Path, content: &str) -> Result<()> {
        let path = normalize(path);
        let mut state = self.inner.lock().unwrap();
        if state.files.contains_key(&path) || state.dirs.contains(&path) {
            return Err(Error::new(format!(
                "file already exists: {}",
                path.display()
            )));
        }
        state.tick += 1;
        let tick = state.tick;
        state
            .files
            .insert(path.clone(), content.as_bytes().to_vec());
        state.mtimes.insert(path, tick);
        Ok(())
    }

    fn write_text_atomic(&self, path: &Path, content: &str) -> Result<()> {
        let path = normalize(path);
        let mut state = self.inner.lock().unwrap();
        let parent = path.parent().unwrap_or(Path::new("/")).to_path_buf();
        state.dirs.insert(parent);
        state.tick += 1;
        let tick = state.tick;
        state
            .files
            .insert(path.clone(), content.as_bytes().to_vec());
        state.mtimes.insert(path, tick);
        Ok(())
    }

    fn copy_reflink(&self, src: &Path, dst: &Path) -> Result<()> {
        let src = normalize(src);
        let dst = normalize(dst);
        let mut state = self.inner.lock().unwrap();
        let content = state
            .files
            .get(&src)
            .ok_or_else(|| Error::new(format!("file not found: {}", src.display())))?
            .clone();
        state.tick += 1;
        let tick = state.tick;
        state.files.insert(dst.clone(), content);
        state.mtimes.insert(dst, tick);
        Ok(())
    }

    fn file_size(&self, path: &Path) -> Result<u64> {
        let path = normalize(path);
        let state = self.inner.lock().unwrap();
        state
            .files
            .get(&path)
            .map(|bytes| bytes.len() as u64)
            .ok_or_else(|| Error::new(format!("file not found: {}", path.display())))
    }

    fn mtime_secs(&self, path: &Path) -> Result<u64> {
        let path = normalize(path);
        Ok(*self.inner.lock().unwrap().mtimes.get(&path).unwrap_or(&0))
    }

    fn walk_files(&self, path: &Path) -> Result<Vec<PathBuf>> {
        let path = normalize(path);
        let mut files: Vec<PathBuf> = self
            .inner
            .lock()
            .unwrap()
            .files
            .keys()
            .filter(|candidate| is_under(candidate, &path))
            .cloned()
            .collect();
        files.sort();
        Ok(files)
    }

    fn sync_dir(&self, _path: &Path) -> Result<()> {
        Ok(())
    }

    fn try_lock(&self, path: &Path) -> Result<Option<Self::Lock>> {
        let path = normalize(path);
        let mut state = self.inner.lock().unwrap();
        if !path
            .parent()
            .is_some_and(|parent| state.dirs.contains(parent))
        {
            return Err(Error::new(format!(
                "lock directory does not exist: {}",
                path.display()
            )));
        }
        if !state.locks.insert(path.clone()) {
            return Ok(None);
        }
        state.files.entry(path.clone()).or_default();
        Ok(Some(MockFileLock {
            inner: self.inner.clone(),
            path,
        }))
    }
}

#[cfg(test)]
fn normalize(path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        Path::new("/").join(path)
    }
}

#[cfg(test)]
fn is_under(candidate: &Path, base: &Path) -> bool {
    candidate == base || candidate.starts_with(base)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "teslausb-fs-{name}-{}-{suffix}",
            std::process::id()
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn mock_create_dirs_and_list_entries() {
        let fs = MockFileSystem::new();
        assert!(!fs.exists(Path::new("/test")));

        fs.create_dir_all(Path::new("/test/subdir")).unwrap();
        fs.write_text_atomic(Path::new("/test/a.txt"), "a").unwrap();
        fs.write_text_atomic(Path::new("/test/b.txt"), "b").unwrap();

        assert!(fs.exists(Path::new("/test")));
        assert!(fs.is_dir(Path::new("/test/subdir")));
        assert_eq!(
            fs.list_dir_names(Path::new("/test")).unwrap(),
            ["a.txt", "b.txt", "subdir"]
        );
    }

    #[test]
    fn mock_write_read_stat_and_copy() {
        let fs = MockFileSystem::new();
        fs.create_dir_all(Path::new("/src")).unwrap();
        fs.create_dir_all(Path::new("/dst")).unwrap();
        fs.write_text_atomic(Path::new("/src/file.txt"), "hello")
            .unwrap();

        assert_eq!(fs.read_text("/src/file.txt").unwrap(), "hello");
        assert_eq!(fs.file_size(Path::new("/src/file.txt")).unwrap(), 5);

        fs.copy_reflink(Path::new("/src/file.txt"), Path::new("/dst/file.txt"))
            .unwrap();
        assert_eq!(fs.read_text("/dst/file.txt").unwrap(), "hello");
        assert!(fs.exists(Path::new("/src/file.txt")));
    }

    #[test]
    fn mock_remove_file_and_tree() {
        let fs = MockFileSystem::new();
        fs.create_dir_all(Path::new("/tree/a/b")).unwrap();
        fs.write_text_atomic(Path::new("/tree/file.txt"), "a")
            .unwrap();
        fs.write_text_atomic(Path::new("/tree/a/b/file.txt"), "b")
            .unwrap();

        fs.remove_file(Path::new("/tree/file.txt")).unwrap();
        assert!(!fs.exists(Path::new("/tree/file.txt")));
        assert!(fs.remove_file(Path::new("/tree/missing.txt")).is_err());

        fs.remove_dir_all(Path::new("/tree")).unwrap();
        assert!(!fs.exists(Path::new("/tree/a/b/file.txt")));
        assert!(!fs.exists(Path::new("/tree")));
    }

    #[test]
    fn mock_remove_dir_rejects_non_empty_dirs() {
        let fs = MockFileSystem::new();
        fs.create_dir_all(Path::new("/dir/child")).unwrap();
        assert!(fs.remove_dir(Path::new("/dir")).is_err());
        fs.remove_dir(Path::new("/dir/child")).unwrap();
        fs.remove_dir(Path::new("/dir")).unwrap();
        assert!(!fs.exists(Path::new("/dir")));
    }

    #[test]
    fn mock_walk_files_returns_sorted_recursive_files() {
        let fs = MockFileSystem::new();
        fs.create_dir_all(Path::new("/root/a/b")).unwrap();
        fs.write_text_atomic(Path::new("/root/file1.txt"), "1")
            .unwrap();
        fs.write_text_atomic(Path::new("/root/a/file2.txt"), "2")
            .unwrap();
        fs.write_text_atomic(Path::new("/root/a/b/file3.txt"), "3")
            .unwrap();

        let files = fs.walk_files(Path::new("/root")).unwrap();

        assert_eq!(
            files,
            vec![
                PathBuf::from("/root/a/b/file3.txt"),
                PathBuf::from("/root/a/file2.txt"),
                PathBuf::from("/root/file1.txt"),
            ]
        );
    }

    #[test]
    fn mock_missing_paths_return_errors() {
        let fs = MockFileSystem::new();
        assert!(fs.list_dir_names(Path::new("/missing")).is_err());
        assert!(fs.file_size(Path::new("/missing")).is_err());
        assert!(fs
            .copy_reflink(Path::new("/missing"), Path::new("/dst"))
            .is_err());
        assert!(fs.try_lock(Path::new("/missing/lock")).is_err());
    }

    #[test]
    fn mock_lock_is_shared_by_clones_and_released_on_drop() {
        let fs = MockFileSystem::new();
        let path = Path::new("/lock");
        let guard = fs.try_lock(path).unwrap().unwrap();
        assert!(fs.clone().try_lock(path).unwrap().is_none());
        drop(guard);
        assert!(fs.clone().try_lock(path).unwrap().is_some());
    }

    #[test]
    fn real_lock_is_exclusive_until_its_file_is_dropped() {
        let root = temp_dir("lock");
        let path = root.join("lock");
        let guard = RealFileSystem.try_lock(&path).unwrap().unwrap();
        assert!(RealFileSystem.try_lock(&path).unwrap().is_none());
        drop(guard);
        assert!(RealFileSystem.try_lock(&path).unwrap().is_some());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn private_files_are_exclusive_and_owner_readable() {
        use std::os::unix::fs::PermissionsExt;

        let root = temp_dir("private");
        let path = root.join("log");
        let fs = RealFileSystem;
        fs.create_private_file(&path, "private log").unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert!(fs.create_private_file(&path, "replacement").is_err());
        assert_eq!(fs.read_text(&path).unwrap(), "private log");
        fs::remove_dir_all(root).unwrap();

        let mock = MockFileSystem::new();
        mock.create_private_file(&path, "private log").unwrap();
        assert!(mock.create_private_file(&path, "replacement").is_err());
        assert_eq!(mock.read_text(&path).unwrap(), "private log");
    }

    #[test]
    fn real_filesystem_covers_portable_file_operations() {
        let root = temp_dir("real");
        let fs = RealFileSystem;
        let nested = root.join("a/b");
        let file = nested.join("file.txt");

        fs.create_dir_all(&nested).unwrap();
        fs.write_text_atomic(&file, "hello").unwrap();

        assert!(fs.exists(&file));
        assert!(fs.is_dir(&nested));
        assert_eq!(fs.file_size(&file).unwrap(), 5);
        assert!(fs.mtime_secs(&file).unwrap() > 0);
        assert_eq!(fs::read_to_string(&file).unwrap(), "hello");
        assert_eq!(fs.read_text(&file).unwrap(), "hello");
        assert_eq!(fs.list_dir_names(&root.join("a")).unwrap(), ["b"]);
        assert_eq!(fs.walk_files(&root).unwrap(), vec![file.clone()]);

        fs.remove_file(&file).unwrap();
        assert!(!fs.exists(&file));
        fs.remove_dir(&nested).unwrap();
        assert!(!fs.exists(&nested));

        let _ = fs.remove_dir_all(&root);
    }

    #[test]
    fn real_filesystem_walks_missing_directories_as_empty() {
        let fs = RealFileSystem;
        let missing = temp_dir("missing").join("not-there");

        assert!(fs.walk_files(&missing).unwrap().is_empty());
    }

    #[test]
    fn real_filesystem_reflink_reports_missing_sources() {
        let root = temp_dir("reflink-missing");
        let fs = RealFileSystem;
        let missing = root.join("missing.bin");
        let dst = root.join("dst.bin");

        let err = fs.copy_reflink(&missing, &dst).unwrap_err();

        assert!(err.to_string().contains("reflink copy failed"));
        let _ = fs.remove_dir_all(&root);
    }
}
