use std::collections::HashMap;
use std::fs::{File, OpenOptions};
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{Error, Result};
use crate::filesystem::FileSystem;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotState {
    Ready,
    Archiving,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub id: u64,
    pub path: PathBuf,
    pub created_secs: u64,
    pub refcount: u32,
    pub externally_locked: bool,
}

impl Snapshot {
    pub fn image_path(&self) -> PathBuf {
        self.path.join("snap.bin")
    }

    pub fn toc_path(&self) -> PathBuf {
        self.path.join("snap.toc")
    }

    pub fn metadata_path(&self) -> PathBuf {
        self.path.join("metadata.json")
    }

    pub fn lock_path(&self) -> PathBuf {
        self.path.join("snap.lock")
    }

    pub fn state(&self) -> SnapshotState {
        if self.refcount > 0 || self.externally_locked {
            SnapshotState::Archiving
        } else {
            SnapshotState::Ready
        }
    }

    pub fn is_deletable(&self) -> bool {
        self.refcount == 0 && !self.externally_locked
    }
}

#[derive(Debug)]
struct SnapshotInner {
    snapshots: HashMap<u64, Snapshot>,
    next_id: u64,
    creating: bool,
    process_locks: HashMap<u64, File>,
}

#[derive(Debug, Clone)]
pub struct SnapshotManager<F: FileSystem> {
    fs: F,
    cam_disk_path: PathBuf,
    snapshots_path: PathBuf,
    inner: Arc<Mutex<SnapshotInner>>,
}

impl<F: FileSystem> SnapshotManager<F> {
    pub fn new(fs: F, cam_disk_path: PathBuf, snapshots_path: PathBuf) -> Result<Self> {
        let manager = Self {
            fs,
            cam_disk_path,
            snapshots_path,
            inner: Arc::new(Mutex::new(SnapshotInner {
                snapshots: HashMap::new(),
                next_id: 0,
                creating: false,
                process_locks: HashMap::new(),
            })),
        };
        manager.load_snapshots()?;
        Ok(manager)
    }

    fn load_snapshots(&self) -> Result<()> {
        if !self.fs.exists(&self.snapshots_path) {
            self.fs.create_dir_all(&self.snapshots_path)?;
            return Ok(());
        }

        let mut loaded = HashMap::new();
        let mut next_id = 0;

        for name in self.fs.list_dir_names(&self.snapshots_path)? {
            let Some(id_part) = name.strip_prefix("snap-") else {
                continue;
            };
            let Ok(id) = id_part.parse::<u64>() else {
                eprintln!("warning: invalid snapshot directory name: {}", name);
                continue;
            };
            let snap_path = self.snapshots_path.join(&name);
            if !self.fs.is_dir(&snap_path) {
                continue;
            }

            let toc_path = snap_path.join("snap.toc");
            if !self.fs.exists(&toc_path) {
                eprintln!("warning: cleaning up incomplete snapshot {}", id);
                self.remove_snapshot_dir(&snap_path);
                continue;
            }

            let snapshot = self.reconstruct_snapshot(id, &snap_path);
            if !self.fs.exists(&snapshot.metadata_path()) {
                self.write_metadata(&snapshot)?;
            }
            next_id = next_id.max(id + 1);
            loaded.insert(id, snapshot);
        }

        let mut inner = self.inner.lock().unwrap();
        inner.snapshots = loaded;
        inner.next_id = next_id;
        Ok(())
    }

    fn reconstruct_snapshot(&self, id: u64, path: &Path) -> Snapshot {
        let image_path = path.join("snap.bin");
        let created_secs = self
            .fs
            .mtime_secs(&image_path)
            .unwrap_or_else(|_| now_secs());
        Snapshot {
            id,
            path: path.to_path_buf(),
            created_secs,
            refcount: 0,
            externally_locked: false,
        }
    }

    fn remove_snapshot_dir(&self, path: &Path) {
        if self.fs.exists(path) {
            if let Err(err) = self.fs.remove_dir_all(path) {
                eprintln!("error: failed to remove {}: {}", path.display(), err);
            }
        }
    }

    pub fn create_snapshot(&self) -> Result<Snapshot> {
        let snap_id = {
            let mut inner = self.inner.lock().unwrap();
            if inner.creating {
                return Err(Error::new("snapshot creation already in progress"));
            }
            inner.creating = true;
            inner.next_id
        };

        let result = self.create_snapshot_locked_outside(snap_id);
        let mut inner = self.inner.lock().unwrap();
        inner.creating = false;
        if let Ok(snapshot) = &result {
            inner.next_id = snapshot.id + 1;
            inner.snapshots.insert(snapshot.id, snapshot.clone());
        }
        result
    }

    fn create_snapshot_locked_outside(&self, snap_id: u64) -> Result<Snapshot> {
        let snap_path = self.snapshots_path.join(format!("snap-{snap_id:06}"));
        self.fs.create_dir_all(&snap_path)?;

        let image_path = snap_path.join("snap.bin");
        if let Err(err) = self.fs.copy_reflink(&self.cam_disk_path, &image_path) {
            self.remove_snapshot_dir(&snap_path);
            return Err(err.context("failed to copy cam disk"));
        }

        let snapshot = Snapshot {
            id: snap_id,
            path: snap_path,
            created_secs: now_secs(),
            refcount: 0,
            externally_locked: false,
        };

        self.write_metadata(&snapshot)?;
        self.fs.write_text_atomic(&snapshot.toc_path(), "")?;
        self.fs.sync_dir(&snapshot.path)?;
        Ok(snapshot)
    }

    fn write_metadata(&self, snapshot: &Snapshot) -> Result<()> {
        let metadata = format!(
            "{{\n  \"id\": {},\n  \"path\": \"{}\",\n  \"created_at_unix\": {}\n}}\n",
            snapshot.id,
            json_escape(&snapshot.path.display().to_string()),
            snapshot.created_secs
        );
        self.fs
            .write_text_atomic(&snapshot.metadata_path(), &metadata)
    }

    pub fn acquire(&self, snapshot_id: u64) -> Result<SnapshotHandle<F>> {
        let snapshot = {
            let mut inner = self.inner.lock().unwrap();
            let snapshot = inner
                .snapshots
                .get(&snapshot_id)
                .ok_or_else(|| Error::new(format!("snapshot {} not found", snapshot_id)))?;
            if snapshot.refcount == 0 {
                self.refresh_external_lock_locked(&mut inner, snapshot_id)?;
                let snapshot = inner.snapshots.get(&snapshot_id).unwrap().clone();
                if snapshot.externally_locked {
                    return Err(Error::new(format!(
                        "snapshot {} is locked by another process",
                        snapshot_id
                    )));
                }
                self.acquire_process_lock_locked(&mut inner, &snapshot)?;
            }

            let snapshot = inner.snapshots.get_mut(&snapshot_id).unwrap();
            snapshot.refcount += 1;
            snapshot.externally_locked = false;
            snapshot.clone()
        };
        Ok(SnapshotHandle {
            manager: self.clone(),
            snapshot,
            released: false,
        })
    }

    fn release(&self, snapshot_id: u64) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(snapshot) = inner.snapshots.get_mut(&snapshot_id) {
            snapshot.refcount = snapshot.refcount.saturating_sub(1);
            if snapshot.refcount == 0 {
                self.release_process_lock_locked(&mut inner, snapshot_id);
            }
        }
    }

    pub fn get_snapshots(&self) -> Vec<Snapshot> {
        let mut inner = self.inner.lock().unwrap();
        let ids = inner.snapshots.keys().copied().collect::<Vec<_>>();
        for snapshot_id in ids {
            if let Err(err) = self.refresh_external_lock_locked(&mut inner, snapshot_id) {
                eprintln!(
                    "warning: failed to refresh lock for snapshot {}: {}",
                    snapshot_id, err
                );
            }
        }
        let mut snapshots: Vec<_> = inner.snapshots.values().cloned().collect();
        snapshots.sort_by_key(|snapshot| (snapshot.created_secs, snapshot.id));
        snapshots
    }

    #[cfg(test)]
    pub fn get_snapshot(&self, snapshot_id: u64) -> Option<Snapshot> {
        self.inner
            .lock()
            .unwrap()
            .snapshots
            .get(&snapshot_id)
            .cloned()
    }

    pub fn get_deletable_snapshots(&self) -> Vec<Snapshot> {
        self.get_snapshots()
            .into_iter()
            .filter(Snapshot::is_deletable)
            .collect()
    }

    pub fn delete_snapshot(&self, snapshot_id: u64) -> Result<bool> {
        let snapshot = {
            let mut inner = self.inner.lock().unwrap();
            if !inner.snapshots.contains_key(&snapshot_id) {
                return Ok(false);
            }
            self.refresh_external_lock_locked(&mut inner, snapshot_id)?;
            let snapshot = inner.snapshots.get(&snapshot_id).unwrap().clone();
            if !snapshot.is_deletable() {
                return Err(Error::new(format!("snapshot {} is in use", snapshot_id)));
            }
            inner.snapshots.remove(&snapshot_id);
            snapshot
        };

        if self.fs.exists(&snapshot.toc_path()) {
            if let Err(err) = self.fs.remove_file(&snapshot.toc_path()) {
                eprintln!(
                    "warning: failed to remove {}: {}",
                    snapshot.toc_path().display(),
                    err
                );
            }
        }
        let _ = self.fs.sync_dir(&snapshot.path);
        self.remove_snapshot_dir(&snapshot.path);
        Ok(true)
    }

    pub fn delete_oldest_if_deletable(&self) -> Result<bool> {
        let Some(oldest) = self.get_deletable_snapshots().into_iter().next() else {
            return Ok(false);
        };
        self.delete_snapshot(oldest.id)
    }

    fn acquire_process_lock_locked(
        &self,
        inner: &mut SnapshotInner,
        snapshot: &Snapshot,
    ) -> Result<()> {
        if !self.fs.supports_process_locks() || inner.process_locks.contains_key(&snapshot.id) {
            return Ok(());
        }

        let lock_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(snapshot.lock_path())?;
        if !try_lock_exclusive(&lock_file)? {
            return Err(Error::new(format!(
                "snapshot {} is locked by another process",
                snapshot.id
            )));
        }
        lock_file.set_len(0)?;
        use std::io::Write;
        writeln!(&lock_file, "{}", std::process::id())?;
        inner.process_locks.insert(snapshot.id, lock_file);
        Ok(())
    }

    fn release_process_lock_locked(&self, inner: &mut SnapshotInner, snapshot_id: u64) {
        let Some(lock_file) = inner.process_locks.remove(&snapshot_id) else {
            return;
        };
        if let Err(err) = unlock_file(&lock_file) {
            eprintln!(
                "warning: failed to release lock for snapshot {}: {}",
                snapshot_id, err
            );
        }
    }

    fn refresh_external_lock_locked(
        &self,
        inner: &mut SnapshotInner,
        snapshot_id: u64,
    ) -> Result<()> {
        if !self.fs.supports_process_locks() || inner.process_locks.contains_key(&snapshot_id) {
            if let Some(snapshot) = inner.snapshots.get_mut(&snapshot_id) {
                snapshot.externally_locked = false;
            }
            return Ok(());
        }

        let Some(snapshot) = inner.snapshots.get(&snapshot_id).cloned() else {
            return Ok(());
        };
        let locked = self.is_locked_by_other_process(&snapshot)?;
        if let Some(snapshot) = inner.snapshots.get_mut(&snapshot_id) {
            snapshot.externally_locked = locked;
        }
        Ok(())
    }

    fn is_locked_by_other_process(&self, snapshot: &Snapshot) -> Result<bool> {
        if !snapshot.lock_path().exists() {
            return Ok(false);
        }
        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(snapshot.lock_path())?;
        let locked = !try_lock_exclusive(&lock_file)?;
        if !locked {
            unlock_file(&lock_file)?;
        }
        Ok(locked)
    }
}

#[derive(Debug)]
pub struct SnapshotHandle<F: FileSystem> {
    manager: SnapshotManager<F>,
    snapshot: Snapshot,
    released: bool,
}

impl<F: FileSystem> SnapshotHandle<F> {
    pub fn snapshot(&self) -> Result<&Snapshot> {
        if self.released {
            Err(Error::new("snapshot handle has been released"))
        } else {
            Ok(&self.snapshot)
        }
    }

    pub fn release(&mut self) {
        if !self.released {
            self.manager.release(self.snapshot.id);
            self.released = true;
        }
    }
}

impl<F: FileSystem> Drop for SnapshotHandle<F> {
    fn drop(&mut self) {
        self.release();
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn json_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(unix)]
fn try_lock_exclusive(file: &File) -> std::io::Result<bool> {
    const LOCK_EX: i32 = 2;
    const LOCK_NB: i32 = 4;

    unsafe extern "C" {
        fn flock(fd: i32, operation: i32) -> i32;
    }

    let result = unsafe { flock(file.as_raw_fd(), LOCK_EX | LOCK_NB) };
    if result == 0 {
        Ok(true)
    } else {
        let err = std::io::Error::last_os_error();
        if err.kind() == std::io::ErrorKind::WouldBlock {
            Ok(false)
        } else {
            Err(err)
        }
    }
}

#[cfg(not(unix))]
fn try_lock_exclusive(_file: &File) -> std::io::Result<bool> {
    Ok(true)
}

#[cfg(unix)]
fn unlock_file(file: &File) -> std::io::Result<()> {
    const LOCK_UN: i32 = 8;

    unsafe extern "C" {
        fn flock(fd: i32, operation: i32) -> i32;
    }

    let result = unsafe { flock(file.as_raw_fd(), LOCK_UN) };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(unix))]
fn unlock_file(_file: &File) -> std::io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::filesystem::{MockFileSystem, RealFileSystem};

    use super::*;

    fn manager() -> SnapshotManager<MockFileSystem> {
        let fs = MockFileSystem::new();
        manager_with_fs(fs)
    }

    fn manager_with_fs(fs: MockFileSystem) -> SnapshotManager<MockFileSystem> {
        fs.create_dir_all(Path::new("/backingfiles/snapshots"))
            .unwrap();
        fs.write_bytes("/backingfiles/cam_disk.bin", b"cam");
        SnapshotManager::new(
            fs,
            PathBuf::from("/backingfiles/cam_disk.bin"),
            PathBuf::from("/backingfiles/snapshots"),
        )
        .unwrap()
    }

    #[test]
    fn creates_complete_snapshot_with_toc() {
        let manager = manager();
        let snapshot = manager.create_snapshot().unwrap();
        assert_eq!(snapshot.id, 0);
        assert_eq!(snapshot.state(), SnapshotState::Ready);
        assert!(snapshot.is_deletable());
        assert!(manager.fs.exists(&snapshot.image_path()));
        assert!(manager.fs.exists(&snapshot.toc_path()));
        assert!(manager.fs.exists(&snapshot.metadata_path()));
    }

    #[test]
    fn snapshot_paths_are_stable() {
        let snapshot = Snapshot {
            id: 1,
            path: PathBuf::from("/backingfiles/snapshots/snap-000001"),
            created_secs: 0,
            refcount: 0,
            externally_locked: false,
        };

        assert_eq!(
            snapshot.image_path(),
            PathBuf::from("/backingfiles/snapshots/snap-000001/snap.bin")
        );
        assert_eq!(
            snapshot.toc_path(),
            PathBuf::from("/backingfiles/snapshots/snap-000001/snap.toc")
        );
        assert_eq!(
            snapshot.metadata_path(),
            PathBuf::from("/backingfiles/snapshots/snap-000001/metadata.json")
        );
        assert_eq!(
            snapshot.lock_path(),
            PathBuf::from("/backingfiles/snapshots/snap-000001/snap.lock")
        );
    }

    #[test]
    fn creates_multiple_snapshots_with_monotonic_ids() {
        let manager = manager();
        let first = manager.create_snapshot().unwrap();
        let second = manager.create_snapshot().unwrap();
        let third = manager.create_snapshot().unwrap();

        assert_eq!((first.id, second.id, third.id), (0, 1, 2));
        assert_eq!(manager.get_snapshots().len(), 3);
    }

    #[test]
    fn cleanup_incomplete_snapshot_without_toc_on_load() {
        let fs = MockFileSystem::new();
        fs.create_dir_all(Path::new("/backingfiles/snapshots/snap-000001"))
            .unwrap();
        fs.write_bytes("/backingfiles/snapshots/snap-000001/snap.bin", b"partial");
        fs.write_bytes("/backingfiles/cam_disk.bin", b"cam");

        let manager = SnapshotManager::new(
            fs.clone(),
            PathBuf::from("/backingfiles/cam_disk.bin"),
            PathBuf::from("/backingfiles/snapshots"),
        )
        .unwrap();

        assert!(manager.get_snapshots().is_empty());
        assert!(!fs.exists(Path::new("/backingfiles/snapshots/snap-000001")));
    }

    #[test]
    fn handle_prevents_delete_until_drop() {
        let manager = manager();
        let snapshot = manager.create_snapshot().unwrap();
        let handle = manager.acquire(snapshot.id).unwrap();
        assert!(manager.delete_snapshot(snapshot.id).is_err());
        drop(handle);
        assert!(manager.delete_snapshot(snapshot.id).unwrap());
    }

    #[test]
    fn acquire_nonexistent_snapshot_returns_error() {
        let manager = manager();
        assert!(manager.acquire(999).is_err());
    }

    #[test]
    fn multiple_acquires_increment_refcount_and_release_on_drop() {
        let manager = manager();
        let snapshot = manager.create_snapshot().unwrap();

        let handle1 = manager.acquire(snapshot.id).unwrap();
        assert_eq!(manager.get_snapshot(snapshot.id).unwrap().refcount, 1);
        let mut handle2 = manager.acquire(snapshot.id).unwrap();
        assert_eq!(manager.get_snapshot(snapshot.id).unwrap().refcount, 2);

        drop(handle1);
        assert_eq!(manager.get_snapshot(snapshot.id).unwrap().refcount, 1);
        assert!(manager.delete_snapshot(snapshot.id).is_err());

        handle2.release();
        handle2.release();
        assert_eq!(manager.get_snapshot(snapshot.id).unwrap().refcount, 0);
        assert!(manager.delete_snapshot(snapshot.id).unwrap());
    }

    #[test]
    fn handle_access_after_release_returns_error() {
        let manager = manager();
        let snapshot = manager.create_snapshot().unwrap();
        let mut handle = manager.acquire(snapshot.id).unwrap();

        handle.release();

        assert!(handle.snapshot().is_err());
    }

    #[test]
    fn delete_oldest_skips_in_use_snapshots() {
        let manager = manager();
        let snap1 = manager.create_snapshot().unwrap();
        let snap2 = manager.create_snapshot().unwrap();
        let snap3 = manager.create_snapshot().unwrap();
        let _handle = manager.acquire(snap1.id).unwrap();

        assert!(manager.delete_oldest_if_deletable().unwrap());

        assert!(manager.get_snapshot(snap1.id).is_some());
        assert!(manager.get_snapshot(snap2.id).is_none());
        assert!(manager.get_snapshot(snap3.id).is_some());
    }

    #[test]
    fn get_deletable_snapshots_excludes_acquired_snapshots() {
        let manager = manager();
        let snap1 = manager.create_snapshot().unwrap();
        let snap2 = manager.create_snapshot().unwrap();
        let snap3 = manager.create_snapshot().unwrap();
        let _handle = manager.acquire(snap2.id).unwrap();

        let ids = manager
            .get_deletable_snapshots()
            .into_iter()
            .map(|snapshot| snapshot.id)
            .collect::<Vec<_>>();

        assert_eq!(ids, vec![snap1.id, snap3.id]);
    }

    #[test]
    fn load_existing_snapshots_and_continue_id_sequence() {
        let fs = MockFileSystem::new();
        let manager1 = manager_with_fs(fs.clone());
        manager1.create_snapshot().unwrap();
        manager1.create_snapshot().unwrap();

        let manager2 = SnapshotManager::new(
            fs,
            PathBuf::from("/backingfiles/cam_disk.bin"),
            PathBuf::from("/backingfiles/snapshots"),
        )
        .unwrap();

        assert_eq!(manager2.get_snapshots().len(), 2);
        assert_eq!(manager2.create_snapshot().unwrap().id, 2);
    }

    #[test]
    fn interrupted_deletion_without_toc_is_completed_on_load() {
        let fs = MockFileSystem::new();
        fs.create_dir_all(Path::new("/backingfiles/snapshots/snap-000002"))
            .unwrap();
        fs.write_bytes("/backingfiles/snapshots/snap-000002/snap.bin", b"data");
        fs.write_bytes("/backingfiles/cam_disk.bin", b"cam");

        let manager = SnapshotManager::new(
            fs.clone(),
            PathBuf::from("/backingfiles/cam_disk.bin"),
            PathBuf::from("/backingfiles/snapshots"),
        )
        .unwrap();

        assert!(manager.get_snapshots().is_empty());
        assert!(!fs.exists(Path::new("/backingfiles/snapshots/snap-000002")));
    }

    #[test]
    fn complete_snapshot_with_toc_is_loaded() {
        let fs = MockFileSystem::new();
        fs.create_dir_all(Path::new("/backingfiles/snapshots/snap-000003"))
            .unwrap();
        fs.write_bytes("/backingfiles/snapshots/snap-000003/snap.bin", b"data");
        fs.write_bytes("/backingfiles/snapshots/snap-000003/snap.toc", b"");
        fs.write_bytes("/backingfiles/cam_disk.bin", b"cam");

        let manager = SnapshotManager::new(
            fs,
            PathBuf::from("/backingfiles/cam_disk.bin"),
            PathBuf::from("/backingfiles/snapshots"),
        )
        .unwrap();

        let snapshots = manager.get_snapshots();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].id, 3);
        assert_eq!(snapshots[0].state(), SnapshotState::Ready);
        assert_eq!(snapshots[0].refcount, 0);
    }

    #[test]
    fn legacy_snapshot_without_metadata_is_reconstructed_and_metadata_saved() {
        let fs = MockFileSystem::new();
        let snap_path = Path::new("/backingfiles/snapshots/snap-000005");
        fs.create_dir_all(snap_path).unwrap();
        fs.write_bytes(snap_path.join("snap.bin"), b"legacy");
        fs.write_bytes(snap_path.join("snap.toc"), b"");
        fs.write_bytes("/backingfiles/cam_disk.bin", b"cam");

        let manager = SnapshotManager::new(
            fs.clone(),
            PathBuf::from("/backingfiles/cam_disk.bin"),
            PathBuf::from("/backingfiles/snapshots"),
        )
        .unwrap();

        let snapshots = manager.get_snapshots();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].id, 5);
        assert!(fs.exists(&snap_path.join("metadata.json")));
        assert!(fs
            .read_text(snap_path.join("metadata.json"))
            .unwrap()
            .contains("\"id\": 5"));
    }

    #[test]
    fn invalid_snapshot_names_are_ignored() {
        let fs = MockFileSystem::new();
        fs.create_dir_all(Path::new("/backingfiles/snapshots/snap-not-a-number"))
            .unwrap();
        fs.write_bytes("/backingfiles/snapshots/snap-not-a-number/snap.toc", b"");
        fs.write_bytes("/backingfiles/cam_disk.bin", b"cam");

        let manager = SnapshotManager::new(
            fs,
            PathBuf::from("/backingfiles/cam_disk.bin"),
            PathBuf::from("/backingfiles/snapshots"),
        )
        .unwrap();

        assert!(manager.get_snapshots().is_empty());
    }

    #[test]
    fn real_filesystem_process_lock_blocks_other_managers() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "teslausb-snapshot-lock-{}-{suffix}",
            std::process::id()
        ));
        let snapshots_path = root.join("snapshots");
        let snap_path = snapshots_path.join("snap-000000");
        std::fs::create_dir_all(&snap_path).unwrap();
        std::fs::write(root.join("cam_disk.bin"), b"cam").unwrap();
        std::fs::write(snap_path.join("snap.bin"), b"snapshot").unwrap();
        std::fs::write(snap_path.join("snap.toc"), b"").unwrap();

        let manager1 = SnapshotManager::new(
            RealFileSystem,
            root.join("cam_disk.bin"),
            snapshots_path.clone(),
        )
        .unwrap();
        let mut handle = manager1.acquire(0).unwrap();

        let manager2 = SnapshotManager::new(
            RealFileSystem,
            root.join("cam_disk.bin"),
            snapshots_path.clone(),
        )
        .unwrap();

        let snapshot = manager2.get_snapshots().pop().unwrap();
        assert_eq!(snapshot.state(), SnapshotState::Archiving);
        assert!(!snapshot.is_deletable());
        assert!(manager2.get_deletable_snapshots().is_empty());
        assert!(manager2.acquire(0).is_err());
        assert!(manager2.delete_snapshot(0).is_err());

        handle.release();
        assert_eq!(manager2.get_deletable_snapshots()[0].id, 0);
        assert!(manager2.delete_snapshot(0).unwrap());

        let _ = std::fs::remove_dir_all(root);
    }
}
