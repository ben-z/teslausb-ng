use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{Error, Result};
use crate::filesystem::FileSystem;

pub const RECOVERY_DIRECTORY: &str = "recovery";

pub fn validate_camera_image(fs: &impl FileSystem, path: &Path) -> Result<()> {
    if !fs.exists(path) {
        return Err(Error::new(format!(
            "camera disk not found: {}; run 'teslausb init'",
            path.display()
        )));
    }
    if fs.is_dir(path) {
        return Err(Error::new(format!(
            "camera disk is a directory: {}",
            path.display()
        )));
    }
    if fs.file_size(path)? == 0 {
        return Err(Error::new(format!(
            "camera disk is empty: {}; run 'teslausb init'",
            path.display()
        )));
    }
    Ok(())
}

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
struct SnapshotInner<L> {
    snapshots: HashMap<u64, Snapshot>,
    next_id: u64,
    process_locks: HashMap<u64, L>,
}

#[derive(Debug)]
pub struct SnapshotManager<F: FileSystem> {
    fs: F,
    cam_disk_path: PathBuf,
    snapshots_path: PathBuf,
    inner: Arc<Mutex<SnapshotInner<F::Lock>>>,
}

impl<F: FileSystem> Clone for SnapshotManager<F> {
    fn clone(&self) -> Self {
        Self {
            fs: self.fs.clone(),
            cam_disk_path: self.cam_disk_path.clone(),
            snapshots_path: self.snapshots_path.clone(),
            inner: self.inner.clone(),
        }
    }
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
                process_locks: HashMap::new(),
            })),
        };
        manager.fs.create_dir_all(&manager.snapshots_path)?;
        let _catalog_lock = manager.lock_catalog()?;
        manager.load_snapshots(&mut manager.inner.lock().unwrap(), false)?;
        Ok(manager)
    }

    fn lock_catalog(&self) -> Result<F::Lock> {
        // Keep this inode outside snapshot directories so deletion cannot replace it.
        self.fs
            .try_lock(&self.snapshots_path.join(".snapshots.lock"))?
            .ok_or_else(|| Error::new("snapshot catalog is in use by another operation"))
    }

    // The caller holds the catalog lock for the entire scan and any subsequent mutation.
    fn load_snapshots(
        &self,
        inner: &mut SnapshotInner<F::Lock>,
        recover_incomplete: bool,
    ) -> Result<()> {
        let mut loaded = HashMap::new();
        let mut next_id = inner.next_id;
        let counter_path = self.snapshots_path.join(".next-id");
        if self.fs.exists(&counter_path) {
            let counter = self.fs.read_text(&counter_path)?;
            let counter = counter
                .trim()
                .parse::<u64>()
                .map_err(|error| Error::new(format!("invalid snapshot ID counter: {}", error)))?;
            next_id = next_id.max(counter);
        }
        for name in self.fs.list_dir_names(&self.snapshots_path)? {
            let Some(id_part) = name.strip_prefix("snap-") else {
                continue;
            };
            let Ok(id) = id_part.parse::<u64>() else {
                eprintln!("warning: invalid snapshot directory name: {}", name);
                continue;
            };
            let path = self.snapshots_path.join(&name);
            if !self.fs.is_dir(&path) {
                continue;
            }
            next_id = next_id.max(
                id.checked_add(1)
                    .ok_or_else(|| Error::new("snapshot ID space is exhausted"))?,
            );

            if !self.fs.exists(&path.join("snap.toc")) {
                if recover_incomplete {
                    if let Some(_lock) = self.fs.try_lock(&path.join("snap.lock"))? {
                        eprintln!("warning: cleaning up incomplete snapshot {}", id);
                        self.fs.remove_dir_all(&path)?;
                    }
                }
                continue;
            }

            let mut snapshot = match inner.snapshots.get(&id) {
                Some(snapshot) => snapshot.clone(),
                None => Snapshot {
                    id,
                    path,
                    created_secs: self
                        .fs
                        .mtime_secs(&self.snapshots_path.join(&name).join("snap.bin"))?,
                    refcount: 0,
                    externally_locked: false,
                },
            };
            if !self.fs.exists(&snapshot.image_path()) {
                return Err(Error::new(format!(
                    "complete snapshot {} is missing its camera image",
                    id
                )));
            }
            snapshot.externally_locked = if inner.process_locks.contains_key(&id) {
                false
            } else {
                self.fs.try_lock(&snapshot.lock_path())?.is_none()
            };
            loaded.insert(id, snapshot);
        }
        for id in inner.process_locks.keys() {
            if !loaded.contains_key(id) {
                return Err(Error::new(format!("active snapshot {} disappeared", id)));
            }
        }
        inner.snapshots = loaded;
        inner.next_id = next_id;
        Ok(())
    }

    pub fn recover_incomplete(&self) -> Result<()> {
        let _catalog_lock = self.lock_catalog()?;
        self.load_snapshots(&mut self.inner.lock().unwrap(), true)
    }

    pub fn create_snapshot(&self) -> Result<Snapshot> {
        let _catalog_lock = self.lock_catalog()?;
        if self
            .fs
            .list_dir_names(&self.snapshots_path)?
            .iter()
            .any(|name| name == RECOVERY_DIRECTORY)
        {
            return Err(Error::new(format!(
                "snapshot recovery evidence already exists at {}; inspect retained files before archiving again",
                self.snapshots_path.join(RECOVERY_DIRECTORY).display()
            )));
        }
        validate_camera_image(&self.fs, &self.cam_disk_path)?;
        let mut inner = self.inner.lock().unwrap();
        self.load_snapshots(&mut inner, true)?;
        let snap_id = inner.next_id;
        let next_id = snap_id
            .checked_add(1)
            .ok_or_else(|| Error::new("snapshot ID space is exhausted"))?;
        self.fs
            .write_text_atomic(&self.snapshots_path.join(".next-id"), &next_id.to_string())?;
        let snap_path = self.snapshots_path.join(format!("snap-{snap_id:06}"));
        self.fs.create_dir_all(&snap_path)?;

        let snapshot = Snapshot {
            id: snap_id,
            path: snap_path,
            created_secs: now_secs(),
            refcount: 0,
            externally_locked: false,
        };
        if let Err(error) = self
            .fs
            .copy_reflink(&self.cam_disk_path, &snapshot.image_path())
        {
            self.fs
                .remove_dir_all(&snapshot.path)
                .map_err(|cleanup_error| {
                    Error::new(format!(
                        "failed to copy cam disk: {}; failed to remove incomplete snapshot: {}",
                        error, cleanup_error
                    ))
                })?;
            return Err(error.context("failed to copy cam disk"));
        }
        self.write_metadata(&snapshot)?;
        self.fs.write_text_atomic(&snapshot.toc_path(), "")?;
        self.fs.sync_dir(&snapshot.path)?;
        self.fs.sync_dir(&self.snapshots_path)?;
        inner.next_id = next_id;
        inner.snapshots.insert(snapshot.id, snapshot.clone());
        Ok(snapshot)
    }

    fn write_metadata(&self, snapshot: &Snapshot) -> Result<()> {
        let metadata = serde_json::json!({
            "id": snapshot.id,
            "path": snapshot.path,
            "created_at_unix": snapshot.created_secs,
        });
        self.fs
            .write_text_atomic(&snapshot.metadata_path(), &metadata.to_string())
    }

    pub fn acquire(&self, snapshot_id: u64) -> Result<SnapshotHandle<F>> {
        let _catalog_lock = self.lock_catalog()?;
        let mut inner = self.inner.lock().unwrap();
        self.load_snapshots(&mut inner, false)?;
        let snapshot = inner
            .snapshots
            .get(&snapshot_id)
            .ok_or_else(|| Error::new(format!("snapshot {} not found", snapshot_id)))?;
        if snapshot.refcount == 0 {
            let lock = self.fs.try_lock(&snapshot.lock_path())?.ok_or_else(|| {
                Error::new(format!(
                    "snapshot {} is locked by another process",
                    snapshot_id
                ))
            })?;
            inner.process_locks.insert(snapshot_id, lock);
        }
        let snapshot = inner.snapshots.get_mut(&snapshot_id).unwrap();
        snapshot.refcount += 1;
        snapshot.externally_locked = false;
        Ok(SnapshotHandle {
            manager: self.clone(),
            snapshot: snapshot.clone(),
            released: false,
        })
    }

    fn release(&self, snapshot_id: u64) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(snapshot) = inner.snapshots.get_mut(&snapshot_id) {
            snapshot.refcount -= 1;
            if snapshot.refcount == 0 {
                inner.process_locks.remove(&snapshot_id);
            }
        }
    }

    pub fn get_snapshots(&self) -> Result<Vec<Snapshot>> {
        let _catalog_lock = self.lock_catalog()?;
        let mut inner = self.inner.lock().unwrap();
        self.load_snapshots(&mut inner, false)?;
        let mut snapshots: Vec<_> = inner.snapshots.values().cloned().collect();
        snapshots.sort_by_key(|snapshot| (snapshot.created_secs, snapshot.id));
        Ok(snapshots)
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

    pub fn get_deletable_snapshots(&self) -> Result<Vec<Snapshot>> {
        Ok(self
            .get_snapshots()?
            .into_iter()
            .filter(Snapshot::is_deletable)
            .collect())
    }

    pub fn delete_snapshot(&self, snapshot_id: u64) -> Result<bool> {
        let _catalog_lock = self.lock_catalog()?;
        let mut inner = self.inner.lock().unwrap();
        self.load_snapshots(&mut inner, false)?;
        let Some(snapshot) = inner.snapshots.get(&snapshot_id) else {
            return Ok(false);
        };
        if snapshot.refcount > 0 {
            return Err(Error::new(format!("snapshot {} is in use", snapshot_id)));
        }
        let _snapshot_lock = self
            .fs
            .try_lock(&snapshot.lock_path())?
            .ok_or_else(|| Error::new(format!("snapshot {} is in use", snapshot_id)))?;

        self.fs.remove_file(&snapshot.toc_path())?;
        self.fs.sync_dir(&snapshot.path)?;
        self.fs.remove_dir_all(&snapshot.path)?;
        self.fs.sync_dir(&self.snapshots_path)?;
        inner.snapshots.remove(&snapshot_id);
        Ok(true)
    }

    pub fn delete_oldest_if_deletable(&self) -> Result<bool> {
        let Some(oldest) = self.get_deletable_snapshots()?.into_iter().next() else {
            return Ok(false);
        };
        self.delete_snapshot(oldest.id)
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

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use crate::filesystem::MockFileSystem;

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
    fn retained_recovery_evidence_prevents_new_snapshots_and_id_allocation() {
        let manager = manager();
        let recovery = manager.snapshots_path.join(RECOVERY_DIRECTORY);
        manager.fs.create_dir_all(&recovery).unwrap();
        manager
            .fs
            .write_bytes(recovery.join("raw.bin"), b"original");
        manager
            .fs
            .write_bytes(recovery.join("recovered.bin"), b"replayed");
        let catalog_before = manager.fs.list_dir_names(&manager.snapshots_path).unwrap();

        for _ in 0..3 {
            manager
                .fs
                .write_bytes(&manager.cam_disk_path, b"new recording");
            let error = manager.create_snapshot().unwrap_err().to_string();
            assert!(error.contains("recovery evidence already exists"));
            assert!(error.contains(recovery.to_str().unwrap()));
            assert_eq!(
                manager.fs.list_dir_names(&manager.snapshots_path).unwrap(),
                catalog_before
            );
            assert!(!manager.fs.exists(&manager.snapshots_path.join(".next-id")));
            assert_eq!(
                manager.fs.read_bytes(recovery.join("raw.bin")).unwrap(),
                b"original"
            );
            assert_eq!(
                manager
                    .fs
                    .read_bytes(recovery.join("recovered.bin"))
                    .unwrap(),
                b"replayed"
            );
        }

        manager.fs.remove_dir_all(&recovery).unwrap();
        let snapshot = manager.create_snapshot().unwrap();
        assert_eq!(snapshot.id, 0);
        assert_eq!(
            manager.fs.read_bytes(snapshot.image_path()).unwrap(),
            b"new recording"
        );
    }

    #[test]
    fn invalid_camera_image_cannot_publish_or_recover_snapshots() {
        for condition in ["missing", "empty", "directory"] {
            let manager = manager();
            manager.fs.remove_file(&manager.cam_disk_path).unwrap();
            match condition {
                "empty" => manager.fs.write_bytes(&manager.cam_disk_path, b""),
                "directory" => manager.fs.create_dir_all(&manager.cam_disk_path).unwrap(),
                _ => {}
            }
            let incomplete = manager.snapshots_path.join("snap-000000");
            manager.fs.create_dir_all(&incomplete).unwrap();
            manager
                .fs
                .write_bytes(incomplete.join("snap.bin"), b"recoverable");

            let error = manager.create_snapshot().unwrap_err().to_string();
            assert!(error.contains("camera disk"), "{condition}: {error}");
            assert!(manager.fs.exists(&incomplete.join("snap.bin")));
            assert!(!manager.fs.exists(&incomplete.join("snap.toc")));
            assert!(!manager.fs.exists(&manager.snapshots_path.join(".next-id")));
        }
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
        assert_eq!(manager.get_snapshots().unwrap().len(), 3);
    }

    #[test]
    fn inspecting_an_incomplete_snapshot_does_not_remove_it() {
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

        assert!(manager.get_snapshots().unwrap().is_empty());
        assert!(fs.exists(Path::new("/backingfiles/snapshots/snap-000001")));
        manager.recover_incomplete().unwrap();
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
    fn failure_to_remove_completion_marker_preserves_the_snapshot() {
        let manager = manager();
        let snapshot = manager.create_snapshot().unwrap();
        manager.fs.fail_removal(&snapshot.toc_path());
        assert!(manager.delete_snapshot(snapshot.id).is_err());
        assert!(manager.fs.exists(&snapshot.toc_path()));
        assert!(manager.fs.exists(&snapshot.image_path()));
        assert_eq!(manager.get_snapshots().unwrap().len(), 1);
    }

    #[test]
    fn failed_snapshot_removal_is_reported_and_recovered() {
        let manager = manager();
        let snapshot = manager.create_snapshot().unwrap();
        manager.fs.fail_removal(&snapshot.path);
        assert!(manager.delete_snapshot(snapshot.id).is_err());
        assert!(!manager.fs.exists(&snapshot.toc_path()));
        assert!(manager.fs.exists(&snapshot.image_path()));
        assert!(manager.get_snapshots().unwrap().is_empty());
        assert!(manager.recover_incomplete().is_err());

        manager.fs.allow_removal(&snapshot.path);
        manager.recover_incomplete().unwrap();
        assert!(manager.get_snapshots().unwrap().is_empty());
        assert!(!manager.fs.exists(&snapshot.path));
    }

    #[test]
    fn failed_reflink_does_not_publish_a_snapshot_or_reuse_its_id() {
        let manager = manager();
        manager.fs.fail_next_reflink();
        assert!(manager
            .create_snapshot()
            .unwrap_err()
            .to_string()
            .contains("injected reflink failure"));
        assert!(manager.get_snapshots().unwrap().is_empty());
        assert_eq!(manager.create_snapshot().unwrap().id, 1);
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
            .unwrap()
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

        assert_eq!(manager2.get_snapshots().unwrap().len(), 2);
        assert_eq!(manager2.create_snapshot().unwrap().id, 2);
    }

    #[test]
    fn interrupted_deletion_without_toc_is_completed_during_recovery() {
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

        assert!(manager.get_snapshots().unwrap().is_empty());
        assert!(fs.exists(Path::new("/backingfiles/snapshots/snap-000002")));
        manager.recover_incomplete().unwrap();
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

        let snapshots = manager.get_snapshots().unwrap();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].id, 3);
        assert_eq!(snapshots[0].state(), SnapshotState::Ready);
        assert_eq!(snapshots[0].refcount, 0);
    }

    #[test]
    fn legacy_snapshot_without_metadata_can_be_inspected_without_writing_metadata() {
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

        let snapshots = manager.get_snapshots().unwrap();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].id, 5);
        assert!(!fs.exists(&snap_path.join("metadata.json")));
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

        assert!(manager.get_snapshots().unwrap().is_empty());
    }

    #[test]
    fn process_lock_blocks_other_managers_until_handle_release() {
        let fs = MockFileSystem::new();
        let manager1 = manager_with_fs(fs.clone());
        manager1.create_snapshot().unwrap();
        let mut handle = manager1.acquire(0).unwrap();
        let manager2 = manager_with_fs(fs);

        let snapshot = manager2.get_snapshots().unwrap().pop().unwrap();
        assert_eq!(snapshot.state(), SnapshotState::Archiving);
        assert!(manager2.get_deletable_snapshots().unwrap().is_empty());
        assert!(manager2.acquire(0).is_err());
        assert!(manager2.delete_snapshot(0).is_err());

        handle.release();
        assert_eq!(manager2.get_deletable_snapshots().unwrap()[0].id, 0);
        assert!(manager2.delete_snapshot(0).unwrap());
        assert!(manager1.acquire(0).is_err());
        assert!(manager1.get_snapshots().unwrap().is_empty());
    }

    #[test]
    fn separate_managers_allocate_distinct_snapshot_ids() {
        let fs = MockFileSystem::new();
        let manager1 = manager_with_fs(fs.clone());
        let manager2 = manager_with_fs(fs);
        assert_eq!(manager1.create_snapshot().unwrap().id, 0);
        assert_eq!(manager2.create_snapshot().unwrap().id, 1);
        assert_eq!(manager1.create_snapshot().unwrap().id, 2);
        assert_eq!(manager2.get_snapshots().unwrap().len(), 3);
    }

    #[test]
    fn deleted_snapshot_ids_are_not_reused_by_stale_or_new_managers() {
        let fs = MockFileSystem::new();
        let first = manager_with_fs(fs.clone());
        let stale = manager_with_fs(fs.clone());
        let snapshot = first.create_snapshot().unwrap();
        first.delete_snapshot(snapshot.id).unwrap();
        assert_eq!(stale.create_snapshot().unwrap().id, 1);
        assert!(first.acquire(snapshot.id).is_err());
        stale.delete_snapshot(1).unwrap();
        assert_eq!(manager_with_fs(fs).create_snapshot().unwrap().id, 2);
    }

    #[test]
    fn corrupt_snapshot_id_counter_fails_loudly() {
        let manager = manager();
        manager
            .fs
            .write_bytes(manager.snapshots_path.join(".next-id"), b"broken");
        assert!(manager.create_snapshot().is_err());
        assert!(manager.get_snapshots().is_err());
    }

    #[test]
    fn catalog_lock_protects_snapshot_being_created() {
        let fs = MockFileSystem::new();
        let manager = manager_with_fs(fs.clone());
        let catalog_lock = manager.lock_catalog().unwrap();
        let path = Path::new("/backingfiles/snapshots/snap-000000");
        fs.create_dir_all(path).unwrap();
        fs.write_bytes(path.join("snap.bin"), b"partial");

        assert!(SnapshotManager::new(
            fs.clone(),
            manager.cam_disk_path.clone(),
            manager.snapshots_path.clone()
        )
        .is_err());
        assert!(manager.get_snapshots().is_err());
        assert!(manager.create_snapshot().is_err());
        assert!(manager.delete_snapshot(0).is_err());
        assert!(manager.acquire(0).is_err());
        assert!(fs.exists(&path.join("snap.bin")));

        drop(catalog_lock);
        assert!(manager.get_snapshots().unwrap().is_empty());
        assert!(fs.exists(path));
        manager.recover_incomplete().unwrap();
        assert!(!fs.exists(path));
        assert_eq!(manager.create_snapshot().unwrap().id, 1);
    }

    #[test]
    fn locked_incomplete_snapshot_is_preserved() {
        let fs = MockFileSystem::new();
        let manager = manager_with_fs(fs.clone());
        let path = Path::new("/backingfiles/snapshots/snap-000000");
        fs.create_dir_all(path).unwrap();
        fs.write_bytes(path.join("snap.bin"), b"partial");
        let lock = fs.try_lock(&path.join("snap.lock")).unwrap().unwrap();

        assert!(manager.get_snapshots().unwrap().is_empty());
        assert!(fs.exists(&path.join("snap.bin")));
        drop(lock);
        assert!(manager.get_snapshots().unwrap().is_empty());
        manager.recover_incomplete().unwrap();
        assert!(!fs.exists(path));
    }

    #[test]
    fn complete_snapshot_missing_image_fails_loudly() {
        let fs = MockFileSystem::new();
        let manager = manager_with_fs(fs.clone());
        let snapshot = manager.create_snapshot().unwrap();
        fs.remove_file(&snapshot.image_path()).unwrap();
        assert!(manager.get_snapshots().is_err());
        assert!(manager.acquire(snapshot.id).is_err());
        assert!(fs.exists(&snapshot.toc_path()));
    }
}
