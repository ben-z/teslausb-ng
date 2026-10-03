#![cfg(target_os = "linux")]

use std::env;
use std::ffi::CString;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);
const IMAGE_BYTES: u64 = 128 * 1024 * 1024;

unsafe extern "C" {
    fn umount(target: *const std::ffi::c_char) -> std::ffi::c_int;
}

#[derive(Clone, Copy, Debug)]
enum Format {
    Fat32,
    Ext4,
}

impl Format {
    fn name(self) -> &'static str {
        match self {
            Self::Fat32 => "fat32",
            Self::Ext4 => "ext4",
        }
    }

    fn mount_type(self) -> &'static str {
        match self {
            Self::Fat32 => "vfat",
            Self::Ext4 => "ext4",
        }
    }
}

struct Volume {
    root: PathBuf,
    image: PathBuf,
    mount: PathBuf,
    device: Option<String>,
    mounted: bool,
}

impl Volume {
    fn new(format: Format) -> Self {
        assert_eq!(
            env::var("TESLAUSB_RUN_FILESYSTEM_FAILURES").as_deref(),
            Ok("1")
        );
        assert_eq!(text(&checked("id", &["-u"])), "0", "root is required");
        let artifacts =
            PathBuf::from(env::var_os("TESLAUSB_FAILURE_ARTIFACT_DIR").expect(
                "TESLAUSB_FAILURE_ARTIFACT_DIR must name the local test artifact directory",
            ));
        fs::create_dir_all(&artifacts).unwrap();
        let artifacts = fs::canonicalize(artifacts).unwrap();
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = artifacts.join(format!(
            "{}-{}-{suffix}-{}",
            format.name(),
            std::process::id(),
            NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        let image = root.join("filesystem.img");
        let mount = root.join("mounted");
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&image)
            .unwrap()
            .set_len(IMAGE_BYTES)
            .unwrap();
        fs::create_dir(&mount).unwrap();
        let image_arg = image.to_str().unwrap();
        match format {
            Format::Fat32 => {
                checked("mkfs.vfat", &["-F", "32", "-n", "TESLACAM", image_arg]);
            }
            Format::Ext4 => {
                checked(
                    "mkfs.ext4",
                    &[
                        "-F", "-b", "4096", "-I", "256", "-m", "0",
                        "-O", "none,has_journal,ext_attr,resize_inode,dir_index,filetype,extent,flex_bg,sparse_super,large_file,huge_file,dir_nlink,extra_isize",
                        "-L",
                        "TESLACAM",
                        "-E",
                        "lazy_itable_init=0,lazy_journal_init=0",
                        image_arg,
                    ],
                );
            }
        }
        let device = text(&checked("losetup", &["--find", "--show", image_arg]));
        let mut volume = Self {
            root,
            image,
            mount,
            device: Some(device),
            mounted: false,
        };
        volume.mounted = true;
        let output = run(
            "mount",
            &[
                "-t",
                format.mount_type(),
                "-o",
                "rw",
                volume.device.as_deref().unwrap(),
                volume.mount.to_str().unwrap(),
            ],
        );
        if !output.status.success() {
            let state = run("mountpoint", &["-q", volume.mount.to_str().unwrap()]);
            if state.status.code() == Some(32) {
                volume.mounted = false;
            }
            panic!(
                "mount failed: {}; mount ownership probe: {}",
                String::from_utf8_lossy(&output.stderr),
                state.status
            );
        }
        volume
    }

    fn clean_up(&mut self) -> Result<(), String> {
        if self.mounted {
            let target = CString::new(self.mount.as_os_str().as_bytes()).unwrap();
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                if unsafe { umount(target.as_ptr()) } == 0 {
                    break;
                }
                let error = std::io::Error::last_os_error();
                if error.kind() != ErrorKind::ResourceBusy || Instant::now() >= deadline {
                    return Err(format!("unmount {}: {error}", self.mount.display()));
                }
                thread::sleep(Duration::from_millis(20));
            }
            self.mounted = false;
        }
        // An interrupted detach may have freed the loop number for another owner.
        if let Some(device) = self.device.take() {
            let output = try_run("losetup", &["-d", &device])?;
            if !output.status.success() {
                return Err(format!(
                    "detach {device}: {}",
                    String::from_utf8_lossy(&output.stderr)
                ));
            }
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                let listing = try_run("losetup", &["-j", self.image.to_str().unwrap()])?;
                if !listing.status.success() {
                    return Err(format!(
                        "inspect owned loop: {}",
                        String::from_utf8_lossy(&listing.stderr)
                    ));
                }
                if listing.stdout.is_empty() {
                    break;
                }
                if Instant::now() >= deadline {
                    return Err(format!("{} remains attached", self.image.display()));
                }
                thread::sleep(Duration::from_millis(20));
            }
        }
        Ok(())
    }
}

impl Drop for Volume {
    fn drop(&mut self) {
        if let Err(error) = self.clean_up() {
            eprintln!(
                "retained filesystem test artifacts at {}: {error}",
                self.root.display()
            );
            if !thread::panicking() {
                panic!("{error}");
            }
        }
    }
}

fn try_run(program: &str, args: &[&str]) -> Result<Output, String> {
    Command::new("timeout")
        .args(["30s", program])
        .args(args)
        .output()
        .map_err(|error| format!("run {program}: {error}"))
}

fn run(program: &str, args: &[&str]) -> Output {
    try_run(program, args).unwrap_or_else(|error| panic!("{error}"))
}

fn checked(program: &str, args: &[&str]) -> Output {
    let output = run(program, args);
    assert!(
        output.status.success(),
        "{program} {args:?}: status={} stdout={} stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn text(output: &Output) -> String {
    String::from_utf8(output.stdout.clone())
        .unwrap()
        .trim()
        .to_string()
}

fn sync_directory(path: &Path) {
    File::open(path).unwrap().sync_all().unwrap();
}

fn write_durable(path: &Path, content: &[u8]) {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap();
    file.write_all(content).unwrap();
    file.sync_all().unwrap();
    sync_directory(path.parent().unwrap());
}

fn create_event(parent: &Path, name: &str) -> PathBuf {
    let event = parent.join(name);
    fs::create_dir(&event).unwrap();
    sync_directory(parent);
    write_durable(&event.join("event.json"), b"{\"reason\":\"test\"}\n");
    write_durable(&event.join("thumb.png"), b"test thumbnail\n");
    event
}

fn reproduce_legacy_event_loss(format: Format) {
    let mut volume = Volume::new(format);
    let camera = volume.mount.join("TeslaCam");
    let recent = camera.join("RecentClips");
    fs::create_dir_all(&recent).unwrap();
    let payload: Vec<u8> = (0..65536).map(|index| (index % 251) as u8).collect();
    for category in ["SavedClips", "SentryClips"] {
        let events = camera.join(category);
        fs::create_dir(&events).unwrap();
        sync_directory(&camera);
        let filename = "2026-10-02_02-13-03-front.mp4";
        let source = recent.join(filename);
        write_durable(&source, &payload);
        let event = create_event(&events, "2026-10-02_02-13-03");

        // Model the legacy archive cleanup, followed by the car promoting a clip.
        fs::remove_file(&source).unwrap();
        sync_directory(&recent);
        let error = fs::copy(&source, event.join(filename)).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound);
        assert_eq!(fs::read_dir(&event).unwrap().count(), 2);
        assert!(!event.join(filename).exists());

        // A marker-only event can still receive video after metadata was archived.
        let unfinished = create_event(&events, "2026-10-02_02-14-03");
        fs::remove_dir_all(&unfinished).unwrap();
        sync_directory(&events);
        let error = File::create(unfinished.join(filename)).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound);

        // Preserving both source and event allows the same promotion to complete.
        write_durable(&source, &payload);
        let retained = create_event(&events, "2026-10-02_02-15-03");
        fs::copy(&source, retained.join(filename)).unwrap();
        File::open(retained.join(filename))
            .unwrap()
            .sync_all()
            .unwrap();
        sync_directory(&retained);
        assert_eq!(fs::read(retained.join(filename)).unwrap(), payload);
        assert_eq!(fs::read(&source).unwrap(), payload);
        fs::remove_file(&source).unwrap();
        sync_directory(&recent);
    }
    volume.clean_up().unwrap();
    let report = serde_json::json!({
        "filesystem": format.name(),
        "image": volume.image,
        "fault_model": "application deletes source or unfinished event before video promotion",
        "categories": ["SavedClips", "SentryClips"],
        "early_source_deletion": "ENOENT; metadata remains without video",
        "early_event_deletion": "ENOENT when later video arrives",
        "preserve_source_and_event": "video promotion retains exact bytes",
        "power_cut_simulated": false
    });
    fs::write(
        volume.root.join("legacy-cleanup-result.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    println!("{report}");
}

#[test]
#[ignore = "requires explicit local Linux filesystem-fault environment, root, loop devices, and FAT32"]
fn fat32_reproduces_legacy_saved_sentry_loss() {
    reproduce_legacy_event_loss(Format::Fat32);
}

#[test]
#[ignore = "requires explicit local Linux filesystem-fault environment, root, loop devices, and ext4"]
fn ext4_reproduces_legacy_saved_sentry_loss() {
    reproduce_legacy_event_loss(Format::Ext4);
}

fn reproduce_disk_full(format: Format) {
    let mut volume = Volume::new(format);
    let camera = volume.mount.join("TeslaCam");
    let recent = camera.join("RecentClips");
    fs::create_dir_all(&recent).unwrap();
    let payload = vec![0x67; 65536];
    let filename = "2026-10-02_02-13-03-front.mp4";
    let source = recent.join(filename);
    write_durable(&source, &payload);
    let mut destinations = Vec::new();
    for category in ["SavedClips", "SentryClips"] {
        let events = camera.join(category);
        fs::create_dir(&events).unwrap();
        let event = create_event(&events, "2026-10-02_02-13-03");
        let destination = event.join(filename);
        write_durable(&destination, b"");
        destinations.push(destination);
    }
    let filler_path = volume.mount.join("owned-filler.bin");
    let mut filler = OpenOptions::new()
        .custom_flags(0x1000) // O_DSYNC: account for durable allocation, not delayed writes.
        .write(true)
        .create_new(true)
        .open(&filler_path)
        .unwrap();
    let mut bytes = 0;
    // Consume small remaining extents after a large allocation reaches ENOSPC.
    for chunk_bytes in [1024 * 1024, 4096] {
        let chunk = vec![0x39; chunk_bytes];
        loop {
            match filler.write(&chunk) {
                Ok(length) => {
                    assert!(length > 0, "filler write made no progress");
                    bytes += length;
                    assert!(bytes <= IMAGE_BYTES as usize, "filesystem did not fill");
                }
                Err(error) => {
                    assert_eq!(error.raw_os_error(), Some(28), "expected ENOSPC: {error}");
                    break;
                }
            }
        }
    }
    filler.sync_all().unwrap();
    drop(filler);
    sync_directory(&volume.mount);
    for destination in &destinations {
        assert_eq!(fs::read(&source).unwrap(), payload);
        let error = fs::copy(&source, destination)
            .and_then(|_| File::open(destination)?.sync_all())
            .unwrap_err();
        assert_eq!(error.raw_os_error(), Some(28), "expected ENOSPC: {error}");
        assert_eq!(fs::read(&source).unwrap(), payload);
    }

    fs::remove_file(&filler_path).unwrap();
    sync_directory(&volume.mount);
    for destination in &destinations {
        fs::copy(&source, destination).unwrap();
        File::open(destination).unwrap().sync_all().unwrap();
        sync_directory(destination.parent().unwrap());
        assert_eq!(fs::read(destination).unwrap(), payload);
        assert_eq!(fs::read(&source).unwrap(), payload);
    }
    volume.clean_up().unwrap();
    let report = serde_json::json!({
        "filesystem": format.name(),
        "image": volume.image,
        "fault_model": "explicit writes exhaust filesystem space before event promotion",
        "categories": ["SavedClips", "SentryClips"],
        "while_full": "ENOSPC; original durable clip remains intact",
        "after_removing_owned_filler": "promotion succeeds with exact durable bytes"
    });
    fs::write(
        volume.root.join("disk-full-result.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    println!("{report}");
}

#[test]
#[ignore = "requires explicit local Linux filesystem-fault environment, root, loop devices, and FAT32"]
fn fat32_reports_disk_full_without_losing_durable_clips() {
    reproduce_disk_full(Format::Fat32);
}

#[test]
#[ignore = "requires explicit local Linux filesystem-fault environment, root, loop devices, and ext4"]
fn ext4_reports_disk_full_without_losing_durable_clips() {
    reproduce_disk_full(Format::Ext4);
}
