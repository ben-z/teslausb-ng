"""Archive layout and complete-event retention regressions."""

import json
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from teslausb.archive import (
    ArchivedFile,
    ArchiveManager,
    ArchiveResult,
    ArchiveState,
    MockArchiveBackend,
    RcloneBackend,
)
from teslausb.filesystem import MockFilesystem
from teslausb.snapshot import SnapshotManager


@pytest.fixture
def manager():
    fs = MockFilesystem()
    fs.mkdir(Path("/source/event"), parents=True)
    fs.mkdir(Path("/backingfiles/snapshots"), parents=True)
    snapshots = SnapshotManager(
        fs, Path("/backingfiles/cam_disk.bin"), Path("/backingfiles/snapshots")
    )
    return ArchiveManager(fs, snapshots, MockArchiveBackend(), event_stability_seconds=600)


def write_event(manager, video=True):
    files = [ArchivedFile("event/event.json", 2, 0)]
    manager.fs.write_text(Path("/source/event/event.json"), "{}")
    if video:
        manager.fs.write_text(Path("/source/event/front.mp4"), "video")
        files.append(ArchivedFile("event/front.mp4", 5, 0))
    return files


def test_recent_clips_copied_into_date_folders(monkeypatch):
    fs = MockFilesystem()
    src = Path("/RecentClips")
    fs.mkdir(src)
    names = [
        "2026-10-01_23-59-00-front.mp4",
        "2026-10-02_00-00-00-front.mp4",
        "thumb.png",
        "event.json",
    ]
    for name in names:
        fs.write_text(src / name, "video")
    calls = []

    def run(cmd, **kwargs):
        calls.append((cmd, kwargs["input_data"]))
        output = "\n".join(
            json.dumps({"object": name, "msg": "Copied (new)"})
            for name in kwargs["input_data"].decode().splitlines()
        )
        return subprocess.CompletedProcess(cmd, 0, b"", output.encode())

    monkeypatch.setattr(RcloneBackend, "_run_command", staticmethod(run))
    result = RcloneBackend("drive", path="camera", fs=fs).copy_directory(src, "RecentClips")
    assert result.success
    assert [cmd[3] for cmd, _ in calls] == [
        "drive:camera/RecentClips/2026-10-01",
        "drive:camera/RecentClips/2026-10-02",
        "drive:camera/RecentClips/metadata",
    ]
    assert {name for _, data in calls for name in data.decode().splitlines()} == set(names)
    assert all("--no-traverse" in cmd and "--files-from-raw" in cmd for cmd, _ in calls)
    assert {file.relative_path for file in result.archived_files} == set(names)


def test_invalid_recent_date_fails_before_upload():
    fs = MockFilesystem()
    fs.mkdir(Path("/RecentClips"))
    fs.write_text(Path("/RecentClips/2026-02-30_bad.mp4"), "video")
    with (
        patch("teslausb.archive.RcloneBackend._run_command") as run,
        pytest.raises(ValueError, match="date-partition"),
    ):
        RcloneBackend("drive", fs=fs).copy_directory(Path("/RecentClips"), "RecentClips")
    run.assert_not_called()


def test_successful_dry_run_never_authorizes_deletion():
    fs = MockFilesystem()
    fs.mkdir(Path("/SavedClips"))
    fs.write_text(Path("/SavedClips/front.mp4"), "video")
    output = json.dumps({"object": "front.mp4", "msg": "Skipped copy as --dry-run is set"}).encode()
    with patch(
        "teslausb.archive.RcloneBackend._run_command",
        return_value=subprocess.CompletedProcess([], 0, b"", output),
    ):
        result = RcloneBackend("drive", fs=fs, flags=["--dry-run"]).copy_directory(
            Path("/SavedClips"), "SavedClips"
        )
    assert result.success
    assert result.archived_files == []


def test_large_subprocess_output_is_drained(monkeypatch):
    fs = MockFilesystem()
    fs.mkdir(Path("/SavedClips"))
    fs.write_text(Path("/SavedClips/front.mp4"), "video")
    real_popen = subprocess.Popen
    script = (
        'import sys,json; print("x"*1048576); print("x"*1048576,file=sys.stderr); '
        'print(json.dumps({"object":"front.mp4","msg":"Copied (new)"}),file=sys.stderr)'
    )

    def popen(cmd, **kwargs):
        return real_popen([sys.executable, "-c", script], **kwargs)

    monkeypatch.setattr(subprocess, "Popen", popen)
    result = RcloneBackend("drive", fs=fs, timeout=5).copy_directory(
        Path("/SavedClips"), "SavedClips"
    )
    assert result.success
    assert result.files_transferred == 1


def test_new_event_is_retained_until_stable(manager):
    files = write_event(manager)
    with patch("teslausb.archive.time.monotonic", return_value=100):
        assert manager._deletable_files(Path("/source"), "SavedClips", files) == []
    with patch("teslausb.archive.time.monotonic", return_value=700):
        assert manager._deletable_files(Path("/source"), "SavedClips", files) == files


def test_event_change_restarts_stability_window(manager):
    files = write_event(manager)
    with patch("teslausb.archive.time.monotonic", return_value=0):
        manager._deletable_files(Path("/source"), "SavedClips", files)
    manager.fs.write_text(Path("/source/event/back.mp4"), "new video")
    files.append(ArchivedFile("event/back.mp4", 9, 0))
    with patch("teslausb.archive.time.monotonic", return_value=600):
        assert manager._deletable_files(Path("/source"), "SavedClips", files) == []
    with patch("teslausb.archive.time.monotonic", return_value=1200):
        assert {
            file.relative_path
            for file in manager._deletable_files(Path("/source"), "SavedClips", files)
        } == {file.relative_path for file in files}


def test_marker_only_event_retained_and_warning_delayed(manager, caplog):
    files = write_event(manager, video=False)
    with patch("teslausb.archive.time.monotonic", return_value=0):
        assert manager._deletable_files(Path("/source"), "SentryClips", files) == []
    assert "no video" not in caplog.text
    with patch("teslausb.archive.time.monotonic", return_value=600):
        assert manager._deletable_files(Path("/source"), "SentryClips", files) == []
    assert "no video" in caplog.text


def test_partial_event_confirmation_retained(manager):
    files = write_event(manager)
    manager.event_stability_seconds = 0
    assert manager._deletable_files(Path("/source"), "SavedClips", files[1:]) == []


def test_recent_clips_never_deleted(manager):
    path = Path("/cam/TeslaCam/RecentClips/front.mp4")
    manager.fs.mkdir(path.parent, parents=True)
    manager.fs.write_text(path, "video")
    result = ArchiveResult(
        1, ArchiveState.COMPLETED, archived_files={"RecentClips": [ArchivedFile("front.mp4", 5, 0)]}
    )
    assert manager.delete_archived_files(result, Path("/cam")) == (0, 1)
    assert manager.fs.exists(path)


def test_live_event_changed_after_snapshot_is_retained(manager):
    base = Path("/cam/TeslaCam/SavedClips/event")
    manager.fs.mkdir(base, parents=True)
    manager.fs.write_text(base / "front.mp4", "video")
    manager.fs.write_text(base / "back.mp4", "new video")
    result = ArchiveResult(
        1,
        ArchiveState.COMPLETED,
        archived_files={"SavedClips": [ArchivedFile("event/front.mp4", 5, 0)]},
    )
    assert manager.delete_archived_files(result, Path("/cam")) == (0, 1)
    assert manager.fs.exists(base / "front.mp4")


def test_live_event_same_size_modified_file_is_retained(manager):
    base = Path("/cam/TeslaCam/SavedClips/event")
    manager.fs.mkdir(base, parents=True)
    manager.fs.write_text(base / "front.mp4", "video")
    result = ArchiveResult(
        1,
        ArchiveState.COMPLETED,
        archived_files={"SavedClips": [ArchivedFile("event/front.mp4", 5, 1)]},
    )
    assert manager.delete_archived_files(result, Path("/cam")) == (0, 1)
    assert manager.fs.exists(base / "front.mp4")


def test_reachability_drains_large_directory_listing(monkeypatch):
    real_popen = subprocess.Popen

    def popen(cmd, **kwargs):
        return real_popen(
            [
                sys.executable,
                "-c",
                'import sys; print("x"*1048576); print("x"*1048576,file=sys.stderr)',
            ],
            **kwargs,
        )

    monkeypatch.setattr(subprocess, "Popen", popen)
    assert RcloneBackend("drive", fs=MockFilesystem()).is_reachable()


def test_stop_reaps_actual_copy_child_and_preserves_confirmed_files(monkeypatch):
    import time
    from threading import Event, Timer

    fs = MockFilesystem()
    fs.mkdir(Path("/SavedClips"))
    fs.write_text(Path("/SavedClips/front.mp4"), "video")
    fs.write_text(Path("/SavedClips/back.mp4"), "pending")
    stop = Event()
    real_popen = subprocess.Popen
    children = []
    timers = []
    script = (
        "import json,signal,sys,time; "
        "signal.signal(signal.SIGTERM,signal.SIG_IGN); "
        'print(json.dumps({"object":"front.mp4","msg":"Copied (new)"}),'
        "file=sys.stderr,flush=True); "
        'print("ready",flush=True); time.sleep(30)'
    )

    def popen(cmd, **kwargs):
        child = real_popen([sys.executable, "-c", script], **kwargs)
        children.append(child)
        assert child.stdout.readline() == b"ready\n"
        timer = Timer(0.2, stop.set)
        timers.append(timer)
        timer.start()
        return child

    monkeypatch.setattr(subprocess, "Popen", popen)
    started = time.monotonic()
    try:
        result = RcloneBackend("drive", fs=fs, stop_event=stop).copy_directory(
            Path("/SavedClips"), "SavedClips"
        )
    finally:
        for timer in timers:
            timer.join()
    assert time.monotonic() - started < 3
    assert not result.success
    assert result.error == "Stopped"
    assert result.archived_files == [ArchivedFile("front.mp4", 5, 0)]
    assert result.files_transferred == 1
    assert children[0].returncode is not None


def test_timeout_reaps_actual_copy_child_and_preserves_output(monkeypatch):
    fs = MockFilesystem()
    fs.mkdir(Path("/SavedClips"))
    fs.write_text(Path("/SavedClips/front.mp4"), "video")
    real_popen = subprocess.Popen
    children = []
    script = (
        "import json,sys,time; "
        'print(json.dumps({"object":"front.mp4","msg":"Copied (new)"}),'
        "file=sys.stderr,flush=True); time.sleep(30)"
    )

    def popen(cmd, **kwargs):
        child = real_popen([sys.executable, "-c", script], **kwargs)
        children.append(child)
        return child

    monkeypatch.setattr(subprocess, "Popen", popen)
    result = RcloneBackend("drive", fs=fs, timeout=1).copy_directory(
        Path("/SavedClips"), "SavedClips"
    )
    assert not result.success
    assert result.error == "Timeout"
    assert result.archived_files == [ArchivedFile("front.mp4", 5, 0)]
    assert children[0].returncode is not None


def test_stopped_copy_never_starts_child():
    from threading import Event

    fs = MockFilesystem()
    fs.mkdir(Path("/SavedClips"))
    fs.write_text(Path("/SavedClips/front.mp4"), "video")
    stop = Event()
    stop.set()
    with patch("teslausb.archive.subprocess.Popen") as popen:
        result = RcloneBackend("drive", fs=fs, stop_event=stop).copy_directory(
            Path("/SavedClips"), "SavedClips"
        )
    popen.assert_not_called()
    assert not result.success
    assert result.error == "Stopped"
    assert result.archived_files == []


def test_command_retries_preserve_unsent_input():
    backend = RcloneBackend("drive", fs=MockFilesystem())
    payload = b"clip-name\n" * 200_000
    result = backend._run_command(
        [
            sys.executable,
            "-c",
            "import sys,time; time.sleep(0.3); print(len(sys.stdin.buffer.read()))",
        ],
        timeout=5,
        input_data=payload,
    )
    assert result.returncode == 0
    assert int(result.stdout) == len(payload)
