"""Exercise archive accounting and date batches against the installed rclone binary."""

from pathlib import Path

import pytest

from teslausb.archive import RcloneBackend
from teslausb.filesystem import RealFilesystem

pytestmark = pytest.mark.integration


def test_rclone_dates_metadata_and_unchanged_confirmations(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    source = tmp_path / "source"
    source.mkdir()
    names = ["2026-10-01_23-59-00-front.mp4", "2026-10-02_00-00-00-front.mp4", "thumb.png"]
    for name in names:
        (source / name).write_bytes(b"recorded footage")
    backend = RcloneBackend(":local:", path="archive", fs=RealFilesystem(), timeout=10)

    copied = backend.copy_directory(source, "RecentClips")
    assert copied.success, copied.error
    assert copied.files_transferred == len(names)
    assert {file.relative_path for file in copied.archived_files} == set(names)
    assert (
        tmp_path / "archive/RecentClips/2026-10-01" / names[0]
    ).read_bytes() == b"recorded footage"
    assert (
        tmp_path / "archive/RecentClips/2026-10-02" / names[1]
    ).read_bytes() == b"recorded footage"
    assert (tmp_path / "archive/RecentClips/metadata/thumb.png").read_bytes() == b"recorded footage"

    unchanged = backend.copy_directory(source, "RecentClips")
    assert unchanged.success, unchanged.error
    assert unchanged.files_transferred == 0
    assert {file.relative_path for file in unchanged.archived_files} == set(names)


def test_real_rclone_dry_run_never_confirms_files(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    source = tmp_path / "source"
    source.mkdir()
    (source / "front.mp4").write_bytes(b"recorded footage")
    backend = RcloneBackend(
        ":local:", path="archive", fs=RealFilesystem(), flags=["--dry-run"], timeout=10
    )
    result = backend.copy_directory(source, "SavedClips")
    assert result.success, result.error
    assert result.archived_files == []
    assert not (tmp_path / "archive/SavedClips/front.mp4").exists()
