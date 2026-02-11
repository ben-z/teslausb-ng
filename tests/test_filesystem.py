"""Tests for the filesystem abstraction layer."""

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from teslausb.filesystem import (
    FileNotFoundError_,
    MockFilesystem,
    RealFilesystem,
    StatResult,
    StatVfsResult,
)


class TestMockFilesystem:
    """Tests for MockFilesystem."""

    def test_mkdir_and_exists(self):
        """Test creating directories and checking existence."""
        fs = MockFilesystem()

        assert not fs.exists(Path("/test"))

        fs.mkdir(Path("/test"))
        assert fs.exists(Path("/test"))
        assert fs.is_dir(Path("/test"))
        assert not fs.is_file(Path("/test"))

    def test_mkdir_parents(self):
        """Test creating nested directories with parents=True."""
        fs = MockFilesystem()

        fs.mkdir(Path("/a/b/c"), parents=True)
        assert fs.exists(Path("/a"))
        assert fs.exists(Path("/a/b"))
        assert fs.exists(Path("/a/b/c"))

    def test_mkdir_without_parents_fails(self):
        """Test that creating nested dirs without parents fails."""
        fs = MockFilesystem()

        with pytest.raises(FileNotFoundError_):
            fs.mkdir(Path("/a/b/c"), parents=False)

    def test_write_and_read_text(self):
        """Test writing and reading text files."""
        fs = MockFilesystem()
        fs.mkdir(Path("/test"))

        fs.write_text(Path("/test/file.txt"), "hello world")

        assert fs.exists(Path("/test/file.txt"))
        assert fs.is_file(Path("/test/file.txt"))
        assert fs.read_text(Path("/test/file.txt")) == "hello world"

    def test_read_nonexistent_file(self):
        """Test reading a nonexistent file raises error."""
        fs = MockFilesystem()

        with pytest.raises(FileNotFoundError_):
            fs.read_text(Path("/nonexistent.txt"))

    def test_stat_file(self):
        """Test stat on a file."""
        fs = MockFilesystem()
        fs.mkdir(Path("/test"))
        fs.write_text(Path("/test/file.txt"), "hello")

        stat = fs.stat(Path("/test/file.txt"))

        assert isinstance(stat, StatResult)
        assert stat.size == 5  # len("hello")
        assert stat.is_file
        assert not stat.is_dir

    def test_stat_directory(self):
        """Test stat on a directory."""
        fs = MockFilesystem()
        fs.mkdir(Path("/test"))

        stat = fs.stat(Path("/test"))

        assert stat.is_dir
        assert not stat.is_file

    def test_statvfs(self):
        """Test statvfs (filesystem stats)."""
        fs = MockFilesystem()
        fs.set_total_space(100 * 1024 * 1024)  # 100 MB

        statvfs = fs.statvfs(Path("/"))

        assert isinstance(statvfs, StatVfsResult)
        assert statvfs.total_bytes == 100 * 1024 * 1024
        assert statvfs.free_bytes == 100 * 1024 * 1024  # Nothing used yet

    def test_statvfs_with_files(self):
        """Test statvfs accounts for file sizes."""
        fs = MockFilesystem()
        fs.set_total_space(100 * 1024 * 1024)  # 100 MB
        fs.mkdir(Path("/test"))

        # Write 10 MB of data
        fs.write_bytes(Path("/test/data.bin"), b"x" * (10 * 1024 * 1024))

        statvfs = fs.statvfs(Path("/"))

        assert statvfs.total_bytes == 100 * 1024 * 1024
        # Free should be approximately 90 MB (minus some for block alignment)
        assert statvfs.free_bytes < 100 * 1024 * 1024

    def test_listdir(self):
        """Test listing directory contents."""
        fs = MockFilesystem()
        fs.mkdir(Path("/test"))
        fs.mkdir(Path("/test/subdir"))
        fs.write_text(Path("/test/file1.txt"), "a")
        fs.write_text(Path("/test/file2.txt"), "b")

        entries = fs.listdir(Path("/test"))

        assert sorted(entries) == ["file1.txt", "file2.txt", "subdir"]

    def test_listdir_empty(self):
        """Test listing empty directory."""
        fs = MockFilesystem()
        fs.mkdir(Path("/empty"))

        entries = fs.listdir(Path("/empty"))

        assert entries == []

    def test_remove_file(self):
        """Test removing a file."""
        fs = MockFilesystem()
        fs.mkdir(Path("/test"))
        fs.write_text(Path("/test/file.txt"), "data")

        assert fs.exists(Path("/test/file.txt"))
        fs.remove(Path("/test/file.txt"))
        assert not fs.exists(Path("/test/file.txt"))

    def test_remove_nonexistent(self):
        """Test removing nonexistent file raises error."""
        fs = MockFilesystem()

        with pytest.raises(FileNotFoundError_):
            fs.remove(Path("/nonexistent.txt"))

    def test_rmtree(self):
        """Test removing directory tree."""
        fs = MockFilesystem()
        fs.mkdir(Path("/test/subdir"), parents=True)
        fs.write_text(Path("/test/file.txt"), "a")
        fs.write_text(Path("/test/subdir/file.txt"), "b")

        fs.rmtree(Path("/test"))

        assert not fs.exists(Path("/test"))
        assert not fs.exists(Path("/test/subdir"))
        assert not fs.exists(Path("/test/file.txt"))

    def test_copy(self):
        """Test copying a file."""
        fs = MockFilesystem()
        fs.mkdir(Path("/src"))
        fs.mkdir(Path("/dst"))
        fs.write_text(Path("/src/file.txt"), "content")

        fs.copy(Path("/src/file.txt"), Path("/dst/file.txt"))

        assert fs.exists(Path("/dst/file.txt"))
        assert fs.read_text(Path("/dst/file.txt")) == "content"
        # Original still exists
        assert fs.exists(Path("/src/file.txt"))

    def test_copy_reflink(self):
        """Test copy_reflink (same as copy in mock)."""
        fs = MockFilesystem()
        fs.mkdir(Path("/src"))
        fs.mkdir(Path("/dst"))
        fs.write_text(Path("/src/file.txt"), "content")

        fs.copy_reflink(Path("/src/file.txt"), Path("/dst/file.txt"))

        assert fs.exists(Path("/dst/file.txt"))
        assert fs.read_text(Path("/dst/file.txt")) == "content"

    def test_rename(self):
        """Test renaming a file."""
        fs = MockFilesystem()
        fs.mkdir(Path("/test"))
        fs.write_text(Path("/test/old.txt"), "data")

        fs.rename(Path("/test/old.txt"), Path("/test/new.txt"))

        assert not fs.exists(Path("/test/old.txt"))
        assert fs.exists(Path("/test/new.txt"))
        assert fs.read_text(Path("/test/new.txt")) == "data"

    def test_rename_directory(self):
        """Test renaming a directory."""
        fs = MockFilesystem()
        fs.mkdir(Path("/old/subdir"), parents=True)
        fs.write_text(Path("/old/file.txt"), "data")
        fs.write_text(Path("/old/subdir/file.txt"), "nested")

        fs.rename(Path("/old"), Path("/new"))

        assert not fs.exists(Path("/old"))
        assert fs.exists(Path("/new"))
        assert fs.exists(Path("/new/subdir"))
        assert fs.read_text(Path("/new/file.txt")) == "data"
        assert fs.read_text(Path("/new/subdir/file.txt")) == "nested"

    def test_symlink(self):
        """Test creating symbolic links."""
        fs = MockFilesystem()
        fs.mkdir(Path("/test"))
        fs.write_text(Path("/test/target.txt"), "data")

        fs.symlink(Path("/test/target.txt"), Path("/test/link.txt"))

        assert fs.exists(Path("/test/link.txt"))
        assert fs.is_symlink(Path("/test/link.txt"))
        assert fs.readlink(Path("/test/link.txt")) == Path("/test/target.txt")

    def test_walk(self):
        """Test walking directory tree."""
        fs = MockFilesystem()
        fs.mkdir(Path("/root/a/b"), parents=True)
        fs.write_text(Path("/root/file1.txt"), "1")
        fs.write_text(Path("/root/a/file2.txt"), "2")
        fs.write_text(Path("/root/a/b/file3.txt"), "3")

        walked = list(fs.walk(Path("/root")))

        # Should have 3 entries: /root, /root/a, /root/a/b
        assert len(walked) == 3

        # Check root entry
        root_entry = walked[0]
        assert root_entry[0] == Path("/root")
        assert "a" in root_entry[1]  # dirnames
        assert "file1.txt" in root_entry[2]  # filenames

    def test_set_free_space(self):
        """Test setting free space."""
        fs = MockFilesystem()
        fs.mkdir(Path("/test"))
        fs.write_bytes(Path("/test/data.bin"), b"x" * 4096)  # Exactly one block

        # Set free space to 8 blocks (32768 bytes)
        fs.set_free_space(8 * 4096)

        statvfs = fs.statvfs(Path("/"))
        # Total should be used (4096) + free (32768) = 36864
        # With 4096 block size, that's 9 blocks
        assert statvfs.total_bytes == 9 * 4096
        # Free should be 8 blocks
        assert statvfs.free_bytes == 8 * 4096


class TestRealFilesystem:
    """Tests for RealFilesystem."""

    def test_statvfs_calls_syncfs_before_reading(self, tmp_path):
        """statvfs flushes the XFS journal via syncfs before reading counters."""
        fs = RealFilesystem()
        expected = os.statvfs(tmp_path)
        with patch("teslausb.filesystem._syncfs") as mock_syncfs, \
             patch("teslausb.filesystem.os.statvfs", return_value=expected):
            result = fs.statvfs(tmp_path)
            mock_syncfs.assert_called_once()
            assert result.block_size == expected.f_frsize
