"""Filesystem abstraction layer for testability.

This module provides a protocol for filesystem operations and two implementations:
- RealFilesystem: Uses actual system calls (production)
- MockFilesystem: In-memory implementation (testing)
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
from abc import ABC, abstractmethod

from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator

logger = logging.getLogger(__name__)


@dataclass
class StatResult:
    """Result of stat() call."""

    size: int
    mtime: float
    is_dir: bool
    is_file: bool


@dataclass
class StatVfsResult:
    """Result of statvfs() call (filesystem stats)."""

    block_size: int
    total_blocks: int
    free_blocks: int
    available_blocks: int

    @property
    def total_bytes(self) -> int:
        return self.block_size * self.total_blocks

    @property
    def free_bytes(self) -> int:
        return self.block_size * self.free_blocks

    @property
    def available_bytes(self) -> int:
        return self.block_size * self.available_blocks


class FilesystemError(Exception):
    """Base exception for filesystem errors."""


class FileNotFoundError_(FilesystemError):
    """File or directory not found."""


class PermissionError_(FilesystemError):
    """Permission denied."""


class ReflinkNotSupportedError(FilesystemError):
    """Filesystem does not support reflinks (COW copies)."""


class Filesystem(ABC):
    """Abstract base class for filesystem operations."""

    @abstractmethod
    def exists(self, path: Path) -> bool:
        """Check if path exists."""

    @abstractmethod
    def is_file(self, path: Path) -> bool:
        """Check if path is a file."""

    @abstractmethod
    def is_dir(self, path: Path) -> bool:
        """Check if path is a directory."""

    @abstractmethod
    def stat(self, path: Path) -> StatResult:
        """Get file/directory stats."""

    @abstractmethod
    def statvfs(self, path: Path) -> StatVfsResult:
        """Get filesystem stats (free space, etc.)."""

    @abstractmethod
    def listdir(self, path: Path) -> list[str]:
        """List directory contents."""

    @abstractmethod
    def walk(self, path: Path) -> Iterator[tuple[Path, list[str], list[str]]]:
        """Walk directory tree, yielding (dirpath, dirnames, filenames)."""

    @abstractmethod
    def mkdir(self, path: Path, parents: bool = False, exist_ok: bool = False) -> None:
        """Create directory."""

    @abstractmethod
    def remove(self, path: Path) -> None:
        """Remove file."""

    @abstractmethod
    def rmtree(self, path: Path) -> None:
        """Remove directory tree."""

    @abstractmethod
    def rmdir(self, path: Path) -> None:
        """Remove empty directory."""

    @abstractmethod
    def copy(self, src: Path, dst: Path) -> None:
        """Copy file (regular copy)."""

    @abstractmethod
    def copy_reflink(self, src: Path, dst: Path) -> None:
        """Copy file using COW reflink (XFS/btrfs). Falls back to regular copy."""

    @abstractmethod
    def read_text(self, path: Path) -> str:
        """Read file as text."""

    @abstractmethod
    def write_text(self, path: Path, content: str) -> None:
        """Write text to file."""

    @abstractmethod
    def rename(self, src: Path, dst: Path) -> None:
        """Rename/move file or directory."""

    @abstractmethod
    def symlink(self, src: Path, dst: Path) -> None:
        """Create symbolic link at dst pointing to src."""

    @abstractmethod
    def readlink(self, path: Path) -> Path:
        """Read symbolic link target."""

    @abstractmethod
    def is_symlink(self, path: Path) -> bool:
        """Check if path is a symbolic link."""


class RealFilesystem(Filesystem):
    """Real filesystem implementation using actual system calls."""

    def exists(self, path: Path) -> bool:
        return path.exists()

    def is_file(self, path: Path) -> bool:
        return path.is_file()

    def is_dir(self, path: Path) -> bool:
        return path.is_dir()

    def stat(self, path: Path) -> StatResult:
        try:
            st = path.stat()
            return StatResult(
                size=st.st_size,
                mtime=st.st_mtime,
                is_dir=path.is_dir(),
                is_file=path.is_file(),
            )
        except FileNotFoundError as e:
            raise FileNotFoundError_(str(path)) from e
        except PermissionError as e:
            raise PermissionError_(str(path)) from e

    def statvfs(self, path: Path) -> StatVfsResult:
        try:
            # XFS lazy superblock counters (sb_lazysbcount) aggregate per-CPU
            # free block counts on demand. After unlink(), the cached aggregate
            # is stale. The first statvfs() triggers aggregation; the second
            # reads the accurate result (~0.5ms total).
            os.statvfs(path)
            st = os.statvfs(path)
            return StatVfsResult(
                block_size=st.f_frsize,
                total_blocks=st.f_blocks,
                free_blocks=st.f_bfree,
                available_blocks=st.f_bavail,
            )
        except FileNotFoundError as e:
            raise FileNotFoundError_(str(path)) from e
        except PermissionError as e:
            raise PermissionError_(str(path)) from e

    def listdir(self, path: Path) -> list[str]:
        try:
            return os.listdir(path)
        except FileNotFoundError as e:
            raise FileNotFoundError_(str(path)) from e
        except PermissionError as e:
            raise PermissionError_(str(path)) from e

    def walk(self, path: Path) -> Iterator[tuple[Path, list[str], list[str]]]:
        for dirpath, dirnames, filenames in os.walk(path):
            yield Path(dirpath), dirnames, filenames

    def mkdir(self, path: Path, parents: bool = False, exist_ok: bool = False) -> None:
        try:
            path.mkdir(parents=parents, exist_ok=exist_ok)
        except FileNotFoundError as e:
            raise FileNotFoundError_(str(path)) from e
        except PermissionError as e:
            raise PermissionError_(str(path)) from e

    def remove(self, path: Path) -> None:
        try:
            path.unlink()
        except FileNotFoundError as e:
            raise FileNotFoundError_(str(path)) from e
        except PermissionError as e:
            raise PermissionError_(str(path)) from e

    def rmtree(self, path: Path) -> None:
        try:
            shutil.rmtree(path)
        except FileNotFoundError as e:
            raise FileNotFoundError_(str(path)) from e
        except PermissionError as e:
            raise PermissionError_(str(path)) from e

    def rmdir(self, path: Path) -> None:
        try:
            path.rmdir()
        except FileNotFoundError as e:
            raise FileNotFoundError_(str(path)) from e
        except PermissionError as e:
            raise PermissionError_(str(path)) from e
        except OSError as e:
            # Directory not empty
            raise FilesystemError(str(e)) from e

    def copy(self, src: Path, dst: Path) -> None:
        try:
            shutil.copy2(src, dst)
        except FileNotFoundError as e:
            raise FileNotFoundError_(str(src)) from e
        except PermissionError as e:
            raise PermissionError_(str(src)) from e

    def copy_reflink(self, src: Path, dst: Path) -> None:
        """Copy using reflink (COW). Raises error if not supported.

        Reflinks are required for efficient snapshots - without them,
        each snapshot would copy the entire disk image.

        Raises:
            ReflinkNotSupportedError: If reflink copy fails for any reason
        """
        result = subprocess.run(
            ["cp", "--reflink=always", str(src), str(dst)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if result.returncode == 0:
            return

        stderr = result.stderr.decode().strip() if result.stderr else "unknown error"
        raise ReflinkNotSupportedError(
            f"Reflink copy failed. TeslaUSB requires XFS or btrfs. Error: {stderr}"
        )

    def read_text(self, path: Path) -> str:
        try:
            return path.read_text()
        except FileNotFoundError as e:
            raise FileNotFoundError_(str(path)) from e
        except PermissionError as e:
            raise PermissionError_(str(path)) from e

    def write_text(self, path: Path, content: str) -> None:
        try:
            path.write_text(content)
        except FileNotFoundError as e:
            raise FileNotFoundError_(str(path)) from e
        except PermissionError as e:
            raise PermissionError_(str(path)) from e

    def rename(self, src: Path, dst: Path) -> None:
        try:
            src.rename(dst)
        except FileNotFoundError as e:
            raise FileNotFoundError_(str(src)) from e
        except PermissionError as e:
            raise PermissionError_(str(src)) from e

    def symlink(self, src: Path, dst: Path) -> None:
        try:
            dst.symlink_to(src)
        except FileNotFoundError as e:
            raise FileNotFoundError_(str(dst.parent)) from e
        except PermissionError as e:
            raise PermissionError_(str(dst)) from e

    def readlink(self, path: Path) -> Path:
        try:
            return Path(os.readlink(path))
        except FileNotFoundError as e:
            raise FileNotFoundError_(str(path)) from e
        except PermissionError as e:
            raise PermissionError_(str(path)) from e

    def is_symlink(self, path: Path) -> bool:
        return path.is_symlink()


@dataclass
class MockFile:
    """Represents a file in the mock filesystem."""

    content: bytes = b""
    mtime: float = 0.0


@dataclass
class MockDir:
    """Represents a directory in the mock filesystem."""

    mtime: float = 0.0


@dataclass
class MockSymlink:
    """Represents a symbolic link in the mock filesystem."""

    target: Path
    mtime: float = 0.0


@dataclass
class MockFilesystem(Filesystem):
    """In-memory mock filesystem for testing.

    Simulates a filesystem with configurable total/free space.
    """

    # Storage for files, directories, and symlinks
    _files: dict[Path, MockFile] = field(default_factory=dict)
    _dirs: dict[Path, MockDir] = field(default_factory=dict)
    _symlinks: dict[Path, MockSymlink] = field(default_factory=dict)

    # Filesystem stats
    _total_bytes: int = 100 * 1024 * 1024 * 1024  # 100 GB default
    _block_size: int = 4096

    def __post_init__(self) -> None:
        # Root always exists
        self._dirs[Path("/")] = MockDir()

    def _normalize(self, path: Path) -> Path:
        """Normalize path to absolute."""
        if not path.is_absolute():
            path = Path("/") / path
        return path.resolve()

    def _used_bytes(self) -> int:
        """Calculate total used bytes."""
        return sum(len(f.content) for f in self._files.values())

    def set_total_space(self, total_bytes: int) -> None:
        """Set total filesystem space (for testing)."""
        self._total_bytes = total_bytes

    def set_free_space(self, free_bytes: int) -> None:
        """Set free space by adjusting total (for testing)."""
        self._total_bytes = self._used_bytes() + free_bytes

    def exists(self, path: Path) -> bool:
        path = self._normalize(path)
        return path in self._files or path in self._dirs or path in self._symlinks

    def is_file(self, path: Path) -> bool:
        path = self._normalize(path)
        if path in self._symlinks:
            target = self._resolve_symlink(path)
            return target in self._files
        return path in self._files

    def is_dir(self, path: Path) -> bool:
        path = self._normalize(path)
        if path in self._symlinks:
            target = self._resolve_symlink(path)
            return target in self._dirs
        return path in self._dirs

    def _resolve_symlink(self, path: Path) -> Path:
        """Resolve symlink to its target."""
        if path in self._symlinks:
            return self._symlinks[path].target
        return path

    def stat(self, path: Path) -> StatResult:
        path = self._normalize(path)

        if path in self._symlinks:
            target = self._resolve_symlink(path)
            return self.stat(target)

        if path in self._files:
            f = self._files[path]
            return StatResult(size=len(f.content), mtime=f.mtime, is_dir=False, is_file=True)

        if path in self._dirs:
            d = self._dirs[path]
            return StatResult(size=0, mtime=d.mtime, is_dir=True, is_file=False)

        raise FileNotFoundError_(str(path))

    def statvfs(self, path: Path) -> StatVfsResult:
        path = self._normalize(path)
        if not self.exists(path):
            raise FileNotFoundError_(str(path))

        used = self._used_bytes()
        free = max(0, self._total_bytes - used)
        total_blocks = self._total_bytes // self._block_size
        free_blocks = free // self._block_size

        return StatVfsResult(
            block_size=self._block_size,
            total_blocks=total_blocks,
            free_blocks=free_blocks,
            available_blocks=free_blocks,
        )

    def listdir(self, path: Path) -> list[str]:
        path = self._normalize(path)
        if path not in self._dirs:
            raise FileNotFoundError_(str(path))

        entries: set[str] = set()

        # Find all entries directly under this path
        for p in list(self._files.keys()) + list(self._dirs.keys()) + list(self._symlinks.keys()):
            if p.parent == path and p != path:
                entries.add(p.name)

        return sorted(entries)

    def walk(self, path: Path) -> Iterator[tuple[Path, list[str], list[str]]]:
        path = self._normalize(path)
        if path not in self._dirs:
            return

        dirnames: list[str] = []
        filenames: list[str] = []

        for entry in self.listdir(path):
            entry_path = path / entry
            if entry_path in self._dirs:
                dirnames.append(entry)
            elif entry_path in self._files or entry_path in self._symlinks:
                filenames.append(entry)

        yield path, dirnames, filenames

        for dirname in dirnames:
            yield from self.walk(path / dirname)

    def mkdir(self, path: Path, parents: bool = False, exist_ok: bool = False) -> None:
        path = self._normalize(path)

        if path in self._dirs:
            if exist_ok:
                return
            raise FilesystemError(f"Directory exists: {path}")

        if path in self._files:
            raise FilesystemError(f"File exists at path: {path}")

        parent = path.parent
        if parent not in self._dirs:
            if parents:
                self.mkdir(parent, parents=True, exist_ok=True)
            else:
                raise FileNotFoundError_(str(parent))

        self._dirs[path] = MockDir()

    def remove(self, path: Path) -> None:
        path = self._normalize(path)

        if path in self._symlinks:
            del self._symlinks[path]
        elif path in self._files:
            del self._files[path]
        else:
            raise FileNotFoundError_(str(path))

    def rmtree(self, path: Path) -> None:
        path = self._normalize(path)

        if path not in self._dirs:
            raise FileNotFoundError_(str(path))

        # Remove all entries under this path
        to_remove_files = [p for p in self._files if p == path or str(p).startswith(str(path) + "/")]
        to_remove_dirs = [p for p in self._dirs if p == path or str(p).startswith(str(path) + "/")]
        to_remove_symlinks = [
            p for p in self._symlinks if p == path or str(p).startswith(str(path) + "/")
        ]

        for p in to_remove_files:
            del self._files[p]
        for p in to_remove_symlinks:
            del self._symlinks[p]
        for p in sorted(to_remove_dirs, reverse=True):  # Remove deepest first
            del self._dirs[p]

    def rmdir(self, path: Path) -> None:
        path = self._normalize(path)

        if path not in self._dirs:
            raise FileNotFoundError_(str(path))

        # Check if directory is empty
        has_children = any(
            str(p).startswith(str(path) + "/")
            for p in list(self._files.keys()) + list(self._dirs.keys()) + list(self._symlinks.keys())
        )
        if has_children:
            raise FilesystemError(f"Directory not empty: {path}")

        del self._dirs[path]

    def copy(self, src: Path, dst: Path) -> None:
        src = self._normalize(src)
        dst = self._normalize(dst)

        if src not in self._files:
            raise FileNotFoundError_(str(src))

        if dst in self._dirs:
            dst = dst / src.name

        if dst.parent not in self._dirs:
            raise FileNotFoundError_(str(dst.parent))

        self._files[dst] = MockFile(content=self._files[src].content, mtime=self._files[src].mtime)

    def copy_reflink(self, src: Path, dst: Path) -> None:
        # In mock, reflink is same as copy (no COW simulation)
        self.copy(src, dst)

    def read_text(self, path: Path) -> str:
        path = self._normalize(path)
        if path not in self._files:
            raise FileNotFoundError_(str(path))
        return self._files[path].content.decode("utf-8")

    def write_text(self, path: Path, content: str) -> None:
        path = self._normalize(path)
        if path.parent not in self._dirs:
            raise FileNotFoundError_(str(path.parent))
        self._files[path] = MockFile(content=content.encode("utf-8"))

    def write_bytes(self, path: Path, content: bytes) -> None:
        """Write bytes to file (for testing)."""
        path = self._normalize(path)
        if path.parent not in self._dirs:
            raise FileNotFoundError_(str(path.parent))
        self._files[path] = MockFile(content=content)

    def rename(self, src: Path, dst: Path) -> None:
        src = self._normalize(src)
        dst = self._normalize(dst)

        if src in self._files:
            self._files[dst] = self._files.pop(src)
        elif src in self._dirs:
            # Need to rename all entries under this dir too
            old_prefix = str(src)
            new_prefix = str(dst)

            # Collect all paths to rename
            files_to_rename = [(p, self._files[p]) for p in self._files if str(p).startswith(old_prefix)]
            dirs_to_rename = [(p, self._dirs[p]) for p in self._dirs if str(p).startswith(old_prefix)]
            symlinks_to_rename = [(p, self._symlinks[p]) for p in self._symlinks if str(p).startswith(old_prefix)]

            # Remove old paths
            for p, _ in files_to_rename:
                del self._files[p]
            for p, _ in dirs_to_rename:
                del self._dirs[p]
            for p, _ in symlinks_to_rename:
                del self._symlinks[p]

            # Add new paths
            for p, f in files_to_rename:
                new_path = Path(str(p).replace(old_prefix, new_prefix, 1))
                self._files[new_path] = f
            for p, d in dirs_to_rename:
                new_path = Path(str(p).replace(old_prefix, new_prefix, 1))
                self._dirs[new_path] = d
            for p, s in symlinks_to_rename:
                new_path = Path(str(p).replace(old_prefix, new_prefix, 1))
                self._symlinks[new_path] = s
        else:
            raise FileNotFoundError_(str(src))

    def symlink(self, src: Path, dst: Path) -> None:
        dst = self._normalize(dst)
        if dst.parent not in self._dirs:
            raise FileNotFoundError_(str(dst.parent))
        self._symlinks[dst] = MockSymlink(target=src)

    def readlink(self, path: Path) -> Path:
        path = self._normalize(path)
        if path not in self._symlinks:
            raise FileNotFoundError_(str(path))
        return self._symlinks[path].target

    def is_symlink(self, path: Path) -> bool:
        path = self._normalize(path)
        return path in self._symlinks
