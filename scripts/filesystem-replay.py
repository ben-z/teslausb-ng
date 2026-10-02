#!/usr/bin/env python3
"""Compare recovery of real filesystem block-write prefixes in an isolated Linux VM."""

import argparse
import ctypes
from dataclasses import dataclass
import errno
import hashlib
import json
import os
from pathlib import Path
import signal
import shutil
import struct
import subprocess
import tempfile
import time


@dataclass(frozen=True)
class Settings:
    image_bytes: int = 128 * 1024 * 1024
    log_bytes: int = 256 * 1024 * 1024
    command_timeout: int = 60
    detach_timeout: int = 10
    poll_seconds: float = 0.02
    payload_bytes: int = 65536


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def durable_file(path, data):
    with path.open('xb') as output:
        output.write(data)
        output.flush()
        os.fsync(output.fileno())
    sync_directory(path.parent)


class Experiment:
    def __init__(self, filesystem, promotion, settings):
        require(os.geteuid() == 0, 'Filesystem failure tests require root in the local Linux VM')
        require(os.environ.get('TESLAUSB_RUN_FILESYSTEM_FAILURES') == '1',
                'Set TESLAUSB_RUN_FILESYSTEM_FAILURES=1 in the isolated local test environment')
        parent = Path(os.environ['TESLAUSB_FAILURE_ARTIFACT_DIR'])
        parent.mkdir(parents=True, exist_ok=True)
        self.root = Path(tempfile.mkdtemp(prefix=f'{filesystem}-{promotion}-replay-', dir=parent)).resolve()
        self.filesystem = filesystem
        self.promotion = promotion
        self.settings = settings
        self.loops = []
        self.mounts = []
        self.mapper = None
        self.manifest = []
        self.acknowledgements = {}

    def command(self, args):
        child = subprocess.Popen([str(arg) for arg in args], stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, start_new_session=True)
        try:
            stdout, stderr = child.communicate(timeout=self.settings.command_timeout)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid, signal.SIGKILL)
            child.communicate()
            raise RuntimeError(f'Command timed out: {args}')
        return {'code': child.returncode, 'stdout': stdout.decode('utf-8', errors='strict'),
                'stderr': stderr.decode('utf-8', errors='strict')}

    def checked(self, args):
        result = self.command(args)
        require(result['code'] == 0, f'{args}: {result}')
        return result['stdout'].strip()

    def image(self, name, length):
        path = self.root / name
        with path.open('xb') as image:
            image.truncate(length)
        return path

    def attach(self, image, readonly):
        args = ['losetup', '--find', '--show']
        if readonly:
            args.append('--read-only')
        device = self.checked([*args, image])
        require(device.startswith('/dev/loop'), f'Unexpected loop device: {device}')
        self.loops.append((device, image))
        if readonly:
            require(self.checked(['blockdev', '--getro', device]) == '1',
                    f'Test inspection device is writable: {device}')
        return device

    def mount(self, device, name, readonly):
        path = self.root / name
        path.mkdir()
        options = 'ro,noload' if readonly and self.filesystem == 'ext4' else ('ro' if readonly else 'rw')
        self.mounts.append(path)
        try:
            self.checked(['mount', '-t', 'vfat' if self.filesystem == 'fat32' else 'ext4',
                          '-o', options, device, path])
        except BaseException:
            state = self.command(['mountpoint', '-q', path])
            if state['code'] == 32:
                self.mounts.remove(path)
            require(state['code'] in (0, 32), f'Cannot establish mount ownership: {path}: {state}')
            raise
        return path

    def unmount(self, path):
        libc = ctypes.CDLL(None, use_errno=True)
        deadline = time.monotonic() + self.settings.detach_timeout
        while libc.umount(os.fsencode(path)) != 0:
            error = ctypes.get_errno()
            require(error == errno.EBUSY and time.monotonic() < deadline,
                    f'Cannot unmount owned test path {path}: {os.strerror(error)}')
            time.sleep(self.settings.poll_seconds)
        self.mounts.remove(path)

    def detach(self, device):
        image = next(image for owned, image in self.loops if owned == device)
        self.checked(['losetup', '-d', device])
        deadline = time.monotonic() + self.settings.detach_timeout
        while self.checked(['losetup', '-j', image]):
            require(time.monotonic() < deadline, f'Owned image remains attached: {image}')
            time.sleep(self.settings.poll_seconds)
        self.loops.remove((device, image))

    def clean_up(self):
        for mount in list(reversed(self.mounts)):
            self.unmount(mount)
        if self.mapper is not None:
            self.checked(['dmsetup', 'remove', self.mapper])
            self.mapper = None
        for device, _ in list(reversed(self.loops)):
            self.detach(device)

    def mark(self, name):
        self.checked(['dmsetup', 'message', self.mapper, '0', 'mark', name])

    def baseline(self):
        image = self.image('recording.img', self.settings.image_bytes)
        if self.filesystem == 'fat32':
            self.checked(['mkfs.vfat', '-F', '32', '-n', 'TESLACAM', image])
        else:
            self.checked(['mkfs.ext4', '-F', '-b', '4096', '-I', '256', '-m', '0',
                          '-L', 'TESLACAM', '-O',
                          'none,has_journal,ext_attr,resize_inode,dir_index,filetype,extent,flex_bg,sparse_super,large_file,huge_file,dir_nlink,extra_isize',
                          '-E', 'lazy_itable_init=0,lazy_journal_init=0', image])
        device = self.attach(image, False)
        mount = self.mount(device, 'recording-mount', False)
        cameras = ('front', 'back', 'left_repeater', 'right_repeater', 'left_pillar', 'right_pillar')
        for category, minute in (('SavedClips', '11'), ('SentryClips', '12')):
            (mount / 'TeslaCam' / category).mkdir(parents=True)
            recent = mount / 'TeslaCam' / 'RecentClips'
            recent.mkdir(exist_ok=True)
            event = f'TeslaCam/{category}/2026-10-02_02-13-03'
            for number, camera in enumerate(cameras):
                filename = f'2026-10-02_02-{minute}-03-{camera}.mp4'
                source = f'TeslaCam/RecentClips/{filename}'
                payload = bytes((index + number + int(minute)) % 251
                                for index in range(self.settings.payload_bytes))
                durable_file(mount / source, payload)
                self.manifest.append({'source': source, 'destination': f'{event}/{filename}',
                                      'sha256': digest(payload), 'size': len(payload),
                                      'category': category, 'camera': camera})
        sync_directory(mount / 'TeslaCam')
        sync_directory(mount)
        self.unmount(mount)
        baseline = self.root / 'baseline.img'
        self.checked(['cp', '--sparse=always', image, baseline])
        return image, device, baseline

    def record(self, image, device):
        log_image = self.image('writes.log', self.settings.log_bytes)
        log_device = self.attach(log_image, False)
        mapper = 'teslausb-replay-' + self.root.name
        sectors = self.settings.image_bytes // 512
        self.checked(['dmsetup', 'create', mapper, '--table',
                      f'0 {sectors} log-writes {device} {log_device}'])
        self.mapper = mapper
        mount = self.mount(f'/dev/mapper/{self.mapper}', 'trace-mount', False)
        self.mark('begin')
        for category in ('SavedClips', 'SentryClips'):
            event = mount / f'TeslaCam/{category}/2026-10-02_02-13-03'
            event.mkdir()
            sync_directory(event.parent)
            durable_file(event / 'event.json', b'{"reason":"filesystem failure test"}\n')
            durable_file(event / 'thumb.png', b'test thumbnail\n')
            self.mark(f'{category}-markers')
            for item in [item for item in self.manifest if item['category'] == category]:
                if self.promotion == 'rename':
                    os.rename(mount / item['source'], mount / item['destination'])
                else:
                    with (mount / item['destination']).open('xb') as copied:
                        data = (mount / item['source']).read_bytes()
                        require(copied.write(data) == len(data), 'Short promoted clip write')
                with (mount / item['destination']).open('rb') as moved:
                    os.fsync(moved.fileno())
                sync_directory((mount / item['source']).parent)
                sync_directory(event)
                name = f'{category}-{item["camera"]}-acknowledged'
                self.mark(name)
                self.acknowledgements[item['source']] = name
        self.mark('complete')
        self.unmount(mount)
        self.checked(['dmsetup', 'remove', self.mapper])
        self.mapper = None
        self.detach(log_device)
        self.detach(device)
        return log_image

    def read_log(self, path):
        entries = []
        with path.open('rb') as source:
            magic, version, count, sector_size = struct.unpack('<QQQI', source.read(28))
            require(magic == 0x6A736677736872 and version == 1, 'Unsupported dm-log-writes format')
            require(sector_size == 512, f'This sector-tear experiment requires 512-byte sectors, got {sector_size}')
            source.seek(sector_size)
            for index in range(count):
                header = source.read(sector_size)
                require(len(header) == sector_size, f'Truncated log entry {index}')
                sector, sectors, flags, extra = struct.unpack('<QQQQ', header[:32])
                require(extra <= sector_size - 32, f'Invalid log mark size at {index}')
                entry = {'index': index, 'sector': sector, 'sectors': sectors, 'flags': flags,
                         'data_offset': source.tell()}
                if flags & 8:
                    entry['mark'] = header[32:32 + extra].rstrip(b'\0').decode('utf-8')
                if sectors and not flags & 4:
                    source.seek(sectors * sector_size, os.SEEK_CUR)
                entries.append(entry)
        return entries

    def inspect(self, image, name, marks):
        result = {'missing_durable_clips': [], 'wrong_durable_bytes': [],
                  'wrong_acknowledged_path': [], 'wrong_retained_source': []}
        device = self.attach(image, True)
        mount = self.mount(device, name, True)
        result['read_errors'] = []
        for item in self.manifest:
            if self.promotion == 'copy':
                source = mount / item['source']
                try:
                    if (not source.is_file() or source.stat().st_size != item['size']
                            or digest(source.read_bytes()) != item['sha256']):
                        result['wrong_retained_source'].append(item['source'])
                except OSError as error:
                    require(error.errno == errno.EIO, f'Unexpected source inspection error: {error}')
                    result['wrong_retained_source'].append(item['source'])
            try:
                found = [relative for relative in (item['source'], item['destination'])
                         if (mount / relative).is_file()]
                if not found:
                    result['missing_durable_clips'].append(item['source'])
                elif not any((mount / relative).stat().st_size == item['size']
                             and digest((mount / relative).read_bytes()) == item['sha256']
                             for relative in found):
                    result['wrong_durable_bytes'].append(item['source'])
                if self.acknowledgements[item['source']] in marks:
                    destination = mount / item['destination']
                    if (not destination.is_file() or destination.stat().st_size != item['size']
                            or digest(destination.read_bytes()) != item['sha256']):
                        result['wrong_acknowledged_path'].append(item['destination'])
            except OSError as error:
                require(error.errno == errno.EIO, f'Unexpected inspection error: {error}')
                result['read_errors'].append({'source': item['source'], 'error': str(error)})
        self.unmount(mount)
        self.detach(device)
        return result

    def fsck(self, args):
        result = self.command(args)
        accepted = (0, 1, 4, 5) if self.filesystem == 'ext4' else (0, 1)
        require(result['code'] in accepted, f'Filesystem checker operational failure: {args}: {result}')
        return result

    def evaluate(self, baseline, log, entries, count, tear):
        name = f'cut-{count:04}' + ('-sector-tear' if tear else '')
        raw = self.root / (name + '.img')
        repaired = self.root / (name + '-repaired.img')
        self.checked(['cp', '--sparse=always', baseline, raw])
        if count:
            self.checked(['replay-log', '--log', log, '--replay', raw, '--limit', str(count)])
        if tear:
            entry = entries[count]
            with log.open('rb') as source, raw.open('r+b', buffering=0) as target:
                source.seek(entry['data_offset'])
                sector = source.read(512)
                require(len(sector) == 512, 'Short sector read from block log')
                target.seek(entry['sector'] * 512)
                require(target.write(sector) == len(sector), 'Short torn-sector write')
                os.fsync(target.fileno())
        marks = {entry['mark'] for entry in entries[:count] if 'mark' in entry}
        result = {'cut': count, 'tear': tear, 'raw_image': str(raw), 'repaired_image': str(repaired)}
        if self.filesystem == 'ext4':
            archive_image = self.root / (name + '-archive.img')
            self.checked(['cp', '--sparse=always', raw, archive_image])
            journal = self.fsck(['e2fsck', '-p', '-E', 'journal_only', archive_image])
            journal_check = self.fsck(['e2fsck', '-f', '-n', archive_image])
            result['journal_replay'] = journal
            result['journal_check'] = journal_check
            if journal['code'] in (0, 1) and journal_check['code'] == 0:
                result['archive'] = self.inspect(archive_image, name + '-archive-mount', marks)
            else:
                result['archive_rejected'] = True
        else:
            result['archive'] = self.inspect(raw, name + '-archive-mount', marks)
        self.checked(['cp', '--sparse=always', raw, repaired])
        tool = 'fsck.fat' if self.filesystem == 'fat32' else 'e2fsck'
        check_args = ['-n'] if self.filesystem == 'fat32' else ['-f', '-n']
        result['fsck_before'] = self.fsck([tool, *check_args, repaired])
        result['automatic_repair'] = self.fsck([tool, '-p', repaired])
        result['fsck_after'] = self.fsck([tool, *check_args, repaired])
        if result['automatic_repair']['code'] in (0, 1) and result['fsck_after']['code'] == 0:
            result['maintenance'] = self.inspect(repaired, name + '-maintenance-mount', marks)
        else:
            result['automatic_recovery_failed'] = True
        (self.root / (name + '.json')).write_text(json.dumps(result, indent=2) + '\n')
        return result

    def execute(self):
        targets = self.checked(['dmsetup', 'targets'])
        require(any(line.startswith('log-writes ') for line in targets.splitlines()),
                'dm-log-writes is required; do not substitute killing a userspace process')
        require(shutil.which('replay-log') is not None, 'replay-log is required')
        image, device, baseline = self.baseline()
        log = self.record(image, device)
        entries = self.read_log(log)
        require(len(entries) == int(self.checked(['replay-log', '--log', log, '--num-entries'])),
                'Block-log parser count differs from replay-log')
        complete = next(entry['index'] + 1 for entry in entries if entry.get('mark') == 'complete')
        metadata = {'filesystem': self.filesystem, 'promotion': self.promotion, 'kernel': self.checked(['uname', '-r']),
                    'fault_model': 'completed-write prefixes plus first-sector-only persistence of one multi-sector write',
                    'sector_bytes': 512, 'manifest': self.manifest,
                    'acknowledgements': self.acknowledgements, 'entries': entries,
                    'baseline': str(baseline), 'log': str(log)}
        (self.root / 'manifest.json').write_text(json.dumps(metadata, indent=2) + '\n')
        controls = {count: self.evaluate(baseline, log, entries, count, False)
                    for count in (0, complete)}
        for control in controls.values():
            self.verify_control(control)
        results = []
        for count in range(complete + 1):
            result = controls[count] if count in controls else self.evaluate(baseline, log, entries, count, False)
            results.append(result)
            if count and entries[count - 1].get('mark', '').endswith('-acknowledged'):
                self.verify_control(result)
            print(json.dumps({'event': 'prefix_checked', 'filesystem': self.filesystem,
                              'cut': count, 'total': complete,
                              'missing': (len(result['maintenance']['missing_durable_clips'])
                                          if 'maintenance' in result else None),
                              'automatic_recovery_failed': result.get('automatic_recovery_failed', False)}), flush=True)
            if count < complete:
                entry = entries[count]
                if entry['sectors'] > 1 and not entry['flags'] & (4 | 8):
                    results.append(self.evaluate(baseline, log, entries, count, True))
        summary = {'filesystem': self.filesystem, 'promotion': self.promotion, 'artifact_directory': str(self.root),
                   'prefix_cases': sum(not result['tear'] for result in results),
                   'sector_tear_cases': sum(result['tear'] for result in results),
                   'repair_exit_one_cases': sum(result['automatic_repair']['code'] == 1 for result in results),
                   'automatic_recovery_failures': sum(result.get('automatic_recovery_failed', False) for result in results),
                   'acknowledgement_controls': len(self.acknowledgements),
                   'archive_rejected': sum(result.get('archive_rejected', False) for result in results)}
        for phase in ('archive', 'maintenance'):
            inspected = [result[phase] for result in results if phase in result]
            summary[phase] = {key: sum(bool(result[key]) for result in inspected)
                              for key in ('missing_durable_clips', 'wrong_durable_bytes', 'wrong_acknowledged_path', 'read_errors', 'wrong_retained_source')}
            summary[phase]['inspected_cases'] = len(inspected)
        (self.root / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
        print(json.dumps({'event': 'filesystem_recovery_summary', **summary}), flush=True)
        print(json.dumps({'event': 'experiment_complete', 'filesystem': self.filesystem,
                          'meaning': 'all cuts evaluated; recovery outcomes are in summary.json'}), flush=True)

    def verify_control(self, control):
        require(not control.get('automatic_recovery_failed', False)
                and not control.get('archive_rejected', False), f'Positive control failed: {control}')
        for phase in ('archive', 'maintenance'):
            require(not any(control[phase][key] for key in
                            ('missing_durable_clips', 'wrong_durable_bytes', 'wrong_acknowledged_path', 'read_errors', 'wrong_retained_source')),
                    f'Positive control failed: {control}')

def main():
    arguments = argparse.ArgumentParser()
    arguments.add_argument('--filesystem', choices=('fat32', 'ext4'), required=True)
    arguments.add_argument('--promotion', choices=('rename', 'copy'), required=True)
    args = arguments.parse_args()
    experiment = Experiment(args.filesystem, args.promotion, Settings())
    try:
        experiment.execute()
    finally:
        experiment.clean_up()


if __name__ == '__main__':
    main()
