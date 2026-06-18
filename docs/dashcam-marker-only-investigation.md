# Dashcam Marker-Only Event Investigation

This document records an investigation into Tesla Dashcam/Sentry event folders
that contained only marker files (`event.json` and `thumb.png`) and no MP4
video files. It is written for a public repository, so private hostnames, local
paths, IP addresses, cloud paths, exact locations, and raw event metadata are
intentionally omitted.

## Summary

The observed failure was not that the USB mass-storage gadget could never
accept MP4 writes. The car continued to write `RecentClips` MP4 files, and
after clearing the car's dashcam clip set, manual saves again produced full
`SavedClips` event folders with MP4 videos.

The strongest current hypothesis is that the car's dashcam state or the FAT
directory state became wedged. The in-car **Delete Dashcam Clips** action
removed the stale clip set, reset the relevant TeslaCam directories, and the
next save succeeded. Because this was fixed by deleting dashcam clips rather
than by recreating the backing image or changing filesystems, formatting or
switching to ext4 should be held as follow-up experiments rather than treated as
the immediate fix.

The silent part of the failure is the real operational risk: a save can appear
to succeed in the car UI while the archived event later contains only
`event.json` and `thumb.png`. TeslaUSB should eventually detect this condition,
but detection must avoid false positives while the car is still writing an
event.

## What Was Observed

- Several Saved/Sentry event folders in the archive contained only
  `event.json` and `thumb.png`.
- During the same period, `RecentClips` still contained MP4 files, so the car
  was capable of writing video data to the exposed disk.
- Manual save attempts initially produced marker-only event folders.
- A full power-cycle sequence did not consistently fix the issue. One save
  produced a partial event with an MP4, but later saves again produced
  marker-only folders.
- Deleting an old clip through the car UI briefly created and then deleted a
  full event folder, showing that the car could still write full MP4 event
  folders under some UI operations.
- The in-car **Delete Dashcam Clips** action removed the stale TeslaCam clip
  set and recreated the active dashcam structure.
- After **Delete Dashcam Clips**, a manual save produced a full event with 18
  MP4 files plus `event.json` and `thumb.png`.
- After restarting TeslaUSB, two more manual saves succeeded:
  - first save: 60 MP4 files plus marker files
  - second save: 12 MP4 files plus marker files
- USB stayed configured during the successful save tests.
- The kernel log did not show mass-storage or FAT errors during the successful
  save tests.
- TeslaUSB was archiving from a read-only snapshot while the car continued to
  write to the live gadget. That concurrent activity is expected and did not
  prevent the successful saves.

## FAT/LFN Findings

FAT stores long filenames through Long File Name (LFN) directory entries. The
underlying short `8.3` entry can look like `2026-0~4.MP4` or
`20AC1B~7.MP4`. If the LFN entries are stale, missing, or malformed, tools may
surface those short names directly.

During the investigation:

- The image was FAT32, created by `mkfs.fat`.
- Read-only FAT inspection found invalid or orphaned LFN state before the
  in-car delete operation.
- After **Delete Dashcam Clips**, the active `RecentClips` and `SavedClips`
  directory state was clean enough for saves to produce full events again.
- Some short-name-like MP4 entries can be transient while the car is actively
  writing and finalizing long filenames. Do not treat a short name as
  corruption unless it persists after the car is idle or across repeated scans.
- Tesla itself can write `thumb.png` in `RecentClips`; its presence there is not
  by itself evidence that TeslaUSB created or moved the file.

## What This Suggests

Most likely:

- The car's dashcam clip index or FAT directory state got into a bad state.
- The car could still write MP4 files to `RecentClips`.
- The car sometimes failed to assemble saved/sentry event folders from those
  recent recordings.
- Clearing dashcam clips from the car UI reset enough state for saved events to
  work again.

Less likely based on the current evidence:

- A general inability of the USB gadget to accept MP4 writes.
- A general network/archive problem.
- A pure Google Drive display/listing issue.
- A requirement to switch away from FAT immediately.

Still possible:

- FAT directory corruption or stale LFN state contributed to the car's internal
  dashcam index failure.
- Abrupt USB disconnects, power loss, or unsafe removal can contribute to FAT
  inconsistency.
- TeslaUSB behavior that cleans up files may still need hardening so it never hides useful
  evidence when marker-only events appear.

## Data Loss Assessment

For event folders that only contain `event.json` and `thumb.png`, the MP4s were
not present in those event folders at archive time. That is real loss from the
event-folder perspective.

However, some video data may still be recoverable if:

- the corresponding MP4 segments still exist in `RecentClips`;
- FAT recovery files such as `FSCK*.REC` contain playable video fragments;
- orphaned or short-name MP4 files can be matched back to events; or
- embedded video metadata can correlate recovered files to event timestamps.

Recovery was intentionally put on hold during this investigation so the live
write failure could be isolated first.

## Important False-Positive Caveat

Do not define "event folder has marker files and no MP4s" as an immediate
failure.

The car may create `event.json` and `thumb.png` before it has finished moving or
renaming MP4 segments into the event folder. A detector that flags marker-only
folders immediately could produce false alarms during normal writes.

A safer detector should classify events in stages:

- **candidate**: event has `event.json` or `thumb.png` and no MP4s;
- **suspect**: candidate remains marker-only after the car is idle and after a
  grace period;
- **confirmed**: suspect remains marker-only across a later scan, snapshot, or
  archive cycle.

Suggested initial grace period: at least 5-10 minutes after the event directory
or marker file modification time, plus a quiet write interval from the idle
detector. The exact value should be configurable.

## What Not To Delete Automatically

Avoid automatically deleting these without first preserving evidence:

- `FSCK*.REC` files
- exposed short-name MP4 files such as `*~[0-9].MP4`
- marker-only Saved/Sentry event folders
- files whose size or modification time is still changing
- files in events that have not passed the marker-only grace period

It is reasonable to archive these files, but deleting them immediately can hide
the failure and make later recovery impossible.

## Next Steps

### 1. Finish and deploy idle gating

TeslaUSB should only snapshot/archive after the car has stopped writing for a
stable interval. The local worktree already contains changes that wire the
process-based idle detector into `run` and `archive`, and changes timeout
behavior so a timed-out idle wait skips that archive cycle instead of
proceeding anyway.

Before deploying:

- run the unit tests;
- review the idle timeout and quiet interval;
- verify the coordinator logs show an idle wait before snapshots;
- verify a save during an archive does not race with live clean up.

### 2. Add persistent health detection

Add a health scan that can run in `teslausb status --json`, coordinator logs,
or a separate diagnostic command.

It should report:

- candidate/suspect/confirmed marker-only Saved/Sentry events;
- event age and latest modification time;
- whether writes were recently observed;
- short-name MP4 files that persist after idle;
- `FSCK*.REC` recovery files;
- `fsck` results from the last file clean up cycle;
- current USB gadget state and file-storage PID;
- snapshot count and any stale snapshot warnings.

The detector should not require listing huge Google Drive `RecentClips`
directories. For archive-side checks, use a lightweight index or the existing
Google Drive serving tool rather than broad `rclone` listings.

### 3. Preserve marker-only evidence

When a marker-only event becomes suspect or confirmed, TeslaUSB should preserve
enough evidence for recovery. Options:

- copy marker-only event folders to a diagnostic/evidence area before any clean
  up;
- keep marker-only folders on the live cam disk until they are old enough and
  explicitly confirmed;
- log event names and file lists without logging private metadata;
- archive short-name and recovery files separately before removing them.

This behavior must be age-gated to avoid interfering with normal event
completion.

### 4. Add recovery tooling later

Recovery should be a separate pass, not mixed with live write debugging.

Potential recovery inputs:

- `FSCK*.REC`
- exposed short-name MP4 files
- orphaned MP4 data
- `RecentClips` segments that overlap marker-only event timestamps
- embedded MP4 metadata

Tesla publishes a dashcam metadata extraction project at
<https://github.com/teslamotors/dashcam>. Concepts from that tool may help
correlate recovered videos with event timestamps, cameras, and vehicle state.

### 5. Keep FAT recreation as the next destructive diagnostic

If marker-only events return, the next destructive test should probably be to
recreate the FAT image from TeslaUSB.

Reasoning:

- **Delete Dashcam Clips** fixed the issue once.
- Recreating the FAT image is a stronger version of the same reset.
- It keeps TeslaUSB's current Linux tooling path simple (`mkfs.fat`,
  `fsck.fat`/`fsck`, and normal mount behavior).
- It is less of a compatibility change than switching filesystem type.

Before recreating FAT:

- preserve current recovery files and short-name MP4s;
- record a read-only FAT scan;
- record `fsck` output;
- record the current TeslaCam tree summary;
- ensure any valuable archive uploads are complete or intentionally abandoned.

### 6. Try Tesla-side format as a diagnostic, with caveats

Tesla's public owner documentation says the in-car **Format USB Drive** action
formats as exFAT and creates TeslaCam/TeslaTrackMode folders. The same
documentation says manually formatted drives may use exFAT, MS-DOS FAT, ext3,
or ext4.

Caveats for TeslaUSB:

- If the car formats the image as exFAT, the device must have kernel and user
  space support to mount/check exFAT during archive and file clean up.
- Existing TeslaUSB code currently creates a FAT32 image, not exFAT.
- Tesla-side format is destructive and may make current Linux-side tooling
  insufficient until exFAT support is installed and tested.

Use this after preserving evidence and after deciding whether the goal is
diagnosis or normal operation.

Tesla manual reference:
<https://www.tesla.com/ownersmanual/model3/en_us/GUID-F311BBCA-2532-4D04-B88C-DBA784ADEE21.html>

### 7. Try ext4 only after validating the simpler fixes

Ext4 may help by avoiding FAT LFN fragility and adding journaling, but it is a
bigger behavior change.

Potential benefits:

- no FAT LFN entries;
- stronger Linux repair tools;
- journaled metadata;
- simpler detection of normal Unix file state.

Potential risks:

- Tesla compatibility may vary by firmware/model/region despite public docs;
- the car's in-vehicle format action will not create ext4;
- some desktop inspection workflows become less convenient;
- TeslaUSB's image creation, mount, and fsck paths need explicit ext4 support;
- unsafe removal can still cause issues even with journaling.

Ext4 should be tested only after:

- idle gating is validated;
- monitoring is in place;
- FAT recreation has been tried or deliberately skipped;
- there is a rollback path to FAT32.

## Operational Runbook If It Happens Again

When a marker-only Saved/Sentry event is suspected:

1. Stop destructive clean up first. Do not delete `FSCK*.REC`, short-name MP4s,
   or marker-only event folders.
2. Capture a read-only filesystem summary of TeslaCam.
3. Record USB gadget state, file-storage PID, service state, and recent kernel
   logs.
4. Capture whether the car is still writing by watching file-storage I/O.
5. Wait for idle plus the marker-only grace period before declaring failure.
6. Compare the suspect event timestamps with `RecentClips` segments.
7. Preserve recovery files and short-name MP4s before running repair or clean up.
8. If the issue blocks saves, try **Delete Dashcam Clips** as the least
   destructive car-side reset that has already worked once.
9. If it recurs after that, recreate the FAT image.
10. Consider Tesla-side format or ext4 only after the evidence is preserved.

## Watchpoints

Watch for:

- marker-only Saved/Sentry events that remain marker-only after idle and grace;
- persistent short-name MP4 files;
- `FSCK*.REC` files;
- repeated `fsck: errors corrected`;
- FAT dirty-bit repairs;
- USB disconnect/reset messages outside expected file clean up windows;
- file-storage PID changes during active recording;
- stale snapshots that are not explained by an active archive;
- large reads caused by clip viewing in the car UI;
- uploads still running from read-only snapshots while the live gadget remains
  writable.

## Current Recommended Position

Do not switch filesystems yet.

The best current path is:

1. keep the current FAT32 image running after **Delete Dashcam Clips** fixed
   saves;
2. finish and deploy idle gating;
3. add age-gated marker-only health detection;
4. preserve recovery evidence instead of immediately cleaning it up;
5. recreate FAT if the issue returns;
6. test Tesla-side exFAT formatting or ext4 only as later, controlled
   experiments.
