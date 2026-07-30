"""Read, inspect, and upload bounded bancho.py binary assets."""

from __future__ import annotations

import asyncio
import csv
import hashlib
import os
import stat
from dataclasses import dataclass
from pathlib import Path

from perfcho.infra.logging import log_event
from perfcho.modules.common import ObjectStorage, ObjectUnavailable, StoredObject

MAX_BEATMAP_FILE_BYTES = 16 * 1024 * 1024
MAX_REPLAY_FILE_BYTES = 16 * 1024 * 1024
_UPLOAD_ATTEMPTS = 3


class MigrationStorageError(Exception):
    """Base an actionable migration asset failure."""


class SourceFileMissing(MigrationStorageError):
    """Indicate that an expected legacy asset does not exist."""


class SourceFileInvalid(MigrationStorageError):
    """Indicate that a legacy asset is unsafe, malformed, or too large."""


class BeatmapChecksumMismatch(MigrationStorageError):
    """Indicate that an on-disk beatmap is not the database revision."""


class ObjectUploadFailed(MigrationStorageError):
    """Indicate that an immutable source asset could not be staged."""


@dataclass(frozen=True, slots=True)
class BeatmapFileMetadata:
    """Describe one verified legacy .osu payload and parsed structure."""

    path: Path
    content: bytes
    md5: bytes
    sha256: bytes
    size_bytes: int
    first_object_time_ms: int
    last_object_time_ms: int
    drain_length_ms: int
    object_count: int
    circle_count: int
    slider_count: int
    spinner_count: int
    has_storyboard: bool
    has_video: bool


@dataclass(frozen=True, slots=True)
class ReplayFileMetadata:
    """Describe one bounded legacy .osr payload."""

    path: Path
    content: bytes
    sha256: bytes
    size_bytes: int


@dataclass(frozen=True, slots=True)
class _ParsedOsu:
    first_object_time_ms: int
    last_object_time_ms: int
    drain_length_ms: int
    object_count: int
    circle_count: int
    slider_count: int
    spinner_count: int
    has_storyboard: bool
    has_video: bool


def read_beatmap_file(
    data_directory: Path,
    map_id: int,
    expected_md5: str | bytes,
    *,
    maximum_bytes: int = MAX_BEATMAP_FILE_BYTES,
) -> BeatmapFileMetadata:
    """Read and structurally inspect one expected `.data/osu` beatmap."""
    digest = _md5_digest(expected_md5)
    path, content = _read_source_file(data_directory, "osu", map_id, ".osu", maximum_bytes)
    actual_md5 = hashlib.md5(content, usedforsecurity=False).digest()
    if actual_md5 != digest:
        raise BeatmapChecksumMismatch(f"beatmap {map_id} at {path} has MD5 {actual_md5.hex()}, expected {digest.hex()}")
    parsed = _parse_osu(content, map_id=map_id, path=path)
    return BeatmapFileMetadata(
        path=path,
        content=content,
        md5=actual_md5,
        sha256=hashlib.sha256(content).digest(),
        size_bytes=len(content),
        first_object_time_ms=parsed.first_object_time_ms,
        last_object_time_ms=parsed.last_object_time_ms,
        drain_length_ms=parsed.drain_length_ms,
        object_count=parsed.object_count,
        circle_count=parsed.circle_count,
        slider_count=parsed.slider_count,
        spinner_count=parsed.spinner_count,
        has_storyboard=parsed.has_storyboard,
        has_video=parsed.has_video,
    )


def read_replay_file(
    data_directory: Path,
    score_id: int,
    *,
    maximum_bytes: int = MAX_REPLAY_FILE_BYTES,
) -> ReplayFileMetadata:
    """Read one bounded `.data/osr` replay without manufacturing missing data."""
    path, content = _read_source_file(data_directory, "osr", score_id, ".osr", maximum_bytes)
    return ReplayFileMetadata(path, content, hashlib.sha256(content).digest(), len(content))


async def upload_beatmap_file(
    object_storage: ObjectStorage,
    metadata: BeatmapFileMetadata,
    *,
    source_code: str,
    beatmapset_id: int,
    beatmap_id: int,
    invocation_id: str | None = None,
    migration_id: str | None = None,
) -> StoredObject:
    """Idempotently upload a verified beatmap using the runtime's canonical key."""
    _positive_identifier("beatmapset_id", beatmapset_id)
    _positive_identifier("beatmap_id", beatmap_id)
    if (
        not source_code
        or source_code in {".", ".."}
        or len(source_code) > 32
        or any(
            character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-"
            for character in source_code
        )
    ):
        raise ValueError("source_code must be one safe storage-key segment")
    key = f"beatmaps/{source_code}/{beatmapset_id}/{beatmap_id}/{metadata.sha256.hex()}.osu"
    return await _upload(
        object_storage,
        key,
        metadata.content,
        media_type="application/x-osu-beatmap",
        sha256=metadata.sha256,
        object_kind="beatmap",
        invocation_id=invocation_id,
        migration_id=migration_id,
    )


async def upload_replay_file(
    object_storage: ObjectStorage,
    metadata: ReplayFileMetadata,
    *,
    account_id: int,
    invocation_id: str | None = None,
    migration_id: str | None = None,
) -> StoredObject:
    """Idempotently upload a replay using the Stable content-addressed key."""
    _positive_identifier("account_id", account_id)
    key = f"replays/stable/{account_id}/{metadata.sha256.hex()}.osr"
    return await _upload(
        object_storage,
        key,
        metadata.content,
        media_type="application/octet-stream",
        sha256=metadata.sha256,
        object_kind="replay",
        invocation_id=invocation_id,
        migration_id=migration_id,
    )


def _read_source_file(
    data_directory: Path,
    subdirectory: str,
    identifier: int,
    suffix: str,
    maximum_bytes: int,
) -> tuple[Path, bytes]:
    _positive_identifier("source identifier", identifier)
    if maximum_bytes < 1:
        raise ValueError("maximum_bytes must be positive")
    base = data_directory.expanduser().resolve()
    data_root = base if base.name == ".data" else base / ".data"
    expected_parent = (data_root / subdirectory).resolve()
    unresolved = expected_parent / f"{identifier}{suffix}"
    try:
        path = unresolved.resolve(strict=True)
    except FileNotFoundError as error:
        raise SourceFileMissing(f"expected source file is missing: {unresolved}") from error
    try:
        path.relative_to(expected_parent)
    except ValueError as error:
        raise SourceFileInvalid(f"source file escapes {expected_parent}: {unresolved}") from error

    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except FileNotFoundError as error:
        raise SourceFileMissing(f"expected source file is missing: {path}") from error
    except OSError as error:
        raise SourceFileInvalid(f"source file cannot be opened safely: {path}") from error
    try:
        details = os.fstat(descriptor)
        if not stat.S_ISREG(details.st_mode):
            raise SourceFileInvalid(f"source path is not a regular file: {path}")
        if details.st_size <= 0:
            raise SourceFileInvalid(f"source file is empty: {path}")
        if details.st_size > maximum_bytes:
            raise SourceFileInvalid(f"source file exceeds {maximum_bytes} bytes: {path} ({details.st_size} bytes)")
        with os.fdopen(descriptor, "rb", closefd=False) as source:
            content = source.read(maximum_bytes + 1)
        if len(content) > maximum_bytes:
            raise SourceFileInvalid(f"source file exceeds {maximum_bytes} bytes while reading: {path}")
        if len(content) != details.st_size:
            raise SourceFileInvalid(f"source file changed while it was being read: {path}")
        return path, content
    finally:
        os.close(descriptor)


def _parse_osu(content: bytes, *, map_id: int, path: Path) -> _ParsedOsu:
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError as error:
        raise SourceFileInvalid(f"beatmap {map_id} is not valid UTF-8: {path}") from error
    lines = text.splitlines()
    if not lines or not lines[0].strip().lower().startswith("osu file format v"):
        raise SourceFileInvalid(f"beatmap {map_id} has no osu file format header: {path}")

    section = ""
    object_times: list[int] = []
    breaks: list[tuple[int, int]] = []
    circle_count = 0
    slider_count = 0
    spinner_count = 0
    has_storyboard = False
    has_video = False
    for line_number, raw_line in enumerate(lines[1:], start=2):
        line = raw_line.strip()
        if not line or line.startswith("//"):
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1].strip().casefold()
            continue
        try:
            fields = next(csv.reader((line,), skipinitialspace=True))
        except csv.Error as error:
            raise SourceFileInvalid(f"beatmap {map_id} has invalid CSV at line {line_number}: {path}") from error
        if section == "hitobjects":
            if len(fields) < 4:
                raise SourceFileInvalid(f"beatmap {map_id} has an invalid hit object at line {line_number}: {path}")
            try:
                object_time = int(fields[2])
                object_type = int(fields[3])
            except ValueError as error:
                raise SourceFileInvalid(
                    f"beatmap {map_id} has a non-integer hit object at line {line_number}: {path}"
                ) from error
            if not 0 <= object_time <= 2_147_483_647 or object_type <= 0:
                raise SourceFileInvalid(f"beatmap {map_id} has an invalid hit object at line {line_number}: {path}")
            object_times.append(object_time)
            if object_type & 8:
                spinner_count += 1
            elif object_type & 2:
                slider_count += 1
            else:
                circle_count += 1
        elif section == "events":
            event_type = fields[0].strip().casefold() if fields else ""
            if event_type in {"video", "1"}:
                has_video = True
            elif event_type in {"sprite", "animation", "4", "5", "6"}:
                has_storyboard = True
            elif event_type in {"break", "2"} and len(fields) >= 3:
                try:
                    start, end = int(fields[1]), int(fields[2])
                except ValueError as error:
                    raise SourceFileInvalid(
                        f"beatmap {map_id} has an invalid break at line {line_number}: {path}"
                    ) from error
                if start < 0 or end <= start:
                    raise SourceFileInvalid(f"beatmap {map_id} has an invalid break at line {line_number}: {path}")
                breaks.append((start, end))

    if not object_times:
        raise SourceFileInvalid(f"beatmap {map_id} contains no hit objects: {path}")
    first_object = min(object_times)
    last_object = max(object_times)
    break_length = _covered_break_time(breaks, first_object, last_object)
    return _ParsedOsu(
        first_object_time_ms=first_object,
        last_object_time_ms=last_object,
        drain_length_ms=max(0, last_object - first_object - break_length),
        object_count=len(object_times),
        circle_count=circle_count,
        slider_count=slider_count,
        spinner_count=spinner_count,
        has_storyboard=has_storyboard,
        has_video=has_video,
    )


def _covered_break_time(breaks: list[tuple[int, int]], start: int, end: int) -> int:
    clipped = sorted((max(start, left), min(end, right)) for left, right in breaks if right > start and left < end)
    covered = 0
    current_start = current_end = 0
    for left, right in clipped:
        if right <= left:
            continue
        if current_end == 0:
            current_start, current_end = left, right
        elif left <= current_end:
            current_end = max(current_end, right)
        else:
            covered += current_end - current_start
            current_start, current_end = left, right
    if current_end:
        covered += current_end - current_start
    return covered


async def _upload(
    object_storage: ObjectStorage,
    key: str,
    content: bytes,
    *,
    media_type: str,
    sha256: bytes,
    object_kind: str,
    invocation_id: str | None,
    migration_id: str | None,
) -> StoredObject:
    last_error: ObjectUnavailable | None = None
    for attempt in range(1, _UPLOAD_ATTEMPTS + 1):
        try:
            stored = await object_storage.put(key, content, media_type=media_type, expected_sha256=sha256)
        except ObjectUnavailable as error:
            last_error = error
            if attempt < _UPLOAD_ATTEMPTS:
                delay_seconds = 0.1 * 2 ** (attempt - 1)
                log_event(
                    "WARNING",
                    "migration.storage.retry_scheduled",
                    invocation_id=invocation_id,
                    migration_id=migration_id,
                    object_kind=object_kind,
                    size_bytes=len(content),
                    attempt=attempt,
                    maximum_attempts=_UPLOAD_ATTEMPTS,
                    delay_ms=int(delay_seconds * 1000),
                    error_type=type(error).__name__,
                )
                await asyncio.sleep(delay_seconds)
                continue
            log_event(
                "ERROR",
                "migration.storage.failed",
                invocation_id=invocation_id,
                migration_id=migration_id,
                object_kind=object_kind,
                size_bytes=len(content),
                attempts=attempt,
                failure="provider_unavailable",
                error_type=type(error).__name__,
            )
            continue
        if stored.storage_key != key or stored.size_bytes != len(content) or stored.sha256 != sha256:
            log_event(
                "ERROR",
                "migration.storage.failed",
                invocation_id=invocation_id,
                migration_id=migration_id,
                object_kind=object_kind,
                size_bytes=len(content),
                attempts=attempt,
                failure="metadata_mismatch",
            )
            raise ObjectUploadFailed("object storage returned inconsistent metadata")
        return stored
    raise ObjectUploadFailed(f"object storage write failed after {_UPLOAD_ATTEMPTS} attempts") from last_error


def _md5_digest(value: str | bytes) -> bytes:
    if isinstance(value, bytes):
        if len(value) != 16:
            raise ValueError("expected beatmap MD5 must contain 16 bytes")
        return value
    if len(value) != 32:
        raise ValueError("expected beatmap MD5 must contain 32 hexadecimal characters")
    try:
        digest = bytes.fromhex(value)
    except ValueError as error:
        raise ValueError("expected beatmap MD5 must be hexadecimal") from error
    if len(digest) != 16:
        raise ValueError("expected beatmap MD5 must contain 16 bytes")
    return digest


def _positive_identifier(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")
