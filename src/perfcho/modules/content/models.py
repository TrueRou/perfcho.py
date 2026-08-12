"""Define immutable canonical beatmap and community content values."""

import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from perfcho.modules.common.models import StoredObject


@dataclass(frozen=True, slots=True)
class BeatmapRevisionView:
    """Describe one immutable beatmap file revision and its logical content."""

    beatmap_id: int
    external_beatmap_id: int
    beatmapset_id: int
    external_beatmapset_id: int
    revision_id: int
    md5: bytes
    sha256: bytes
    file_name: str
    artist: str
    title: str
    creator: str
    difficulty_name: str
    ruleset: str
    status: str
    source_updated_at: datetime
    total_length_ms: int
    drain_length_ms: int
    bpm: Decimal
    circle_size: Decimal
    overall_difficulty: Decimal
    approach_rate: Decimal
    health_drain: Decimal
    object_count: int
    max_combo: int
    star_rating: Decimal | None
    has_video: bool
    is_current: bool
    file_storage_key: str | None = None
    file_media_type: str | None = None
    file_size_bytes: int | None = None

    def __post_init__(self) -> None:
        """Require object metadata to be absent or complete."""
        object_metadata = (self.file_storage_key, self.file_media_type, self.file_size_bytes)
        if any(value is not None for value in object_metadata) and not all(
            value is not None for value in object_metadata
        ):
            raise ValueError("beatmap file object metadata must be complete")
        if self.file_size_bytes is not None and self.file_size_bytes < 0:
            raise ValueError("beatmap file size must not be negative")

    @property
    def md5_hex(self) -> str:
        """Return the hexadecimal revision checksum."""
        return self.md5.hex()


@dataclass(frozen=True, slots=True)
class BeatmapsetView:
    """Describe a beatmapset and its current difficulties."""

    beatmapset_id: int
    external_beatmapset_id: int
    artist: str
    title: str
    creator: str
    status: str
    last_updated_at: datetime | None
    available: bool
    has_video: bool
    beatmaps: tuple[BeatmapRevisionView, ...]


@dataclass(frozen=True, slots=True)
class ContentSearch:
    """Request one bounded beatmapset search page."""

    query: str = ""
    ruleset: str | None = None
    statuses: tuple[str, ...] = ()
    page: int = 0
    page_size: int = 100

    def __post_init__(self) -> None:
        """Validate bounded content search input."""
        if self.page < 0 or not 1 <= self.page_size <= 100:
            raise ValueError("content search page is invalid")
        if len(self.query) > 255:
            raise ValueError("content search query is too long")


@dataclass(frozen=True, slots=True)
class ContentSearchPage:
    """Return a bounded search result and whether another page exists."""

    items: tuple[BeatmapsetView, ...]
    has_more: bool


@dataclass(frozen=True, slots=True)
class FavouriteResult:
    """Return the resulting favourite state and whether it changed."""

    account_id: int
    beatmapset_id: int
    favourited: bool
    changed: bool


@dataclass(frozen=True, slots=True)
class RatingSummary:
    """Return aggregate rating and the requesting account's vote."""

    beatmap_id: int
    average: Decimal | None
    vote_count: int
    account_rating: int | None


@dataclass(frozen=True, slots=True)
class CommentView:
    """Describe one visible position-aware content comment."""

    comment_id: int
    author_account_id: int
    target: str
    position_ms: int
    body: str
    color: str | None
    created_at: datetime


@dataclass(frozen=True, slots=True)
class UpstreamBeatmapSnapshot:
    """Describe one upstream beatmap and the immutable revision to synchronize."""

    external_beatmap_id: int
    md5: bytes
    file_name: str
    difficulty_name: str
    ruleset: str
    status: str
    source_updated_at: datetime
    total_length_ms: int
    drain_length_ms: int
    bpm: Decimal
    circle_size: Decimal
    overall_difficulty: Decimal
    approach_rate: Decimal
    health_drain: Decimal
    circle_count: int
    slider_count: int
    spinner_count: int
    max_combo: int
    star_rating: Decimal | None
    has_storyboard: bool
    has_video: bool

    def __post_init__(self) -> None:
        """Reject malformed upstream metadata before any object writes."""
        if self.external_beatmap_id < 1 or len(self.md5) != 16:
            raise ValueError("upstream beatmap identity is invalid")
        if not self.file_name or len(self.file_name) > 255 or "/" in self.file_name or "\\" in self.file_name:
            raise ValueError("upstream beatmap filename is invalid")
        if self.ruleset not in {"osu", "taiko", "fruits", "mania"}:
            raise ValueError("upstream beatmap ruleset is invalid")
        if self.status not in {"graveyard", "wip", "pending", "ranked", "approved", "qualified", "loved"}:
            raise ValueError("upstream beatmap status is invalid")
        if self.source_updated_at.tzinfo is None or self.source_updated_at.utcoffset() is None:
            raise ValueError("upstream beatmap update time must be timezone-aware")
        counts = (self.circle_count, self.slider_count, self.spinner_count, self.max_combo)
        if self.total_length_ms < 0 or self.drain_length_ms < 0 or any(value < 0 for value in counts):
            raise ValueError("upstream beatmap lengths and counts must not be negative")

    @property
    def object_count(self) -> int:
        """Return the canonical sum of hit object counts."""
        return self.circle_count + self.slider_count + self.spinner_count


@dataclass(frozen=True, slots=True)
class UpstreamBeatmapsetSnapshot:
    """Describe one complete authoritative upstream beatmapset snapshot."""

    source_code: str
    external_beatmapset_id: int
    creator_external_id: int | None
    creator_name: str
    artist: str
    artist_unicode: str | None
    title: str
    title_unicode: str | None
    source_text: str | None
    tags: str
    genre_id: int | None
    language_id: int | None
    description: str | None
    status: str
    submitted_at: datetime | None
    ranked_at: datetime | None
    last_updated_at: datetime
    available: bool
    nsfw: bool
    beatmaps: tuple[UpstreamBeatmapSnapshot, ...]
    etag: str | None = None
    last_modified: str | None = None

    def __post_init__(self) -> None:
        """Require one bounded set of unique upstream beatmaps."""
        if not self.source_code or self.external_beatmapset_id < 1 or not self.creator_name:
            raise ValueError("upstream beatmapset identity is invalid")
        if not 1 <= len(self.beatmaps) <= 128:
            raise ValueError("upstream beatmapset must contain between 1 and 128 beatmaps")
        identifiers = tuple(beatmap.external_beatmap_id for beatmap in self.beatmaps)
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("upstream beatmap IDs must be unique")
        if self.last_updated_at.tzinfo is None or self.last_updated_at.utcoffset() is None:
            raise ValueError("upstream beatmapset update time must be timezone-aware")


@dataclass(frozen=True, slots=True)
class SyncedBeatmapFile:
    """Bind one upstream revision to its verified immutable object."""

    beatmap: UpstreamBeatmapSnapshot
    asset_id: uuid.UUID
    stored_object: StoredObject

    def __post_init__(self) -> None:
        """Require the stored payload digest used by the revision."""
        if self.stored_object.sha256 is None:
            raise ValueError("synchronized beatmap objects require a sha256 digest")


@dataclass(frozen=True, slots=True)
class ContentSyncResult:
    """Summarize one atomically persisted upstream beatmapset synchronization."""

    beatmapset_id: int
    external_beatmapset_id: int
    created_revision_count: int
    unchanged_revision_count: int
    removed_beatmap_count: int
    published: bool = True
