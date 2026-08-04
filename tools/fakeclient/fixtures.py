"""Seed deterministic content through production application adapters."""

import asyncio
import hashlib
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from perfcho.infra.db.engine import create_engine, create_session_factory
from perfcho.infra.db.repositories.content import SqlAlchemyContentRepository
from perfcho.infra.db.repositories.outbox import SqlAlchemyOutboxWriter
from perfcho.infra.db.uow import SqlAlchemyUnitOfWorkFactory
from perfcho.infra.glue.common import SystemClock, Uuid7Generator
from perfcho.infra.settings import settings
from perfcho.infra.storage import S3ObjectStorage
from perfcho.modules.content import ContentSyncService, UpstreamBeatmapsetSnapshot, UpstreamBeatmapSnapshot

BEATMAPSET_ID = 900_001
BEATMAP_ID = 900_002
BEATMAP_FILE_NAME = "Perfcho - Perfcho E2E (fakeclient) [Normal].osu"
_DATA_PATH = Path(__file__).with_name("data") / "baseline.osu"


class FixtureContentSource:
    """Return one immutable local beatmap fixture without external HTTP."""

    def __init__(self, content: bytes) -> None:
        """Bind the fixed beatmap bytes used by all source methods."""
        self._content = content
        updated_at = datetime(2026, 7, 31, tzinfo=UTC)
        beatmap = UpstreamBeatmapSnapshot(
            external_beatmap_id=BEATMAP_ID,
            md5=hashlib.md5(content, usedforsecurity=False).digest(),
            file_name=BEATMAP_FILE_NAME,
            difficulty_name="Normal",
            ruleset="osu",
            status="ranked",
            source_updated_at=updated_at,
            total_length_ms=4_000,
            drain_length_ms=3_000,
            bpm=Decimal(120),
            circle_size=Decimal(4),
            overall_difficulty=Decimal(7),
            approach_rate=Decimal(9),
            health_drain=Decimal(5),
            circle_count=3,
            slider_count=0,
            spinner_count=0,
            max_combo=3,
            star_rating=Decimal("1.25"),
            has_storyboard=False,
            has_video=False,
        )
        self.snapshot = UpstreamBeatmapsetSnapshot(
            source_code="osu",
            external_beatmapset_id=BEATMAPSET_ID,
            creator_external_id=None,
            creator_name="fakeclient",
            artist="Perfcho",
            artist_unicode=None,
            title="Perfcho E2E",
            title_unicode=None,
            source_text=None,
            tags="perfcho e2e fakeclient",
            genre_id=None,
            language_id=None,
            description="Deterministic fakeclient fixture.",
            status="ranked",
            submitted_at=updated_at,
            ranked_at=updated_at,
            last_updated_at=updated_at,
            available=True,
            nsfw=False,
            beatmaps=(beatmap,),
        )

    async def fetch_beatmapset(self, external_beatmapset_id: int) -> UpstreamBeatmapsetSnapshot:
        """Return the fixture set after validating its public identifier."""
        if external_beatmapset_id != BEATMAPSET_ID:
            raise ValueError("unknown fakeclient beatmapset")
        return self.snapshot

    async def lookup_beatmapset_id(self, checksum: str, file_name: str) -> int:
        """Resolve only the fixture checksum or filename."""
        expected = hashlib.md5(self._content, usedforsecurity=False).hexdigest()
        if checksum != expected and file_name != BEATMAP_FILE_NAME:
            raise ValueError("unknown fakeclient beatmap")
        return BEATMAPSET_ID

    async def fetch_beatmap_file(self, external_beatmap_id: int) -> bytes:
        """Return exact fixture bytes for the known beatmap."""
        if external_beatmap_id != BEATMAP_ID:
            raise ValueError("unknown fakeclient beatmap")
        return self._content


async def seed() -> None:
    """Synchronize the deterministic fixture into PostgreSQL and MinIO."""
    content = _DATA_PATH.read_bytes()
    engine = await create_engine()
    try:
        session_factory = create_session_factory(engine)
        uow_factory = SqlAlchemyUnitOfWorkFactory(session_factory)
        service = ContentSyncService(
            uow_factory,
            lambda session: SqlAlchemyContentRepository(session),
            lambda session: SqlAlchemyOutboxWriter(session),
            FixtureContentSource(content),
            S3ObjectStorage.from_settings(settings),
            SystemClock(),
            Uuid7Generator(),
        )
        result = await service.synchronize(BEATMAPSET_ID)
        if result.external_beatmapset_id != BEATMAPSET_ID:
            raise RuntimeError("fixture synchronization returned the wrong beatmapset")
    finally:
        await engine.dispose()


def main() -> int:
    """Seed fixtures as an isolated subprocess entry point."""
    asyncio.run(seed())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
