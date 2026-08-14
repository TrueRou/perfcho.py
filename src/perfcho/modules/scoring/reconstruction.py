"""Reconstruct lazer solo-score replays from the spectator frame stream.

The lazer client submits scores without replay bytes; frames arrive separately
through the spectator hub and are retained in bounded realtime state. This
service encodes the retained frame stream into a stable-compatible ``.osr`` and
persists it as a staged replay manifest for the accepted score.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import cast

from perfcho.infra.logging import log_event
from perfcho.modules.scoring.replay_encoding import encode_replay, replay_digest
from perfcho.modules.common.ports import ObjectStorage
from perfcho.modules.realtime import RealtimeStateRepository, SessionFence
from perfcho.modules.scoring.models import (
    ReplayReconstructionFacts,
    StagedReplayManifest,
)
from perfcho.modules.scoring.ports import ReplayRepository, ScoringUnitOfWork


class ReplayReconstructionService:
    """Encode and persist one replay from score facts and retained frames."""

    def __init__(
        self,
        uow_factory: Callable[[], ScoringUnitOfWork],
        repository_factory: Callable[[object], ReplayRepository],
        object_storage: ObjectStorage,
        realtime: RealtimeStateRepository,
    ) -> None:
        """Bind persistence, storage, and realtime frame dependencies."""
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory
        self._object_storage = object_storage
        self._realtime = realtime

    async def reconstruct(self, score_id: int, host_fence: SessionFence) -> StagedReplayManifest | None:
        """Build and persist one replay, returning None when facts or frames are missing."""
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            facts = cast(ReplayReconstructionFacts | None, await repository.get_replay_reconstruction_facts(score_id))
            if facts is None:
                return None
            window = await self._realtime.read_spectator_frames(
                facts.account_id,
                host_fence=host_fence,
                after_cursor=None,
                limit=10_000,
                at=facts.ended_at,
            )
            flattened = tuple(f for window_frame in window.frames for f in window_frame.frames)
            replay = encode_replay(
                ruleset=facts.ruleset,
                username=facts.display_name,
                beatmap_md5=facts.beatmap_md5,
                hits=dict(facts.statistics),
                total_score=facts.total_score,
                max_combo=facts.max_combo,
                perfect=facts.perfect,
                mods=tuple(mod.acronym for mod in facts.mods),
                ended_at=facts.ended_at,
                frames=flattened,
            )
            digest = replay_digest(replay)
            storage_key = f"replays/lazer/{facts.account_id}/{digest.hex()}.osr"
            stored = await self._object_storage.put(
                storage_key,
                replay,
                media_type="application/x-osu-replay",
                expected_sha256=digest,
            )
            manifest = StagedReplayManifest(
                format="lazer",
                sha256=digest,
                size_bytes=stored.size_bytes,
                storage_key=stored.storage_key,
                client_version=None,
            )
            await repository.persist_replay(score_id, manifest)
            await uow.commit()
            log_event(
                "INFO",
                "replay.reconstruction.completed",
                score_id=score_id,
                account_id=facts.account_id,
                frame_count=len(flattened),
                size_bytes=stored.size_bytes,
            )
            return manifest
