"""Coordinate standalone difficulty attribute calculation with DB + Redis caching."""

from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import cast

import orjson

from perfcho.infra.cache.backend import CacheBackend
from perfcho.modules.common.ports import ObjectUrlProvider
from perfcho.modules.performance.models import DifficultyCalculationResult, DifficultyRequest
from perfcho.modules.performance.ports import (
    DifficultyRepositoryFactory,
    PerformanceCalculator,
    PerformanceUnitOfWork,
)
from perfcho.modules.scoring.models import Ruleset


class DifficultyQueryService:
    """Read, calculate, and cache difficulty attributes for a beatmap and mods."""

    def __init__(
        self,
        uow_factory: Callable[[], PerformanceUnitOfWork],
        repository_factory: DifficultyRepositoryFactory,
        cache: CacheBackend,
        calculator: PerformanceCalculator,
        object_url_provider: ObjectUrlProvider,
        *,
        beatmap_url_expiry_seconds: int,
    ) -> None:
        """Bind persistence, cache, calculator, and URL signing dependencies."""
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory
        self._cache = cache
        self._calculator = calculator
        self._object_url_provider = object_url_provider
        self._beatmap_url_expiry_seconds = beatmap_url_expiry_seconds

    async def resolve(
        self,
        *,
        beatmap_revision_id: int,
        beatmap_sha256: bytes,
        beatmap_storage_key: str,
        ruleset: Ruleset,
        mods_digest: bytes,
        mods: tuple,
    ) -> DifficultyCalculationResult:
        """Return cached or freshly calculated difficulty attributes."""
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            release = await repository.active_difficulty_release(ruleset)
            if release is None:
                raise RuntimeError("no active difficulty release is configured")
            request = DifficultyRequest(
                beatmap_revision_id=beatmap_revision_id,
                beatmap_sha256=beatmap_sha256,
                beatmap_storage_key=beatmap_storage_key,
                ruleset=ruleset,
                mods_digest=mods_digest,
                mods=mods,
                difficulty_formula_code=cast(str, release["formula_code"]),
                difficulty_release_version=cast(str, release["version"]),
                difficulty_release_id=cast(uuid.UUID, release["release_id"]),
                calculator=cast(str, release["calculator"]),
            )

        cache_key = self._cache_key(request)
        cached = await self._cache.get(cache_key)
        if cached is not None:
            return self._decode(cached)

        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            persisted = await repository.get(
                beatmap_revision_id=request.beatmap_revision_id,
                ruleset=request.ruleset,
                mods_digest=request.mods_digest,
                release_id=request.difficulty_release_id,
            )
            if persisted is not None:
                await self._cache.set(cache_key, self._encode(persisted), ttl_seconds=3600)
                return persisted

            beatmap_url = await self._object_url_provider.presign_read(
                request.beatmap_storage_key,
                expires_in_seconds=self._beatmap_url_expiry_seconds,
            )
            result = await self._calculator.calculate_difficulty(request, beatmap_url=beatmap_url)
            await repository.put(request, result)
            await uow.commit()

        await self._cache.set(cache_key, self._encode(result), ttl_seconds=3600)
        return result

    def _cache_key(self, request: DifficultyRequest) -> str:
        return self._cache.key(
            "scoring",
            "difficulty-attributes",
            f"{request.beatmap_revision_id}:{request.ruleset.value}:{request.mods_digest.hex()}:{request.difficulty_release_version}",
        )

    @staticmethod
    def _encode(result: DifficultyCalculationResult) -> bytes:
        return orjson.dumps(
            {
                "star_rating": str(result.star_rating),
                "max_combo": result.max_combo,
                "attributes": result.attributes,
            },
            option=orjson.OPT_SORT_KEYS,
        )

    @staticmethod
    def _decode(raw: bytes) -> DifficultyCalculationResult:
        from decimal import Decimal

        payload = orjson.loads(raw)
        return DifficultyCalculationResult(
            star_rating=Decimal(payload["star_rating"]),
            max_combo=int(payload["max_combo"]),
            attributes=payload["attributes"],
        )
