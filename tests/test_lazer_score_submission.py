import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import cast

import httpx
import pytest
from fastapi import FastAPI

from perfcho.api import router
from perfcho.api.canonical.dependencies import get_canonical_services
from perfcho.infra.compose import StableServices
from perfcho.infra.settings import Settings
from perfcho.modules.authorization import AuthorizationQueryService
from perfcho.modules.common import Clock, IdGenerator
from perfcho.modules.identity import AuthenticatedAccount, IdentityService, InvalidAccessToken
from perfcho.modules.realtime import RealtimeStateRepository
from perfcho.modules.scoring import (
    AcceptedScoreResult,
    AcceptScore,
    CanonicalMod,
    IssueSoloScoreToken,
    Ruleset,
    ScoreDetailView,
    ScoreGrade,
    ScoreOutcome,
    ScoringService,
    SoloScoreToken,
)

NOW = datetime(2026, 8, 10, 12, tzinfo=UTC)


class FakeIdentity:
    def __init__(self) -> None:
        self.tokens: list[str] = []
        self.invalid = False

    async def authenticate_access_token(self, token: str) -> AuthenticatedAccount:
        self.tokens.append(token)
        if self.invalid:
            raise InvalidAccessToken()
        return AuthenticatedAccount(
            account_id=42,
            current_name="Alice",
            account_type="user",
            country_code="JP",
            registered_at=NOW,
            last_seen_at=NOW,
            session_id=uuid.uuid7(),
            scope_codes=("public", "identify", "lazer"),
        )


class FakeScoring:
    def __init__(self) -> None:
        self.issues: list[IssueSoloScoreToken] = []
        self.acceptances: list[AcceptScore] = []

    async def issue_solo_token(self, command: IssueSoloScoreToken) -> SoloScoreToken:
        self.issues.append(command)
        return SoloScoreToken(
            token_id=700,
            account_id=42,
            beatmap_id=123,
            beatmap_revision_id=456,
            ruleset=command.ruleset,
            started_at=NOW,
            expires_at=NOW + timedelta(hours=2),
        )

    async def accept(self, command: AcceptScore) -> AcceptedScoreResult:
        self.acceptances.append(command)
        return AcceptedScoreResult(
            attempt_id=uuid.uuid7(),
            score_id=900,
            beatmap_id=123,
            beatmap_revision_id=456,
            ruleset=command.ruleset,
            mods=command.mods,
            mods_digest=b"m" * 32,
            outcome=command.score.outcome,
        )


class FakeScoreQuery:
    async def get(self, score_id: int, ruleset: Ruleset | None = None) -> ScoreDetailView | None:
        assert score_id == 900
        if ruleset is not None and ruleset is not Ruleset.OSU:
            return None
        return ScoreDetailView(
            score_id=900,
            account_id=42,
            display_name="Alice",
            country_code="JP",
            beatmap_id=123,
            ruleset=Ruleset.OSU,
            total_score=987654,
            classic_score=765432,
            accuracy=Decimal("0.95"),
            max_combo=321,
            grade=ScoreGrade.A,
            outcome=ScoreOutcome.PASSED,
            mods=(CanonicalMod("HD"), CanonicalMod("DT", {"speed_change": 1.25})),
            statistics={"great": 95, "ok": 5, "meh": 0, "miss": 0},
            maximum_statistics={"great": 100, "ok": 5},
            started_at=NOW,
            ended_at=NOW,
            has_replay=False,
            ranked=True,
            pp=None,
            position=None,
        )


class FixedClock:
    def now(self) -> datetime:
        return NOW


class FakeIds:
    def new(self) -> uuid.UUID:
        return uuid.uuid7()


def lazer_app(identity: FakeIdentity, scoring: FakeScoring) -> FastAPI:
    services = StableServices(
        identity=cast(IdentityService, identity),
        authorization=cast(AuthorizationQueryService, object()),
        realtime=cast(RealtimeStateRepository, object()),
        clock=cast(Clock, FixedClock()),
        id_generator=cast(IdGenerator, FakeIds()),
        settings=Settings(
            argon2_time_cost=1,
            argon2_memory_cost_kib=8,
            argon2_parallelism=1,
        ),
        scoring=cast(ScoringService, scoring),
        score_query=cast(object, FakeScoreQuery()),
    )
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_canonical_services] = lambda: services
    return app


@pytest.mark.asyncio
async def test_create_solo_score_token_matches_lazer_form_contract() -> None:
    identity = FakeIdentity()
    scoring = FakeScoring()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=lazer_app(identity, scoring)),
        base_url="http://api.test",
    ) as client:
        response = await client.post(
            "/api/v2/beatmaps/123/solo/scores",
            headers={"Authorization": "Bearer access-value", "x-api-version": "20260810"},
            data={
                "beatmap_hash": "00112233445566778899aabbccddeeff",
                "ruleset_id": "0",
                "version_hash": "ignored-client-version-hash",
            },
        )

    assert response.status_code == 200
    assert response.json() == {"id": 700}
    assert identity.tokens == ["access-value"]
    command = scoring.issues[0]
    assert command.beatmap.beatmap_id == 123
    assert command.beatmap.md5 == bytes.fromhex("00112233445566778899aabbccddeeff")
    assert command.ruleset is Ruleset.OSU
    assert command.meta.actor is not None and command.meta.actor.account_id == 42
    assert command.meta.client.family == "lazer"


@pytest.mark.asyncio
async def test_submit_solo_score_normalizes_lazer_json_to_canonical_command() -> None:
    identity = FakeIdentity()
    scoring = FakeScoring()
    body = {
        "rank": "A",
        "total_score": 987654,
        "total_score_without_mods": 765432,
        "accuracy": 0.95,
        "max_combo": 321,
        "ruleset_id": 0,
        "passed": True,
        "mods": [{"acronym": "HD"}, {"acronym": "DT", "settings": {"speed_change": 1.25}}],
        "statistics": {"great": 95, "ok": 5},
        "maximum_statistics": {"great": 100, "ok": 5},
        "pauses": [1234],
    }
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=lazer_app(identity, scoring)),
        base_url="http://api.test",
    ) as client:
        response = await client.put(
            "/api/v2/beatmaps/123/solo/scores/700",
            headers={"Authorization": "Bearer access-value", "x-api-version": "20260810"},
            json=body,
        )

    assert response.status_code == 200
    assert response.json()["id"] == 900
    assert response.json()["position"] is None
    assert response.json()["pp"] is None
    command = scoring.acceptances[0]
    assert command.solo_token_id == 700
    assert command.score.outcome is ScoreOutcome.PASSED
    assert command.score.classic_score == 765432
    assert command.score.accuracy == Decimal("0.95")
    assert command.ruleset is Ruleset.OSU
    assert command.mods == (CanonicalMod("HD"), CanonicalMod("DT", {"speed_change": 1.25}))
    assert {hit.hit_result: hit.actual for hit in command.score.hits} == {
        "great": 95,
        "meh": 0,
        "miss": 0,
        "ok": 5,
    }
    assert command.attempt.client_metadata["pauses"] == (1234,)
    assert command.attestation.verification_state == "pending"
    assert command.attestation.evidence == {}


@pytest.mark.asyncio
async def test_solo_score_endpoints_require_lazer_bearer_token() -> None:
    identity = FakeIdentity()
    scoring = FakeScoring()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=lazer_app(identity, scoring)),
        base_url="http://api.test",
    ) as client:
        response = await client.post(
            "/api/v2/beatmaps/123/solo/scores",
            data={
                "beatmap_hash": "00112233445566778899aabbccddeeff",
                "ruleset_id": "0",
            },
        )

    assert response.status_code == 401
    assert scoring.issues == []
