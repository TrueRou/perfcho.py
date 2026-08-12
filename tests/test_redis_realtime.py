import os
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import msgpack
import pytest
from redis.asyncio import Redis

from perfcho.infra.redis.bubbles import encode_bubble
from perfcho.infra.redis.realtime import RedisRealtimeStateRepository
from perfcho.infra.redis.state import (
    RealtimeKeys,
    datetime_to_milliseconds,
    decode_ordered_frame,
    decode_presence,
    encode_presence,
    sequence_token,
)
from perfcho.modules.realtime import (
    CanonicalReplayFrame,
    CanonicalScoreFrame,
    InvalidFrame,
    PlayerActivity,
    PlayerStatistics,
    PresenceCapacityReached,
    PresenceIdentity,
    PresenceSnapshot,
    PresenceSubscription,
    RealtimeSessionFenced,
    RealtimeSessionNotFound,
    RealtimeStateRepository,
    SessionFence,
    SpectatorFrameAction,
    SpectatorFrameBubble,
)

NOW = datetime.now(UTC).replace(microsecond=0)
SESSION_EXPIRY = NOW + timedelta(seconds=40)
DURABLE_EXPIRY = NOW + timedelta(minutes=5)
PRESENCE_EXPIRY = NOW + timedelta(seconds=30)
PREFIX = "tests:realtime"


def presence_snapshot(
    account_id: int,
    revision: int,
    name: str,
    expires_at: datetime,
    session_id: uuid.UUID,
) -> PresenceSnapshot:
    return PresenceSnapshot(
        account_id,
        revision,
        PresenceIdentity(name, "JP", 9, frozenset({"account.login"})),
        PlayerActivity("playing", "map", 12, "checksum", "osu", ("HD",)),
        PlayerStatistics(1000, 0.98, 20, 2000, 7, 123),
        expires_at,
        session_id,
    )


@dataclass(slots=True)
class RepositoryDouble:
    redis: AsyncMock
    repository: RedisRealtimeStateRepository
    scripts: dict[str, AsyncMock]


@pytest.fixture
def repository_double() -> RepositoryDouble:
    redis = AsyncMock(spec=Redis)
    redis.hget = AsyncMock()
    redis.hgetall = AsyncMock()
    scripts: dict[str, AsyncMock] = {}

    def register(source: str) -> AsyncMock:
        marker = source.splitlines()[0].removeprefix("-- perfcho:").removesuffix(":v2")
        script = AsyncMock(name=marker)
        scripts[marker] = script
        return script

    redis.register_script.side_effect = register
    repository = RedisRealtimeStateRepository(
        redis,
        prefix=PREFIX,
        session_ttl=timedelta(minutes=1),
        presence_ttl=timedelta(minutes=1),
        max_frame_count=3,
        max_frame_bytes=48,
        max_channels_per_session=4,
        max_spectators_per_host=3,
    )
    return RepositoryDouble(redis, repository, scripts)


def frame_value(cursor: int, sequence: int, expires_at: datetime, payload: bytes) -> bytes:
    return f"{sequence_token(cursor)}:{datetime_to_milliseconds(expires_at)}:{sequence:05d}:".encode() + payload


def spectator_frame(
    host_account_id: int, sequence: int, *, action: SpectatorFrameAction = SpectatorFrameAction.UPDATE
) -> SpectatorFrameBubble:
    return SpectatorFrameBubble(
        host_account_id,
        sequence,
        action,
        (CanonicalReplayFrame(10, 1.0, 2.0, 1, 0),),
        CanonicalScoreFrame(10, 1, 1, 0, 0, 0, 0, 0, 300, 1, 1, True, 255, 0, False),
        0,
    )


def stored_frame_value(cursor: int, frame: SpectatorFrameBubble, expires_at: datetime) -> bytes:
    return frame_value(cursor, frame.sequence, expires_at, encode_bubble(frame))


def test_repository_registers_v2_atomic_consistency_scripts(repository_double: RepositoryDouble) -> None:
    assert isinstance(repository_double.repository, RealtimeStateRepository)
    assert len(repository_double.scripts) == 16

    sources = {
        call.args[0].splitlines()[0]: call.args[0] for call in repository_double.redis.register_script.call_args_list
    }
    assert all(marker.endswith(":v2") for marker in sources)
    heartbeat = sources["-- perfcho:heartbeat-session:v2"]
    assert "SMEMBERS" in heartbeat
    assert "spectator_session_id" in heartbeat
    assert "durable_expires_at" in heartbeat
    assert "math.min(requested_expiry, durable_expiry, now + ttl)" in heartbeat
    assert "return {'OK', ARGV[1], ARGV[3], tostring(expiry)}" in heartbeat
    open_session = sources["-- perfcho:open-session:v2"]
    assert "math.min(requested_expiry, durable_expiry, now + ttl)" in open_session
    assert "return {'OK', ARGV[1], tostring(revision), tostring(expiry)}" in open_session
    publish = sources["-- perfcho:publish-frame:v2"]
    assert "ZPOPMIN" in publish
    assert "% 65536" in publish
    assert "local reset_sequence = ARGV[16] == '1'" in publish
    assert "if reset_sequence or (stored_session" in publish
    assert "LPUSH" not in publish
    attach = sources["-- perfcho:attach-spectator:v2"]
    assert "latest_frame_window" in attach
    assert "relation_id" in attach


def test_state_codecs_include_owner_and_independent_frame_cursor() -> None:
    session_id = uuid.uuid7()
    snapshot = presence_snapshot(42, 7, "player", PRESENCE_EXPIRY, session_id)
    assert decode_presence(encode_presence(snapshot)) == snapshot
    frame = decode_ordered_frame(frame_value(9, 0, PRESENCE_EXPIRY, b"frame"))
    assert frame.cursor == 9
    assert frame.sequence == 0
    assert frame.payload == b"frame"

    keys = RealtimeKeys("deployment:")
    assert keys.base == "deployment:v2"
    assert keys.account_session(42) == "deployment:v2:account:42:session"
    assert keys.session_channels(session_id).endswith(f"session:{session_id}:channels")
    assert keys.session_revision(session_id).endswith(f"session:{session_id}:revision")


@pytest.mark.parametrize(
    "mutate",
    [
        lambda body: body | {"v": 2},
        lambda body: body | {"extra": True},
        lambda body: body | {"identity": body["identity"] | {"extra": True}},
        lambda body: body | {"session_id": b"short"},
        lambda body: body | {"expires_at": True},
    ],
)
def test_presence_codec_strictly_rejects_unknown_versions_fields_and_types(
    mutate: Callable[[dict[str, object]], dict[str, object]],
) -> None:
    snapshot = presence_snapshot(42, 7, "player", PRESENCE_EXPIRY, uuid.uuid7())
    body = msgpack.unpackb(encode_presence(snapshot), raw=False)
    malformed = msgpack.packb(mutate(body), use_bin_type=True)

    with pytest.raises(ValueError, match="stored presence"):
        decode_presence(malformed)


@pytest.mark.asyncio
async def test_session_open_and_heartbeat_carry_all_owned_state(repository_double: RepositoryDouble) -> None:
    repository = repository_double.repository
    session_id = uuid.uuid7()
    expiry_ms = datetime_to_milliseconds(SESSION_EXPIRY)
    open_script = repository_double.scripts["open-session"]
    open_script.return_value = [b"OK", b"42", b"1", str(expiry_ms).encode()]

    session = await repository.open_session(
        session_id=session_id,
        account_id=42,
        expires_at=SESSION_EXPIRY,
        durable_expires_at=DURABLE_EXPIRY,
    )

    assert session.fence == SessionFence(session_id, 1)
    open_call = open_script.await_args
    assert open_call is not None
    assert open_call.kwargs["keys"] == [
        f"{PREFIX}:v2:session:{session_id}",
        f"{PREFIX}:v2:account:42:session",
        f"{PREFIX}:v2:presence:42",
        f"{PREFIX}:v2:presence:index",
        f"{PREFIX}:v2:preference:42",
        f"{PREFIX}:v2:session:{session_id}:channels",
        f"{PREFIX}:v2:spectator:viewer:42:host",
        f"{PREFIX}:v2:spectator:viewer:42:revision",
        f"{PREFIX}:v2:spectator:host:42:viewers",
        f"{PREFIX}:v2:spectator:host:42:frames",
        f"{PREFIX}:v2:spectator:host:42:frame-bytes",
        f"{PREFIX}:v2:spectator:host:42:frame-state",
        f"{PREFIX}:v2:session:{session_id}:revision",
    ]
    assert open_call.kwargs["args"][2:5] == [
        expiry_ms,
        datetime_to_milliseconds(DURABLE_EXPIRY),
        60_000,
    ]

    repository_double.redis.hget.return_value = b"42"
    heartbeat = repository_double.scripts["heartbeat-session"]
    heartbeat.return_value = [b"OK", b"42", b"1", str(expiry_ms).encode()]
    assert (await repository.heartbeat_session(session_id, expected_revision=1, expires_at=SESSION_EXPIRY)) == session
    heartbeat_call = heartbeat.await_args
    assert heartbeat_call is not None
    assert heartbeat_call.kwargs["keys"][2:8] == [
        f"{PREFIX}:v2:presence:42",
        f"{PREFIX}:v2:presence:index",
        f"{PREFIX}:v2:preference:42",
        f"{PREFIX}:v2:session:{session_id}:channels",
        f"{PREFIX}:v2:spectator:viewer:42:host",
        f"{PREFIX}:v2:spectator:host:42:viewers",
    ]

    heartbeat.return_value = [b"FENCED"]
    with pytest.raises(RealtimeSessionFenced):
        await repository.heartbeat_session(session_id, expected_revision=1, expires_at=SESSION_EXPIRY)


@pytest.mark.asyncio
async def test_presence_claim_passes_capacity_to_atomic_script_and_maps_overflow(
    repository_double: RepositoryDouble,
) -> None:
    repository = repository_double.repository
    session_id = uuid.uuid7()
    snapshot = presence_snapshot(42, 1, "player", PRESENCE_EXPIRY, session_id)
    set_presence = repository_double.scripts["set-presence"]
    set_presence.return_value = [b"CAPACITY"]

    with pytest.raises(PresenceCapacityReached):
        await repository.set_presence(snapshot, session_id=session_id, capacity=17)

    call = set_presence.await_args
    assert call is not None
    assert call.kwargs["args"][-1] == 17


@pytest.mark.asyncio
async def test_list_presences_decodes_pipeline_values_without_per_account_reads(
    repository_double: RepositoryDouble,
) -> None:
    first_session = uuid.uuid7()
    second_session = uuid.uuid7()
    index_pipeline = MagicMock()
    index_pipeline.__aenter__.return_value = index_pipeline
    index_pipeline.execute = AsyncMock(return_value=[0, [b"42", b"43"]])
    hash_pipeline = MagicMock()
    hash_pipeline.__aenter__.return_value = hash_pipeline
    first = presence_snapshot(42, 2, "first", PRESENCE_EXPIRY, first_session)
    second = presence_snapshot(43, 3, "second", PRESENCE_EXPIRY, second_session)
    hash_pipeline.execute = AsyncMock(
        return_value=[
            {
                b"account_id": b"42",
                b"revision": b"2",
                b"expires_at": str(datetime_to_milliseconds(PRESENCE_EXPIRY)).encode(),
                b"session_id": str(first_session).encode(),
                b"state": encode_presence(first),
            },
            {
                b"account_id": b"43",
                b"revision": b"3",
                b"expires_at": str(datetime_to_milliseconds(PRESENCE_EXPIRY)).encode(),
                b"session_id": str(second_session).encode(),
                b"state": encode_presence(second),
            },
        ]
    )
    repository_double.redis.pipeline.side_effect = [index_pipeline, hash_pipeline]

    snapshots = await repository_double.repository.list_presences(at=NOW, limit=10)

    assert snapshots == (
        first,
        second,
    )
    repository_double.redis.hgetall.assert_not_awaited()
    assert repository_double.redis.pipeline.call_count == 2


@pytest.mark.asyncio
async def test_spectator_contract_returns_atomic_handoff_and_live_recipients(
    repository_double: RepositoryDouble,
) -> None:
    repository = repository_double.repository
    host = SessionFence(uuid.uuid7(), 2)
    spectator = SessionFence(uuid.uuid7(), 5)
    relation_id = uuid.uuid7()
    attach = repository_double.scripts["attach-spectator"]
    attach.return_value = [
        b"OK",
        b"7",
        b"0000000000000000004",
        b"0000000000000000005",
        b"0",
        stored_frame_value(4, spectator_frame(42, 65535), PRESENCE_EXPIRY),
        stored_frame_value(5, spectator_frame(42, 0), PRESENCE_EXPIRY),
    ]

    attachment = await repository.attach_spectator(
        42,
        43,
        relation_id=relation_id,
        host_fence=host,
        spectator_fence=spectator,
        expires_at=PRESENCE_EXPIRY,
        history_limit=3,
    )

    assert attachment.relation.relation_id == relation_id
    assert attachment.relation.host_fence == host
    assert tuple(frame.sequence for frame in attachment.history.frames) == (65535, 0)
    assert attachment.history.latest_cursor == 5

    publish = repository_double.scripts["publish-frame"]
    recipient_44 = SessionFence(uuid.uuid7(), 3)
    publish.return_value = [
        b"OK",
        b"6",
        b"43",
        str(spectator.session_id).encode(),
        str(spectator.revision).encode(),
        str(datetime_to_milliseconds(PRESENCE_EXPIRY)).encode(),
        b"44",
        str(recipient_44.session_id).encode(),
        str(recipient_44.revision).encode(),
        str(datetime_to_milliseconds(PRESENCE_EXPIRY)).encode(),
    ]
    published = await repository.publish_spectator_frame(
        42,
        host_fence=host,
        frame=spectator_frame(42, 1),
        reset_history=False,
        expires_at=PRESENCE_EXPIRY,
    )
    assert published.frame.cursor == 6
    assert tuple(recipient.account_id for recipient in published.recipients) == (43, 44)
    assert published.recipients[0].fence == spectator
    assert publish.call_args.kwargs["args"][-1] == 0

    publish.return_value = [b"OK", b"1"]
    reset = await repository.publish_spectator_frame(
        42,
        host_fence=host,
        frame=spectator_frame(42, 0, action=SpectatorFrameAction.NEW_PLAY),
        reset_history=True,
        expires_at=PRESENCE_EXPIRY,
    )
    assert reset.frame.cursor == 1
    assert publish.call_args.kwargs["args"][-1] == 1

    publish.return_value = [b"FRAME_TOO_LARGE"]
    with pytest.raises(InvalidFrame, match="frame_too_large"):
        await repository.publish_spectator_frame(
            42,
            host_fence=host,
            frame=spectator_frame(42, 2),
            reset_history=False,
            expires_at=PRESENCE_EXPIRY,
        )

    detach = repository_double.scripts["detach-spectator"]
    detach.return_value = [b"STALE"]
    assert not await repository.detach_spectator(
        42,
        43,
        relation_id=relation_id,
        expected_revision=7,
        host_fence=host,
        spectator_fence=spectator,
    )


async def _open(
    repository: RedisRealtimeStateRepository,
    account_id: int,
    now: datetime,
    *,
    session_id: uuid.UUID | None = None,
) -> tuple[uuid.UUID, SessionFence]:
    session_id = session_id or uuid.uuid7()
    session = await repository.open_session(
        session_id=session_id,
        account_id=account_id,
        expires_at=now + timedelta(seconds=20),
        durable_expires_at=now + timedelta(minutes=2),
    )
    return session_id, session.fence


@pytest.mark.skipif(not os.getenv("TEST_REDIS_URL"), reason="TEST_REDIS_URL is not configured")
@pytest.mark.asyncio
async def test_real_redis_presence_capacity_is_atomic() -> None:
    redis = Redis.from_url(os.environ["TEST_REDIS_URL"], decode_responses=False)
    repository = RedisRealtimeStateRepository(
        redis,
        prefix=f"tests:realtime-capacity:{uuid.uuid4()}",
        session_ttl=timedelta(seconds=30),
        presence_ttl=timedelta(seconds=30),
        max_frame_count=2,
        max_frame_bytes=4096,
    )
    now = datetime.now(UTC)
    try:
        _, first = await _open(repository, 42, now)
        _, second = await _open(repository, 43, now)
        await repository.set_presence(
            presence_snapshot(42, first.revision, "first", now + timedelta(seconds=12), first.session_id),
            session_id=first.session_id,
            capacity=1,
        )
        with pytest.raises(PresenceCapacityReached):
            await repository.set_presence(
                presence_snapshot(43, second.revision, "second", now + timedelta(seconds=12), second.session_id),
                session_id=second.session_id,
                capacity=1,
            )
    finally:
        await redis.aclose()


@pytest.mark.skipif(not os.getenv("TEST_REDIS_URL"), reason="TEST_REDIS_URL is not configured")
@pytest.mark.asyncio
async def test_real_redis_open_session_tolerates_application_clock_skew() -> None:
    redis = Redis.from_url(os.environ["TEST_REDIS_URL"], decode_responses=False)
    repository = RedisRealtimeStateRepository(
        redis,
        prefix=f"tests:realtime-clock-skew:{uuid.uuid4()}",
        session_ttl=timedelta(seconds=30),
        presence_ttl=timedelta(seconds=30),
        max_frame_count=2,
        max_frame_bytes=4096,
    )
    application_now = datetime.now(UTC) + timedelta(seconds=5)
    requested_expiry = application_now + timedelta(seconds=30)
    try:
        session = await repository.open_session(
            session_id=uuid.uuid7(),
            account_id=42,
            expires_at=requested_expiry,
            durable_expires_at=application_now + timedelta(minutes=5),
        )
        assert session.expires_at < requested_expiry
        assert session.expires_at <= datetime.now(UTC) + timedelta(seconds=31)
    finally:
        await redis.aclose()


@pytest.mark.skipif(not os.getenv("TEST_REDIS_URL"), reason="TEST_REDIS_URL is not configured")
@pytest.mark.asyncio
async def test_real_redis_epoch_heartbeat_and_spectator_history_consistency() -> None:
    redis = Redis.from_url(os.environ["TEST_REDIS_URL"], decode_responses=False)
    prefix = f"tests:realtime:{uuid.uuid4()}"
    repository = RedisRealtimeStateRepository(
        redis,
        prefix=prefix,
        session_ttl=timedelta(seconds=30),
        presence_ttl=timedelta(seconds=30),
        max_frame_count=2,
        max_frame_bytes=4096,
        max_channels_per_session=4,
        max_spectators_per_host=1,
    )
    now = datetime.now(UTC)

    try:
        host_id, host = await _open(repository, 42, now)
        spectator_id, spectator = await _open(repository, 43, now)
        await repository.set_presence(
            presence_snapshot(42, host.revision, "host", now + timedelta(seconds=12), host.session_id),
            session_id=host.session_id,
        )
        await repository.set_presence(
            presence_snapshot(43, spectator.revision, "spectator", now + timedelta(seconds=12), spectator.session_id),
            session_id=spectator.session_id,
        )
        await repository.set_presence_subscription(
            42,
            session_id=host.session_id,
            expected_revision=host.revision,
            subscription=PresenceSubscription.FOLLOWED,
        )
        await repository.set_away_message(
            42,
            session_id=host.session_id,
            expected_revision=host.revision,
            message="away",
        )
        await repository.join_channel(7, session_id=host.session_id, expected_revision=host.revision)

        renewed_host = await repository.heartbeat_session(
            host.session_id,
            expected_revision=host.revision,
            expires_at=now + timedelta(seconds=24),
        )
        presence = await repository.get_presence(42, at=now)
        assert presence is not None and presence.expires_at == renewed_host.expires_at
        assert await repository.get_presence_subscription(42) is PresenceSubscription.FOLLOWED
        assert await repository.get_away_message(42) == "away"
        assert await repository.list_channel_members(7) == frozenset({42})

        first_publish = await repository.publish_spectator_frame(
            42,
            host_fence=host,
            frame=spectator_frame(42, 65535),
            reset_history=False,
            expires_at=now + timedelta(seconds=18),
        )
        assert first_publish.recipients == ()
        first_relation_id = uuid.uuid7()
        first_attachment = await repository.attach_spectator(
            42,
            43,
            relation_id=first_relation_id,
            host_fence=host,
            spectator_fence=spectator,
            expires_at=now + timedelta(seconds=18),
            history_limit=2,
        )
        assert tuple(frame.sequence for frame in first_attachment.history.frames) == (65535,)
        _, other_host = await _open(repository, 44, now)
        _, other_spectator = await _open(repository, 45, now)
        await repository.set_presence(
            presence_snapshot(
                44, other_host.revision, "other host", now + timedelta(seconds=12), other_host.session_id
            ),
            session_id=other_host.session_id,
        )
        await repository.set_presence(
            presence_snapshot(
                45,
                other_spectator.revision,
                "other spectator",
                now + timedelta(seconds=12),
                other_spectator.session_id,
            ),
            session_id=other_spectator.session_id,
        )
        await repository.attach_spectator(
            44,
            45,
            relation_id=uuid.uuid7(),
            host_fence=other_host,
            spectator_fence=other_spectator,
            expires_at=now + timedelta(seconds=10),
            history_limit=2,
        )
        with pytest.raises(ValueError, match="limit"):
            await repository.attach_spectator(
                44,
                43,
                relation_id=uuid.uuid7(),
                host_fence=other_host,
                spectator_fence=spectator,
                expires_at=now + timedelta(seconds=10),
                history_limit=2,
            )
        unchanged = await repository.get_spectator_relation(43, spectator_fence=spectator, at=now)
        assert unchanged is not None and unchanged.host_account_id == 42
        renewed_spectator_before_relogin = await repository.heartbeat_session(
            spectator.session_id,
            expected_revision=spectator.revision,
            expires_at=now + timedelta(seconds=24),
        )
        await repository.heartbeat_session(
            host.session_id,
            expected_revision=host.revision,
            expires_at=now + timedelta(seconds=25),
        )
        renewed_relation = await repository.get_spectator_relation(43, spectator_fence=spectator, at=now)
        assert renewed_relation is not None
        assert renewed_relation.expires_at == renewed_spectator_before_relogin.expires_at

        zero = await repository.publish_spectator_frame(
            42,
            host_fence=host,
            frame=spectator_frame(42, 0),
            reset_history=False,
            expires_at=now + timedelta(seconds=18),
        )
        one = await repository.publish_spectator_frame(
            42,
            host_fence=host,
            frame=spectator_frame(42, 1),
            reset_history=False,
            expires_at=now + timedelta(seconds=18),
        )
        assert tuple(recipient.account_id for recipient in zero.recipients) == (43,)
        assert tuple(recipient.account_id for recipient in one.recipients) == (43,)
        assert zero.recipients[0].fence == spectator
        latest = await repository.read_spectator_frames(
            42,
            host_fence=host,
            after_cursor=None,
            limit=2,
            at=now,
        )
        assert tuple(frame.sequence for frame in latest.frames) == (0, 1)
        assert latest.oldest_cursor == zero.frame.cursor
        after_evicted = await repository.read_spectator_frames(
            42,
            host_fence=host,
            after_cursor=0,
            limit=2,
            at=now,
        )
        assert after_evicted.truncated
        with pytest.raises(InvalidFrame, match="non_monotonic"):
            await repository.publish_spectator_frame(
                42,
                host_fence=host,
                frame=spectator_frame(42, 0),
                reset_history=False,
                expires_at=now + timedelta(seconds=18),
            )
        reset = await repository.publish_spectator_frame(
            42,
            host_fence=host,
            frame=spectator_frame(42, 0, action=SpectatorFrameAction.NEW_PLAY),
            reset_history=True,
            expires_at=now + timedelta(seconds=18),
        )
        reset_window = await repository.read_spectator_frames(
            42,
            host_fence=host,
            after_cursor=None,
            limit=2,
            at=now,
        )
        assert reset.frame.cursor == 1
        assert tuple(frame.sequence for frame in reset_window.frames) == (0,)
        with pytest.raises(InvalidFrame, match="frame_too_large"):
            await repository.publish_spectator_frame(
                42,
                host_fence=host,
                frame=SpectatorFrameBubble(
                    42,
                    2,
                    SpectatorFrameAction.UPDATE,
                    tuple(CanonicalReplayFrame(index, 1.0, 2.0, 1, 0) for index in range(500)),
                    spectator_frame(42, 2).score,
                    0,
                ),
                reset_history=False,
                expires_at=now + timedelta(seconds=18),
            )

        second_relation_id = uuid.uuid7()
        second_attachment = await repository.attach_spectator(
            42,
            43,
            relation_id=second_relation_id,
            host_fence=host,
            spectator_fence=spectator,
            expires_at=now + timedelta(seconds=18),
            history_limit=2,
        )
        assert not await repository.detach_spectator(
            42,
            43,
            relation_id=first_relation_id,
            expected_revision=first_attachment.relation.revision,
            host_fence=host,
            spectator_fence=spectator,
        )
        assert (
            await repository.get_spectator_relation(43, spectator_fence=spectator, at=now)
        ) == second_attachment.relation

        new_spectator_id, new_spectator = await _open(repository, 43, now)
        assert new_spectator_id != spectator_id
        assert await repository.list_spectators(42, host_fence=host, at=now) == ()
        await repository.set_presence(
            presence_snapshot(
                43,
                new_spectator.revision,
                "new spectator",
                now + timedelta(seconds=12),
                new_spectator.session_id,
            ),
            session_id=new_spectator.session_id,
        )
        renewed_spectator = await repository.heartbeat_session(
            new_spectator.session_id,
            expected_revision=new_spectator.revision,
            expires_at=now + timedelta(seconds=24),
        )
        relation = await repository.attach_spectator(
            42,
            43,
            relation_id=uuid.uuid7(),
            host_fence=host,
            spectator_fence=new_spectator,
            expires_at=now + timedelta(seconds=20),
            history_limit=2,
        )
        assert relation.relation.expires_at <= renewed_spectator.expires_at

        keys = [key async for key in redis.scan_iter(match=f"{prefix}:v2:*")]
        assert keys
        ttls = [await redis.pttl(key) for key in keys]
        assert all(ttl > 0 for ttl in ttls)
        await repository.fence_session(host_id, expected_revision=host.revision)
        with pytest.raises(RealtimeSessionNotFound):
            await repository.resolve_session(host_id, at=now)
        reopened = await repository.open_session(
            session_id=host_id,
            account_id=42,
            expires_at=now + timedelta(seconds=20),
            durable_expires_at=now + timedelta(minutes=2),
        )
        assert reopened.revision == host.revision + 1
        with pytest.raises(RealtimeSessionFenced, match="superseded"):
            await repository.publish_spectator_frame(
                42,
                host_fence=host,
                frame=spectator_frame(42, 2),
                reset_history=False,
                expires_at=now + timedelta(seconds=10),
            )
        with pytest.raises(RealtimeSessionFenced, match="superseded"):
            await repository.attach_spectator(
                42,
                43,
                relation_id=uuid.uuid7(),
                host_fence=host,
                spectator_fence=new_spectator,
                expires_at=now + timedelta(seconds=10),
                history_limit=2,
            )
    finally:
        keys = [key async for key in redis.scan_iter(match=f"{prefix}:v2:*")]
        if keys:
            await redis.delete(*keys)
        await redis.aclose()


@pytest.mark.skipif(not os.getenv("TEST_REDIS_URL"), reason="TEST_REDIS_URL is not configured")
@pytest.mark.asyncio
async def test_real_redis_publish_cleans_viewer_missing_relation_without_corrupting_history() -> None:
    redis = Redis.from_url(os.environ["TEST_REDIS_URL"], decode_responses=False)
    prefix = f"tests:realtime-stale-viewer:{uuid.uuid4()}"
    repository = RedisRealtimeStateRepository(
        redis,
        prefix=prefix,
        session_ttl=timedelta(seconds=30),
        presence_ttl=timedelta(seconds=30),
        max_frame_count=2,
        max_frame_bytes=4096,
    )
    now = datetime.now(UTC)
    viewers_key = f"{prefix}:v2:spectator:host:42:viewers"
    relation_key = f"{prefix}:v2:spectator:viewer:43:host"

    try:
        _, host = await _open(repository, 42, now)
        await repository.set_presence(
            presence_snapshot(42, host.revision, "host", now + timedelta(seconds=12), host.session_id),
            session_id=host.session_id,
        )
        await redis.zadd(viewers_key, {"43": int((now + timedelta(seconds=18)).timestamp() * 1000)})
        assert not await redis.exists(relation_key)

        published = await repository.publish_spectator_frame(
            42,
            host_fence=host,
            frame=spectator_frame(42, 1),
            reset_history=False,
            expires_at=now + timedelta(seconds=10),
        )

        assert published.recipients == ()
        assert not await redis.exists(viewers_key)
        history = await repository.read_spectator_frames(
            42,
            host_fence=host,
            after_cursor=None,
            limit=2,
            at=now,
        )
        assert history.frames == (published.frame,)
        assert history.oldest_cursor == published.frame.cursor
        assert history.latest_cursor == published.frame.cursor
    finally:
        keys = [key async for key in redis.scan_iter(match=f"{prefix}:v2:*")]
        if keys:
            await redis.delete(*keys)
        await redis.aclose()
