import struct
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest

from perfcho.api.stable.bubbles import StableBubbleRenderer, canonicalize_spectator_frame
from perfcho.api.stable.dispatcher import StableRuntimeContext
from perfcho.api.stable.dispatcher import dispatch_packets as dispatch_bubbles
from perfcho.api.stable.realtime import (
    ClientPacket,
    PacketReader,
    PacketWriter,
    ReplayAction,
    ReplayFrame,
    ReplayFrameBundle,
    ScoreFrame,
    ServerPacket,
    build_packet,
)
from perfcho.api.stable.realtime.models import UserPresence, UserStats
from perfcho.infra.compose import StableServices
from perfcho.infra.redis.realtime import RedisRealtimeStateRepository
from perfcho.infra.settings import Settings
from perfcho.modules.authorization import AuthorizationQueryService
from perfcho.modules.common import Clock, IdGenerator
from perfcho.modules.identity import IdentityService, ResolvedClientSession
from perfcho.modules.realtime import (
    PlayerActivity,
    PlayerStatistics,
    PresenceIdentity,
    PresenceSnapshot,
    RealtimeBubble,
    RealtimeBubbleBus,
    RealtimeSession,
    SessionFence,
    SpectatorAttachment,
    SpectatorFrame,
    SpectatorFrameBubble,
    SpectatorFramePublish,
    SpectatorFrameWindow,
    SpectatorRecipient,
    SpectatorRelation,
)

NOW = datetime(2026, 7, 29, 12, 30, tzinfo=UTC)


async def dispatch_packets(body: bytes, context: StableRuntimeContext, services: StableServices) -> bytes:
    bubbles = await dispatch_bubbles(body, context, services)
    rendered = StableBubbleRenderer().render_many(bubbles, max_bytes=services.settings.stable_max_response_bytes)
    return rendered + bytes(context.stable_output)


EXPIRY = NOW + timedelta(minutes=5)


class FixedClock:
    def now(self) -> datetime:
        return NOW


class FakeIds:
    def new(self) -> uuid.UUID:
        return uuid.uuid7()


class FakeBubbleBus:
    def __init__(self) -> None:
        self.published: list[tuple[SessionFence, RealtimeBubble]] = []

    async def publish(self, recipient_fence: SessionFence, bubble: RealtimeBubble) -> int:
        self.published.append((recipient_fence, bubble))
        return 1

    async def publish_many(self, recipient_fences: Sequence[SessionFence], bubble: RealtimeBubble) -> int:
        self.published.extend((fence, bubble) for fence in recipient_fences)
        return len(recipient_fences)


class SpectatorRealtime(RedisRealtimeStateRepository):
    def __init__(self) -> None:
        self.bubbles = FakeBubbleBus()
        self.fences = {account_id: SessionFence(uuid.uuid7(), 1) for account_id in (2, 3, 9)}
        self.presences = {
            account_id: PresenceSnapshot(
                account_id,
                fence.revision,
                PresenceIdentity(f"user-{account_id}", None, 0, frozenset({"account.login"})),
                PlayerActivity("idle"),
                PlayerStatistics(),
                EXPIRY,
                fence.session_id,
            )
            for account_id, fence in self.fences.items()
        }
        self.relations = {
            9: SpectatorRelation(
                2,
                9,
                uuid.uuid7(),
                1,
                self.fences[2],
                self.fences[9],
                EXPIRY,
            )
        }
        self.frames: dict[int, list[SpectatorFrame]] = {}
        self.detach_current = True

    async def get_presence(self, account_id: int, *, at: datetime) -> PresenceSnapshot | None:
        presence = self.presences.get(account_id)
        return presence if presence is not None and presence.expires_at > at else None

    async def get_spectator_relation(
        self,
        spectator_account_id: int,
        *,
        spectator_fence: SessionFence,
        at: datetime,
    ) -> SpectatorRelation | None:
        assert spectator_fence == self.fences[spectator_account_id]
        relation = self.relations.get(spectator_account_id)
        return relation if relation is not None and relation.expires_at > at else None

    async def list_spectators(
        self,
        host_account_id: int,
        *,
        host_fence: SessionFence,
        at: datetime,
    ) -> tuple[SpectatorRelation, ...]:
        assert host_fence == self.fences[host_account_id]
        return tuple(
            relation
            for relation in self.relations.values()
            if relation.host_account_id == host_account_id and relation.expires_at > at
        )

    async def attach_spectator(
        self,
        host_account_id: int,
        spectator_account_id: int,
        *,
        relation_id: uuid.UUID,
        host_fence: SessionFence,
        spectator_fence: SessionFence,
        expires_at: datetime,
        history_limit: int,
    ) -> SpectatorAttachment:
        assert host_fence == self.fences[host_account_id]
        assert spectator_fence == self.fences[spectator_account_id]
        previous = self.relations.get(spectator_account_id)
        relation = SpectatorRelation(
            host_account_id,
            spectator_account_id,
            relation_id,
            1 if previous is None else previous.revision + 1,
            host_fence,
            spectator_fence,
            expires_at,
        )
        self.relations[spectator_account_id] = relation
        frames = tuple(self.frames.get(host_account_id, ())[-history_limit:])
        return SpectatorAttachment(relation, _frame_window(frames))

    async def detach_spectator(
        self,
        host_account_id: int,
        spectator_account_id: int,
        *,
        relation_id: uuid.UUID,
        expected_revision: int,
        host_fence: SessionFence,
        spectator_fence: SessionFence,
    ) -> bool:
        relation = self.relations.get(spectator_account_id)
        if (
            self.detach_current
            and relation is not None
            and relation.host_account_id == host_account_id
            and relation.relation_id == relation_id
            and relation.revision == expected_revision
            and relation.host_fence == host_fence
            and relation.spectator_fence == spectator_fence
        ):
            del self.relations[spectator_account_id]
            return True
        return False

    async def publish_spectator_frame(
        self,
        host_account_id: int,
        *,
        host_fence: SessionFence,
        frame: SpectatorFrameBubble,
        reset_history: bool,
        expires_at: datetime,
    ) -> SpectatorFramePublish:
        assert expires_at > NOW
        assert host_fence == self.fences[host_account_id]
        if reset_history:
            self.frames.pop(host_account_id, None)
        stored = SpectatorFrame(
            len(self.frames.get(host_account_id, ())) + 1,
            frame.host_account_id,
            frame.sequence,
            frame.action,
            frame.frames,
            frame.score,
            frame.extra,
        )
        self.frames.setdefault(host_account_id, []).append(stored)
        recipients: list[SpectatorRecipient] = []
        for relation in await self.list_spectators(host_account_id, host_fence=host_fence, at=NOW):
            recipients.append(
                SpectatorRecipient(
                    relation.spectator_account_id,
                    relation.spectator_fence,
                    relation.expires_at,
                )
            )
        return SpectatorFramePublish(stored, tuple(recipients))

    async def read_spectator_frames(
        self,
        host_account_id: int,
        *,
        host_fence: SessionFence,
        after_cursor: int | None,
        limit: int,
        at: datetime,
    ) -> SpectatorFrameWindow:
        del at
        assert host_fence == self.fences[host_account_id]
        frames = tuple(
            frame
            for frame in self.frames.get(host_account_id, [])
            if after_cursor is None or frame.cursor > after_cursor
        )[:limit]
        return _frame_window(frames)


def _frame_window(frames: tuple[SpectatorFrame, ...]) -> SpectatorFrameWindow:
    return SpectatorFrameWindow(
        frames,
        frames[0].cursor if frames else None,
        frames[-1].cursor if frames else None,
        False,
    )


def context(account_id: int, name: str, realtime: SpectatorRealtime) -> StableRuntimeContext:
    fence = realtime.fences[account_id]
    return StableRuntimeContext(
        identity=ResolvedClientSession(
            account_id,
            name,
            1,
            fence.session_id,
            None,
            "b20260711.1",
            None,
            EXPIRY,
        ),
        realtime=RealtimeSession(fence.session_id, account_id, fence.revision, EXPIRY),
        presence=UserPresence(account_id, name, 0, 0, 1, 0, 0.0, 0.0, 0),
        stats=UserStats(account_id, 0, "", "", 0, 0, 0, 0, 0.0, 0, 0, 0, 0),
    )


def services(realtime: SpectatorRealtime) -> StableServices:
    return StableServices(
        identity=cast(IdentityService, object()),
        authorization=cast(AuthorizationQueryService, object()),
        realtime=realtime,
        clock=cast(Clock, FixedClock()),
        id_generator=cast(IdGenerator, FakeIds()),
        settings=Settings(),
        bubbles=cast(RealtimeBubbleBus, realtime.bubbles),
    )


def spectator_frame_packet(sequence: int, *, action: ReplayAction = ReplayAction.STANDARD) -> bytes:
    bundle = ReplayFrameBundle(
        frames=(ReplayFrame(0, 0, 1.0, 2.0, 10),),
        score_frame=ScoreFrame(10, 1, 1, 0, 0, 0, 0, 0, 300, 1, 1, True, 255, 0, False),
        action=action,
        extra=0,
        sequence=sequence,
    )
    writer = PacketWriter()
    with writer.packet(ClientPacket.SPECTATE_FRAMES):
        writer.write_replay_frame_bundle(bundle)
    return writer.to_bytes()


def packet_types(payloads: list[bytes] | bytes) -> list[ServerPacket]:
    payload = b"".join(payloads) if isinstance(payloads, list) else payloads
    result: list[ServerPacket] = []
    for packet in PacketReader(payload, packet_enum=ServerPacket):
        assert isinstance(packet.packet_type, ServerPacket)
        result.append(packet.packet_type)
    return result


def delivered_packet_types(realtime: SpectatorRealtime, account_id: int) -> list[ServerPacket]:
    fence = realtime.fences[account_id]
    rendered = [
        StableBubbleRenderer().render(bubble) for target, bubble in realtime.bubbles.published if target == fence
    ]
    return packet_types(rendered)


@pytest.mark.parametrize("action", tuple(ReplayAction))
def test_stable_frame_decode_canonical_encode_preserves_complete_bundle(action: ReplayAction) -> None:
    bundle = ReplayFrameBundle(
        frames=(ReplayFrame(3, 4, 12.5, -8.25, -10),),
        score_frame=ScoreFrame(10, 2, 3, 4, 5, 6, 7, 8, 9000, 10, 9, False, 200, 11, True, 0.25, 0.75),
        action=action,
        extra=-3,
        sequence=65535,
    )
    writer = PacketWriter()
    with writer.packet(ClientPacket.SPECTATE_FRAMES):
        writer.write_replay_frame_bundle(bundle)
    ingress = next(PacketReader(writer.to_bytes()))
    canonical = canonicalize_spectator_frame(2, ingress.payload.read_replay_frame_bundle())
    rendered = next(PacketReader(StableBubbleRenderer().render(canonical), packet_enum=ServerPacket))

    assert rendered.packet_type is ServerPacket.SPECTATE_FRAMES
    assert rendered.payload.read_replay_frame_bundle() == bundle
    rendered.payload.require_exhausted()


@pytest.mark.asyncio
async def test_spectator_join_frames_and_stop_notify_host_and_fellows(monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib

    dispatcher_module = importlib.import_module("perfcho.api.stable.dispatcher.packets")
    events: list[tuple[str, str, dict[str, object]]] = []

    def capture(level: str, event: str, **fields: object) -> None:
        events.append((level, event, fields))

    monkeypatch.setattr(dispatcher_module, "log_event", capture)
    realtime = SpectatorRealtime()
    stable_services = services(realtime)
    object.__setattr__(stable_services, "settings", Settings(log_hot_path_sample_rate=1))

    joined = await dispatch_packets(
        build_packet(ClientPacket.START_SPECTATING, struct.pack("<i", 2)),
        context(3, "spectator", realtime),
        stable_services,
    )

    assert realtime.relations[3].host_account_id == 2
    assert packet_types(joined) == [ServerPacket.FELLOW_SPECTATOR_JOINED]
    assert delivered_packet_types(realtime, 2) == [ServerPacket.SPECTATOR_JOINED]
    assert delivered_packet_types(realtime, 9) == [ServerPacket.FELLOW_SPECTATOR_JOINED]

    await dispatch_packets(spectator_frame_packet(1), context(2, "host", realtime), stable_services)

    assert delivered_packet_types(realtime, 3) == [ServerPacket.SPECTATE_FRAMES]
    assert delivered_packet_types(realtime, 9)[-1] is ServerPacket.SPECTATE_FRAMES

    await dispatch_packets(
        build_packet(ClientPacket.STOP_SPECTATING),
        context(3, "spectator", realtime),
        stable_services,
    )

    assert 3 not in realtime.relations
    assert delivered_packet_types(realtime, 2)[-1] is ServerPacket.SPECTATOR_LEFT
    assert delivered_packet_types(realtime, 9)[-1] is ServerPacket.FELLOW_SPECTATOR_LEFT
    assert any(event == "stable.spectator.attach" and fields["outcome"] == "attached" for _, event, fields in events)
    assert any(event == "stable.spectator.detach" and fields["outcome"] == "detached" for _, event, fields in events)
    frame_event = next(
        fields for level, event, fields in events if level == "DEBUG" and event == "stable.spectator.frame_summary"
    )
    assert frame_event["outcome"] == "published"
    assert frame_event["recipient_count"] == 2
    assert "spectator" not in frame_event.values()
    assert "host" not in frame_event.values()


@pytest.mark.asyncio
async def test_new_song_resets_spectator_frame_history_before_sequence_restarts() -> None:
    realtime = SpectatorRealtime()
    stable_services = services(realtime)
    host = context(2, "host", realtime)

    await dispatch_packets(spectator_frame_packet(28), host, stable_services)
    await dispatch_packets(spectator_frame_packet(0, action=ReplayAction.NEW_SONG), host, stable_services)
    await dispatch_packets(spectator_frame_packet(1), host, stable_services)

    assert [frame.sequence for frame in realtime.frames[2]] == [0, 1]


@pytest.mark.asyncio
async def test_spectator_attach_returns_atomic_history_and_duplicate_start_is_noop() -> None:
    realtime = SpectatorRealtime()
    stable_services = services(realtime)
    await dispatch_packets(spectator_frame_packet(1), context(2, "host", realtime), stable_services)
    start = build_packet(ClientPacket.START_SPECTATING, struct.pack("<i", 2))
    spectator = context(3, "spectator", realtime)

    joined = await dispatch_packets(start, spectator, stable_services)
    relation_id = realtime.relations[3].relation_id
    published_count = len(realtime.bubbles.published)
    duplicate = await dispatch_packets(start, spectator, stable_services)

    assert packet_types(joined) == [ServerPacket.FELLOW_SPECTATOR_JOINED, ServerPacket.SPECTATE_FRAMES]
    assert duplicate == b""
    assert realtime.relations[3].relation_id == relation_id
    assert len(realtime.bubbles.published) == published_count

    await dispatch_packets(spectator_frame_packet(2), context(2, "host", realtime), stable_services)
    assert delivered_packet_types(realtime, 3)[-1] is ServerPacket.SPECTATE_FRAMES


@pytest.mark.asyncio
async def test_stale_spectator_detach_does_not_emit_leave_notifications() -> None:
    realtime = SpectatorRealtime()
    stable_services = services(realtime)
    spectator = context(3, "spectator", realtime)
    await dispatch_packets(
        build_packet(ClientPacket.START_SPECTATING, struct.pack("<i", 2)),
        spectator,
        stable_services,
    )
    published_count = len(realtime.bubbles.published)
    realtime.detach_current = False

    await dispatch_packets(build_packet(ClientPacket.STOP_SPECTATING), spectator, stable_services)

    assert 3 in realtime.relations
    assert len(realtime.bubbles.published) == published_count


@pytest.mark.asyncio
async def test_cant_spectate_uses_each_relation_recipient_fence() -> None:
    realtime = SpectatorRealtime()
    stable_services = services(realtime)
    spectator = context(3, "spectator", realtime)
    await dispatch_packets(
        build_packet(ClientPacket.START_SPECTATING, struct.pack("<i", 2)),
        spectator,
        stable_services,
    )
    realtime.bubbles.published.clear()

    await dispatch_packets(build_packet(ClientPacket.CANT_SPECTATE), spectator, stable_services)

    assert {fence for fence, _ in realtime.bubbles.published} == {
        realtime.fences[2],
        realtime.fences[3],
        realtime.fences[9],
    }
    assert [
        packet_type for account_id in (2, 3, 9) for packet_type in delivered_packet_types(realtime, account_id)
    ] == [ServerPacket.SPECTATOR_CANT_SPECTATE] * 3
