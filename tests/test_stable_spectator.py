import struct
import uuid
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest

from perfcho.api.stable.dispatcher import StableRuntimeContext, dispatch_packets
from perfcho.infra.composition import StableServices
from perfcho.infra.settings import Settings
from perfcho.modules.authorization import AuthorizationQueryService
from perfcho.modules.common import Clock, IdGenerator
from perfcho.modules.identity import IdentityService, ResolvedStableSession
from perfcho.modules.realtime import (
    MailboxPacket,
    PresenceSnapshot,
    RealtimeSession,
    SessionFence,
    SpectatorAttachment,
    SpectatorFrame,
    SpectatorFramePublish,
    SpectatorFrameWindow,
    SpectatorRelation,
)
from perfcho.modules.realtime.stable import (
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
from perfcho.modules.realtime.stable.models import UserPresence, UserStats

NOW = datetime(2026, 7, 29, 12, 30, tzinfo=UTC)
EXPIRY = NOW + timedelta(minutes=5)


class FixedClock:
    def now(self) -> datetime:
        return NOW


class FakeIds:
    def new(self) -> uuid.UUID:
        return uuid.uuid7()


class SpectatorRealtime:
    def __init__(self) -> None:
        self.fences = {account_id: SessionFence(uuid.uuid7(), 1) for account_id in (2, 3, 9)}
        self.presences = {
            account_id: PresenceSnapshot(
                account_id,
                fence.revision,
                b"presence",
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
        self.mailboxes: dict[int, list[bytes]] = {}
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

    async def enqueue_mailbox(
        self,
        account_id: int,
        payload: bytes,
        *,
        recipient_fence: SessionFence,
        expires_at: datetime,
    ) -> MailboxPacket:
        assert expires_at > NOW
        assert recipient_fence == self.fences[account_id]
        packets = self.mailboxes.setdefault(account_id, [])
        packets.append(payload)
        return MailboxPacket(len(packets), payload)

    async def publish_spectator_frame(
        self,
        host_account_id: int,
        *,
        host_fence: SessionFence,
        sequence: int,
        payload: bytes,
        expires_at: datetime,
    ) -> SpectatorFramePublish:
        assert expires_at > NOW
        assert host_fence == self.fences[host_account_id]
        frame = SpectatorFrame(len(self.frames.get(host_account_id, ())) + 1, sequence, payload)
        self.frames.setdefault(host_account_id, []).append(frame)
        recipients: list[int] = []
        for relation in await self.list_spectators(host_account_id, host_fence=host_fence, at=NOW):
            await self.enqueue_mailbox(
                relation.spectator_account_id,
                payload,
                recipient_fence=relation.spectator_fence,
                expires_at=relation.expires_at,
            )
            recipients.append(relation.spectator_account_id)
        return SpectatorFramePublish(frame, tuple(recipients))

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
        identity=ResolvedStableSession(
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
    )


def spectator_frame_packet(sequence: int) -> bytes:
    bundle = ReplayFrameBundle(
        frames=(ReplayFrame(0, 0, 1.0, 2.0, 10),),
        score_frame=ScoreFrame(10, 1, 1, 0, 0, 0, 0, 0, 300, 1, 1, True, 255, 0, False),
        action=ReplayAction.STANDARD,
        extra=0,
        sequence=sequence,
        raw_data=memoryview(b""),
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


@pytest.mark.asyncio
async def test_spectator_join_frames_and_stop_notify_host_and_fellows() -> None:
    realtime = SpectatorRealtime()
    stable_services = services(realtime)

    joined = await dispatch_packets(
        build_packet(ClientPacket.START_SPECTATING, struct.pack("<i", 2)),
        context(3, "spectator", realtime),
        stable_services,
    )

    assert realtime.relations[3].host_account_id == 2
    assert packet_types(joined) == [ServerPacket.FELLOW_SPECTATOR_JOINED]
    assert packet_types(realtime.mailboxes[2]) == [ServerPacket.SPECTATOR_JOINED]
    assert packet_types(realtime.mailboxes[9]) == [ServerPacket.FELLOW_SPECTATOR_JOINED]

    await dispatch_packets(spectator_frame_packet(1), context(2, "host", realtime), stable_services)

    assert packet_types(realtime.mailboxes[3]) == [ServerPacket.SPECTATE_FRAMES]
    assert packet_types(realtime.mailboxes[9])[-1] is ServerPacket.SPECTATE_FRAMES

    await dispatch_packets(
        build_packet(ClientPacket.STOP_SPECTATING),
        context(3, "spectator", realtime),
        stable_services,
    )

    assert 3 not in realtime.relations
    assert packet_types(realtime.mailboxes[2])[-1] is ServerPacket.SPECTATOR_LEFT
    assert packet_types(realtime.mailboxes[9])[-1] is ServerPacket.FELLOW_SPECTATOR_LEFT


@pytest.mark.asyncio
async def test_spectator_attach_returns_atomic_history_and_duplicate_start_is_noop() -> None:
    realtime = SpectatorRealtime()
    history_wire = build_packet(ServerPacket.SPECTATE_FRAMES, b"history")
    realtime.frames[2] = [SpectatorFrame(1, 1, history_wire)]
    stable_services = services(realtime)
    start = build_packet(ClientPacket.START_SPECTATING, struct.pack("<i", 2))
    spectator = context(3, "spectator", realtime)

    joined = await dispatch_packets(start, spectator, stable_services)
    relation_id = realtime.relations[3].relation_id
    mailbox_sizes = {account_id: len(payloads) for account_id, payloads in realtime.mailboxes.items()}
    duplicate = await dispatch_packets(start, spectator, stable_services)

    assert packet_types(joined) == [ServerPacket.FELLOW_SPECTATOR_JOINED, ServerPacket.SPECTATE_FRAMES]
    assert duplicate == b""
    assert realtime.relations[3].relation_id == relation_id
    assert {account_id: len(payloads) for account_id, payloads in realtime.mailboxes.items()} == mailbox_sizes


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
    mailbox_sizes = {account_id: len(payloads) for account_id, payloads in realtime.mailboxes.items()}
    realtime.detach_current = False

    await dispatch_packets(build_packet(ClientPacket.STOP_SPECTATING), spectator, stable_services)

    assert 3 in realtime.relations
    assert {account_id: len(payloads) for account_id, payloads in realtime.mailboxes.items()} == mailbox_sizes


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
    realtime.mailboxes.clear()

    await dispatch_packets(build_packet(ClientPacket.CANT_SPECTATE), spectator, stable_services)

    assert set(realtime.mailboxes) == {2, 3, 9}
    for payloads in realtime.mailboxes.values():
        assert packet_types(payloads) == [ServerPacket.SPECTATOR_CANT_SPECTATE]
