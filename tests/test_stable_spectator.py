import struct
import uuid
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest

from perfcho.composition import StableServices
from perfcho.infra.settings import Settings
from perfcho.modules.authorization import AuthorizationQueryService
from perfcho.modules.common import Clock, IdGenerator
from perfcho.modules.identity import IdentityService, ResolvedStableSession
from perfcho.modules.realtime import (
    MailboxPacket,
    PresenceSnapshot,
    RealtimeRepository,
    RealtimeSession,
    SpectatorRelation,
)
from perfcho.realtime.stable import (
    ClientPacket,
    PacketReader,
    PacketWriter,
    ReplayFrame,
    ReplayFrameBundle,
    ScoreFrame,
    ServerPacket,
    build_packet,
)
from perfcho.realtime.stable.dispatcher import StableRuntimeContext, dispatch_packets
from perfcho.realtime.stable.models import UserPresence, UserStats

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
        self.presences = {
            2: PresenceSnapshot(2, 1, b"host", EXPIRY),
            3: PresenceSnapshot(3, 1, b"spectator", EXPIRY),
            9: PresenceSnapshot(9, 1, b"fellow", EXPIRY),
        }
        self.relations = {9: SpectatorRelation(2, 9, 1, EXPIRY)}
        self.mailboxes: dict[int, list[bytes]] = {}
        self.frames: dict[int, list[MailboxPacket]] = {}

    async def get_presence(self, account_id: int, *, at: datetime) -> PresenceSnapshot | None:
        presence = self.presences.get(account_id)
        return presence if presence is not None and presence.expires_at > at else None

    async def get_spectator_relation(
        self,
        spectator_account_id: int,
        *,
        at: datetime,
    ) -> SpectatorRelation | None:
        relation = self.relations.get(spectator_account_id)
        return relation if relation is not None and relation.expires_at > at else None

    async def list_spectators(self, host_account_id: int, *, at: datetime) -> frozenset[int]:
        return frozenset(
            spectator_id
            for spectator_id, relation in self.relations.items()
            if relation.host_account_id == host_account_id and relation.expires_at > at
        )

    async def attach_spectator(
        self,
        host_account_id: int,
        spectator_account_id: int,
        *,
        expires_at: datetime,
    ) -> SpectatorRelation:
        previous = self.relations.get(spectator_account_id)
        relation = SpectatorRelation(
            host_account_id,
            spectator_account_id,
            1 if previous is None else previous.revision + 1,
            expires_at,
        )
        self.relations[spectator_account_id] = relation
        return relation

    async def detach_spectator(
        self,
        host_account_id: int,
        spectator_account_id: int,
        *,
        expected_revision: int,
    ) -> None:
        relation = self.relations.get(spectator_account_id)
        if (
            relation is not None
            and relation.host_account_id == host_account_id
            and relation.revision == expected_revision
        ):
            del self.relations[spectator_account_id]

    async def enqueue_mailbox(
        self,
        account_id: int,
        payload: bytes,
        *,
        expires_at: datetime,
    ) -> MailboxPacket:
        assert expires_at > NOW
        packets = self.mailboxes.setdefault(account_id, [])
        packets.append(payload)
        return MailboxPacket(len(packets), payload)

    async def publish_spectator_frame(
        self,
        host_account_id: int,
        *,
        sequence: int,
        payload: bytes,
        expires_at: datetime,
    ) -> MailboxPacket:
        assert expires_at > NOW
        frame = MailboxPacket(sequence, payload)
        self.frames.setdefault(host_account_id, []).append(frame)
        return frame

    async def read_spectator_frames(
        self,
        host_account_id: int,
        *,
        after_sequence: int,
        limit: int,
        at: datetime,
    ) -> tuple[MailboxPacket, ...]:
        del at
        return tuple(frame for frame in self.frames.get(host_account_id, []) if frame.sequence > after_sequence)[:limit]


def context(account_id: int, name: str) -> StableRuntimeContext:
    session_id = uuid.uuid7()
    return StableRuntimeContext(
        identity=ResolvedStableSession(
            account_id,
            name,
            1,
            session_id,
            None,
            "b20260711.1",
            None,
            EXPIRY,
        ),
        realtime=RealtimeSession(session_id, account_id, 1, EXPIRY),
        presence=UserPresence(account_id, name, 0, 0, 1, 0, 0.0, 0.0, 0),
        stats=UserStats(account_id, 0, "", "", 0, 0, 0, 0, 0.0, 0, 0, 0, 0),
    )


def services(realtime: SpectatorRealtime) -> StableServices:
    return StableServices(
        identity=cast(IdentityService, object()),
        authorization=cast(AuthorizationQueryService, object()),
        realtime=cast(RealtimeRepository, realtime),
        clock=cast(Clock, FixedClock()),
        id_generator=cast(IdGenerator, FakeIds()),
        settings=Settings(),
    )


def spectator_frame_packet(sequence: int) -> bytes:
    bundle = ReplayFrameBundle(
        frames=(ReplayFrame(0, 0, 1.0, 2.0, 10),),
        score_frame=ScoreFrame(10, 1, 1, 0, 0, 0, 0, 0, 300, 1, 1, True, 255, 0, False),
        action=0,
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
        context(3, "spectator"),
        stable_services,
    )

    assert realtime.relations[3].host_account_id == 2
    assert packet_types(joined) == [ServerPacket.FELLOW_SPECTATOR_JOINED]
    assert packet_types(realtime.mailboxes[2]) == [ServerPacket.SPECTATOR_JOINED]
    assert packet_types(realtime.mailboxes[9]) == [ServerPacket.FELLOW_SPECTATOR_JOINED]

    await dispatch_packets(spectator_frame_packet(1), context(2, "host"), stable_services)

    assert packet_types(realtime.mailboxes[3]) == [ServerPacket.SPECTATE_FRAMES]
    assert packet_types(realtime.mailboxes[9])[-1] is ServerPacket.SPECTATE_FRAMES

    await dispatch_packets(build_packet(ClientPacket.STOP_SPECTATING), context(3, "spectator"), stable_services)

    assert 3 not in realtime.relations
    assert packet_types(realtime.mailboxes[2])[-1] is ServerPacket.SPECTATOR_LEFT
    assert packet_types(realtime.mailboxes[9])[-1] is ServerPacket.FELLOW_SPECTATOR_LEFT
