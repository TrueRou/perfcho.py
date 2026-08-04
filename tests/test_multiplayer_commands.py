import hashlib
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest

from perfcho.modules.bot import BotCommandService, BotInvocation
from perfcho.modules.common import Actor, ClientContext, CommandMeta
from perfcho.modules.multiplayer import (
    MultiplayerService,
    RoomRecord,
    RoomSettings,
    RoomSlot,
    RoomState,
    SlotStatus,
    StartRound,
    TeamMode,
    UpdateRoomSettings,
    WinCondition,
)
from perfcho.modules.multiplayer.commands import (
    AccountResolver,
    BeatmapResolver,
    MultiplayerCommandDependencies,
    build_multiplayer_commands,
    build_pool_commands,
)
from perfcho.modules.scoring import Ruleset, ScoreboardVariant

NOW = datetime(2026, 8, 3, tzinfo=UTC)


class FakeMultiplayer:
    def __init__(self) -> None:
        settings = RoomSettings(
            "Room",
            "Artist - Title [Hard]",
            42,
            b"m" * 16,
            Ruleset.OSU,
            ScoreboardVariant.VANILLA,
            TeamMode.HEAD_TO_HEAD,
            WinCondition.SCORE,
        )
        room = RoomRecord(uuid.uuid7(), 7, uuid.uuid7(), 3, 10, 10, 16, settings)
        slots = (RoomSlot(0, SlotStatus.NOT_READY, 10),) + tuple(
            RoomSlot(position, SlotStatus.OPEN) for position in range(1, 16)
        )
        self.state = RoomState(room, 1, slots, False, NOW + timedelta(hours=1))
        self.started: StartRound | None = None
        self.updated: UpdateRoomSettings | None = None

    async def find_room_for_account(self, account_id: int) -> RoomState | None:
        return self.state if account_id == 10 else None

    async def start_round(self, command: StartRound) -> RoomState:
        self.started = command
        return self.state

    async def update_settings(self, command: UpdateRoomSettings) -> RoomState:
        self.updated = command
        self.state = replace(
            self.state,
            room=replace(self.state.room, version=self.state.room.version + 1, settings=command.settings),
        )
        return self.state


def invocation(content: str) -> BotInvocation:
    digest = hashlib.sha256(content.encode()).digest()
    return BotInvocation(
        CommandMeta(
            uuid.uuid7(),
            f"test:{digest.hex()}",
            digest,
            Actor(10, uuid.uuid7()),
            ClientContext("stable", "b20260711.1", None, "127.0.0.1", "osu!"),
            NOW,
        ),
        "Tester",
        content,
        "#multiplayer",
        False,
    )


def service(multiplayer: FakeMultiplayer) -> BotCommandService:
    bot = BotCommandService()
    bot.register_group(
        build_multiplayer_commands(
            MultiplayerCommandDependencies(
                service=cast(MultiplayerService, multiplayer),
                resolve_account=cast(AccountResolver, object()),
                resolve_beatmap=cast(BeatmapResolver, object()),
            )
        )
    )
    bot.register_group(build_pool_commands())
    return bot


@pytest.mark.asyncio
async def test_multiplayer_commands_call_canonical_service_commands() -> None:
    multiplayer = FakeMultiplayer()
    bot = service(multiplayer)

    started = await bot.try_execute(invocation("!mp start"))
    titled = await bot.try_execute(invocation('!mp title "Tournament Room"'))

    assert started is not None and started.response == "Starting match."
    assert multiplayer.started is not None
    assert multiplayer.started.public_id == 7
    assert multiplayer.started.expected_version == 3
    assert titled is not None and titled.response == "Match title changed to Tournament Room."
    assert multiplayer.updated is not None
    assert multiplayer.updated.settings.name == "Tournament Room"


@pytest.mark.asyncio
async def test_pool_commands_are_owned_by_multiplayer_catalog() -> None:
    bot = service(FakeMultiplayer())

    result = await bot.try_execute(invocation("!pool create cup"))

    assert {group.name for group in bot.registry.get_groups()} == {"mp", "pool"}
    assert result is not None and result.response == "Create a new mappool not yet implemented."
