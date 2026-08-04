import hashlib
import uuid
from datetime import UTC, datetime

import pytest

from perfcho.modules.bot import BotCommandService, BotInvocation
from perfcho.modules.common import Actor, ClientContext, CommandMeta
from perfcho.modules.social.commands import build_clan_commands

NOW = datetime(2026, 8, 3, tzinfo=UTC)


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
        "#osu",
        False,
    )


@pytest.mark.asyncio
async def test_clan_commands_are_owned_by_social_catalog() -> None:
    bot = BotCommandService()
    bot.register_group(build_clan_commands())

    invalid = await bot.try_execute(invocation("!clan create TOOLONG Clan Name"))

    assert {group.name for group in bot.registry.get_groups()} == {"clan"}
    assert invalid is not None and invalid.response == "Clan tag must be 1-6 characters."
