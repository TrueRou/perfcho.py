import hashlib
import uuid
from datetime import UTC, datetime

import pytest

from perfcho.modules.bot import (
    BotCommandService,
    BotDirective,
    BotInvocation,
    CommandContext,
    CommandGroup,
    CommandRegistry,
    ParsedArguments,
    command,
    register_core_commands,
)
from perfcho.modules.common import Actor, ClientContext, CommandMeta

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


def service() -> BotCommandService:
    bot = BotCommandService()
    register_core_commands(bot)
    return bot


@pytest.mark.asyncio
async def test_builder_parses_quotes_types_rest_arguments_and_options() -> None:
    registry = CommandRegistry("!")

    async def handler(context: CommandContext, parsed: ParsedArguments) -> str:
        del context
        values = parsed.values
        options = parsed.options
        return f"{values['message']}:{values['words']}:{options['count']}:{options['verbose']}"

    registry.register(
        command("repeat", "<message:string> [...words:string]")
        .alias("r")
        .option("count", "-c --count <count:number>")
        .option("verbose", "-v --verbose")
        .action(handler)
    )

    result = await registry.try_execute(invocation('!r "hello world" one two --count=3 -v'))

    assert result is not None
    assert result.response == "hello world:('one', 'two'):3:True"
    assert result.execution_time_ms >= 0


@pytest.mark.asyncio
async def test_builder_rejects_missing_and_invalid_required_values_with_usage() -> None:
    registry = CommandRegistry("$")
    registry.register(command("add", "<value:number>").action(lambda _context, _parsed: "ok"))

    missing = await registry.try_execute(invocation("$add"))
    invalid = await registry.try_execute(invocation("$add nope"))

    assert missing is not None and missing.response == "Usage: $add <value:number>"
    assert invalid is not None and invalid.response == "Usage: $add <value:number>"


@pytest.mark.asyncio
async def test_registry_executes_group_aliases_and_generates_group_help() -> None:
    registry = CommandRegistry()
    registry.register_group(
        CommandGroup(
            "mp",
            "Multiplayer commands",
            (
                command("freemods")
                .alias("fm")
                .description("Toggle Free Mod")
                .action(lambda _context, _parsed: "enabled"),
            ),
        )
    )

    executed = await registry.try_execute(invocation("!mp fm"))
    help_result = await registry.try_execute(invocation("!mp help"))

    assert executed is not None and executed.response == "enabled"
    assert help_result is not None and "!mp freemods (fm)" in (help_result.response or "")


@pytest.mark.asyncio
async def test_core_catalog_contains_only_bot_owned_commands() -> None:
    bot = service()

    help_result = await bot.try_execute(invocation("!help"))
    command_names = {definition.name for definition in bot.registry.get_commands()}

    assert command_names == {"roll", "server", "reconnect", "quit", "help"}
    assert bot.registry.get_groups() == ()
    assert help_result is not None
    assert "!roll" in (help_result.response or "")
    assert "Command groups" not in (help_result.response or "")


@pytest.mark.asyncio
async def test_failed_command_logs_context_and_returns_error_reply(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[tuple[str, str, dict[str, object]]] = []

    def capture(level: str, event: str, **fields: object) -> None:
        events.append((level, event, fields))

    monkeypatch.setattr("perfcho.modules.bot.registry.log_event", capture)

    registry = CommandRegistry()

    def boom(_context: CommandContext, _parsed: ParsedArguments) -> str:
        raise ValueError("boom")

    registry.register(command("boom", "<value:string>").action(boom))

    result = await registry.try_execute(invocation("!boom value"))

    assert result is not None and result.response == "An error occurred while executing the command."
    assert len(events) == 1
    level, event, fields = events[0]
    assert (level, event) == ("ERROR", "bot.command.execution_failed")
    assert fields["command"] == "boom"
    assert fields["args"] == ("value",)
    assert fields["sender"] == "Tester"
    assert fields["recipient"] == "#osu"
    assert fields["private"] is False
    assert fields["error_type"] == "ValueError"
    assert isinstance(fields["duration_ms"], float) and fields["duration_ms"] >= 0
    assert isinstance(fields["exception"], ValueError)
    assert fields["exception"].args == ("boom",)


@pytest.mark.asyncio
async def test_core_roll_and_lifecycle_directives_are_executable() -> None:
    bot = service()

    roll = await bot.try_execute(invocation("!roll 10"))
    reconnect = await bot.try_execute(invocation("!reconnect"))
    quit_result = await bot.try_execute(invocation("!quit"))

    assert roll is not None and roll.response is not None
    assert roll.response.startswith("Tester rolls ") and roll.response.endswith(" points!")
    assert reconnect is not None and reconnect.directive is BotDirective.RECONNECT
    assert quit_result is not None and quit_result.directive is BotDirective.QUIT
