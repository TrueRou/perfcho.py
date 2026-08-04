"""Define commands that belong to the bot core itself."""

import os
import platform
import random
import time

from perfcho.modules.bot.builder import command
from perfcho.modules.bot.models import BotDirective, BotReply, CommandContext, CommandDefinition, ParsedArguments
from perfcho.modules.bot.service import BotCommandService


def register_core_commands(bot: BotCommandService) -> None:
    """Register help and lifecycle commands owned by the bot core."""
    bot.register_many(_user_commands(bot))
    bot.register(_help_command())


def _user_commands(bot: BotCommandService) -> tuple[CommandDefinition, ...]:
    async def roll(context: CommandContext, parsed: ParsedArguments) -> str:
        value = parsed.values["max"]
        maximum = min(value if isinstance(value, int) else 100, 0x7FFF)
        if maximum < 1:
            return "Roll what?"
        return f"{context.invocation.sender_name} rolls {random.randrange(maximum)} points!"

    async def server(_: CommandContext, __: ParsedArguments) -> str:
        uptime = _format_duration(int(time.monotonic() - bot.started_at))
        return "\n".join(
            (
                f"perfcho.py | uptime: {uptime}",
                f"python: {platform.python_version()}",
                f"cpu: {os.cpu_count() or 1}x {platform.processor() or 'Unknown'}",
                f"ram: {_resident_memory_mb()}MB process",
            )
        )

    return (
        command("roll", "[max:number]").description("Roll an n-sided die (default 100)").action(roll),
        command("server").description("Show server information and uptime").action(server),
        command("reconnect")
        .description("Reconnect to the server")
        .action(lambda _context, _parsed: BotReply("See you next time ~", BotDirective.RECONNECT)),
        command("quit")
        .description("Close your client")
        .action(lambda _context, _parsed: BotReply("Goodbye ~", BotDirective.QUIT)),
    )


def _help_command() -> CommandDefinition:
    async def help_handler(context: CommandContext, _: ParsedArguments) -> str:
        lines = ["Individual commands", "-----------"]
        for definition in context.registry.get_commands():
            if definition.hidden or not definition.description:
                continue
            aliases = f" ({', '.join(definition.aliases)})" if definition.aliases else ""
            lines.append(f"{context.registry.prefix}{definition.name}{aliases}: {definition.description}")
        groups = context.registry.get_groups()
        if groups:
            lines.extend(("", "Command groups", "-----------"))
            lines.extend(f"{context.registry.prefix}{group.name}: {group.description}" for group in groups)
        return "\n".join(lines)

    return command("help").alias("h").description("Show all available commands").hidden().action(help_handler)


def _format_duration(seconds: int) -> str:
    values = (
        (seconds // 86400, "d"),
        ((seconds % 86400) // 3600, "h"),
        ((seconds % 3600) // 60, "m"),
        (seconds % 60, "s"),
    )
    parts = [f"{value}{suffix}" for value, suffix in values if value]
    return " ".join(parts) if parts else "0s"


def _resident_memory_mb() -> int:
    try:
        import resource

        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return int(usage / 1024 if platform.system() != "Darwin" else usage / 1024 / 1024)
    except ImportError, ValueError:
        return 0
