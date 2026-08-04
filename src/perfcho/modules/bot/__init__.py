"""Expose the protocol-neutral bot command kernel."""

from .builder import CommandBuilder, command
from .commands import register_core_commands
from .models import (
    BotDirective,
    BotInvocation,
    BotReply,
    CommandContext,
    CommandDefinition,
    CommandGroup,
    CommandResult,
    ParsedArguments,
)
from .registry import CommandRegistry
from .service import BotCommandService, BotIdentity

__all__ = [
    "BotCommandService",
    "BotIdentity",
    "BotDirective",
    "BotInvocation",
    "BotReply",
    "CommandBuilder",
    "CommandContext",
    "CommandDefinition",
    "CommandGroup",
    "CommandRegistry",
    "CommandResult",
    "ParsedArguments",
    "register_core_commands",
    "command",
]
