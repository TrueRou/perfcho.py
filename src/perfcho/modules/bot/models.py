"""Define protocol-neutral bot command values."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from perfcho.modules.common import CommandMeta

if TYPE_CHECKING:
    from perfcho.modules.bot.registry import CommandRegistry


class BotDirective(StrEnum):
    """Request a protocol adapter to perform a client lifecycle action."""

    RECONNECT = "reconnect"
    QUIT = "quit"


@dataclass(frozen=True, slots=True)
class BotReply:
    """Carry command output and an optional adapter lifecycle directive."""

    response: str | None = None
    directive: BotDirective | None = None


@dataclass(frozen=True, slots=True)
class BotInvocation:
    """Describe one authenticated bot command candidate."""

    meta: CommandMeta
    sender_name: str
    content: str
    recipient: str
    private: bool


@dataclass(slots=True)
class CommandContext:
    """Expose normalized invocation data to one command handler."""

    invocation: BotInvocation
    args: tuple[str, ...]
    trigger: str
    registry: CommandRegistry

    @property
    def sender_account_id(self) -> int:
        """Return the required authenticated actor account ID."""
        actor = self.invocation.meta.actor
        if actor is None:
            raise ValueError("bot commands require an authenticated actor")
        return actor.account_id


type CommandHandler = Callable[[CommandContext], Awaitable[str | BotReply | None] | str | BotReply | None]
type GuardFunction = Callable[[CommandContext], Awaitable[bool] | bool]


@dataclass(frozen=True, slots=True)
class ArgumentDefinition:
    """Describe one positional command argument."""

    name: str
    required: bool
    value_type: str = "string"
    rest: bool = False


@dataclass(frozen=True, slots=True)
class OptionDefinition:
    """Describe one named command option."""

    name: str
    flags: tuple[str, ...]
    value_type: str = "boolean"
    description: str | None = None


@dataclass(frozen=True, slots=True)
class CommandDefinition:
    """Describe one executable command."""

    name: str
    handler: CommandHandler
    aliases: tuple[str, ...] = ()
    description: str | None = None
    hidden: bool = False
    can_execute: GuardFunction | None = None
    arguments: tuple[ArgumentDefinition, ...] = ()
    options: tuple[OptionDefinition, ...] = ()
    usage: str | None = None


@dataclass(frozen=True, slots=True)
class CommandGroup:
    """Group related subcommands under one trigger."""

    name: str
    description: str
    commands: tuple[CommandDefinition, ...]
    can_execute: GuardFunction | None = None


@dataclass(frozen=True, slots=True)
class CommandResult:
    """Summarize one recognized command execution."""

    response: str | None
    hidden: bool
    execution_time_ms: float
    directive: BotDirective | None = None


@dataclass(slots=True)
class ParsedArguments:
    """Hold converted positional arguments and options."""

    values: dict[str, Any] = field(default_factory=dict)
    options: dict[str, Any] = field(default_factory=dict)
