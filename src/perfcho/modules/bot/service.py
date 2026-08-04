"""Own bot identity and execute a pre-assembled command registry."""

from dataclasses import dataclass
from time import monotonic

from perfcho.modules.bot.models import BotInvocation, CommandDefinition, CommandGroup, CommandResult
from perfcho.modules.bot.registry import CommandRegistry


@dataclass(frozen=True, slots=True)
class BotIdentity:
    """Describe the account used to send bot replies."""

    account_id: int = 1
    name: str = "BanchoBot"

    def __post_init__(self) -> None:
        """Validate the configured bot identity."""
        if self.account_id < 1 or not self.name.strip():
            raise ValueError("bot identity is invalid")
        object.__setattr__(self, "name", self.name.strip())


class BotCommandService:
    """Execute commands from a registry assembled by the application root."""

    def __init__(self, *, prefix: str = "!", bot_account_id: int = 1, bot_name: str = "BanchoBot") -> None:
        """Create an empty registry and bind the reply identity."""
        self.registry = CommandRegistry(prefix)
        self.identity = BotIdentity(bot_account_id, bot_name)
        self.started_at = monotonic()

    @property
    def bot_account_id(self) -> int:
        """Return the configured bot account ID."""
        return self.identity.account_id

    @property
    def bot_name(self) -> str:
        """Return the configured bot display name."""
        return self.identity.name

    def register(self, definition: CommandDefinition) -> None:
        """Register one core or domain command."""
        self.registry.register(definition)

    def register_many(self, definitions: tuple[CommandDefinition, ...]) -> None:
        """Register several commands in declaration order."""
        self.registry.register_many(definitions)

    def register_group(self, group: CommandGroup) -> None:
        """Register one domain command group."""
        self.registry.register_group(group)

    async def try_execute(self, invocation: BotInvocation) -> CommandResult | None:
        """Execute a recognized command or return None for ordinary chat."""
        return await self.registry.try_execute(invocation)
