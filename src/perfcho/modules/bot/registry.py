"""Register, parse, and execute protocol-neutral bot commands."""

import inspect
import shlex
import time

from perfcho.modules.bot.models import (
    BotInvocation,
    BotReply,
    CommandContext,
    CommandDefinition,
    CommandGroup,
    CommandResult,
)


class CommandRegistry:
    """Own command names, aliases, groups, and execution policy."""

    def __init__(self, prefix: str = "!") -> None:
        """Create an empty registry with one protocol prefix."""
        if not prefix or len(prefix) > 8 or any(character.isspace() for character in prefix):
            raise ValueError("command prefix must contain 1-8 non-whitespace characters")
        self.prefix = prefix
        self._commands: dict[str, CommandDefinition] = {}
        self._groups: dict[str, CommandGroup] = {}

    def register(self, definition: CommandDefinition) -> None:
        """Register a command and reject ambiguous triggers."""
        triggers = (definition.name, *definition.aliases)
        normalized = tuple(trigger.casefold() for trigger in triggers)
        if len(normalized) != len(set(normalized)):
            raise ValueError(f'command "{definition.name}" repeats a trigger')
        collision = next(
            (trigger for trigger in normalized if trigger in self._commands or trigger in self._groups), None
        )
        if collision is not None:
            raise ValueError(f'command trigger "{collision}" is already registered')
        for trigger in normalized:
            self._commands[trigger] = definition

    def register_many(self, definitions: tuple[CommandDefinition, ...]) -> None:
        """Register commands in declaration order."""
        for definition in definitions:
            self.register(definition)

    def register_group(self, group: CommandGroup) -> None:
        """Register a command group and all of its subcommand aliases."""
        name = group.name.casefold()
        if name in self._commands or name in self._groups:
            raise ValueError(f'command group "{name}" is already registered')
        group_triggers: set[str] = set()
        for definition in group.commands:
            for trigger in (definition.name, *definition.aliases):
                normalized = trigger.casefold()
                if normalized in group_triggers:
                    raise ValueError(f'group command trigger "{normalized}" is already registered')
                group_triggers.add(normalized)
        self._groups[name] = group

    async def try_execute(self, invocation: BotInvocation) -> CommandResult | None:
        """Parse and execute a recognized command candidate."""
        if not invocation.content.startswith(self.prefix):
            return None
        try:
            parts = shlex.split(invocation.content[len(self.prefix) :])
        except ValueError:
            return CommandResult("The command syntax is invalid.", False, 0.0)
        if not parts:
            return None
        started = time.perf_counter_ns()
        trigger = parts[0].casefold()
        args = tuple(parts[1:])
        group = self._groups.get(trigger)
        if group is not None:
            context = CommandContext(invocation, args, trigger, self)
            if group.can_execute is not None and not await _allowed(group.can_execute, context):
                return _result(started, hidden=True)
            if not args or args[0].casefold() in {"help", "h"}:
                return _result(started, response=self._format_group_help(group))
            subtrigger = args[0].casefold()
            definition = next(
                (
                    command
                    for command in group.commands
                    if subtrigger in {command.name.casefold(), *(alias.casefold() for alias in command.aliases)}
                ),
                None,
            )
            if definition is None:
                return _result(started, response=f"Unknown {trigger} subcommand: {args[0]}")
            return await self._execute(definition, invocation, tuple(args[1:]), subtrigger, started)

        definition = self._commands.get(trigger)
        if definition is None:
            return None
        return await self._execute(definition, invocation, args, trigger, started)

    def get_commands(self) -> tuple[CommandDefinition, ...]:
        """Return unique top-level commands in registration order."""
        return tuple(dict.fromkeys(self._commands.values()))

    def get_groups(self) -> tuple[CommandGroup, ...]:
        """Return command groups in registration order."""
        return tuple(self._groups.values())

    async def _execute(
        self,
        definition: CommandDefinition,
        invocation: BotInvocation,
        args: tuple[str, ...],
        trigger: str,
        started: int,
    ) -> CommandResult:
        context = CommandContext(invocation, args, trigger, self)
        try:
            if definition.can_execute is not None and not await _allowed(definition.can_execute, context):
                return _result(started, hidden=True)
            reply = definition.handler(context)
            if inspect.isawaitable(reply):
                reply = await reply
            if isinstance(reply, BotReply):
                return _result(
                    started,
                    response=reply.response,
                    hidden=definition.hidden,
                    directive=reply.directive,
                    effect=reply.effect,
                )
            return _result(started, response=reply, hidden=definition.hidden)
        except Exception:
            return _result(started, response="An error occurred while executing the command.")

    def _format_group_help(self, group: CommandGroup) -> str:
        lines = [f"{group.name} - {group.description}", "", "Available subcommands:"]
        for definition in group.commands:
            if definition.hidden:
                continue
            aliases = f" ({', '.join(definition.aliases)})" if definition.aliases else ""
            description = f" - {definition.description}" if definition.description else ""
            lines.append(f"  {self.prefix}{group.name} {definition.name}{aliases}{description}")
        return "\n".join(lines)


async def _allowed(guard: object, context: CommandContext) -> bool:
    result = guard(context)  # type: ignore[operator]
    return bool(await result) if inspect.isawaitable(result) else bool(result)


def _result(
    started: int,
    *,
    response: str | None = None,
    hidden: bool = False,
    directive: object = None,
    effect: object = None,
) -> CommandResult:
    elapsed = (time.perf_counter_ns() - started) / 1_000_000
    return CommandResult(response, hidden, elapsed, directive, effect)  # type: ignore[arg-type]
