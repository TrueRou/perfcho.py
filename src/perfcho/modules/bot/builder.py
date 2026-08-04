"""Build bot command definitions with a compact argument signature DSL."""

import inspect
import re
from collections.abc import Callable
from typing import Any, Self

from perfcho.modules.bot.models import (
    ArgumentDefinition,
    BotReply,
    CommandContext,
    CommandDefinition,
    GuardFunction,
    OptionDefinition,
    ParsedArguments,
)

_ARGUMENT_PATTERN = re.compile(r"<[^>]+>|\[[^\]]+\]")
_VALUE_PATTERN = re.compile(r"[<\[]([^:\]>]+)(?::([a-z]+))?[>\]]")
_SUPPORTED_TYPES = frozenset({"string", "number", "boolean", "user", "channel"})

type ParsedHandler = Callable[
    [CommandContext, ParsedArguments],
    str | BotReply | None | object,
]


class CommandBuilder:
    """Create immutable command definitions through a chainable API."""

    def __init__(self, name: str, signature: str = "") -> None:
        """Bind a command trigger and parse its positional signature."""
        self._name = _trigger(name)
        self._signature = signature.strip()
        self._arguments = _parse_signature(self._signature)
        self._aliases: list[str] = []
        self._description: str | None = None
        self._hidden = False
        self._options: list[OptionDefinition] = []
        self._guard: GuardFunction | None = None

    def description(self, value: str) -> Self:
        """Set help text for this command."""
        self._description = value.strip()
        return self

    def alias(self, *values: str) -> Self:
        """Register alternative triggers."""
        self._aliases.extend(_trigger(value) for value in values)
        return self

    def hidden(self, value: bool = True) -> Self:
        """Control whether generated help includes the command."""
        self._hidden = value
        return self

    def option(self, name: str, flags: str, description: str | None = None) -> Self:
        """Add a boolean or valued command option."""
        option = _parse_option(name, flags, description)
        existing = {flag for item in self._options for flag in item.flags}
        if existing.intersection(option.flags):
            raise ValueError("command option flags must be unique")
        self._options.append(option)
        return self

    def check(self, guard: GuardFunction) -> Self:
        """Set an execution guard."""
        self._guard = guard
        return self

    def action(self, handler: ParsedHandler) -> CommandDefinition:
        """Finalize this command around typed argument conversion."""

        async def wrapped(context: CommandContext) -> str | BotReply | None:
            try:
                parsed = _parse_values(context.args, self._arguments, tuple(self._options))
            except ValueError:
                return _usage(context, self._name, self._signature)
            result = handler(context, parsed)
            if inspect.isawaitable(result):
                result = await result
            if result is not None and not isinstance(result, str | BotReply):
                raise TypeError("command handlers must return str, BotReply, or None")
            return result

        return CommandDefinition(
            name=self._name,
            handler=wrapped,
            aliases=tuple(self._aliases),
            description=self._description,
            hidden=self._hidden,
            can_execute=self._guard,
            arguments=self._arguments,
            options=tuple(self._options),
            usage=self._signature or None,
        )


def command(name: str, signature: str = "") -> CommandBuilder:
    """Start building one command definition."""
    return CommandBuilder(name, signature)


def _parse_signature(signature: str) -> tuple[ArgumentDefinition, ...]:
    arguments: list[ArgumentDefinition] = []
    tokens = _ARGUMENT_PATTERN.findall(signature)
    if " ".join(tokens) != signature:
        if signature:
            raise ValueError("invalid command argument signature")
        return ()
    for index, token in enumerate(tokens):
        required = token.startswith("<")
        content = token[1:-1].strip()
        rest = content.startswith("...")
        if rest:
            content = content[3:]
        name, separator, value_type = content.partition(":")
        name = name.strip()
        value_type = value_type.strip() if separator else "string"
        if not name or value_type not in _SUPPORTED_TYPES:
            raise ValueError("invalid command argument definition")
        if rest and required:
            raise ValueError("rest arguments must be optional")
        if rest and index != len(tokens) - 1:
            raise ValueError("rest argument must be last")
        arguments.append(ArgumentDefinition(name, required, value_type, rest))
    return tuple(arguments)


def _parse_option(name: str, flags: str, description: str | None) -> OptionDefinition:
    normalized_name = name.strip()
    parts = flags.split()
    option_flags = tuple(part for part in parts if part.startswith("-"))
    match = _VALUE_PATTERN.search(flags)
    value_type = match.group(2) or "string" if match else "boolean"
    if not normalized_name or not option_flags or value_type not in _SUPPORTED_TYPES:
        raise ValueError("invalid command option definition")
    return OptionDefinition(normalized_name, option_flags, value_type, description)


def _parse_values(
    raw_args: tuple[str, ...],
    arguments: tuple[ArgumentDefinition, ...],
    options: tuple[OptionDefinition, ...],
) -> ParsedArguments:
    remaining = list(raw_args)
    parsed_options: dict[str, Any] = {}
    for option in options:
        parsed_options[option.name] = None
        for _index, value in enumerate(tuple(remaining)):
            matched_flag = next((flag for flag in option.flags if value == flag or value.startswith(f"{flag}=")), None)
            if matched_flag is None:
                continue
            actual_index = remaining.index(value)
            if option.value_type == "boolean":
                parsed_options[option.name] = True
                remaining.pop(actual_index)
            elif value == matched_flag:
                if actual_index + 1 >= len(remaining):
                    raise ValueError("option value is missing")
                parsed_options[option.name] = _convert(remaining[actual_index + 1], option.value_type)
                del remaining[actual_index : actual_index + 2]
            else:
                parsed_options[option.name] = _convert(value[len(matched_flag) + 1 :], option.value_type)
                remaining.pop(actual_index)
            break

    values: dict[str, Any] = {}
    position = 0
    for argument in arguments:
        if argument.rest:
            values[argument.name] = tuple(_convert(value, argument.value_type) for value in remaining[position:])
            position = len(remaining)
            continue
        if position >= len(remaining):
            if argument.required:
                raise ValueError("required argument is missing")
            values[argument.name] = None
            continue
        values[argument.name] = _convert(remaining[position], argument.value_type)
        position += 1
    return ParsedArguments(values, parsed_options)


def _convert(value: str, value_type: str) -> object:
    if value_type in {"string", "user", "channel"}:
        return value
    if value_type == "number":
        try:
            number = float(value)
        except ValueError as error:
            raise ValueError("argument is not numeric") from error
        return int(number) if number.is_integer() else number
    if value_type == "boolean":
        normalized = value.casefold()
        if normalized in {"true", "1", "yes", "on"}:
            return True
        if normalized in {"false", "0", "no", "off"}:
            return False
        raise ValueError("argument is not boolean")
    raise ValueError("unsupported command argument type")


def _trigger(value: str) -> str:
    normalized = value.strip().casefold()
    if not normalized or any(character.isspace() or character == ":" for character in normalized):
        raise ValueError("command triggers must be non-empty single words")
    return normalized


def _usage(context: CommandContext, name: str, signature: str) -> str:
    suffix = f" {signature}" if signature else ""
    return f"Usage: {context.registry.prefix}{name}{suffix}"
