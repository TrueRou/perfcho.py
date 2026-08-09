"""Define bot commands owned by the social and team domain."""

from perfcho.modules.bot import CommandContext, CommandDefinition, CommandGroup, ParsedArguments, command


def build_clan_commands() -> CommandGroup:
    """Build the clan compatibility group until TeamService is available."""

    async def create(context: CommandContext, parsed: ParsedArguments) -> str:
        tag = parsed.values["tag"]
        name = " ".join(parsed.values["name"])
        if not 1 <= len(tag) <= 6:
            return "Clan tag must be 1-6 characters."
        if not 2 <= len(name) <= 32:
            return "Clan name must be 2-32 characters."
        return "Create clan not yet implemented."

    return CommandGroup(
        "clan",
        "Clan commands",
        (
            command("create", "<tag:string> [...name:string]")
            .alias("c")
            .description("Create a new clan")
            .action(create),
            *_placeholder_commands(
                (
                    ("disband", ("delete", "d"), "Disband your clan"),
                    ("info", ("i",), "Show clan information"),
                    ("leave", (), "Leave your current clan"),
                    ("list", ("l",), "List all clans"),
                    ("invite", ("inv",), "Invite a player to your clan"),
                    ("kick", ("k",), "Kick a member from your clan"),
                )
            ),
        ),
    )


def _placeholder_commands(
    definitions: tuple[tuple[str, tuple[str, ...], str], ...],
) -> tuple[CommandDefinition, ...]:
    commands: list[CommandDefinition] = []
    for name, aliases, description in definitions:

        def placeholder(_context: CommandContext, _parsed: ParsedArguments, *, feature: str = description) -> str:
            return f"{feature} not yet implemented."

        commands.append(command(name).alias(*aliases).description(description).action(placeholder))
    return tuple(commands)
