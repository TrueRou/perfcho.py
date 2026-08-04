"""Define bot commands owned by the multiplayer domain."""

import secrets
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace

from perfcho.modules.bot import CommandContext, CommandDefinition, CommandGroup, ParsedArguments, command
from perfcho.modules.content import BeatmapRevisionView
from perfcho.modules.multiplayer.models import (
    ChangeHost,
    ChangeRoomPassword,
    CompleteRound,
    KickParticipant,
    RoomSettings,
    RoomState,
    StartRound,
    TeamMode,
    UpdateRoomSettings,
    WinCondition,
)
from perfcho.modules.multiplayer.services import MultiplayerService
from perfcho.modules.scoring import CanonicalMod, Ruleset
from perfcho.modules.social import AccountIdentityView

type AccountResolver = Callable[[str], Awaitable[AccountIdentityView]]
type BeatmapResolver = Callable[[str], Awaitable[BeatmapRevisionView]]


@dataclass(frozen=True, slots=True)
class MultiplayerCommandDependencies:
    """Bind multiplayer commands to canonical operations and narrow queries."""

    service: MultiplayerService
    resolve_account: AccountResolver
    resolve_beatmap: BeatmapResolver


def build_multiplayer_commands(dependency: MultiplayerCommandDependencies) -> CommandGroup:
    """Build the room-control command group."""

    async def start(context: CommandContext, _: ParsedArguments) -> str:
        state = await _room(context, dependency.service)
        await dependency.service.start_round(
            StartRound(context.invocation.meta, state.room.public_id, state.room.version)
        )
        return "Starting match."

    async def abort(context: CommandContext, _: ParsedArguments) -> str:
        state = await _room(context, dependency.service)
        await dependency.service.complete_round(
            CompleteRound(context.invocation.meta, state.room.public_id, state.room.version, aborted=True)
        )
        return "Match aborted."

    async def host(context: CommandContext, parsed: ParsedArguments) -> str:
        state = await _room(context, dependency.service)
        target = await dependency.resolve_account(parsed.values["username"])
        await dependency.service.change_host(
            ChangeHost(context.invocation.meta, state.room.public_id, state.room.version, target.account_id)
        )
        return f"Match host changed to {target.display_name}."

    async def kick(context: CommandContext, parsed: ParsedArguments) -> str:
        state = await _room(context, dependency.service)
        target = await dependency.resolve_account(parsed.values["username"])
        await dependency.service.kick_participant(
            KickParticipant(context.invocation.meta, state.room.public_id, state.room.version, target.account_id)
        )
        return f"{target.display_name} was kicked from the match."

    async def password(context: CommandContext, parsed: ParsedArguments) -> str:
        value = " ".join(parsed.values["password"])
        await _change_password(context, dependency.service, value)
        return "Match password updated." if value else "Match password removed."

    async def random_password(context: CommandContext, _: ParsedArguments) -> str:
        value = secrets.token_urlsafe(8)
        await _change_password(context, dependency.service, value)
        return f"Match password: {value}"

    async def title(context: CommandContext, parsed: ParsedArguments) -> str:
        value = " ".join(parsed.values["title"]).strip()
        if not value:
            return f"Usage: {context.registry.prefix}mp title <title>"
        await _change_settings(context, dependency.service, lambda current: replace(current, name=value))
        return f"Match title changed to {value}."

    async def condition(context: CommandContext, parsed: ParsedArguments) -> str:
        values = {
            "score": WinCondition.SCORE,
            "accuracy": WinCondition.ACCURACY,
            "combo": WinCondition.COMBO,
            "scorev2": WinCondition.SCORE_V2,
            "score_v2": WinCondition.SCORE_V2,
        }
        value = values.get(parsed.values["condition"].casefold())
        if value is None:
            return "Win condition must be score, accuracy, combo, or scorev2."
        await _change_settings(
            context,
            dependency.service,
            lambda current: replace(current, win_condition=value),
        )
        return f"Win condition changed to {value.value}."

    async def team_type(context: CommandContext, parsed: ParsedArguments) -> str:
        values = {
            "headtohead": TeamMode.HEAD_TO_HEAD,
            "h2h": TeamMode.HEAD_TO_HEAD,
            "tagcoop": TeamMode.TAG_COOP,
            "teamvs": TeamMode.TEAM_VS,
            "tagteamvs": TeamMode.TAG_TEAM_VS,
        }
        value = values.get(parsed.values["mode"].replace("_", "").casefold())
        if value is None:
            return "Team type must be headtohead, tagcoop, teamvs, or tagteamvs."
        await _change_settings(context, dependency.service, lambda current: replace(current, team_mode=value))
        return f"Team type changed to {value.value}."

    async def mods(context: CommandContext, parsed: ParsedArguments) -> str:
        selected = _parse_mods(parsed.values["mods"])
        await _change_settings(context, dependency.service, lambda current: replace(current, mods=selected))
        return "Match mods updated."

    async def free_mods(context: CommandContext, parsed: ParsedArguments) -> str:
        requested = parsed.values["enabled"]

        def update(current: RoomSettings) -> RoomSettings:
            enabled = not current.free_mods if requested is None else requested
            return replace(current, free_mods=enabled)

        state = await _change_settings(context, dependency.service, update)
        return f"Free Mod {'enabled' if state.room.settings.free_mods else 'disabled'}."

    async def map_command(context: CommandContext, parsed: ParsedArguments) -> str:
        beatmap = await dependency.resolve_beatmap(parsed.values["id_or_hash"])

        def update(current: RoomSettings) -> RoomSettings:
            return replace(
                current,
                beatmap_name=f"{beatmap.artist} - {beatmap.title} [{beatmap.difficulty_name}]",
                external_beatmap_id=beatmap.external_beatmap_id,
                beatmap_md5=beatmap.md5,
                ruleset=Ruleset(beatmap.ruleset),
            )

        await _change_settings(context, dependency.service, update)
        return f"Changed map to {beatmap.artist} - {beatmap.title} [{beatmap.difficulty_name}]."

    commands = (
        command("start").description("Start the match").action(start),
        command("abort").description("Abort the current match").action(abort),
        command("map", "<id_or_hash:string>").description("Set the match beatmap").action(map_command),
        command("mods", "[...mods:string]").description("Set match mods").action(mods),
        command("freemods", "[enabled:boolean]")
        .alias("fm", "fmods")
        .description("Toggle Free Mod mode")
        .action(free_mods),
        command("host", "<username:user>").description("Set the match host").action(host),
        command("kick", "<username:user>").description("Kick a player from the match").action(kick),
        command("password", "[...password:string]")
        .alias("pw")
        .description("Set or remove match password")
        .action(password),
        command("randompassword").alias("randpw").description("Generate a random password").action(random_password),
        command("title", "[...title:string]").alias("name").description("Set match title").action(title),
        command("condition", "<condition:string>")
        .alias("cond", "scoringtype", "st")
        .description("Set win condition")
        .action(condition),
        command("teamtype", "<mode:string>").alias("tt").description("Set team type").action(team_type),
        *_placeholder_commands(
            (
                ("invite", ("inv",), "Invite a player to the match"),
                ("lock", (), "Lock all empty slots"),
                ("unlock", (), "Unlock all locked slots"),
                ("size", (), "Set the match size (1-16)"),
                ("move", (), "Move a player to a slot"),
                ("team", (), "Set player team color"),
                ("ban", (), "Ban a player from the match"),
                ("autoref", (), "Toggle auto-referee mode"),
                ("close", ("end",), "Close the match"),
            )
        ),
    )
    return CommandGroup("mp", "Multiplayer commands", commands)


def build_pool_commands() -> CommandGroup:
    """Build compatibility commands for the tournament pool subdomain."""
    definitions = (
        ("create", ("c",), "Create a new mappool"),
        ("delete", (), "Delete a mappool"),
        ("add", (), "Add a map to a pool"),
        ("remove", ("rm",), "Remove a map from a pool"),
        ("list", (), "List all mappools"),
        ("info", (), "Show mappool details"),
    )
    return CommandGroup("pool", "Mappool commands", _placeholder_commands(definitions, hidden=True))


async def _room(context: CommandContext, service: MultiplayerService) -> RoomState:
    state = await service.find_room_for_account(context.sender_account_id)
    if state is None:
        raise RuntimeError("sender is not in a multiplayer room")
    return state


async def _change_settings(
    context: CommandContext,
    service: MultiplayerService,
    update: Callable[[RoomSettings], RoomSettings],
) -> RoomState:
    state = await _room(context, service)
    return await service.update_settings(
        UpdateRoomSettings(
            context.invocation.meta,
            state.room.public_id,
            state.room.version,
            update(state.room.settings),
        )
    )


async def _change_password(context: CommandContext, service: MultiplayerService, password: str) -> None:
    state = await _room(context, service)
    await service.change_password(
        ChangeRoomPassword(context.invocation.meta, state.room.public_id, state.room.version, password)
    )


def _parse_mods(values: tuple[str, ...]) -> tuple[CanonicalMod, ...]:
    normalized = "".join(values).replace(",", "").upper()
    if normalized in {"", "NM", "NOMOD"}:
        return ()
    if len(normalized) % 2:
        raise ValueError("mods must use two-character acronyms")
    return tuple(CanonicalMod(normalized[index : index + 2]) for index in range(0, len(normalized), 2))


def _placeholder_commands(
    definitions: tuple[tuple[str, tuple[str, ...], str], ...],
    *,
    hidden: bool = False,
) -> tuple[CommandDefinition, ...]:
    commands: list[CommandDefinition] = []
    for name, aliases, description in definitions:
        builder = command(name).description(description).alias(*aliases).hidden(hidden)
        commands.append(
            builder.action(lambda _context, _parsed, feature=description: f"{feature} not yet implemented.")
        )
    return tuple(commands)
