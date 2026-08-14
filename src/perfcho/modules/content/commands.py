"""Define bot commands owned by the content domain."""

from dataclasses import dataclass

from perfcho.modules.authorization import AuthorizationQueryService
from perfcho.modules.bot import BotReply, CommandContext, CommandGroup, ParsedArguments, command
from perfcho.modules.content import ContentService, InvalidStatusTransition


@dataclass(frozen=True, slots=True)
class ContentCommandDependencies:
    """Bind content status commands to canonical operations and authorization."""

    service: ContentService
    authorization: AuthorizationQueryService


def build_content_commands(dependency: ContentCommandDependencies) -> CommandGroup:
    """Build the beatmap ranking status command group."""

    async def can_manage(context: CommandContext) -> bool:
        effective = await dependency.authorization.get_effective(context.sender_account_id)
        return effective.allows("content.manage")

    async def apply(context: CommandContext, parsed: ParsedArguments, method: str) -> BotReply:
        beatmapset_id = parsed.values["beatmapset_id"]
        handler = getattr(dependency.service, method)
        try:
            state = await handler(context.sender_account_id, beatmapset_id)
        except InvalidStatusTransition as error:
            return BotReply(str(error))
        return BotReply(f"Beatmapset {state.external_beatmapset_id} is now {state.status}.")

    return CommandGroup(
        name="content",
        description="Manage beatmap ranking status",
        can_execute=can_manage,
        commands=(
            command("qualify", "<beatmapset_id:number>")
            .description("Qualify a pending beatmapset")
            .action(lambda c, p: apply(c, p, "qualify")),
            command("rank", "<beatmapset_id:number>")
            .description("Rank a qualified beatmapset")
            .action(lambda c, p: apply(c, p, "rank")),
            command("love", "<beatmapset_id:number>")
            .description("Move a ranked beatmapset to loved")
            .action(lambda c, p: apply(c, p, "love")),
            command("unlove", "<beatmapset_id:number>")
            .description("Move a loved beatmapset back to ranked")
            .action(lambda c, p: apply(c, p, "unlove")),
            command("disqualify", "<beatmapset_id:number>")
            .description("Disqualify a qualified beatmapset back to pending")
            .action(lambda c, p: apply(c, p, "disqualify")),
            command("graveyard", "<beatmapset_id:number>")
            .description("Move a beatmapset to graveyard")
            .action(lambda c, p: apply(c, p, "graveyard")),
            command("pending", "<beatmapset_id:number>")
            .description("Restore a graveyard beatmapset to pending")
            .action(lambda c, p: apply(c, p, "restore_pending")),
            command("revert", "<beatmapset_id:number>")
            .description("Revert a local status override to upstream")
            .action(lambda c, p: apply(c, p, "revert_status")),
        ),
    )
