"""Canonical Presence ingress and Stable Bubble rendering."""

from collections.abc import Iterable
from enum import StrEnum

from perfcho.api.stable.authorization import StablePrivilege
from perfcho.api.stable.canonize.scoring import LEGACY_MOD_BITS, parse_legacy_mods, project_legacy_mods
from perfcho.api.stable.realtime.builders import (
    channel_info,
    channel_join,
    channel_kick,
    dispose_match,
    fellow_spectator_joined,
    fellow_spectator_left,
    match_abort,
    match_all_players_loaded,
    match_complete,
    match_invite,
    match_join_fail,
    match_join_success,
    match_player_failed,
    match_player_skipped,
    match_score_update,
    match_skip,
    match_start,
    match_transfer_host,
    new_match,
    restart,
    send_message,
    spectate_frames,
    spectator_cant_spectate,
    spectator_joined,
    spectator_left,
    toast,
    update_match,
    user_logout,
    user_presence,
    user_stats,
)
from perfcho.api.stable.realtime.countries import stable_country_id
from perfcho.api.stable.realtime.models import (
    Channel,
    Message,
    MultiplayerMatch,
    ReplayAction,
    ReplayFrame,
    ReplayFrameBundle,
    ScoreFrame,
    UserPresence,
    UserStats,
)
from perfcho.infra.db.mods import project_scoreboard_variant
from perfcho.infra.logging import log_event, rate_limit
from perfcho.modules.multiplayer import SlotStatus, TeamMode, WinCondition
from perfcho.modules.realtime import (
    CanonicalReplayFrame,
    CanonicalScoreFrame,
    ChannelMembershipAction,
    ChannelUpdatedBubble,
    ChatMessageBubble,
    MultiplayerRoomAction,
    MultiplayerRoomBubble,
    MultiplayerRoomSnapshot,
    MultiplayerSignalBubble,
    MultiplayerSignalKind,
    PlayerActivity,
    PlayerStatistics,
    PresenceIdentity,
    PresenceUpdatedBubble,
    RealtimeBubble,
    SessionControlAction,
    SessionControlBubble,
    SpectatorAction,
    SpectatorFrameAction,
    SpectatorFrameBubble,
    SpectatorLifecycleBubble,
    ToastBubble,
    UserLogoutBubble,
)
from perfcho.modules.scoring import CanonicalMod, Ruleset
from perfcho.modules.scoring.mods import normalize_mods

_ACTIONS = (
    "idle",
    "away",
    "playing",
    "editing",
    "modding",
    "multiplayer",
    "watching",
    "unknown",
    "testing",
    "submitting",
    "paused",
    "lobby",
    "multiplaying",
    "osu_direct",
)
_ACTION_IDS = {action: identifier for identifier, action in enumerate(_ACTIONS)}
_RULESETS = ("osu", "taiko", "fruits", "mania")
_RULESET_IDS = {ruleset: identifier for identifier, ruleset in enumerate(_RULESETS)}
_PLAYER_PERMISSION = "account.login"
_MODERATOR_PERMISSION = "moderation.enforce"
_DEVELOPER_PERMISSION = "admin.access"
_OWNER_ROLE = "administrator"
_SUPPORTER_ENTITLEMENT = "supporter"
_TEAM_MODES = (TeamMode.HEAD_TO_HEAD, TeamMode.TAG_COOP, TeamMode.TEAM_VS, TeamMode.TAG_TEAM_VS)
_WIN_CONDITIONS = (WinCondition.SCORE, WinCondition.ACCURACY, WinCondition.COMBO, WinCondition.SCORE_V2)
_SLOT_STATUS_TO_WIRE = {
    SlotStatus.OPEN: 1,
    SlotStatus.LOCKED: 2,
    SlotStatus.NOT_READY: 4,
    SlotStatus.READY: 8,
    SlotStatus.NO_BEATMAP: 16,
    SlotStatus.PLAYING: 32,
    SlotStatus.COMPLETE: 64,
}

_REPLAY_ACTION_TO_CANONICAL = {
    ReplayAction.STANDARD: SpectatorFrameAction.UPDATE,
    ReplayAction.NEW_SONG: SpectatorFrameAction.NEW_PLAY,
    ReplayAction.SKIP: SpectatorFrameAction.SKIP,
    ReplayAction.COMPLETION: SpectatorFrameAction.COMPLETE,
    ReplayAction.FAIL: SpectatorFrameAction.FAIL,
    ReplayAction.PAUSE: SpectatorFrameAction.PAUSE,
    ReplayAction.UNPAUSE: SpectatorFrameAction.RESUME,
    ReplayAction.SONG_SELECT: SpectatorFrameAction.SELECT_PLAY,
    ReplayAction.WATCHING_OTHER: SpectatorFrameAction.SWITCH_HOST,
}
_CANONICAL_ACTION_TO_REPLAY = {value: key for key, value in _REPLAY_ACTION_TO_CANONICAL.items()}


class UnsupportedBubblePolicy(StrEnum):
    """Choose whether Phase 2 renderer omissions are dropped or rejected."""

    DROP = "drop"
    RAISE = "raise"


class UnsupportedBubbleError(ValueError):
    """Report a Bubble intentionally deferred to a later migration phase."""


def canonical_privileges_from_stable(value: int) -> frozenset[str]:
    """Convert Stable privilege bits into protocol-neutral effective codes."""
    if isinstance(value, bool) or not isinstance(value, int) or value < 0 or value & ~0x1F:
        raise ValueError("Stable privileges contain unknown bits")
    privileges = StablePrivilege(value)
    codes: set[str] = set()
    if privileges & StablePrivilege.PLAYER:
        codes.add(_PLAYER_PERMISSION)
    if privileges & StablePrivilege.MODERATOR:
        codes.add(_MODERATOR_PERMISSION)
    if privileges & StablePrivilege.SUPPORTER:
        codes.add(_SUPPORTER_ENTITLEMENT)
    if privileges & StablePrivilege.OWNER:
        codes.add(_OWNER_ROLE)
    if privileges & StablePrivilege.DEVELOPER:
        codes.add(_DEVELOPER_PERMISSION)
    return frozenset(codes)


def stable_privileges_from_canonical(codes: frozenset[str]) -> StablePrivilege:
    """Project canonical effective codes into Stable's five privilege bits."""
    privileges = StablePrivilege.NONE
    if _PLAYER_PERMISSION in codes:
        privileges |= StablePrivilege.PLAYER
    if _MODERATOR_PERMISSION in codes:
        privileges |= StablePrivilege.MODERATOR
    if _SUPPORTER_ENTITLEMENT in codes:
        privileges |= StablePrivilege.SUPPORTER
    if _OWNER_ROLE in codes:
        privileges |= StablePrivilege.OWNER
    if _DEVELOPER_PERMISSION in codes:
        privileges |= StablePrivilege.DEVELOPER
    return privileges


def canonicalize_presence(
    presence: UserPresence,
    statistics: UserStats,
    *,
    country_code: str | None,
    privilege_codes: frozenset[str] | None = None,
) -> tuple[PresenceIdentity, PlayerActivity, PlayerStatistics]:
    """Convert Stable Presence and Stats values into canonical realtime state."""
    if presence.user_id != statistics.user_id:
        raise ValueError("Stable presence and statistics account IDs differ")
    if presence.mode != statistics.mode:
        raise ValueError("Stable presence and statistics rulesets differ")
    if not 0 <= statistics.action < len(_ACTIONS) or not 0 <= statistics.mode < len(_RULESETS):
        raise ValueError("Stable presence contains an unknown activity or ruleset")
    mods = parse_legacy_mods(statistics.mods)
    return (
        PresenceIdentity(
            display_name=presence.username,
            country_code=country_code,
            utc_offset=presence.utc_offset,
            privileges=(
                canonical_privileges_from_stable(presence.privileges) if privilege_codes is None else privilege_codes
            ),
            longitude=presence.longitude,
            latitude=presence.latitude,
        ),
        PlayerActivity(
            action=_ACTIONS[statistics.action],
            info=statistics.info_text,
            beatmap_id=statistics.beatmap_id,
            beatmap_checksum=statistics.beatmap_md5 or None,
            ruleset=_RULESETS[statistics.mode],
            mods=tuple(mod.acronym for mod in mods),
        ),
        PlayerStatistics(
            ranked_score=statistics.ranked_score,
            accuracy=statistics.accuracy,
            play_count=statistics.play_count,
            total_score=statistics.total_score,
            global_rank=statistics.global_rank or None,
            performance=float(statistics.performance),
        ),
    )


def _legacy_mod_bits(mods: tuple[str, ...]) -> int:
    bits = 0
    for mod in mods:
        try:
            bits |= LEGACY_MOD_BITS[mod]
        except KeyError as error:
            raise ValueError(f"canonical mod {mod!r} has no Stable mapping") from error
    if "NC" in mods:
        bits |= LEGACY_MOD_BITS["DT"]
    if "PF" in mods:
        bits |= LEGACY_MOD_BITS["SD"]
    return bits


def stable_presence_models(bubble: PresenceUpdatedBubble) -> tuple[UserPresence, UserStats]:
    """Project one canonical Presence Bubble into Stable wire-level values."""
    try:
        action = _ACTION_IDS[bubble.activity.action]
        mode = _RULESET_IDS[bubble.activity.ruleset]
    except KeyError as error:
        raise ValueError(f"canonical presence value {error.args[0]!r} has no Stable mapping") from error
    rank = bubble.statistics.global_rank or 0
    mods = _legacy_mod_bits(bubble.activity.mods)
    performance = int(bubble.statistics.performance)
    if not 0 <= performance <= 0xFFFF:
        raise ValueError("performance must fit Stable's unsigned 16-bit field")
    presence = UserPresence(
        user_id=bubble.account_id,
        username=bubble.display_name,
        utc_offset=bubble.utc_offset,
        country_code=stable_country_id(bubble.country_code),
        privileges=int(stable_privileges_from_canonical(bubble.privileges)),
        mode=mode,
        longitude=bubble.longitude,
        latitude=bubble.latitude,
        global_rank=rank,
    )
    statistics = UserStats(
        user_id=bubble.account_id,
        action=action,
        info_text=bubble.activity.info,
        beatmap_md5=bubble.activity.beatmap_checksum or "",
        mods=mods,
        mode=mode,
        beatmap_id=bubble.activity.beatmap_id or 0,
        ranked_score=bubble.statistics.ranked_score,
        accuracy=bubble.statistics.accuracy,
        play_count=bubble.statistics.play_count,
        total_score=bubble.statistics.total_score,
        global_rank=rank,
        performance=performance,
    )
    return presence, statistics


def canonicalize_spectator_frame(host_account_id: int, bundle: ReplayFrameBundle) -> SpectatorFrameBubble:
    """Convert a fully parsed Stable replay bundle at the adapter boundary."""
    score = bundle.score_frame
    return SpectatorFrameBubble(
        host_account_id=host_account_id,
        sequence=bundle.sequence,
        action=_REPLAY_ACTION_TO_CANONICAL[bundle.action],
        frames=tuple(
            CanonicalReplayFrame(
                timestamp_ms=frame.time,
                position_x=frame.x,
                position_y=frame.y,
                input_state=frame.button_state,
                auxiliary_state=frame.taiko_byte,
            )
            for frame in bundle.frames
        ),
        score=CanonicalScoreFrame(
            elapsed_ms=score.time,
            frame_index=score.frame_id,
            count_300=score.count_300,
            count_100=score.count_100,
            count_50=score.count_50,
            count_geki=score.count_geki,
            count_katu=score.count_katu,
            count_miss=score.count_miss,
            total_score=score.total_score,
            max_combo=score.max_combo,
            current_combo=score.current_combo,
            perfect=score.perfect,
            health=score.current_hp,
            tag=score.tag_byte,
            score_v2=score.score_v2,
            combo_portion=score.combo_portion,
            bonus_portion=score.bonus_portion,
        ),
        extra=bundle.extra,
    )


class StableBubbleRenderer:
    """Render supported canonical Bubbles into complete Stable server packets."""

    def __init__(self, *, unsupported: UnsupportedBubblePolicy = UnsupportedBubblePolicy.DROP) -> None:
        """Configure the explicit policy for Bubble types deferred to later phases."""
        self._unsupported = unsupported

    def render(self, bubble: RealtimeBubble) -> bytes:
        """Render one supported Bubble or apply the configured unsupported policy."""
        match bubble:
            case PresenceUpdatedBubble():
                return self.render_presence(bubble)
            case UserLogoutBubble():
                return user_logout(bubble.account_id)
            case ChatMessageBubble():
                text = f"\x01ACTION {bubble.content}\x01" if bubble.is_action else bubble.content
                return send_message(Message(bubble.sender_name, text, bubble.channel_name, bubble.sender_account_id))
            case ChannelUpdatedBubble():
                name = bubble.name if bubble.name.startswith("#") else f"#{bubble.name}"
                transition = (
                    channel_join(name)
                    if bubble.membership_action is ChannelMembershipAction.JOINED
                    else channel_kick(name)
                    if bubble.membership_action is ChannelMembershipAction.LEFT
                    else b""
                )
                return transition + channel_info(Channel(name, bubble.topic, bubble.member_count))
            case ToastBubble():
                return toast(bubble.message)
            case SessionControlBubble():
                # Stable has no graceful close packet; RESTART is its session-control primitive.
                match bubble.action:
                    case SessionControlAction.RECONNECT | SessionControlAction.CLOSE:
                        return restart(bubble.retry_after_ms)
            case MultiplayerRoomBubble():
                return self.render_multiplayer_room(bubble)
            case MultiplayerSignalBubble():
                return self.render_multiplayer_signal(bubble)
            case SpectatorLifecycleBubble():
                return self.render_spectator_lifecycle(bubble)
            case SpectatorFrameBubble():
                return self.render_spectator_frame(bubble)
        raise TypeError(f"unknown Bubble type: {type(bubble).__name__}")

    def render_presence(
        self,
        bubble: PresenceUpdatedBubble,
        *,
        include_identity: bool = True,
        include_statistics: bool = True,
    ) -> bytes:
        """Render either or both Stable Presence packet projections."""
        presence, statistics = stable_presence_models(bubble)
        return (user_presence(presence) if include_identity else b"") + (
            user_stats(statistics) if include_statistics else b""
        )

    def render_multiplayer_room(self, bubble: MultiplayerRoomBubble) -> bytes:
        """Project one complete canonical room event into its Stable packet contract."""
        room = _stable_multiplayer_match(bubble.room, password=bubble.local_admission_credential)
        match bubble.action:
            case MultiplayerRoomAction.CREATED:
                return new_match(room)
            case MultiplayerRoomAction.UPDATED:
                return update_match(room, send_password=False)
            case MultiplayerRoomAction.DISPOSED:
                return dispose_match(bubble.room.room_public_id)
            case MultiplayerRoomAction.JOINED:
                return channel_kick("#lobby") + channel_join("#multiplayer") + match_join_success(room)
            case MultiplayerRoomAction.ROUND_STARTED:
                return match_start(room)
            case MultiplayerRoomAction.ROUND_COMPLETED:
                return match_complete()
            case MultiplayerRoomAction.ROUND_ABORTED:
                return match_abort()
            case MultiplayerRoomAction.LEFT:
                return channel_kick("#multiplayer")
            case MultiplayerRoomAction.KICKED:
                return channel_kick("#multiplayer") + match_join_fail()

    def render_multiplayer_signal(self, bubble: MultiplayerSignalBubble) -> bytes:
        """Project one canonical multiplayer signal into its Stable packet contract."""
        match bubble.kind:
            case MultiplayerSignalKind.PARTICIPANT_LOADING_COMPLETED:
                return match_all_players_loaded()
            case MultiplayerSignalKind.FAILED:
                if bubble.slot_position is None:
                    raise ValueError("failed signal requires a slot position")
                return match_player_failed(bubble.slot_position)
            case MultiplayerSignalKind.SKIPPED:
                if bubble.actor_account_id is None:
                    raise ValueError("skipped signal requires an actor account")
                return match_player_skipped(bubble.actor_account_id)
            case MultiplayerSignalKind.ALL_PLAYERS_SKIPPED:
                return match_skip()
            case MultiplayerSignalKind.HOST_TRANSFERRED:
                return match_transfer_host()
            case MultiplayerSignalKind.SCORE_UPDATED:
                if bubble.score is None:
                    raise ValueError("score-updated signal requires score state")
                score = bubble.score
                return match_score_update(
                    ScoreFrame(
                        score.elapsed_milliseconds,
                        score.slot_position,
                        score.count_300,
                        score.count_100,
                        score.count_50,
                        score.count_geki,
                        score.count_katu,
                        score.count_miss,
                        score.total_score,
                        score.max_combo,
                        score.current_combo,
                        score.perfect,
                        score.current_health,
                        score.tag,
                        score.score_v2,
                        score.combo_portion,
                        score.bonus_portion,
                    )
                )
            case MultiplayerSignalKind.INVITED:
                if bubble.room_public_id is None or bubble.invitation is None:
                    raise ValueError("invited signal requires room and invitation state")
                invitation = bubble.invitation
                return match_invite(
                    Message(
                        invitation.sender_name,
                        f"Come join my game: [osump://{bubble.room_public_id}/{invitation.admission_token} "
                        f"{invitation.room_name}].",
                        invitation.recipient_name,
                        invitation.sender_account_id,
                    )
                )
            case MultiplayerSignalKind.JOIN_FAILED:
                return match_join_fail()

    def render_spectator_lifecycle(self, bubble: SpectatorLifecycleBubble) -> bytes:
        """Project one recipient-scoped spectator relation event."""
        account_id = bubble.spectator_account_id
        match bubble.action:
            case SpectatorAction.ATTACHED_TO_HOST:
                return spectator_joined(account_id)
            case SpectatorAction.DETACHED_FROM_HOST:
                return spectator_left(account_id)
            case SpectatorAction.FELLOW_ATTACHED:
                return fellow_spectator_joined(account_id)
            case SpectatorAction.FELLOW_DETACHED:
                return fellow_spectator_left(account_id)
            case SpectatorAction.PLAYBACK_UNAVAILABLE:
                return spectator_cant_spectate(account_id)

    def render_spectator_frame(self, bubble: SpectatorFrameBubble) -> bytes:
        """Rebuild one Stable replay bundle solely from canonical fields."""
        score = bubble.score
        return spectate_frames(
            ReplayFrameBundle(
                frames=tuple(
                    ReplayFrame(
                        button_state=frame.input_state,
                        taiko_byte=frame.auxiliary_state,
                        x=frame.position_x,
                        y=frame.position_y,
                        time=frame.timestamp_ms,
                    )
                    for frame in bubble.frames
                ),
                score_frame=ScoreFrame(
                    time=score.elapsed_ms,
                    frame_id=score.frame_index,
                    count_300=score.count_300,
                    count_100=score.count_100,
                    count_50=score.count_50,
                    count_geki=score.count_geki,
                    count_katu=score.count_katu,
                    count_miss=score.count_miss,
                    total_score=score.total_score,
                    max_combo=score.max_combo,
                    current_combo=score.current_combo,
                    perfect=score.perfect,
                    current_hp=score.health,
                    tag_byte=score.tag,
                    score_v2=score.score_v2,
                    combo_portion=score.combo_portion,
                    bonus_portion=score.bonus_portion,
                ),
                action=_CANONICAL_ACTION_TO_REPLAY[bubble.action],
                extra=bubble.extra,
                sequence=bubble.sequence,
            )
        )

    def render_many(self, bubbles: Iterable[RealtimeBubble], *, max_bytes: int) -> bytes:
        """Render in order, dropping each Bubble that does not fit the response budget."""
        if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes < 0:
            raise ValueError("max_bytes must be a non-negative integer")
        output = bytearray()
        for bubble in bubbles:
            try:
                rendered = self.render(bubble)
            except Exception as error:
                bubble_type = type(bubble).__name__
                if rate_limit(f"stable.bubble.render_failed:{bubble_type}", interval_seconds=5.0):
                    log_event(
                        "WARNING",
                        "stable.bubble.render_failed",
                        exception=error,
                        bubble_type=bubble_type,
                        outcome="dropped",
                    )
                continue
            if len(output) + len(rendered) <= max_bytes:
                output.extend(rendered)
        return bytes(output)

    def _unsupported_bubble(self, bubble: RealtimeBubble) -> bytes:
        if self._unsupported is UnsupportedBubblePolicy.DROP:
            return b""
        raise UnsupportedBubbleError(f"Stable rendering is not implemented for {type(bubble).__name__}")


def _stable_multiplayer_match(
    snapshot: MultiplayerRoomSnapshot,
    *,
    password: str | None = None,
) -> MultiplayerMatch:
    """Project a canonical room snapshot into Stable's fixed match structure."""
    variant = project_scoreboard_variant(snapshot.mods)
    normalized = normalize_mods(snapshot.ruleset, variant, snapshot.mods)
    legacy_bits = project_legacy_mods(normalized.mods)
    slot_mods = (
        tuple(_stable_slot_mod_bits(snapshot.ruleset, slot.mods) for slot in snapshot.slots)
        if snapshot.free_mods
        else ()
    )
    return MultiplayerMatch(
        match_id=snapshot.room_public_id,
        in_progress=snapshot.in_progress,
        mods=legacy_bits,
        name=snapshot.name,
        password=password if password is not None else "*" if snapshot.password_protected else "",
        beatmap_name=snapshot.beatmap_name,
        beatmap_id=snapshot.external_beatmap_id,
        beatmap_md5=snapshot.beatmap_md5.hex() if snapshot.beatmap_md5 is not None else "",
        slot_statuses=tuple(_SLOT_STATUS_TO_WIRE[slot.status] for slot in snapshot.slots),
        slot_teams=tuple(slot.team for slot in snapshot.slots),
        slot_user_ids=tuple(slot.account_id for slot in snapshot.slots),
        host_id=snapshot.host_account_id,
        mode=(Ruleset.OSU, Ruleset.TAIKO, Ruleset.FRUITS, Ruleset.MANIA).index(snapshot.ruleset),
        win_condition=_WIN_CONDITIONS.index(snapshot.win_condition),
        team_type=_TEAM_MODES.index(snapshot.team_mode),
        freemods=snapshot.free_mods,
        slot_mods=slot_mods,
        seed=snapshot.seed,
    )


def _stable_slot_mod_bits(ruleset: Ruleset, mods: tuple[CanonicalMod, ...]) -> int:
    variant = project_scoreboard_variant(mods)
    return project_legacy_mods(normalize_mods(ruleset, variant, mods).mods)


__all__ = (
    "StableBubbleRenderer",
    "UnsupportedBubbleError",
    "UnsupportedBubblePolicy",
    "canonical_privileges_from_stable",
    "canonicalize_spectator_frame",
    "canonicalize_presence",
    "stable_presence_models",
    "stable_privileges_from_canonical",
)
