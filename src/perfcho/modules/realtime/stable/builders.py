"""Stateless builders for core osu! Stable server packets."""

from __future__ import annotations

from collections.abc import Callable, Collection

from .codec import DEFAULT_LIMITS, CodecLimits, PacketWriter, ReadableBuffer
from .models import Channel, Message, MultiplayerMatch, ScoreFrame, ServerPacket, UserPresence, UserStats


def _build(
    packet_type: ServerPacket,
    payload_writer: Callable[[PacketWriter], None] | None = None,
    *,
    limits: CodecLimits = DEFAULT_LIMITS,
) -> bytes:
    writer = PacketWriter(limits=limits)
    with writer.packet(packet_type):
        if payload_writer is not None:
            payload_writer(writer)
    return writer.to_bytes()


def login_reply(user_id: int, *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build a successful user-id or negative login-failure reply."""
    return _build(ServerPacket.USER_ID, lambda writer: writer.write_i32(user_id), limits=limits)


def pong(*, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build a Stable ping response."""
    return _build(ServerPacket.PONG, limits=limits)


def protocol_version(version: int, *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build the negotiated Bancho protocol-version packet."""
    return _build(ServerPacket.PROTOCOL_VERSION, lambda writer: writer.write_i32(version), limits=limits)


def main_menu_icon(icon_url: str, onclick_url: str, *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build the main-menu icon and click target packet."""
    return _build(
        ServerPacket.MAIN_MENU_ICON,
        lambda writer: writer.write_string(f"{icon_url}|{onclick_url}"),
        limits=limits,
    )


def privileges(value: int, *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build a login privilege packet."""
    return _build(ServerPacket.PRIVILEGES, lambda writer: writer.write_i32(value), limits=limits)


def friends_list(user_ids: Collection[int], *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build the login friend-list packet."""
    return _build(ServerPacket.FRIENDS_LIST, lambda writer: writer.write_i32_list_u16(user_ids), limits=limits)


def notification(text: str, *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build an in-client notification packet."""
    return _build(ServerPacket.NOTIFICATION, lambda writer: writer.write_string(text), limits=limits)


def silence_end(seconds: int, *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build the login silence-duration packet."""
    return _build(ServerPacket.SILENCE_END, lambda writer: writer.write_i32(seconds), limits=limits)


def account_restricted(*, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build an account-restricted marker packet."""
    return _build(ServerPacket.ACCOUNT_RESTRICTED, limits=limits)


def version_update(*, forced: bool = False, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build a normal or forced client-version update packet."""
    packet_type = ServerPacket.VERSION_UPDATE_FORCED if forced else ServerPacket.VERSION_UPDATE
    return _build(packet_type, limits=limits)


def user_presence(presence: UserPresence, *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build a user-presence packet."""
    return _build(ServerPacket.USER_PRESENCE, lambda writer: writer.write_user_presence(presence), limits=limits)


def user_stats(stats: UserStats, *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build a user-statistics packet."""
    return _build(ServerPacket.USER_STATS, lambda writer: writer.write_user_stats(stats), limits=limits)


def user_logout(user_id: int, *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build a user-logout packet with Stable's reserved zero byte."""

    def write_payload(writer: PacketWriter) -> None:
        writer.write_i32(user_id)
        writer.write_u8(0)

    return _build(ServerPacket.USER_LOGOUT, write_payload, limits=limits)


def user_presence_single(user_id: int, *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build a request marker for one user's presence."""
    return _build(ServerPacket.USER_PRESENCE_SINGLE, lambda writer: writer.write_i32(user_id), limits=limits)


def user_presence_bundle(user_ids: Collection[int], *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build a bundled user-presence identifier packet."""
    return _build(
        ServerPacket.USER_PRESENCE_BUNDLE,
        lambda writer: writer.write_i32_list_u16(user_ids),
        limits=limits,
    )


def restart(milliseconds: int = 0, *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Tell a Stable client to reconnect after the supplied delay."""
    return _build(ServerPacket.RESTART, lambda writer: writer.write_i32(milliseconds), limits=limits)


def send_message(message: Message, *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build a public or private chat delivery packet."""
    return _build(ServerPacket.SEND_MESSAGE, lambda writer: writer.write_message(message), limits=limits)


def user_dm_blocked(target: str, *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Tell a Stable client that a recipient rejected its direct message."""
    message = Message("", "", target, 0)
    return _build(ServerPacket.USER_DM_BLOCKED, lambda writer: writer.write_message(message), limits=limits)


def target_is_silenced(target: str, *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Tell a Stable client that a direct-message recipient is silenced."""
    message = Message("", "", target, 0)
    return _build(ServerPacket.TARGET_IS_SILENCED, lambda writer: writer.write_message(message), limits=limits)


def toggle_block_non_friend_dms(*, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build Stable's no-payload direct-message policy toggle marker."""
    return _build(ServerPacket.TOGGLE_BLOCK_NON_FRIEND_DMS, limits=limits)


def match_invite(message: Message, *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build a Stable multiplayer invitation message."""
    return _build(ServerPacket.MATCH_INVITE, lambda writer: writer.write_message(message), limits=limits)


def channel_join(name: str, *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build a channel-join success packet."""
    return _build(ServerPacket.CHANNEL_JOIN_SUCCESS, lambda writer: writer.write_string(name), limits=limits)


def channel_info(channel: Channel, *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build a visible channel-description packet."""
    return _build(ServerPacket.CHANNEL_INFO, lambda writer: writer.write_channel(channel), limits=limits)


def channel_auto_join(channel: Channel, *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build an auto-joined channel-description packet."""
    return _build(ServerPacket.CHANNEL_AUTO_JOIN, lambda writer: writer.write_channel(channel), limits=limits)


def channel_kick(name: str, *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build a channel removal packet."""
    return _build(ServerPacket.CHANNEL_KICK, lambda writer: writer.write_string(name), limits=limits)


def channel_info_end(*, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build the terminator for the login channel list."""
    return _build(ServerPacket.CHANNEL_INFO_END, limits=limits)


def spectator_joined(user_id: int, *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build a spectator-joined notification for a host."""
    return _build(ServerPacket.SPECTATOR_JOINED, lambda writer: writer.write_i32(user_id), limits=limits)


def spectator_left(user_id: int, *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build a spectator-left notification for a host."""
    return _build(ServerPacket.SPECTATOR_LEFT, lambda writer: writer.write_i32(user_id), limits=limits)


def fellow_spectator_joined(user_id: int, *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build a fellow-spectator joined notification."""
    return _build(ServerPacket.FELLOW_SPECTATOR_JOINED, lambda writer: writer.write_i32(user_id), limits=limits)


def fellow_spectator_left(user_id: int, *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build a fellow-spectator left notification."""
    return _build(ServerPacket.FELLOW_SPECTATOR_LEFT, lambda writer: writer.write_i32(user_id), limits=limits)


def spectator_cant_spectate(user_id: int, *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build a spectator playback-failure notification."""
    return _build(ServerPacket.SPECTATOR_CANT_SPECTATE, lambda writer: writer.write_i32(user_id), limits=limits)


def spectate_frames(data: ReadableBuffer, *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Wrap an already validated spectator frame bundle without decoding it again."""
    return _build(ServerPacket.SPECTATE_FRAMES, lambda writer: writer.write_raw(data), limits=limits)


def update_match(
    match: MultiplayerMatch,
    *,
    send_password: bool = True,
    limits: CodecLimits = DEFAULT_LIMITS,
) -> bytes:
    """Build a multiplayer match-state update packet."""
    return _build(
        ServerPacket.UPDATE_MATCH,
        lambda writer: writer.write_multiplayer_match(match, send_password=send_password),
        limits=limits,
    )


def new_match(match: MultiplayerMatch, *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build a new lobby match packet."""
    return _build(
        ServerPacket.NEW_MATCH,
        lambda writer: writer.write_multiplayer_match(match, send_password=False),
        limits=limits,
    )


def dispose_match(match_id: int, *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build a lobby match-removal packet."""
    return _build(ServerPacket.DISPOSE_MATCH, lambda writer: writer.write_i32(match_id), limits=limits)


def match_join_success(match: MultiplayerMatch, *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build a multiplayer join-success packet."""
    return _build(ServerPacket.MATCH_JOIN_SUCCESS, lambda writer: writer.write_multiplayer_match(match), limits=limits)


def match_join_fail(*, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build a multiplayer join-failure packet."""
    return _build(ServerPacket.MATCH_JOIN_FAIL, limits=limits)


def match_start(match: MultiplayerMatch, *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build a multiplayer match-start packet."""
    return _build(ServerPacket.MATCH_START, lambda writer: writer.write_multiplayer_match(match), limits=limits)


def match_score_update(frame: ScoreFrame, *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build a multiplayer score-frame update packet."""
    return _build(ServerPacket.MATCH_SCORE_UPDATE, lambda writer: writer.write_score_frame(frame), limits=limits)


def match_transfer_host(*, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build a multiplayer host-transfer packet."""
    return _build(ServerPacket.MATCH_TRANSFER_HOST, limits=limits)


def match_all_players_loaded(*, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build the all-players-loaded packet used during multiplayer play."""
    return _build(ServerPacket.MATCH_ALL_PLAYERS_LOADED, limits=limits)


def match_player_failed(slot_id: int, *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build a multiplayer player-failed packet."""
    return _build(ServerPacket.MATCH_PLAYER_FAILED, lambda writer: writer.write_i32(slot_id), limits=limits)


def match_complete(*, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build a multiplayer match-complete packet."""
    return _build(ServerPacket.MATCH_COMPLETE, limits=limits)


def match_skip(*, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build a multiplayer skip packet."""
    return _build(ServerPacket.MATCH_SKIP, limits=limits)


def match_player_skipped(user_id: int, *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build a multiplayer player-skipped packet."""
    return _build(ServerPacket.MATCH_PLAYER_SKIPPED, lambda writer: writer.write_i32(user_id), limits=limits)


def match_change_password(password: str, *, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build a multiplayer password-change packet."""
    return _build(ServerPacket.MATCH_CHANGE_PASSWORD, lambda writer: writer.write_string(password), limits=limits)


def match_abort(*, limits: CodecLimits = DEFAULT_LIMITS) -> bytes:
    """Build a multiplayer match-abort packet."""
    return _build(ServerPacket.MATCH_ABORT, limits=limits)
