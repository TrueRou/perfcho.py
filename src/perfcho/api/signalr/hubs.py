"""osu!lazer SignalR hubs mounted into the perfcho FastAPI process."""

from __future__ import annotations

import hashlib
import uuid
from typing import TYPE_CHECKING, Any

from aiosignalr.server import ServerOptions, SignalRServer

from perfcho.api.canonical.router._multiplayer import multiplayer_room
from perfcho.api.signalr.base import PerfchoHub
from perfcho.modules.common import Actor, ClientContext, CommandMeta
from perfcho.modules.multiplayer import (
    ChangeHost,
    CompleteRound,
    CreateRoom,
    JoinRoom,
    KickParticipant,
    LeaveRoom,
    RoomSettings,
    RoomState,
    SlotStatus,
    StartRound,
    TeamMode,
    UpdateRoomSettings,
    WinCondition,
)
from perfcho.modules.realtime import (
    InvalidFrame,
    MultiplayerRoomBubble,
    PresenceUpdatedBubble,
    RealtimeBubble,
    RealtimeSession,
    RealtimeSessionFenced,
    RealtimeSessionNotFound,
    SpectatorAction,
    SpectatorFrame,
    SpectatorFrameAction,
    SpectatorFrameBubble,
    SpectatorHostOffline,
    SpectatorLifecycleBubble,
)
from perfcho.modules.scoring import CanonicalMod, Ruleset, ScoreboardVariant

if TYPE_CHECKING:
    from fastapi import FastAPI

    from perfcho.infra.compose import StableServices

_SPECTATOR_PATH = "/signalr/spectator"
_MULTIPLAYER_PATH = "/signalr/multiplayer"
_METADATA_PATH = "/signalr/metadata"

_MATCH_TYPES = {
    "HeadToHead": TeamMode.HEAD_TO_HEAD,
    "TeamVersus": TeamMode.TEAM_VS,
    "TagCoop": TeamMode.TAG_COOP,
    "TagTeamVersus": TeamMode.TAG_TEAM_VS,
}
_RULESET_IDS = {0: Ruleset.OSU, 1: Ruleset.TAIKO, 2: Ruleset.FRUITS, 3: Ruleset.MANIA}


class SpectatorHub(PerfchoHub):
    """Realtime play observation (frame streaming) hub.

    Maps lazer's ``ISpectatorServer`` invocations onto the canonical
    :class:`~perfcho.modules.realtime.RealtimeStateRepository` spectator
    operations. Frames published by a host are fanned out through the account
    event bus to every spectator, regardless of which worker hosts them.
    """

    # -- server invocations ------------------------------------------------

    async def BeginPlaySessionV2(self, score_token: int | None, state: dict[str, Any]) -> None:
        """Signal the start of a play session."""
        services = self._services()
        if services is None or services.realtime is None or self.account_id is None:
            return
        realtime = await self._realtime(services)
        if realtime is None:
            return
        spectators = await services.realtime.list_spectators(
            self.account_id, host_fence=realtime.fence, at=services.clock.now()
        )
        if services.user_events is not None:
            await services.user_events.publish_many(
                tuple(relation.spectator_account_id for relation in spectators),
                SpectatorLifecycleBubble(SpectatorAction.ATTACHED_TO_HOST, self.account_id, self.account_id),
            )
        self._spectator_state = state

    async def SendFrameDataV2(self, score_token: int | None, data: dict[str, Any]) -> None:
        """Stream a frame bundle during play."""
        services = self._services()
        if services is None or services.realtime is None or self.account_id is None:
            return
        realtime = await self._realtime(services)
        if realtime is None:
            return
        bubble = _frame_bubble(self.account_id, data)
        try:
            result = await services.realtime.publish_spectator_frame(
                self.account_id,
                host_fence=realtime.fence,
                frame=bubble,
                reset_history=bubble.action is SpectatorFrameAction.NEW_PLAY,
                expires_at=realtime.expires_at,
            )
        except (InvalidFrame, SpectatorHostOffline, RealtimeSessionFenced, RealtimeSessionNotFound):
            return
        if services.user_events is not None:
            await services.user_events.publish_many(
                tuple(recipient.account_id for recipient in result.recipients),
                bubble,
            )

    async def EndPlaySessionV2(self, score_token: int | None, final_state: str) -> None:
        """Signal the end of a play session."""
        del final_state
        services = self._services()
        if services is None or self.account_id is None:
            return

    async def StartWatchingUser(self, user_id: int) -> None:
        """Subscribe to another user's plays."""
        services = self._services()
        if services is None or services.realtime is None or self.account_id is None:
            return
        realtime = await self._realtime(services)
        if realtime is None:
            return
        host_presence = await services.realtime.get_presence(user_id, at=services.clock.now())
        if host_presence is None:
            return
        try:
            attachment = await services.realtime.attach_spectator(
                user_id,
                self.account_id,
                relation_id=services.id_generator.new(),
                host_fence=host_presence.fence,
                spectator_fence=realtime.fence,
                expires_at=min(realtime.expires_at, host_presence.expires_at),
                history_limit=50,
            )
        except (SpectatorHostOffline, RealtimeSessionFenced, RealtimeSessionNotFound):
            return
        for frame in attachment.history.frames:
            await self._caller("UserSentFrames", user_id, _frame_data(frame))
        watchers = await services.realtime.list_spectators(
            user_id, host_fence=host_presence.fence, at=services.clock.now()
        )
        await self._caller(
            "UserStartedWatching",
            [{"onlineID": w.spectator_account_id, "username": ""} for w in watchers],
        )

    async def EndWatchingUser(self, user_id: int) -> None:
        """Stop watching another user's plays."""
        services = self._services()
        if services is None or services.realtime is None or self.account_id is None:
            return
        realtime = await self._realtime(services)
        if realtime is None:
            return
        relation = await services.realtime.get_spectator_relation(
            self.account_id, spectator_fence=realtime.fence, at=services.clock.now()
        )
        if relation is not None and relation.host_account_id == user_id:
            await services.realtime.detach_spectator(
                user_id,
                self.account_id,
                relation_id=relation.relation_id,
                expected_revision=relation.revision,
                host_fence=relation.host_fence,
                spectator_fence=relation.spectator_fence,
            )

    async def _realtime(self, services: StableServices | None) -> RealtimeSession | None:
        if services is None or services.realtime is None:
            return None
        try:
            return await services.realtime.resolve_session(self._session_id, at=services.clock.now())
        except (RealtimeSessionNotFound, RealtimeSessionFenced):
            return None

    @property
    def _session_id(self) -> uuid.UUID:
        session_id = self.context.state.get("session_id")
        if isinstance(session_id, uuid.UUID):
            return session_id
        # Lazer connections carry no durable session; derive a stable one.
        return uuid.uuid5(uuid.NAMESPACE_URL, f"lazer:{self.account_id}")

    async def handle_bubble(self, bubble: RealtimeBubble) -> None:
        """Translate spectator bubbles into lazer client callbacks."""
        if isinstance(bubble, SpectatorFrameBubble):
            await self._caller("UserSentFrames", bubble.host_account_id, _frame_data(bubble))
        elif isinstance(bubble, SpectatorLifecycleBubble):
            if bubble.action is SpectatorAction.FELLOW_ATTACHED:
                await self._caller(
                    "UserStartedWatching", [{"onlineID": bubble.spectator_account_id, "username": ""}]
                )
            elif bubble.action is SpectatorAction.FELLOW_DETACHED:
                await self._caller("UserEndedWatching", bubble.spectator_account_id)


class MetadataHub(PerfchoHub):
    """Realtime presence / beatmap-update metadata hub.

    Presence updates are published through the account event bus and replayed
    to clients watching presence.
    """

    async def UpdateActivity(self, activity: dict[str, Any] | None) -> None:
        """Record the caller's current activity."""
        del activity

    async def UpdateStatus(self, status: dict[str, Any] | None) -> None:
        """Record the caller's current status."""
        del status

    async def BeginWatchingUserPresence(self) -> None:
        """Begin watching friend presence."""
        self._watching_presence = True

    async def EndWatchingUserPresence(self) -> None:
        """Stop watching friend presence."""
        self._watching_presence = False

    async def RefreshFriends(self) -> None:
        """Refresh friend presence (no-op; presence pushed via the bus)."""

    async def handle_bubble(self, bubble: RealtimeBubble) -> None:
        """Translate presence bubbles into metadata client callbacks."""
        if isinstance(bubble, PresenceUpdatedBubble):
            await self._caller(
                "UserPresenceUpdated",
                bubble.account_id,
                {
                    "activity": bubble.activity.action,
                    "userID": bubble.account_id,
                    "username": bubble.display_name,
                    "rulesetID": {"osu": 0, "taiko": 1, "fruits": 2, "mania": 3}.get(
                        bubble.activity.ruleset, 0
                    ),
                },
            )


class MultiplayerHub(PerfchoHub):
    """Expose lazer's multiplayer hub methods over the canonical service."""

    # -- room lifecycle ----------------------------------------------------

    async def CreateRoom(self, room: dict[str, Any]) -> dict[str, object]:
        """Create a room and return its projection."""
        services = self._services()
        if services is None or services.multiplayer is None or self.account_id is None:
            raise _unavailable()
        state = await services.multiplayer.create_room(
            CreateRoom(
                meta=self._meta("create"),
                settings=_settings_from_room(room),
                capacity=_max_participants(room) or 16,
                password=_password(room),
            )
        )
        return multiplayer_room(state)

    async def JoinRoomWithPassword(self, room_id: int, password: str) -> dict[str, object]:
        """Join a room with a password."""
        services = self._services()
        if services is None or services.multiplayer is None or self.account_id is None:
            raise _unavailable()
        state = await services.multiplayer.join_room(JoinRoom(self._meta("join"), room_id, password or ""))
        return multiplayer_room(state)

    async def LeaveRoom(self) -> None:
        """Leave the caller's current room."""
        services = self._services()
        if services is None or services.multiplayer is None or self.account_id is None:
            return
        current = await services.multiplayer.find_room_for_account(self.account_id)
        if current is not None:
            await services.multiplayer.leave_room(LeaveRoom(self._meta("leave"), current.room.public_id))

    async def TransferHost(self, user_id: int) -> None:
        """Transfer host authority to another participant."""
        services = self._services()
        state = await self._current_room(services)
        await services.multiplayer.change_host(
            _change_host_cmd(self._meta("transfer_host"), state, user_id)
        )

    async def KickUser(self, user_id: int) -> None:
        """Kick a participant."""
        services = self._services()
        state = await self._current_room(services)
        await services.multiplayer.kick_participant(
            KickParticipant(self._meta("kick"), state.room.public_id, state.state_revision, user_id)
        )

    async def ChangeSettings(self, settings: dict[str, Any]) -> None:
        """Apply new room settings."""
        services = self._services()
        state = await self._current_room(services)
        await services.multiplayer.update_settings(
            _update_settings_cmd(self._meta("settings"), state, settings)
        )

    async def ChangeState(self, new_state: str) -> None:
        """Change the caller's ready state."""
        services = self._services()
        state = await self._current_room(services)
        if self.account_id is None or state.slot_for(self.account_id) is None:
            return
        target = SlotStatus.READY if new_state in {"Ready", "Loaded", "ReadyForGameplay"} else SlotStatus.NOT_READY
        await services.multiplayer.set_slot_status(state.room.public_id, self.account_id, target)

    async def ChangeBeatmapAvailability(self, availability: dict[str, Any]) -> None:
        """No-op beatmap availability acknowledgement for now."""
        del availability

    async def ChangeUserStyle(self, beatmap_id: int | None, ruleset_id: int | None) -> None:
        """No-op user style change for now."""
        del beatmap_id, ruleset_id

    async def ChangeUserMods(self, new_mods: list[dict[str, Any]]) -> None:
        """Apply the caller's local mods."""
        services = self._services()
        state = await self._current_room(services)
        if self.account_id is None:
            return
        mods = tuple(CanonicalMod(str(m.get("acronym", "")), dict(m.get("settings", {}) or {})) for m in new_mods)
        await services.multiplayer.set_slot_mods(state.room.public_id, self.account_id, mods)

    async def SendMatchRequest(self, request: dict[str, Any]) -> None:
        """No-op match request for now."""
        del request

    async def StartMatch(self) -> None:
        """Start the current round."""
        services = self._services()
        state = await self._current_room(services)
        await services.multiplayer.start_round(
            _round_cmd(self._meta("start"), state)
        )

    async def AbortMatch(self) -> None:
        """Abort the current round."""
        services = self._services()
        state = await self._current_room(services)
        await services.multiplayer.complete_round(
            _complete_round_cmd(self._meta("abort"), state, aborted=True)
        )

    async def AbortGameplay(self) -> None:
        """No-op abort gameplay for now."""

    async def AddPlaylistItem(self, item: dict[str, Any]) -> None:
        """No-op playlist add (rooms currently single-beatmap)."""
        del item

    async def EditPlaylistItem(self, item: dict[str, Any]) -> None:
        """No-op playlist edit for now."""
        del item

    async def RemovePlaylistItem(self, playlist_item_id: int) -> None:
        """No-op playlist remove for now."""
        del playlist_item_id

    async def VoteToSkipIntro(self) -> None:
        """No-op skip-intro vote for now."""

    async def InvitePlayer(self, user_id: int) -> None:
        """Invite a player into the current room."""
        del user_id

    # -- helpers -----------------------------------------------------------

    async def _current_room(self, services: StableServices | None) -> RoomState:
        if services is None or services.multiplayer is None or self.account_id is None:
            raise _unavailable()
        state = await services.multiplayer.find_room_for_account(self.account_id)
        if state is None:
            raise _not_joined()
        return state

    def _meta(self, operation: str) -> CommandMeta:
        from datetime import UTC, datetime

        account_id = self.account_id or 0
        digest = hashlib.sha256(f"lazer-multiplayer:{operation}:{account_id}".encode()).digest()
        return CommandMeta(
            request_id=uuid.uuid7(),
            idempotency_key=f"lazer-multiplayer:{operation}:{account_id}:{uuid.uuid7()}",
            request_digest=digest,
            actor=Actor(account_id, uuid.uuid7()),
            client=ClientContext(family="lazer", version=None, variant=None, ip_address="127.0.0.1"),
            received_at=datetime.now(UTC),
        )

    async def handle_bubble(self, bubble: RealtimeBubble) -> None:
        """Translate a room bubble into lazer client callbacks."""
        if not isinstance(bubble, MultiplayerRoomBubble):
            return
        room = bubble.room
        await self._caller("RoomStateChanged", "Playing" if room.in_progress else "Open")
        await self._caller(
            "SettingsChanged",
            {
                "name": room.name,
                "playlistItemId": 0,
                "password": "*" if room.password_protected else "",
                "matchType": _match_type_name(room.team_mode),
                "queueMode": "host_only",
                "autoStartDuration": "00:00:00",
                "autoSkip": False,
                "maxParticipants": room.capacity,
            },
        )


def _change_host_cmd(meta: CommandMeta, state: RoomState, target_account_id: int) -> ChangeHost:
    from perfcho.modules.multiplayer import ChangeHost

    return ChangeHost(meta, state.room.public_id, state.state_revision, target_account_id)


def _update_settings_cmd(meta: CommandMeta, state: RoomState, settings: dict[str, Any]) -> UpdateRoomSettings:
    from perfcho.modules.multiplayer import UpdateRoomSettings

    current = state.room.settings
    next_settings = RoomSettings(
        name=str(settings.get("name", current.name) or current.name),
        beatmap_name=current.beatmap_name,
        external_beatmap_id=current.external_beatmap_id,
        beatmap_md5=current.beatmap_md5,
        ruleset=current.ruleset,
        variant=current.variant,
        team_mode=_MATCH_TYPES.get(str(settings.get("matchType")), current.team_mode),
        win_condition=current.win_condition,
        mods=current.mods,
        free_mods=current.free_mods,
        seed=current.seed,
    )
    return UpdateRoomSettings(meta, state.room.public_id, state.state_revision, next_settings)


def _round_cmd(meta: CommandMeta, state: RoomState) -> StartRound:
    from perfcho.modules.multiplayer import StartRound

    return StartRound(meta, state.room.public_id, state.state_revision)


def _complete_round_cmd(meta: CommandMeta, state: RoomState, *, aborted: bool) -> CompleteRound:
    from perfcho.modules.multiplayer import CompleteRound

    return CompleteRound(meta, state.room.public_id, state.state_revision, aborted)


def _match_type_name(team_mode: TeamMode) -> str:
    for name, value in _MATCH_TYPES.items():
        if value is team_mode:
            return name
    return "HeadToHead"


def _settings_from_room(room: dict[str, Any]) -> RoomSettings:
    settings = room.get("settings", {}) or {}
    playlist = room.get("playlist", []) or []
    first = playlist[0] if playlist else {}
    beatmap_id = int(first.get("beatmapID", -1) or -1)
    checksum = str(first.get("beatmapChecksum", "") or "")
    ruleset_id = int(first.get("rulesetID", 0) or 0)
    ruleset = _RULESET_IDS.get(ruleset_id, Ruleset.OSU)
    required = [
        CanonicalMod(str(m.get("acronym", "")), dict(m.get("settings", {}) or {}))
        for m in first.get("requiredMods", [])
    ]
    return RoomSettings(
        name=str(settings.get("name", "Unnamed room") or "Unnamed room"),
        beatmap_name="",
        external_beatmap_id=beatmap_id,
        beatmap_md5=bytes.fromhex(checksum) if checksum else None,
        ruleset=ruleset,
        variant=ScoreboardVariant.VANILLA,
        team_mode=_MATCH_TYPES.get(str(settings.get("matchType", "HeadToHead")), TeamMode.HEAD_TO_HEAD),
        win_condition=WinCondition.SCORE,
        mods=tuple(required),
        free_mods=True,
    )


def _max_participants(room: dict[str, Any]) -> int | None:
    settings = room.get("settings", {}) or {}
    value = settings.get("maxParticipants")
    return int(value) if value is not None else None


def _password(room: dict[str, Any]) -> str:
    settings = room.get("settings", {}) or {}
    return str(settings.get("password", "") or "")


def _unavailable() -> Exception:
    return RuntimeError("Multiplayer service is unavailable.")


def _not_joined() -> Exception:
    return RuntimeError("The caller is not in a room.")


def _server(hub_type: type[PerfchoHub], *, path: str) -> SignalRServer:
    options = ServerOptions(
        keep_alive_interval=15.0,
        client_timeout_interval=30.0,
        maximum_parallel_invocations=1,
        maximum_receive_message_size=1 << 20,
    )
    return SignalRServer(hub_type, options=options)


def build_signalr_apps() -> dict[str, object]:
    """Return ``mount_path -> ASGI app`` for the three lazer hubs."""
    return {
        _SPECTATOR_PATH: _server(SpectatorHub, path=_SPECTATOR_PATH).asgi_app(path=_SPECTATOR_PATH),
        _MULTIPLAYER_PATH: _server(MultiplayerHub, path=_MULTIPLAYER_PATH).asgi_app(path=_MULTIPLAYER_PATH),
        _METADATA_PATH: _server(MetadataHub, path=_METADATA_PATH).asgi_app(path=_METADATA_PATH),
    }


def register_signalr(asgi_app: FastAPI) -> None:
    """Mount all SignalR hub ASGI apps onto a FastAPI application."""
    for path, hub_app in build_signalr_apps().items():
        asgi_app.mount(path, hub_app)  # type: ignore[arg-type]


def _frame_bubble(host_account_id: int, data: dict[str, Any]) -> SpectatorFrameBubble:
    """Convert a lazer FrameDataBundle dict into a canonical frame bubble."""
    header = data.get("header", {}) or {}
    frames = data.get("frames", []) or []
    from perfcho.modules.realtime import CanonicalReplayFrame, CanonicalScoreFrame

    canonical_frames = tuple(
        CanonicalReplayFrame(
            timestamp_ms=int(frame.get("time", 0) or 0),
            position_x=float(frame.get("mouseX", 0) or 0),
            position_y=float(frame.get("mouseY", 0) or 0),
            input_state=int(frame.get("buttonState", 0) or 0),
            auxiliary_state=0,
        )
        for frame in frames
    )
    score = header.get("score", {}) or {}
    canonical_score = CanonicalScoreFrame(
        elapsed_ms=int(score.get("time", 0) or 0),
        frame_index=0,
        count_300=int(score.get("count300", 0) or 0),
        count_100=int(score.get("count100", 0) or 0),
        count_50=int(score.get("count50", 0) or 0),
        count_geki=0,
        count_katu=0,
        count_miss=int(score.get("countMiss", 0) or 0),
        total_score=int(score.get("totalScore", 0) or 0),
        max_combo=int(score.get("maxCombo", 0) or 0),
        current_combo=int(score.get("currentCombo", 0) or 0),
        perfect=bool(score.get("perfect", False)),
        health=int(score.get("health", 0) or 0),
        tag=0,
        score_v2=False,
    )
    return SpectatorFrameBubble(
        host_account_id,
        0,
        SpectatorFrameAction.UPDATE,
        canonical_frames,
        canonical_score,
        0,
    )


def _frame_data(frame: SpectatorFrameBubble | SpectatorFrame) -> dict[str, object]:
    """Project a canonical frame/bubble into the lazer FrameDataBundle shape."""
    score = frame.score
    return {
        "header": {
            "totalScore": score.total_score,
            "accuracy": 0.0,
            "combo": score.current_combo,
            "maxCombo": score.max_combo,
            "statistics": {
                "count300": score.count_300,
                "count100": score.count_100,
                "count50": score.count_50,
                "countMiss": score.count_miss,
            },
            "receivedTime": None,
            "mods": [],
        },
        "frames": [
            {
                "time": f.timestamp_ms,
                "mouseX": f.position_x,
                "mouseY": f.position_y,
                "buttonState": f.input_state,
            }
            for f in frame.frames
        ],
    }
