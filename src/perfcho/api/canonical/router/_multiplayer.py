"""Serialize canonical room projections into the osu!lazer MultiplayerRoom shape.

lazer rooms are playlist-oriented (participant list + playlist items) while the
canonical projection is slot-oriented. This module projects the shared canonical
:class:`~perfcho.modules.multiplayer.RoomState` into the lazer wire shape, so a
single room is simultaneously addressable by stable (16-slot) and lazer
(participant list) clients.
"""

from perfcho.modules.multiplayer import RoomSlot, RoomState, SlotStatus, TeamMode, WinCondition
from perfcho.modules.scoring import Ruleset

_RULESET_IDS = {Ruleset.OSU: 0, Ruleset.TAIKO: 1, Ruleset.FRUITS: 2, Ruleset.MANIA: 3}
_MATCH_TYPES = {
    TeamMode.HEAD_TO_HEAD: "head_to_head",
    TeamMode.TEAM_VS: "team_vs",
    TeamMode.TAG_COOP: "tag_coop",
    TeamMode.TAG_TEAM_VS: "tag_team_vs",
}
_WIN_CONDITIONS = {
    WinCondition.SCORE: "score",
    WinCondition.ACCURACY: "accuracy",
    WinCondition.COMBO: "combo",
    WinCondition.SCORE_V2: "score_v2",
}


def room_state(state: RoomState) -> str:
    """Return the lazer MultiplayerRoomState name for a projection."""
    if state.in_progress:
        return "Playing"
    return "Open"


def room_settings(state: RoomState) -> dict[str, object]:
    """Return the lazer MultiplayerRoomSettings projection."""
    settings = state.room.settings
    return {
        "name": settings.name,
        "playlistItemId": 0,
        "password": "*" if state.room.password_protected else "",
        "matchType": _MATCH_TYPES.get(settings.team_mode, "head_to_head"),
        "queueMode": "host_only",
        "autoStartDuration": "00:00:00",
        "autoSkip": False,
        "maxParticipants": state.room.capacity,
    }


def room_users(state: RoomState) -> list[dict[str, object]]:
    """Return the lazer MultiplayerRoomUser list from occupied slots."""
    users: list[dict[str, object]] = []
    for slot in state.slots:
        if slot.account_id is None:
            continue
        users.append(
            {
                "userID": slot.account_id,
                "state": _user_state(slot),
                "beatmapAvailability": {"state": "Downloading", "downloadProgress": 0.0},
                "mods": [{"acronym": mod.acronym, "settings": dict(mod.settings)} for mod in slot.mods],
                "matchState": None,
                "rulesetId": None,
                "beatmapId": None,
                "votedToSkipIntro": False,
                "role": "Host" if slot.account_id == state.room.host_account_id else "Participant",
                "user": {"id": slot.account_id, "username": ""},
            }
        )
    return users


def playlist(state: RoomState) -> list[dict[str, object]]:
    """Return the lazer playlist, reduced to the room's active beatmap."""
    settings = state.room.settings
    return [
        {
            "id": 1,
            "ownerID": state.room.host_account_id,
            "beatmapID": settings.external_beatmap_id,
            "beatmapChecksum": settings.beatmap_md5.hex() if settings.beatmap_md5 else "",
            "rulesetID": _RULESET_IDS.get(settings.ruleset, 0),
            "requiredMods": [{"acronym": mod.acronym, "settings": dict(mod.settings)} for mod in settings.mods],
            "allowedMods": [],
            "expired": False,
            "playlistOrder": 0,
            "playedAt": None,
            "starRating": 0.0,
            "freestyle": settings.free_mods,
        }
    ]


def multiplayer_room(state: RoomState) -> dict[str, object]:
    """Return the complete lazer MultiplayerRoom wire projection."""
    users = room_users(state)
    host = next((user for user in users if user["role"] == "Host"), None)
    return {
        "roomID": state.room.public_id,
        "state": room_state(state),
        "settings": room_settings(state),
        "users": users,
        "host": host,
        "matchState": None,
        "playlist": playlist(state),
        "activeCountdowns": [],
        "channelID": 0,
    }


def _user_state(slot: RoomSlot) -> str:
    status = slot.status
    if status is SlotStatus.PLAYING:
        return "Playing"
    if status is SlotStatus.READY:
        return "Ready"
    if status is SlotStatus.NO_BEATMAP:
        return "Idle"
    if status is SlotStatus.COMPLETE:
        return "FinishedPlay"
    return "Idle"
