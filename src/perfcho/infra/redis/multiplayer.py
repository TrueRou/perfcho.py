"""Implement expiring multiplayer projections with Redis compare-and-set writes."""

import json
import uuid
from collections.abc import Callable, Iterable
from contextlib import suppress
from dataclasses import replace
from datetime import datetime, timedelta

from redis.asyncio import Redis
from redis.commands.core import AsyncScript

from perfcho.modules.multiplayer import (
    MatchAlreadyJoined,
    MatchConcurrencyConflict,
    MatchFull,
    MatchNotFound,
    MatchPermissionDenied,
    MatchStateRejected,
    MultiplayerStateRepository,
    ProjectionStatus,
    RoomRecord,
    RoomSettings,
    RoomSlot,
    RoomState,
    SlotStatus,
    TeamMode,
    WinCondition,
)
from perfcho.modules.scoring.models import CanonicalMod, Ruleset, ScoreboardVariant

_CAS_SCRIPT = """-- perfcho:multiplayer-cas:v2
local current = redis.call('GET', KEYS[1])
local expected = tonumber(ARGV[1])
local expected_session = ARGV[2]
local incoming_session = ARGV[3]
local owner = ARGV[6]
if expected == -1 then
    if current then
        local decoded = cjson.decode(current)
        if decoded.session_id == incoming_session then return {'EXISTS'} end
        if decoded.session_id >= incoming_session then return {'EPOCH_FENCED'} end
        local old_owner = tostring(decoded.public_id) .. '|' .. decoded.session_id
        for _, slot in ipairs(decoded.slots) do
            if slot.account_id then
                local account_key = ARGV[8] .. tostring(slot.account_id)
                if redis.call('GET', account_key) == old_owner then redis.call('DEL', account_key) end
            end
        end
    end
else
    if not current then return {'NOT_FOUND'} end
    local decoded = cjson.decode(current)
    if decoded.session_id ~= expected_session or incoming_session ~= expected_session then return {'EPOCH_FENCED'} end
    if tonumber(decoded.state_revision) ~= expected then return {'FENCED'} end
end

local old_count = tonumber(ARGV[9])
local new_count = tonumber(ARGV[10])
for index = 1, new_count do
    local account_key = KEYS[2 + old_count + index]
    local occupied = redis.call('GET', account_key)
    if occupied and occupied ~= owner then
        local separator = string.find(occupied, '|', 1, true)
        local occupied_public = separator and string.sub(occupied, 1, separator - 1) or occupied
        local occupied_session = separator and string.sub(occupied, separator + 1) or occupied
        if occupied_public ~= tostring(ARGV[7]) or occupied_session >= incoming_session then
            return {'ACCOUNT_CONFLICT'}
        end
    end
end

for index = 1, old_count do
    local account_key = KEYS[2 + index]
    if redis.call('GET', account_key) == owner then redis.call('DEL', account_key) end
end
redis.call('SET', KEYS[1], ARGV[4], 'PXAT', ARGV[5])
redis.call('ZADD', KEYS[2], ARGV[5], ARGV[7])
for index = 1, new_count do
    redis.call('SET', KEYS[2 + old_count + index], owner, 'PXAT', ARGV[5])
end
local latest = redis.call('ZREVRANGE', KEYS[2], 0, 0, 'WITHSCORES')
if #latest > 0 then redis.call('PEXPIREAT', KEYS[2], latest[2]) end
return {'OK'}
"""

_REMOVE_SCRIPT = """-- perfcho:multiplayer-remove:v2
local current = redis.call('GET', KEYS[1])
if not current then
    return {'OK'}
end
local decoded = cjson.decode(current)
if decoded.session_id ~= ARGV[2] then return {'EPOCH_FENCED'} end
if tonumber(decoded.state_revision) ~= tonumber(ARGV[1]) then return {'FENCED'} end
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[3])
for index = 3, #KEYS do
    if redis.call('GET', KEYS[index]) == ARGV[4] then redis.call('DEL', KEYS[index]) end
end
local latest = redis.call('ZREVRANGE', KEYS[2], 0, 0, 'WITHSCORES')
if #latest > 0 then redis.call('PEXPIREAT', KEYS[2], latest[2]) end
return {'OK'}
"""


class RedisMultiplayerStateRepository(MultiplayerStateRepository):
    """Store bounded room blobs and account indexes behind atomic CAS scripts."""

    def __init__(
        self,
        redis: Redis,
        *,
        prefix: str,
        state_ttl: timedelta | int | float,
        max_rooms: int = 4096,
        cas_attempts: int = 8,
    ) -> None:
        """Bind a binary Redis client and hard room/CAS limits."""
        if not isinstance(redis, Redis):
            raise TypeError("redis must be a redis.asyncio.Redis instance")
        seconds = state_ttl.total_seconds() if isinstance(state_ttl, timedelta) else float(state_ttl)
        if seconds <= 0:
            raise ValueError("state_ttl must be positive")
        if not 1 <= max_rooms <= 32767:
            raise ValueError("max_rooms must be between 1 and 32767")
        if not 1 <= cas_attempts <= 32:
            raise ValueError("cas_attempts must be between 1 and 32")
        self._redis = redis
        self._base = f"{prefix.rstrip(':')}:v2:multiplayer"
        self._ttl = timedelta(seconds=seconds)
        self._max_rooms = max_rooms
        self._cas_attempts = cas_attempts
        self._cas: AsyncScript = redis.register_script(_CAS_SCRIPT)
        self._remove: AsyncScript = redis.register_script(_REMOVE_SCRIPT)

    async def create(self, state: RoomState) -> RoomState:
        """Publish a sanitized room projection and reserve its account indexes."""
        if (
            len(await self.list_public(at=datetime.now(state.expires_at.tzinfo), limit=self._max_rooms))
            >= self._max_rooms
        ):
            raise MatchFull("active room projection limit reached")
        expiry = min(state.expires_at, datetime.now(state.expires_at.tzinfo) + self._ttl)
        sanitized = replace(state, room=_public_room(state.room), expires_at=expiry)
        return await self._write(sanitized, expected=-1, previous_accounts=())

    async def get(self, public_id: int, *, at: datetime) -> RoomState | None:
        """Decode one live room state and prune expired state lazily."""
        raw = await self._redis.get(self._room_key(public_id))
        if raw is None:
            return None
        state = _decode_state(_bytes(raw))
        if state.room.public_id != public_id:
            raise RuntimeError("stored room state does not match its Redis key")
        if state.expires_at <= at:
            with suppress(MatchConcurrencyConflict):
                await self.remove(
                    public_id,
                    expected_state_revision=state.state_revision,
                    expected_session_id=state.room.session_id,
                )
            return None
        return state

    async def find_for_account(self, account_id: int, *, at: datetime) -> RoomState | None:
        """Resolve and validate an account-to-room index."""
        raw = await self._redis.get(self._account_key(account_id))
        if raw is None:
            return None
        try:
            public_id_text, session_id_text = _text(raw).split("|", 1)
            public_id = int(public_id_text)
            session_id = uuid.UUID(session_id_text)
        except (ValueError, TypeError) as error:
            raise RuntimeError("stored multiplayer account index is invalid") from error
        state = await self.get(public_id, at=at)
        if state is None or state.room.session_id != session_id or state.slot_for(account_id) is None:
            await self._redis.delete(self._account_key(account_id))
            return None
        return state

    async def list_public(self, *, at: datetime, limit: int) -> tuple[RoomState, ...]:
        """Return live public rooms after pruning expired index scores."""
        if not 1 <= limit <= self._max_rooms:
            raise ValueError("room list limit is outside the configured bound")
        at_ms = _milliseconds(at)
        index = self._rooms_key
        async with self._redis.pipeline(transaction=True) as pipeline:
            pipeline.zremrangebyscore(index, 0, at_ms)
            pipeline.zrangebyscore(index, at_ms + 1, "+inf", start=0, num=limit)
            result = await pipeline.execute()
        identifiers = tuple(int(_text(value)) for value in result[1])
        if not identifiers:
            return ()
        values = await self._redis.mget([self._room_key(identifier) for identifier in identifiers])
        states: list[RoomState] = []
        for identifier, value in zip(identifiers, values, strict=True):
            if value is None:
                await self._redis.zrem(index, identifier)
                continue
            state = _decode_state(_bytes(value))
            if state.expires_at > at:
                states.append(state)
        return tuple(states)

    async def replace(
        self,
        state: RoomState,
        *,
        expected_state_revision: int,
        expected_session_id: uuid.UUID,
    ) -> RoomState:
        """Replace a state and its account indexes at one expected revision."""
        current = await self.get(state.room.public_id, at=datetime.now(state.expires_at.tzinfo))
        if current is None:
            raise MatchNotFound("room has no live projection")
        return await self._write(
            state,
            expected=expected_state_revision,
            expected_session_id=expected_session_id,
            previous_accounts=_accounts(current),
        )

    async def remove(
        self,
        public_id: int,
        *,
        expected_state_revision: int,
        expected_session_id: uuid.UUID,
    ) -> None:
        """Atomically remove a room and all account indexes owned by it."""
        current_raw = await self._redis.get(self._room_key(public_id))
        accounts = _accounts(_decode_state(_bytes(current_raw))) if current_raw is not None else ()
        result = await self._remove(
            keys=[self._room_key(public_id), self._rooms_key, *(self._account_key(item) for item in accounts)],
            args=[
                expected_state_revision,
                str(expected_session_id),
                public_id,
                _owner(public_id, expected_session_id),
            ],
            client=self._redis,
        )
        status = _script_status(result)
        if status in {"FENCED", "EPOCH_FENCED"}:
            raise MatchConcurrencyConflict("room state revision changed")
        if status != "OK":
            raise RuntimeError(f"unexpected multiplayer remove status: {status}")

    async def join(self, room: RoomRecord, *, account_id: int, expires_at: datetime) -> RoomState:
        """Occupy the first open slot using bounded optimistic retries."""

        def mutation(current: RoomState) -> RoomState:
            if current.slot_for(account_id) is not None:
                return replace(current, room=_public_room(room))
            target = next((slot for slot in current.slots if slot.status is SlotStatus.OPEN), None)
            if target is None:
                raise MatchFull("room has no open slots")
            slots = tuple(
                RoomSlot(slot.position, SlotStatus.NOT_READY, account_id) if slot.position == target.position else slot
                for slot in current.slots
            )
            return replace(
                current,
                room=_public_room(room),
                state_revision=current.state_revision + 1,
                slots=slots,
                expires_at=min(expires_at, datetime.now(expires_at.tzinfo) + self._ttl),
            )

        return await self._mutate(room.public_id, mutation)

    async def leave(self, public_id: int, *, account_id: int, durable_room: RoomRecord | None) -> RoomState | None:
        """Release an account slot and apply durable host or room closure state."""
        current = await self.get(public_id, at=datetime.now().astimezone())
        if current is None:
            return None
        if durable_room is None:
            await self.remove(
                public_id,
                expected_state_revision=current.state_revision,
                expected_session_id=current.room.session_id,
            )
            return None

        def mutation(state: RoomState) -> RoomState:
            if state.slot_for(account_id) is None:
                return replace(state, room=_public_room(durable_room))
            slots = tuple(
                RoomSlot(slot.position, SlotStatus.OPEN) if slot.account_id == account_id else slot
                for slot in state.slots
            )
            return replace(
                state,
                room=_public_room(durable_room),
                state_revision=state.state_revision + 1,
                slots=slots,
            )

        return await self._mutate(public_id, mutation)

    async def move_slot(self, public_id: int, *, account_id: int, target_position: int) -> RoomState:
        """Move one participant to an open target slot."""

        def mutation(state: RoomState) -> RoomState:
            source = state.slot_for(account_id)
            if source is None:
                raise MatchNotFound("account does not occupy the room")
            if not 0 <= target_position < len(state.slots):
                raise MatchStateRejected("target slot is outside the room")
            target = state.slots[target_position]
            if target.status is not SlotStatus.OPEN:
                raise MatchStateRejected("target slot is not open")
            slots = tuple(
                replace(source, position=target_position)
                if slot.position == target_position
                else RoomSlot(source.position, SlotStatus.OPEN)
                if slot.position == source.position
                else slot
                for slot in state.slots
            )
            slots = tuple(sorted(slots, key=lambda slot: slot.position))
            return replace(state, state_revision=state.state_revision + 1, slots=slots)

        return await self._mutate(public_id, mutation)

    async def lock_slot(self, public_id: int, *, actor_account_id: int, position: int) -> RoomState:
        """Toggle an empty slot between open and locked."""

        def mutation(state: RoomState) -> RoomState:
            if state.room.host_account_id != actor_account_id:
                raise MatchPermissionDenied("only the host can lock slots")
            if not 0 <= position < len(state.slots):
                raise MatchStateRejected("slot is outside the room")
            slot = state.slots[position]
            if slot.account_id is not None:
                raise MatchStateRejected("occupied slots require a durable kick command")
            status = SlotStatus.LOCKED if slot.status is SlotStatus.OPEN else SlotStatus.OPEN
            slots = tuple(replace(slot, status=status) if item.position == position else item for item in state.slots)
            return replace(state, state_revision=state.state_revision + 1, slots=slots)

        return await self._mutate(public_id, mutation)

    async def set_slot_status(self, public_id: int, *, account_id: int, status: SlotStatus) -> RoomState:
        """Set one occupied slot readiness state."""
        if status in {SlotStatus.OPEN, SlotStatus.LOCKED}:
            raise MatchStateRejected("occupied slot cannot use an empty status")
        return await self._update_slot(
            public_id,
            account_id,
            lambda slot: replace(slot, status=status),
        )

    async def set_slot_team(self, public_id: int, *, account_id: int, team: int) -> RoomState:
        """Set one occupied slot team."""
        if team not in {0, 1, 2}:
            raise MatchStateRejected("team must be neutral, red, or blue")
        return await self._update_slot(public_id, account_id, lambda slot: replace(slot, team=team))

    async def set_slot_mods(
        self,
        public_id: int,
        *,
        account_id: int,
        mods: tuple[CanonicalMod, ...],
    ) -> RoomState:
        """Set one occupied slot free-mod selection."""
        return await self._update_slot(public_id, account_id, lambda slot: replace(slot, mods=mods))

    async def mark_loaded(self, public_id: int, *, account_id: int) -> RoomState:
        """Mark one playing participant loaded."""
        return await self._update_slot(
            public_id,
            account_id,
            lambda slot: replace(slot, loaded=True) if slot.status is SlotStatus.PLAYING else _reject_play_state(),
        )

    async def mark_skipped(self, public_id: int, *, account_id: int) -> RoomState:
        """Mark one playing participant skipped."""
        return await self._update_slot(
            public_id,
            account_id,
            lambda slot: replace(slot, skipped=True) if slot.status is SlotStatus.PLAYING else _reject_play_state(),
        )

    async def mark_failed(self, public_id: int, *, account_id: int) -> RoomState:
        """Mark one playing participant failed and complete its slot."""
        return await self._update_slot(
            public_id,
            account_id,
            lambda slot: (
                replace(slot, status=SlotStatus.COMPLETE, failed=True)
                if slot.status is SlotStatus.PLAYING
                else _reject_play_state()
            ),
        )

    async def _update_slot(
        self,
        public_id: int,
        account_id: int,
        update: Callable[[RoomSlot], RoomSlot],
    ) -> RoomState:
        def mutation(state: RoomState) -> RoomState:
            current = state.slot_for(account_id)
            if current is None:
                raise MatchNotFound("account does not occupy the room")
            replacement = update(current)
            slots = tuple(replacement if slot.position == current.position else slot for slot in state.slots)
            return replace(state, state_revision=state.state_revision + 1, slots=slots)

        return await self._mutate(public_id, mutation)

    async def _mutate(self, public_id: int, mutation: Callable[[RoomState], RoomState]) -> RoomState:
        for _ in range(self._cas_attempts):
            current = await self.get(public_id, at=datetime.now().astimezone())
            if current is None:
                raise MatchNotFound("room has no live projection")
            updated = mutation(current)
            try:
                return await self._write(
                    updated,
                    expected=current.state_revision,
                    expected_session_id=current.room.session_id,
                    previous_accounts=_accounts(current),
                )
            except MatchConcurrencyConflict:
                continue
        raise MatchConcurrencyConflict("room state changed too frequently")

    async def _write(
        self,
        state: RoomState,
        *,
        expected: int,
        expected_session_id: uuid.UUID | None = None,
        previous_accounts: Iterable[int],
    ) -> RoomState:
        old_accounts = tuple(previous_accounts)
        new_accounts = _accounts(state)
        sanitized = replace(
            state,
            room=_public_room(state.room),
            projection_status=ProjectionStatus.LIVE,
        )
        payload = _encode_state(sanitized)
        expected_epoch = expected_session_id or state.room.session_id
        owner = _owner(state.room.public_id, state.room.session_id)
        result = await self._cas(
            keys=[
                self._room_key(state.room.public_id),
                self._rooms_key,
                *(self._account_key(item) for item in old_accounts),
                *(self._account_key(item) for item in new_accounts),
            ],
            args=[
                expected,
                str(expected_epoch),
                str(state.room.session_id),
                payload,
                _milliseconds(state.expires_at),
                owner,
                state.room.public_id,
                f"{self._base}:account:",
                len(old_accounts),
                len(new_accounts),
            ],
            client=self._redis,
        )
        status = _script_status(result)
        if status in {"EXISTS", "FENCED", "EPOCH_FENCED"}:
            raise MatchConcurrencyConflict("room state revision changed")
        if status == "NOT_FOUND":
            raise MatchNotFound("room has no live projection")
        if status == "ACCOUNT_CONFLICT":
            raise MatchAlreadyJoined("an account already occupies another room")
        if status != "OK":
            raise RuntimeError(f"unexpected multiplayer CAS status: {status}")
        return _decode_state(payload)

    @property
    def _rooms_key(self) -> str:
        return f"{self._base}:rooms"

    def _room_key(self, public_id: int) -> str:
        return f"{self._base}:room:{public_id}"

    def _account_key(self, account_id: int) -> str:
        return f"{self._base}:account:{account_id}"


def _public_room(room: RoomRecord) -> RoomRecord:
    return replace(room, password_salt=None, password_verifier=None)


def _accounts(state: RoomState) -> tuple[int, ...]:
    return tuple(slot.account_id for slot in state.slots if slot.account_id is not None)


def _encode_state(state: RoomState) -> bytes:
    room = state.room
    settings = room.settings
    payload = {
        "room_id": str(room.room_id),
        "public_id": room.public_id,
        "session_id": str(room.session_id),
        "version": room.version,
        "creator_account_id": room.creator_account_id,
        "host_account_id": room.host_account_id,
        "capacity": room.capacity,
        "requires_password": room.requires_password,
        "settings": {
            "name": settings.name,
            "beatmap_name": settings.beatmap_name,
            "external_beatmap_id": settings.external_beatmap_id,
            "beatmap_md5": settings.beatmap_md5.hex() if settings.beatmap_md5 is not None else None,
            "ruleset": settings.ruleset.value,
            "variant": settings.variant.value,
            "team_mode": settings.team_mode.value,
            "win_condition": settings.win_condition.value,
            "mods": [mod.as_json() for mod in settings.mods],
            "free_mods": settings.free_mods,
            "seed": settings.seed,
        },
        "state_revision": state.state_revision,
        "slots": [
            {
                "position": slot.position,
                "status": slot.status.value,
                "account_id": slot.account_id,
                "team": slot.team,
                "mods": [mod.as_json() for mod in slot.mods],
                "loaded": slot.loaded,
                "skipped": slot.skipped,
                "failed": slot.failed,
            }
            for slot in state.slots
        ],
        "in_progress": state.in_progress,
        "round_id": str(state.round_id) if state.round_id is not None else None,
        "round_participant_account_ids": list(state.round_participant_account_ids),
        "expires_at": state.expires_at.isoformat(),
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _decode_state(payload: bytes) -> RoomState:
    try:
        value = json.loads(payload)
        settings_value = value["settings"]
        settings = RoomSettings(
            name=settings_value["name"],
            beatmap_name=settings_value["beatmap_name"],
            external_beatmap_id=settings_value["external_beatmap_id"],
            beatmap_md5=(bytes.fromhex(settings_value["beatmap_md5"]) if settings_value["beatmap_md5"] else None),
            ruleset=Ruleset(settings_value["ruleset"]),
            variant=ScoreboardVariant(settings_value["variant"]),
            team_mode=TeamMode(settings_value["team_mode"]),
            win_condition=WinCondition(settings_value["win_condition"]),
            mods=_decode_mods(settings_value["mods"]),
            free_mods=settings_value["free_mods"],
            seed=settings_value["seed"],
        )
        room = RoomRecord(
            room_id=uuid.UUID(value["room_id"]),
            public_id=value["public_id"],
            session_id=uuid.UUID(value["session_id"]),
            version=value["version"],
            creator_account_id=value["creator_account_id"],
            host_account_id=value["host_account_id"],
            capacity=value["capacity"],
            settings=settings,
            requires_password=value["requires_password"],
        )
        slots = tuple(
            RoomSlot(
                position=item["position"],
                status=SlotStatus(item["status"]),
                account_id=item["account_id"],
                team=item["team"],
                mods=_decode_mods(item["mods"]),
                loaded=item["loaded"],
                skipped=item["skipped"],
                failed=item["failed"],
            )
            for item in value["slots"]
        )
        return RoomState(
            room=room,
            state_revision=value["state_revision"],
            slots=slots,
            in_progress=value["in_progress"],
            round_id=uuid.UUID(value["round_id"]) if value["round_id"] else None,
            round_participant_account_ids=tuple(value["round_participant_account_ids"]),
            projection_status=ProjectionStatus.LIVE,
            expires_at=datetime.fromisoformat(value["expires_at"]),
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise RuntimeError("stored multiplayer projection is invalid") from error


def _decode_mods(values: object) -> tuple[CanonicalMod, ...]:
    if not isinstance(values, list):
        raise ValueError("mod list is invalid")
    mods: list[CanonicalMod] = []
    for value in values:
        if not isinstance(value, dict) or not isinstance(value.get("acronym"), str):
            raise ValueError("mod entry is invalid")
        settings = value.get("settings", {})
        if not isinstance(settings, dict):
            raise ValueError("mod settings are invalid")
        mods.append(CanonicalMod(value["acronym"], settings))
    return tuple(mods)


def _milliseconds(value: datetime) -> int:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return int(value.timestamp() * 1000)


def _owner(public_id: int, session_id: uuid.UUID) -> str:
    return f"{public_id}|{session_id}"


def _bytes(value: object) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray | memoryview):
        return bytes(value)
    raise RuntimeError("Redis must use decode_responses=False")


def _text(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("ascii")
    if isinstance(value, str):
        return value
    if isinstance(value, int):
        return str(value)
    raise RuntimeError("Redis returned an invalid scalar")


def _script_status(value: object) -> str:
    if not isinstance(value, list | tuple) or not value:
        raise RuntimeError("Redis script returned an invalid result")
    return _text(value[0])


def _reject_play_state() -> RoomSlot:
    raise MatchStateRejected("participant is not currently playing")
