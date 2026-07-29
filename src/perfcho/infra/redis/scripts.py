"""Register bounded Lua programs used by the Redis realtime repository."""

from dataclasses import dataclass

from redis.asyncio import Redis
from redis.commands.core import AsyncScript

_NOW = """
local function now_ms()
    local now = redis.call('TIME')
    return tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
end
"""

_PRESENCE = """
local function unpack_u64(value, offset)
    if not value or #value < offset + 7 then
        return nil
    end
    local number = 0
    for index = offset, offset + 7 do
        number = number * 256 + string.byte(value, index)
    end
    return number
end
"""

_EXPIRING_INDEX = """
local function refresh_expiring_index(key)
    local latest = redis.call('ZREVRANGE', key, 0, 0, 'WITHSCORES')
    if #latest == 0 then
        redis.call('DEL', key)
    else
        redis.call('PEXPIREAT', key, latest[2])
    end
end
"""

_ORDERED = """
local function ordered_stats(key, at, maximum, remove_expired)
    local members = redis.call('ZRANGE', key, 0, maximum)
    if #members > maximum then
        return -1, 0, 0
    end
    local count = 0
    local bytes = 0
    local deadline = 0
    for _, member in ipairs(members) do
        local separator = string.find(member, ':', 21, true)
        if not separator then
            return -1, 0, 0
        end
        local expiry = tonumber(string.sub(member, 21, separator - 1))
        if not expiry then
            return -1, 0, 0
        end
        if expiry <= at and remove_expired then
            redis.call('ZREM', key, member)
        elseif expiry > at then
            count = count + 1
            bytes = bytes + #member - separator
            if expiry > deadline then
                deadline = expiry
            end
        end
    end
    return count, bytes, deadline
end

local function expire_ordered(data_key, bytes_key, count, bytes, deadline)
    if count == 0 then
        redis.call('DEL', data_key, bytes_key)
        return
    end
    redis.call('SET', bytes_key, tostring(bytes))
    redis.call('PEXPIREAT', data_key, deadline)
    redis.call('PEXPIREAT', bytes_key, deadline)
end
"""

OPEN_SESSION = (
    "-- perfcho:open-session:v1\n"
    + _NOW
    + _EXPIRING_INDEX
    + """
local expiry = tonumber(ARGV[2])
local now = now_ms()
if not expiry or expiry <= now or expiry - now > tonumber(ARGV[3]) then
    return {'INVALID_EXPIRY'}
end
local previous_account = redis.call('HGET', KEYS[1], 'account_id')
local previous = redis.call('HGET', KEYS[1], 'revision')
if previous == ARGV[4] then
    return {'REVISION_OVERFLOW'}
end
local revision = redis.call('HINCRBY', KEYS[1], 'revision', 1)
redis.call('HSET', KEYS[1], 'account_id', ARGV[1], 'expires_at', ARGV[2])
redis.call('PEXPIREAT', KEYS[1], expiry)
redis.call('DEL', KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[7])
redis.call('ZREM', KEYS[6], ARGV[1])
if previous_account and previous_account ~= ARGV[1] then
    redis.call('ZREM', KEYS[6], previous_account)
    redis.call('DEL',
        ARGV[5] .. previous_account,
        ARGV[6] .. previous_account .. ':frames',
        ARGV[6] .. previous_account .. ':frame-bytes',
        ARGV[6] .. previous_account .. ':frame-sequence',
        ARGV[7] .. previous_account
    )
end
refresh_expiring_index(KEYS[6])
return {'OK', ARGV[1], tostring(revision), ARGV[2]}
"""
)

RESOLVE_SESSION = """-- perfcho:resolve-session:v1
local account = redis.call('HGET', KEYS[1], 'account_id')
local revision = redis.call('HGET', KEYS[1], 'revision')
local expiry = redis.call('HGET', KEYS[1], 'expires_at')
if not account or not revision or not expiry or tonumber(expiry) <= tonumber(ARGV[1]) then
    return {'NOT_FOUND'}
end
return {'OK', account, revision, expiry}
"""

HEARTBEAT_SESSION = (
    "-- perfcho:heartbeat-session:v1\n"
    + _NOW
    + """
local account = redis.call('HGET', KEYS[1], 'account_id')
local revision = redis.call('HGET', KEYS[1], 'revision')
local current_expiry = redis.call('HGET', KEYS[1], 'expires_at')
local now = now_ms()
if not account or not revision or not current_expiry or tonumber(current_expiry) <= now then
    return {'NOT_FOUND'}
end
if revision ~= ARGV[1] then
    return {'FENCED'}
end
local expiry = tonumber(ARGV[2])
if not expiry or expiry < tonumber(current_expiry) or expiry <= now or expiry - now > tonumber(ARGV[3]) then
    return {'INVALID_EXPIRY'}
end
redis.call('HSET', KEYS[1], 'expires_at', ARGV[2])
redis.call('PEXPIREAT', KEYS[1], expiry)
return {'OK', account, revision, ARGV[2]}
"""
)

FENCE_SESSION = (
    "-- perfcho:fence-session:v1\n"
    + _NOW
    + _EXPIRING_INDEX
    + """
local account = redis.call('HGET', KEYS[1], 'account_id')
local revision = redis.call('HGET', KEYS[1], 'revision')
local expiry = redis.call('HGET', KEYS[1], 'expires_at')
if not account or not revision or not expiry or tonumber(expiry) <= now_ms() then
    return {'NOT_FOUND'}
end
if revision ~= ARGV[1] then
    return {'FENCED'}
end
local presence_key = ARGV[3] .. account
local presence = redis.call('GET', presence_key)
if presence and string.sub(presence, 9, 16) == ARGV[2] then
    redis.call('DEL', presence_key)
end
redis.call('DEL',
    KEYS[1],
    ARGV[4] .. account .. ':frames',
    ARGV[4] .. account .. ':frame-bytes',
    ARGV[4] .. account .. ':frame-sequence',
    ARGV[5] .. account
)
redis.call('ZREM', KEYS[2], account)
refresh_expiring_index(KEYS[2])
return {'OK'}
"""
)

SET_PRESENCE = (
    "-- perfcho:set-presence:v1\n"
    + _NOW
    + _EXPIRING_INDEX
    + """
local account = redis.call('HGET', KEYS[1], 'account_id')
local revision = redis.call('HGET', KEYS[1], 'revision')
local session_expiry = redis.call('HGET', KEYS[1], 'expires_at')
local now = now_ms()
if not account or not revision or not session_expiry or tonumber(session_expiry) <= now then
    return {'NOT_FOUND'}
end
if account ~= ARGV[1] or revision ~= ARGV[2] then
    return {'FENCED'}
end
local expiry = tonumber(ARGV[3])
if not expiry or expiry <= now or expiry > tonumber(session_expiry) or expiry - now > tonumber(ARGV[4]) then
    return {'INVALID_EXPIRY'}
end
redis.call('SET', KEYS[2], ARGV[5])
redis.call('PEXPIREAT', KEYS[2], expiry)
redis.call('ZADD', KEYS[3], expiry, account)
refresh_expiring_index(KEYS[3])
return {'OK'}
"""
)

CLEAR_PRESENCE = (
    "-- perfcho:clear-presence:v1\n"
    + _EXPIRING_INDEX
    + """
local presence = redis.call('GET', KEYS[1])
if not presence or #presence < 16 or string.sub(presence, 9, 16) ~= ARGV[1] then
    return {'STALE'}
end
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[2])
refresh_expiring_index(KEYS[2])
return {'OK'}
"""
)

SET_PREFERENCE = (
    "-- perfcho:set-preference:v1\n"
    + _NOW
    + """
local account = redis.call('HGET', KEYS[1], 'account_id')
local revision = redis.call('HGET', KEYS[1], 'revision')
local expiry = redis.call('HGET', KEYS[1], 'expires_at')
if not account or not revision or not expiry or tonumber(expiry) <= now_ms() then
    return {'NOT_FOUND'}
end
if account ~= ARGV[1] or revision ~= ARGV[2] then return {'FENCED'} end
redis.call('HSET', KEYS[2], ARGV[3], ARGV[4])
redis.call('PEXPIREAT', KEYS[2], expiry)
return {'OK'}
"""
)

_CHANNEL_EXPIRY = """
local function refresh_channel(members_key, epochs_key)
    local latest = redis.call('ZREVRANGE', members_key, 0, 0, 'WITHSCORES')
    if #latest == 0 then
        redis.call('DEL', members_key, epochs_key)
        return
    end
    redis.call('PEXPIREAT', members_key, latest[2])
    redis.call('PEXPIREAT', epochs_key, latest[2])
end
"""

JOIN_CHANNEL = (
    "-- perfcho:join-channel:v1\n"
    + _NOW
    + _CHANNEL_EXPIRY
    + """
local account = redis.call('HGET', KEYS[1], 'account_id')
local revision = redis.call('HGET', KEYS[1], 'revision')
local expiry = redis.call('HGET', KEYS[1], 'expires_at')
if not account or not revision or not expiry or tonumber(expiry) <= now_ms() then
    return {'NOT_FOUND'}
end
if revision ~= ARGV[1] then
    return {'FENCED'}
end
redis.call('ZADD', KEYS[2], expiry, account)
redis.call('HSET', KEYS[3], account, ARGV[2] .. '|' .. revision)
refresh_channel(KEYS[2], KEYS[3])
return {'OK'}
"""
)

LEAVE_CHANNEL = (
    "-- perfcho:leave-channel:v1\n"
    + _NOW
    + _CHANNEL_EXPIRY
    + """
local account = redis.call('HGET', KEYS[1], 'account_id')
local revision = redis.call('HGET', KEYS[1], 'revision')
local expiry = redis.call('HGET', KEYS[1], 'expires_at')
if not account or not revision or not expiry or tonumber(expiry) <= now_ms() then
    return {'NOT_FOUND'}
end
if revision ~= ARGV[1] then
    return {'FENCED'}
end
if redis.call('HGET', KEYS[3], account) == ARGV[2] .. '|' .. revision then
    redis.call('ZREM', KEYS[2], account)
    redis.call('HDEL', KEYS[3], account)
    refresh_channel(KEYS[2], KEYS[3])
end
return {'OK'}
"""
)

LIST_CHANNEL = (
    "-- perfcho:list-channel:v1\n"
    + _NOW
    + _CHANNEL_EXPIRY
    + """
local now = now_ms()
local accounts = redis.call('ZRANGE', KEYS[1], 0, -1, 'WITHSCORES')
local result = {'OK'}
for index = 1, #accounts, 2 do
    local account = accounts[index]
    local expiry = tonumber(accounts[index + 1])
    local epoch = redis.call('HGET', KEYS[2], account)
    local separator = epoch and string.find(epoch, '|', 1, true)
    local valid = expiry and expiry > now and separator
    if valid then
        local session_id = string.sub(epoch, 1, separator - 1)
        local expected_revision = string.sub(epoch, separator + 1)
        local session_key = ARGV[1] .. session_id
        valid = redis.call('HGET', session_key, 'account_id') == account
            and redis.call('HGET', session_key, 'revision') == expected_revision
            and tonumber(redis.call('HGET', session_key, 'expires_at') or '0') > now
    end
    if valid then
        result[#result + 1] = account
    else
        redis.call('ZREM', KEYS[1], account)
        redis.call('HDEL', KEYS[2], account)
    end
end
refresh_channel(KEYS[1], KEYS[2])
return result
"""
)

ENQUEUE_MAILBOX = (
    "-- perfcho:enqueue-mailbox:v1\n"
    + _NOW
    + _ORDERED
    + """
local now = now_ms()
local expiry = tonumber(ARGV[2])
if not expiry or expiry <= now or expiry - now > tonumber(ARGV[3]) then
    return {'INVALID_EXPIRY'}
end
local count, bytes, deadline = ordered_stats(KEYS[1], now, tonumber(ARGV[4]), true)
if count < 0 then
    return {'CORRUPT'}
end
expire_ordered(KEYS[1], KEYS[2], count, bytes, deadline)
if count >= tonumber(ARGV[4]) or bytes + #ARGV[1] > tonumber(ARGV[5]) then
    return {'OVERFLOW'}
end
local previous_deadline = redis.call('PEXPIRETIME', KEYS[3])
if redis.call('GET', KEYS[3]) == ARGV[6] then
    return {'SEQUENCE_OVERFLOW'}
end
redis.call('INCR', KEYS[3])
local sequence = redis.call('GET', KEYS[3])
local token = string.rep('0', 19 - #sequence) .. sequence
local member = token .. ':' .. ARGV[2] .. ':' .. ARGV[1]
redis.call('ZADD', KEYS[1], 0, member)
count = count + 1
bytes = bytes + #ARGV[1]
if expiry > deadline then
    deadline = expiry
end
expire_ordered(KEYS[1], KEYS[2], count, bytes, deadline)
if previous_deadline > deadline then
    deadline = previous_deadline
end
redis.call('PEXPIREAT', KEYS[3], deadline)
return {'OK', sequence}
"""
)

LEASE_MAILBOX = (
    "-- perfcho:lease-mailbox:v1\n"
    + _NOW
    + _ORDERED
    + """
local now = now_ms()
local expiry = tonumber(ARGV[3])
if not expiry or expiry <= now or expiry - now > tonumber(ARGV[4]) then
    return {'INVALID_EXPIRY'}
end
if redis.call('EXISTS', KEYS[3]) == 1 then
    return {'CONFLICT'}
end
local count, bytes, deadline = ordered_stats(KEYS[1], now, tonumber(ARGV[5]), true)
if count < 0 then
    return {'CORRUPT'}
end
expire_ordered(KEYS[1], KEYS[2], count, bytes, deadline)
local packets = redis.call('ZRANGE', KEYS[1], 0, tonumber(ARGV[2]) - 1)
local through = string.rep('0', 19)
if #packets > 0 then
    through = string.sub(packets[#packets], 1, 19)
end
redis.call('SET', KEYS[3], ARGV[1] .. '|' .. through)
redis.call('PEXPIREAT', KEYS[3], expiry)
local result = {'OK'}
for _, packet in ipairs(packets) do
    result[#result + 1] = packet
end
return result
"""
)

ACK_MAILBOX = (
    "-- perfcho:ack-mailbox:v1\n"
    + _ORDERED
    + """
local lease = redis.call('GET', KEYS[3])
local expected_prefix = ARGV[1] .. '|'
if not lease or string.sub(lease, 1, #expected_prefix) ~= expected_prefix then
    return {'CONFLICT'}
end
local leased_through = string.sub(lease, #expected_prefix + 1)
if ARGV[2] > leased_through then
    return {'INVALID_ACK'}
end
local members = redis.call('ZRANGE', KEYS[1], 0, tonumber(ARGV[3]))
if #members > tonumber(ARGV[3]) then
    return {'CORRUPT'}
end
for _, member in ipairs(members) do
    if string.sub(member, 1, 19) <= ARGV[2] then
        redis.call('ZREM', KEYS[1], member)
    end
end
local count, bytes, deadline = ordered_stats(KEYS[1], 0, tonumber(ARGV[3]), false)
expire_ordered(KEYS[1], KEYS[2], count, bytes, deadline)
redis.call('DEL', KEYS[3])
return {'OK'}
"""
)

RELEASE_MAILBOX = """-- perfcho:release-mailbox:v1
local lease = redis.call('GET', KEYS[1])
if not lease then
    return {'OK'}
end
local expected_prefix = ARGV[1] .. '|'
if string.sub(lease, 1, #expected_prefix) ~= expected_prefix then
    return {'CONFLICT'}
end
redis.call('DEL', KEYS[1])
return {'OK'}
"""

_VIEWER_EXPIRY = """
local function refresh_viewers(key)
    local latest = redis.call('ZREVRANGE', key, 0, 0, 'WITHSCORES')
    if #latest == 0 then
        redis.call('DEL', key)
    else
        redis.call('PEXPIREAT', key, latest[2])
    end
end
"""

ATTACH_SPECTATOR = (
    "-- perfcho:attach-spectator:v1\n"
    + _NOW
    + _PRESENCE
    + _VIEWER_EXPIRY
    + """
local now = now_ms()
local presence = redis.call('GET', KEYS[1])
local host_expiry = unpack_u64(presence, 17)
if not host_expiry or host_expiry <= now then
    return {'OFFLINE'}
end
local expiry = tonumber(ARGV[3])
if not expiry or expiry <= now or expiry > host_expiry or expiry - now > tonumber(ARGV[4]) then
    return {'INVALID_EXPIRY'}
end
local previous_host = redis.call('HGET', KEYS[2], 'host_account_id')
local previous_revision = redis.call('HGET', KEYS[2], 'revision')
if previous_revision == ARGV[5] then
    return {'REVISION_OVERFLOW'}
end
local revision = redis.call('HINCRBY', KEYS[2], 'revision', 1)
if previous_host and previous_host ~= ARGV[1] then
    local previous_viewers = ARGV[6] .. previous_host .. ':viewers'
    redis.call('ZREM', previous_viewers, ARGV[2])
    refresh_viewers(previous_viewers)
end
redis.call('HSET', KEYS[2], 'host_account_id', ARGV[1], 'expires_at', ARGV[3])
redis.call('PEXPIREAT', KEYS[2], expiry)
redis.call('ZADD', KEYS[3], expiry, ARGV[2])
refresh_viewers(KEYS[3])
return {'OK', tostring(revision)}
"""
)

DETACH_SPECTATOR = (
    "-- perfcho:detach-spectator:v1\n"
    + _VIEWER_EXPIRY
    + """
local host = redis.call('HGET', KEYS[1], 'host_account_id')
local revision = redis.call('HGET', KEYS[1], 'revision')
if host ~= ARGV[1] or revision ~= ARGV[3] then
    return {'STALE'}
end
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[2])
refresh_viewers(KEYS[2])
return {'OK'}
"""
)

PUBLISH_FRAME = (
    "-- perfcho:publish-frame:v1\n"
    + _NOW
    + _PRESENCE
    + _ORDERED
    + """
local now = now_ms()
local presence = redis.call('GET', KEYS[1])
local host_expiry = unpack_u64(presence, 17)
if not host_expiry or host_expiry <= now then
    return {'OFFLINE'}
end
local expiry = tonumber(ARGV[3])
if not expiry or expiry <= now or expiry > host_expiry or expiry - now > tonumber(ARGV[4]) then
    return {'INVALID_EXPIRY'}
end
local count, bytes, deadline = ordered_stats(KEYS[2], now, tonumber(ARGV[5]), true)
if count < 0 then
    return {'CORRUPT'}
end
expire_ordered(KEYS[2], KEYS[3], count, bytes, deadline)
if count >= tonumber(ARGV[5]) or bytes + #ARGV[2] > tonumber(ARGV[6]) then
    return {'OVERFLOW'}
end
local previous_sequence = redis.call('GET', KEYS[4])
if previous_sequence and ARGV[1] <= previous_sequence then
    return {'NON_MONOTONIC'}
end
local previous_deadline = redis.call('PEXPIRETIME', KEYS[4])
local member = ARGV[1] .. ':' .. ARGV[3] .. ':' .. ARGV[2]
redis.call('ZADD', KEYS[2], 0, member)
count = count + 1
bytes = bytes + #ARGV[2]
if expiry > deadline then
    deadline = expiry
end
expire_ordered(KEYS[2], KEYS[3], count, bytes, deadline)
redis.call('SET', KEYS[4], ARGV[1])
if previous_deadline > deadline then
    deadline = previous_deadline
end
redis.call('PEXPIREAT', KEYS[4], deadline)
return {'OK'}
"""
)

READ_FRAMES = """-- perfcho:read-frames:v1
local members = redis.call('ZRANGE', KEYS[1], 0, tonumber(ARGV[4]))
if #members > tonumber(ARGV[4]) then
    return {'CORRUPT'}
end
local result = {'OK'}
for _, member in ipairs(members) do
    local token = string.sub(member, 1, 19)
    local separator = string.find(member, ':', 21, true)
    local expiry = separator and tonumber(string.sub(member, 21, separator - 1))
    if token > ARGV[1] and expiry and expiry > tonumber(ARGV[3]) and #result <= tonumber(ARGV[2]) then
        result[#result + 1] = member
    end
end
return result
"""


@dataclass(frozen=True, slots=True)
class RealtimeScripts:
    """Hold Redis-registered scripts for all atomic realtime transitions."""

    open_session: AsyncScript
    resolve_session: AsyncScript
    heartbeat_session: AsyncScript
    fence_session: AsyncScript
    set_presence: AsyncScript
    clear_presence: AsyncScript
    set_preference: AsyncScript
    join_channel: AsyncScript
    leave_channel: AsyncScript
    list_channel: AsyncScript
    enqueue_mailbox: AsyncScript
    lease_mailbox: AsyncScript
    ack_mailbox: AsyncScript
    release_mailbox: AsyncScript
    attach_spectator: AsyncScript
    detach_spectator: AsyncScript
    publish_frame: AsyncScript
    read_frames: AsyncScript

    @classmethod
    def register(cls, redis: Redis) -> RealtimeScripts:
        """Register every script against the injected binary Redis client."""
        return cls(
            open_session=redis.register_script(OPEN_SESSION),
            resolve_session=redis.register_script(RESOLVE_SESSION),
            heartbeat_session=redis.register_script(HEARTBEAT_SESSION),
            fence_session=redis.register_script(FENCE_SESSION),
            set_presence=redis.register_script(SET_PRESENCE),
            clear_presence=redis.register_script(CLEAR_PRESENCE),
            set_preference=redis.register_script(SET_PREFERENCE),
            join_channel=redis.register_script(JOIN_CHANNEL),
            leave_channel=redis.register_script(LEAVE_CHANNEL),
            list_channel=redis.register_script(LIST_CHANNEL),
            enqueue_mailbox=redis.register_script(ENQUEUE_MAILBOX),
            lease_mailbox=redis.register_script(LEASE_MAILBOX),
            ack_mailbox=redis.register_script(ACK_MAILBOX),
            release_mailbox=redis.register_script(RELEASE_MAILBOX),
            attach_spectator=redis.register_script(ATTACH_SPECTATOR),
            detach_spectator=redis.register_script(DETACH_SPECTATOR),
            publish_frame=redis.register_script(PUBLISH_FRAME),
            read_frames=redis.register_script(READ_FRAMES),
        )
