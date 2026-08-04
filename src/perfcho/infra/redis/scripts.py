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

_CHANNEL_EXPIRY = """
local function refresh_channel(members_key, epochs_key)
    local latest = redis.call('ZREVRANGE', members_key, 0, 0, 'WITHSCORES')
    if #latest == 0 then
        redis.call('DEL', members_key, epochs_key)
    else
        redis.call('PEXPIREAT', members_key, latest[2])
        redis.call('PEXPIREAT', epochs_key, latest[2])
    end
end
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

_SESSION = """
local function session_status(session_key, epoch_key, account, session_id, revision, now)
    local expected_epoch = session_id .. '|' .. revision
    local current_epoch = redis.call('GET', epoch_key)
    if current_epoch and current_epoch ~= expected_epoch then
        return 'FENCED', nil, nil
    end
    local stored_account = redis.call('HGET', session_key, 'account_id')
    local stored_revision = redis.call('HGET', session_key, 'revision')
    local expiry = redis.call('HGET', session_key, 'expires_at')
    local durable_expiry = redis.call('HGET', session_key, 'durable_expires_at')
    if not stored_account or not stored_revision or not expiry or not durable_expiry
        or tonumber(expiry) <= now then
        return 'NOT_FOUND', nil, nil
    end
    if stored_account ~= account or stored_revision ~= revision
        or current_epoch ~= expected_epoch then
        return 'FENCED', nil, nil
    end
    return 'OK', tonumber(expiry), tonumber(durable_expiry)
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
            if expiry > deadline then deadline = expiry end
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

_FRAMES = """
local function frame_parts(member)
    local second = string.find(member, ':', 21, true)
    local third = second and string.find(member, ':', second + 1, true)
    if not second or not third then return nil, nil, nil end
    local expiry = tonumber(string.sub(member, 21, second - 1))
    local sequence = tonumber(string.sub(member, second + 1, third - 1))
    if not expiry or not sequence then return nil, nil, nil end
    return expiry, sequence, #member - third
end

local function frame_stats(key, at, maximum)
    local members = redis.call('ZRANGE', key, 0, maximum)
    if #members > maximum then return -1, 0, 0 end
    local count = 0
    local bytes = 0
    local deadline = 0
    for _, member in ipairs(members) do
        local expiry, _, payload_bytes = frame_parts(member)
        if not expiry then return -1, 0, 0 end
        if expiry <= at then
            redis.call('ZREM', key, member)
        else
            count = count + 1
            bytes = bytes + payload_bytes
            if expiry > deadline then deadline = expiry end
        end
    end
    return count, bytes, deadline
end

local function expire_frames(data_key, bytes_key, count, bytes, deadline)
    if count == 0 then
        redis.call('DEL', data_key, bytes_key)
        return
    end
    redis.call('SET', bytes_key, tostring(bytes))
    redis.call('PEXPIREAT', data_key, deadline)
    redis.call('PEXPIREAT', bytes_key, deadline)
end

local function latest_frame_window(key, limit)
    local all = redis.call('ZRANGE', key, 0, -1)
    local oldest = ''
    local latest = ''
    if #all > 0 then
        oldest = string.sub(all[1], 1, 19)
        latest = string.sub(all[#all], 1, 19)
    end
    local first = math.max(1, #all - limit + 1)
    local result = {oldest, latest, #all > limit and '1' or '0'}
    for index = first, #all do result[#result + 1] = all[index] end
    return result
end
"""

OPEN_SESSION = (
    "-- perfcho:open-session:v2\n"
    + _NOW
    + _EXPIRING_INDEX
    + _CHANNEL_EXPIRY
    + _VIEWER_EXPIRY
    + """
local now = now_ms()
local requested_expiry = tonumber(ARGV[3])
local durable_expiry = tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])
if not requested_expiry or not durable_expiry or not ttl or ttl <= 0
    or requested_expiry <= now or requested_expiry > durable_expiry then
    return {'INVALID_EXPIRY'}
end
-- Redis owns ephemeral TTLs. Clamp the application deadline to Redis time so
-- small wall-clock skew cannot reject an otherwise valid login.
local expiry = math.min(requested_expiry, durable_expiry, now + ttl)
if expiry <= now then return {'INVALID_EXPIRY'} end
if redis.call('GET', KEYS[17]) == ARGV[6] then return {'REVISION_OVERFLOW'} end
local previous_account = redis.call('HGET', KEYS[1], 'account_id')
if previous_account and previous_account ~= ARGV[1] then return {'FENCED'} end
local old_epoch = redis.call('GET', KEYS[2])
if old_epoch then
    local separator = string.find(old_epoch, '|', 1, true)
    local old_session_id = separator and string.sub(old_epoch, 1, separator - 1)
    local old_revision = separator and string.sub(old_epoch, separator + 1)
    if old_session_id and old_revision then
        redis.call('DEL', ARGV[12] .. ARGV[1] .. ':signal:' .. old_session_id .. ':' .. old_revision)
        local old_channels = ARGV[8] .. old_session_id .. ':channels'
        for _, channel_id in ipairs(redis.call('SMEMBERS', old_channels)) do
            local members = ARGV[9] .. channel_id .. ':members'
            local epochs = ARGV[9] .. channel_id .. ':epochs'
            if redis.call('HGET', epochs, ARGV[1]) == old_epoch then
                redis.call('ZREM', members, ARGV[1])
                redis.call('HDEL', epochs, ARGV[1])
                refresh_channel(members, epochs)
            end
        end
        redis.call('DEL', old_channels)
        local old_relation = redis.call('HGETALL', KEYS[11])
        local relation_values = {}
        for index = 1, #old_relation, 2 do relation_values[old_relation[index]] = old_relation[index + 1] end
        if relation_values['spectator_session_id'] == old_session_id
            and relation_values['spectator_revision'] == old_revision then
            local old_viewers = ARGV[11] .. relation_values['host_account_id'] .. ':viewers'
            redis.call('ZREM', old_viewers, ARGV[1])
            refresh_viewers(old_viewers)
            redis.call('DEL', KEYS[11])
        end
        for _, spectator in ipairs(redis.call('ZRANGE', KEYS[13], 0, -1)) do
            local relation_key = ARGV[10] .. spectator .. ':host'
            if redis.call('HGET', relation_key, 'host_session_id') == old_session_id
                and redis.call('HGET', relation_key, 'host_revision') == old_revision then
                redis.call('DEL', relation_key)
            end
        end
        redis.call('DEL', KEYS[13])
        if old_session_id ~= ARGV[2] then redis.call('DEL', ARGV[7] .. old_session_id) end
    end
end
redis.call('DEL', KEYS[3], KEYS[5], KEYS[6], KEYS[7], KEYS[8], KEYS[9], KEYS[10],
    KEYS[11], KEYS[12], KEYS[13], KEYS[14], KEYS[15], KEYS[16])
redis.call('ZREM', KEYS[4], ARGV[1])
local revision = redis.call('INCR', KEYS[17])
redis.call('PEXPIREAT', KEYS[17], durable_expiry)
redis.call('HSET', KEYS[1], 'account_id', ARGV[1], 'revision', revision, 'expires_at', expiry,
    'durable_expires_at', ARGV[4])
redis.call('PEXPIREAT', KEYS[1], expiry)
redis.call('SET', KEYS[2], ARGV[2] .. '|' .. revision)
redis.call('PEXPIREAT', KEYS[2], expiry)
refresh_expiring_index(KEYS[4])
return {'OK', ARGV[1], tostring(revision), tostring(expiry)}
"""
)

RESOLVE_SESSION = """-- perfcho:resolve-session:v2
local account = redis.call('HGET', KEYS[1], 'account_id')
local revision = redis.call('HGET', KEYS[1], 'revision')
local expiry = redis.call('HGET', KEYS[1], 'expires_at')
if not account or not revision or not expiry or tonumber(expiry) <= tonumber(ARGV[2]) then
    return {'NOT_FOUND'}
end
if redis.call('GET', ARGV[3] .. account .. ':session') ~= ARGV[1] .. '|' .. revision then
    return {'FENCED'}
end
return {'OK', account, revision, expiry}
"""

HEARTBEAT_SESSION = (
    "-- perfcho:heartbeat-session:v2\n"
    + _NOW
    + _EXPIRING_INDEX
    + _CHANNEL_EXPIRY
    + _VIEWER_EXPIRY
    + _SESSION
    + """
local now = now_ms()
local status, current_expiry, durable_expiry = session_status(
    KEYS[1], KEYS[2], ARGV[1], ARGV[2], ARGV[3], now)
if status ~= 'OK' then return {status} end
local requested_expiry = tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])
if not requested_expiry or not ttl or ttl <= 0 or requested_expiry <= now then
    return {'INVALID_EXPIRY'}
end
-- The API and Redis clocks may differ slightly. Redis owns ephemeral TTLs, so
-- clamp the requested wall-clock deadline instead of rejecting normal skew.
local expiry = math.max(current_expiry, math.min(requested_expiry, durable_expiry, now + ttl))
redis.call('HSET', KEYS[1], 'expires_at', tostring(expiry))
redis.call('PEXPIREAT', KEYS[1], expiry)
redis.call('PEXPIREAT', KEYS[2], expiry)
local presence_expiry = math.min(expiry, now + tonumber(ARGV[6]))
if redis.call('HGET', KEYS[3], 'session_id') == ARGV[2]
    and redis.call('HGET', KEYS[3], 'revision') == ARGV[3] then
    redis.call('HSET', KEYS[3], 'expires_at', tostring(presence_expiry))
    redis.call('PEXPIREAT', KEYS[3], presence_expiry)
    redis.call('ZADD', KEYS[4], presence_expiry, ARGV[1])
    refresh_expiring_index(KEYS[4])
end
if redis.call('HGET', KEYS[5], 'session_id') == ARGV[2]
    and redis.call('HGET', KEYS[5], 'revision') == ARGV[3] then
    redis.call('PEXPIREAT', KEYS[5], expiry)
end
local epoch = ARGV[2] .. '|' .. ARGV[3]
for _, channel_id in ipairs(redis.call('SMEMBERS', KEYS[6])) do
    local members = ARGV[7] .. channel_id .. ':members'
    local epochs = ARGV[7] .. channel_id .. ':epochs'
    if redis.call('HGET', epochs, ARGV[1]) == epoch then
        redis.call('ZADD', members, expiry, ARGV[1])
        refresh_channel(members, epochs)
    else
        redis.call('SREM', KEYS[6], channel_id)
    end
end
if redis.call('SCARD', KEYS[6]) > 0 then redis.call('PEXPIREAT', KEYS[6], expiry) end
local relation_host = redis.call('HGET', KEYS[7], 'host_account_id')
if relation_host and redis.call('HGET', KEYS[7], 'spectator_session_id') == ARGV[2]
    and redis.call('HGET', KEYS[7], 'spectator_revision') == ARGV[3] then
    local host_session_id = redis.call('HGET', KEYS[7], 'host_session_id')
    local host_revision = redis.call('HGET', KEYS[7], 'host_revision')
    local host_session = ARGV[10] .. host_session_id
    local host_expiry = redis.call('HGET', host_session, 'expires_at')
    local host_current = redis.call('GET', ARGV[9] .. relation_host .. ':session')
    if host_expiry and tonumber(host_expiry) > now
        and host_current == host_session_id .. '|' .. host_revision then
        local relation_expiry = math.min(expiry, tonumber(host_expiry))
        redis.call('HSET', KEYS[7], 'expires_at', tostring(relation_expiry))
        redis.call('PEXPIREAT', KEYS[7], relation_expiry)
        local host_viewers = ARGV[8] .. relation_host .. ':viewers'
        redis.call('ZADD', host_viewers, relation_expiry, ARGV[1])
        refresh_viewers(host_viewers)
    else
        redis.call('DEL', KEYS[7])
    end
end
for _, spectator in ipairs(redis.call('ZRANGE', KEYS[8], 0, -1)) do
    local relation_key = ARGV[11] .. spectator .. ':host'
    local valid = redis.call('HGET', relation_key, 'host_session_id') == ARGV[2]
        and redis.call('HGET', relation_key, 'host_revision') == ARGV[3]
    local spectator_session_id = valid and redis.call('HGET', relation_key, 'spectator_session_id')
    local spectator_revision = valid and redis.call('HGET', relation_key, 'spectator_revision')
    local spectator_expiry = valid and redis.call('HGET', ARGV[10] .. spectator_session_id, 'expires_at')
    local spectator_current = valid and redis.call('GET', ARGV[9] .. spectator .. ':session')
    valid = spectator_expiry and tonumber(spectator_expiry) > now
        and spectator_current == spectator_session_id .. '|' .. spectator_revision
    if valid then
        local relation_expiry = math.min(expiry, tonumber(spectator_expiry))
        redis.call('HSET', relation_key, 'expires_at', tostring(relation_expiry))
        redis.call('PEXPIREAT', relation_key, relation_expiry)
        redis.call('ZADD', KEYS[8], relation_expiry, spectator)
    else
        if redis.call('HGET', relation_key, 'host_session_id') == ARGV[2]
            and redis.call('HGET', relation_key, 'host_revision') == ARGV[3] then
            redis.call('DEL', relation_key)
        end
        redis.call('ZREM', KEYS[8], spectator)
    end
end
refresh_viewers(KEYS[8])
return {'OK', ARGV[1], ARGV[3], tostring(expiry)}
"""
)

FENCE_SESSION = (
    "-- perfcho:fence-session:v2\n"
    + _NOW
    + _EXPIRING_INDEX
    + _CHANNEL_EXPIRY
    + _VIEWER_EXPIRY
    + _SESSION
    + """
local now = now_ms()
local status = session_status(KEYS[1], KEYS[2], ARGV[1], ARGV[2], ARGV[3], now)
if status ~= 'OK' then return {status} end
local epoch = ARGV[2] .. '|' .. ARGV[3]
redis.call('DEL', ARGV[7] .. ARGV[1] .. ':signal:' .. ARGV[2] .. ':' .. ARGV[3])
for _, channel_id in ipairs(redis.call('SMEMBERS', KEYS[6])) do
    local members = ARGV[4] .. channel_id .. ':members'
    local epochs = ARGV[4] .. channel_id .. ':epochs'
    if redis.call('HGET', epochs, ARGV[1]) == epoch then
        redis.call('ZREM', members, ARGV[1])
        redis.call('HDEL', epochs, ARGV[1])
        refresh_channel(members, epochs)
    end
end
local relation_host = redis.call('HGET', KEYS[11], 'host_account_id')
if relation_host and redis.call('HGET', KEYS[11], 'spectator_session_id') == ARGV[2]
    and redis.call('HGET', KEYS[11], 'spectator_revision') == ARGV[3] then
    local host_viewers = ARGV[6] .. relation_host .. ':viewers'
    redis.call('ZREM', host_viewers, ARGV[1])
    refresh_viewers(host_viewers)
    redis.call('DEL', KEYS[11])
end
for _, spectator in ipairs(redis.call('ZRANGE', KEYS[13], 0, -1)) do
    local relation_key = ARGV[5] .. spectator .. ':host'
    if redis.call('HGET', relation_key, 'host_session_id') == ARGV[2]
        and redis.call('HGET', relation_key, 'host_revision') == ARGV[3] then
        redis.call('DEL', relation_key)
    end
end
redis.call('DEL', KEYS[1], KEYS[2], KEYS[3], KEYS[5], KEYS[6], KEYS[7], KEYS[8],
    KEYS[9], KEYS[10], KEYS[12], KEYS[13], KEYS[14], KEYS[15], KEYS[16])
redis.call('ZREM', KEYS[4], ARGV[1])
refresh_expiring_index(KEYS[4])
return {'OK'}
"""
)

SET_PRESENCE = (
    "-- perfcho:set-presence:v2\n"
    + _NOW
    + _EXPIRING_INDEX
    + _SESSION
    + """
local now = now_ms()
local status, session_expiry = session_status(
    KEYS[1], KEYS[2], ARGV[1], ARGV[2], ARGV[3], now)
if status ~= 'OK' then return {status} end
local expiry = tonumber(ARGV[4])
if not expiry or expiry <= now or expiry > session_expiry
    or expiry - now > tonumber(ARGV[5]) then
    return {'INVALID_EXPIRY'}
end
local capacity = tonumber(ARGV[7])
if not capacity or capacity < 0 then return {'INVALID_CAPACITY'} end
if capacity > 0 then
    redis.call('ZREMRANGEBYSCORE', KEYS[4], 0, now)
    local already_indexed = redis.call('ZSCORE', KEYS[4], ARGV[1])
    if not already_indexed and redis.call('ZCARD', KEYS[4]) >= capacity then
        refresh_expiring_index(KEYS[4])
        return {'CAPACITY'}
    end
end
redis.call('HSET', KEYS[3], 'account_id', ARGV[1], 'session_id', ARGV[2],
    'revision', ARGV[3], 'expires_at', ARGV[4], 'payload', ARGV[6])
redis.call('PEXPIREAT', KEYS[3], expiry)
redis.call('ZADD', KEYS[4], expiry, ARGV[1])
refresh_expiring_index(KEYS[4])
return {'OK'}
"""
)

CLEAR_PRESENCE = (
    "-- perfcho:clear-presence:v2\n"
    + _EXPIRING_INDEX
    + """
if redis.call('HGET', KEYS[1], 'session_id') ~= ARGV[2]
    or redis.call('HGET', KEYS[1], 'revision') ~= ARGV[3] then
    return {'STALE'}
end
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
refresh_expiring_index(KEYS[2])
return {'OK'}
"""
)

SET_PREFERENCE = (
    "-- perfcho:set-preference:v2\n"
    + _NOW
    + _SESSION
    + """
local status, expiry = session_status(
    KEYS[1], KEYS[2], ARGV[1], ARGV[2], ARGV[3], now_ms())
if status ~= 'OK' then return {status} end
redis.call('HSET', KEYS[3], 'session_id', ARGV[2], 'revision', ARGV[3], ARGV[4], ARGV[5])
redis.call('PEXPIREAT', KEYS[3], expiry)
return {'OK'}
"""
)

JOIN_CHANNEL = (
    "-- perfcho:join-channel:v2\n"
    + _NOW
    + _CHANNEL_EXPIRY
    + _SESSION
    + """
local status, expiry = session_status(
    KEYS[1], KEYS[2], ARGV[1], ARGV[2], ARGV[3], now_ms())
if status ~= 'OK' then return {status} end
if redis.call('SISMEMBER', KEYS[5], ARGV[4]) == 0
    and redis.call('SCARD', KEYS[5]) >= tonumber(ARGV[5]) then
    return {'LIMIT'}
end
redis.call('ZADD', KEYS[3], expiry, ARGV[1])
redis.call('HSET', KEYS[4], ARGV[1], ARGV[2] .. '|' .. ARGV[3])
redis.call('SADD', KEYS[5], ARGV[4])
redis.call('PEXPIREAT', KEYS[5], expiry)
refresh_channel(KEYS[3], KEYS[4])
return {'OK'}
"""
)

LEAVE_CHANNEL = (
    "-- perfcho:leave-channel:v2\n"
    + _NOW
    + _CHANNEL_EXPIRY
    + _SESSION
    + """
local status = session_status(KEYS[1], KEYS[2], ARGV[1], ARGV[2], ARGV[3], now_ms())
if status ~= 'OK' then return {status} end
if redis.call('HGET', KEYS[4], ARGV[1]) == ARGV[2] .. '|' .. ARGV[3] then
    redis.call('ZREM', KEYS[3], ARGV[1])
    redis.call('HDEL', KEYS[4], ARGV[1])
    refresh_channel(KEYS[3], KEYS[4])
end
redis.call('SREM', KEYS[5], ARGV[4])
if redis.call('SCARD', KEYS[5]) == 0 then redis.call('DEL', KEYS[5]) end
return {'OK'}
"""
)

LIST_CHANNEL = (
    "-- perfcho:list-channel:v2\n"
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
        local revision = string.sub(epoch, separator + 1)
        valid = redis.call('GET', ARGV[2] .. account .. ':session') == epoch
            and redis.call('HGET', ARGV[1] .. session_id, 'account_id') == account
            and redis.call('HGET', ARGV[1] .. session_id, 'revision') == revision
            and tonumber(redis.call('HGET', ARGV[1] .. session_id, 'expires_at') or '0') > now
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
    "-- perfcho:enqueue-mailbox:v2\n"
    + _NOW
    + _SESSION
    + _ORDERED
    + """
local now = now_ms()
local status, session_expiry = session_status(
    KEYS[1], KEYS[2], ARGV[1], ARGV[2], ARGV[3], now)
if status ~= 'OK' then return {status} end
local expiry = tonumber(ARGV[5])
if not expiry or expiry <= now or expiry > session_expiry
    or expiry - now > tonumber(ARGV[6]) then
    return {'INVALID_EXPIRY'}
end
local count, bytes, deadline = ordered_stats(KEYS[3], now, tonumber(ARGV[7]), true)
if count < 0 then return {'CORRUPT'} end
expire_ordered(KEYS[3], KEYS[4], count, bytes, deadline)
if count >= tonumber(ARGV[7]) or bytes + #ARGV[4] > tonumber(ARGV[8]) then
    return {'OVERFLOW'}
end
if redis.call('GET', KEYS[5]) == ARGV[9] then return {'SEQUENCE_OVERFLOW'} end
local previous_deadline = redis.call('PEXPIRETIME', KEYS[5])
local sequence = redis.call('INCR', KEYS[5])
local token = string.rep('0', 19 - #tostring(sequence)) .. sequence
redis.call('ZADD', KEYS[3], 0, token .. ':' .. ARGV[5] .. ':' .. ARGV[4])
count = count + 1
bytes = bytes + #ARGV[4]
deadline = math.max(deadline, expiry)
expire_ordered(KEYS[3], KEYS[4], count, bytes, deadline)
redis.call('PEXPIREAT', KEYS[5], math.max(deadline, previous_deadline))
redis.call('LPUSH', KEYS[6], token)
redis.call('LTRIM', KEYS[6], 0, 0)
redis.call('PEXPIREAT', KEYS[6], deadline)
return {'OK', tostring(sequence)}
"""
)

LEASE_MAILBOX = (
    "-- perfcho:lease-mailbox:v2\n"
    + _NOW
    + _SESSION
    + _ORDERED
    + """
local now = now_ms()
local status, session_expiry = session_status(
    KEYS[1], KEYS[2], ARGV[1], ARGV[2], ARGV[3], now)
if status ~= 'OK' then return {status} end
local expiry = tonumber(ARGV[6])
if not expiry or expiry <= now or expiry > session_expiry
    or expiry - now > tonumber(ARGV[7]) then
    return {'INVALID_EXPIRY'}
end
if redis.call('EXISTS', KEYS[5]) == 1 then return {'CONFLICT'} end
local count, bytes, deadline = ordered_stats(KEYS[3], now, tonumber(ARGV[8]), true)
if count < 0 then return {'CORRUPT'} end
expire_ordered(KEYS[3], KEYS[4], count, bytes, deadline)
local packets = redis.call('ZRANGE', KEYS[3], 0, tonumber(ARGV[5]) - 1)
if #packets == 0 then redis.call('DEL', KEYS[6]) end
local through = string.rep('0', 19)
if #packets > 0 then through = string.sub(packets[#packets], 1, 19) end
redis.call('SET', KEYS[5], ARGV[2] .. '|' .. ARGV[3] .. '|' .. ARGV[4] .. '|' .. through)
redis.call('PEXPIREAT', KEYS[5], expiry)
local result = {'OK'}
for _, packet in ipairs(packets) do result[#result + 1] = packet end
return result
"""
)

ACK_MAILBOX = (
    "-- perfcho:ack-mailbox:v2\n"
    + _NOW
    + _SESSION
    + _ORDERED
    + """
local status = session_status(KEYS[1], KEYS[2], ARGV[1], ARGV[2], ARGV[3], now_ms())
if status ~= 'OK' then return {status} end
local lease = redis.call('GET', KEYS[5])
local expected_prefix = ARGV[2] .. '|' .. ARGV[3] .. '|' .. ARGV[4] .. '|'
if not lease or string.sub(lease, 1, #expected_prefix) ~= expected_prefix then return {'CONFLICT'} end
local leased_through = string.sub(lease, #expected_prefix + 1)
if ARGV[5] > leased_through then return {'INVALID_ACK'} end
local members = redis.call('ZRANGE', KEYS[3], 0, tonumber(ARGV[6]))
if #members > tonumber(ARGV[6]) then return {'CORRUPT'} end
for _, member in ipairs(members) do
    if string.sub(member, 1, 19) <= ARGV[5] then redis.call('ZREM', KEYS[3], member) end
end
local count, bytes, deadline = ordered_stats(KEYS[3], 0, tonumber(ARGV[6]), false)
expire_ordered(KEYS[3], KEYS[4], count, bytes, deadline)
redis.call('DEL', KEYS[5])
if count == 0 then redis.call('DEL', KEYS[6]) end
return {'OK'}
"""
)

RELEASE_MAILBOX = (
    "-- perfcho:release-mailbox:v2\n"
    + _NOW
    + _SESSION
    + """
local status = session_status(KEYS[1], KEYS[2], ARGV[1], ARGV[2], ARGV[3], now_ms())
if status ~= 'OK' then return {status} end
local lease = redis.call('GET', KEYS[3])
if not lease then return {'OK'} end
local expected_prefix = ARGV[2] .. '|' .. ARGV[3] .. '|' .. ARGV[4] .. '|'
if string.sub(lease, 1, #expected_prefix) ~= expected_prefix then return {'CONFLICT'} end
redis.call('DEL', KEYS[3])
return {'OK'}
"""
)

ATTACH_SPECTATOR = (
    "-- perfcho:attach-spectator:v2\n"
    + _NOW
    + _SESSION
    + _VIEWER_EXPIRY
    + _FRAMES
    + """
local now = now_ms()
local host_status, host_expiry = session_status(
    KEYS[1], KEYS[2], ARGV[1], ARGV[4], ARGV[5], now)
if host_status ~= 'OK' then return {'HOST_' .. host_status} end
local spectator_status, spectator_expiry = session_status(
    KEYS[4], KEYS[5], ARGV[2], ARGV[6], ARGV[7], now)
if spectator_status ~= 'OK' then return {spectator_status} end
local presence_expiry = redis.call('HGET', KEYS[3], 'expires_at')
if redis.call('HGET', KEYS[3], 'session_id') ~= ARGV[4]
    or redis.call('HGET', KEYS[3], 'revision') ~= ARGV[5]
    or not presence_expiry or tonumber(presence_expiry) <= now then
    return {'OFFLINE'}
end
local expiry = tonumber(ARGV[8])
local maximum_expiry = math.min(host_expiry, spectator_expiry, tonumber(presence_expiry))
if not expiry or expiry <= now or expiry > maximum_expiry
    or expiry - now > tonumber(ARGV[9]) then
    return {'INVALID_EXPIRY'}
end
local previous_host = redis.call('HGET', KEYS[6], 'host_account_id')
if redis.call('ZSCORE', KEYS[8], ARGV[2]) == false
    and redis.call('ZCARD', KEYS[8]) >= tonumber(ARGV[11]) then
    return {'LIMIT'}
end
if redis.call('GET', KEYS[7]) == ARGV[10] then return {'REVISION_OVERFLOW'} end
if previous_host then
    local previous_viewers = ARGV[15] .. previous_host .. ':viewers'
    redis.call('ZREM', previous_viewers, ARGV[2])
    refresh_viewers(previous_viewers)
end
local revision = redis.call('INCR', KEYS[7])
redis.call('PEXPIREAT', KEYS[7], spectator_expiry)
redis.call('HSET', KEYS[6], 'host_account_id', ARGV[1], 'spectator_account_id', ARGV[2],
    'relation_id', ARGV[3], 'revision', revision, 'host_session_id', ARGV[4],
    'host_revision', ARGV[5], 'spectator_session_id', ARGV[6],
    'spectator_revision', ARGV[7], 'expires_at', ARGV[8])
redis.call('PEXPIREAT', KEYS[6], expiry)
redis.call('ZADD', KEYS[8], expiry, ARGV[2])
refresh_viewers(KEYS[8])
local count, bytes, deadline = frame_stats(KEYS[9], now, tonumber(ARGV[13]))
if count < 0 then
    redis.call('DEL', KEYS[9], KEYS[10])
else
    expire_frames(KEYS[9], KEYS[10], count, bytes, deadline)
end
local window = latest_frame_window(KEYS[9], tonumber(ARGV[12]))
local result = {'OK', tostring(revision), window[1], window[2], window[3]}
for index = 4, #window do result[#result + 1] = window[index] end
return result
"""
)

DETACH_SPECTATOR = (
    "-- perfcho:detach-spectator:v2\n"
    + _VIEWER_EXPIRY
    + """
local matches = redis.call('HGET', KEYS[1], 'host_account_id') == ARGV[1]
    and redis.call('HGET', KEYS[1], 'spectator_account_id') == ARGV[2]
    and redis.call('HGET', KEYS[1], 'relation_id') == ARGV[3]
    and redis.call('HGET', KEYS[1], 'revision') == ARGV[4]
    and redis.call('HGET', KEYS[1], 'host_session_id') == ARGV[5]
    and redis.call('HGET', KEYS[1], 'host_revision') == ARGV[6]
    and redis.call('HGET', KEYS[1], 'spectator_session_id') == ARGV[7]
    and redis.call('HGET', KEYS[1], 'spectator_revision') == ARGV[8]
if not matches then return {'STALE'} end
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[2])
refresh_viewers(KEYS[2])
return {'OK'}
"""
)

GET_SPECTATOR = (
    "-- perfcho:get-spectator:v2\n"
    + _NOW
    + _SESSION
    + _VIEWER_EXPIRY
    + """
local now = math.max(now_ms(), tonumber(ARGV[4]))
local status = session_status(KEYS[1], KEYS[2], ARGV[1], ARGV[2], ARGV[3], now)
if status ~= 'OK' then return {status} end
local host = redis.call('HGET', KEYS[3], 'host_account_id')
local expiry = redis.call('HGET', KEYS[3], 'expires_at')
if not host or not expiry then return {'NONE'} end
local host_session_id = redis.call('HGET', KEYS[3], 'host_session_id')
local host_revision = redis.call('HGET', KEYS[3], 'host_revision')
local relation_id = redis.call('HGET', KEYS[3], 'relation_id')
local relation_revision = redis.call('HGET', KEYS[3], 'revision')
local valid = tonumber(expiry) > now
    and redis.call('HGET', KEYS[3], 'spectator_session_id') == ARGV[2]
    and redis.call('HGET', KEYS[3], 'spectator_revision') == ARGV[3]
    and redis.call('GET', ARGV[5] .. host .. ':session') == host_session_id .. '|' .. host_revision
    and tonumber(redis.call('HGET', ARGV[6] .. host_session_id, 'expires_at') or '0') > now
if not valid then
    local viewers = ARGV[7] .. host .. ':viewers'
    if redis.call('HGET', KEYS[3], 'relation_id') == relation_id then redis.call('DEL', KEYS[3]) end
    redis.call('ZREM', viewers, ARGV[1])
    refresh_viewers(viewers)
    return {'NONE'}
end
return {'OK', host, relation_id, relation_revision, host_session_id, host_revision,
    ARGV[2], ARGV[3], expiry}
"""
)

LIST_SPECTATORS = (
    "-- perfcho:list-spectators:v2\n"
    + _NOW
    + _SESSION
    + _VIEWER_EXPIRY
    + """
local now = math.max(now_ms(), tonumber(ARGV[4]))
local status = session_status(KEYS[1], KEYS[2], ARGV[1], ARGV[2], ARGV[3], now)
if status ~= 'OK' then return {status} end
local result = {'OK'}
for _, spectator in ipairs(redis.call('ZRANGE', KEYS[3], 0, -1)) do
    local relation_key = ARGV[7] .. spectator .. ':host'
    local relation_id = redis.call('HGET', relation_key, 'relation_id')
    local relation_revision = redis.call('HGET', relation_key, 'revision')
    local spectator_session_id = redis.call('HGET', relation_key, 'spectator_session_id')
    local spectator_revision = redis.call('HGET', relation_key, 'spectator_revision')
    local expiry = redis.call('HGET', relation_key, 'expires_at')
    local valid = redis.call('HGET', relation_key, 'host_account_id') == ARGV[1]
        and redis.call('HGET', relation_key, 'host_session_id') == ARGV[2]
        and redis.call('HGET', relation_key, 'host_revision') == ARGV[3]
        and expiry and tonumber(expiry) > now
        and redis.call('GET', ARGV[5] .. spectator .. ':session') == spectator_session_id .. '|' .. spectator_revision
        and tonumber(redis.call('HGET', ARGV[6] .. spectator_session_id, 'expires_at') or '0') > now
    if valid then
        result[#result + 1] = spectator
        result[#result + 1] = relation_id
        result[#result + 1] = relation_revision
        result[#result + 1] = spectator_session_id
        result[#result + 1] = spectator_revision
        result[#result + 1] = expiry
    else
        if redis.call('HGET', relation_key, 'host_session_id') == ARGV[2]
            and redis.call('HGET', relation_key, 'host_revision') == ARGV[3] then
            redis.call('DEL', relation_key)
        end
        redis.call('ZREM', KEYS[3], spectator)
    end
end
refresh_viewers(KEYS[3])
return result
"""
)

PUBLISH_FRAME = (
    "-- perfcho:publish-frame:v2\n"
    + _NOW
    + _SESSION
    + _VIEWER_EXPIRY
    + _ORDERED
    + _FRAMES
    + """
local now = now_ms()
local status, host_expiry = session_status(
    KEYS[1], KEYS[2], ARGV[1], ARGV[2], ARGV[3], now)
if status ~= 'OK' then return {status} end
local presence_expiry = redis.call('HGET', KEYS[3], 'expires_at')
if redis.call('HGET', KEYS[3], 'session_id') ~= ARGV[2]
    or redis.call('HGET', KEYS[3], 'revision') ~= ARGV[3]
    or not presence_expiry or tonumber(presence_expiry) <= now then
    return {'OFFLINE'}
end
if #ARGV[5] > tonumber(ARGV[10]) then return {'FRAME_TOO_LARGE'} end
local expiry = tonumber(ARGV[6])
if not expiry or expiry <= now or expiry > math.min(host_expiry, tonumber(presence_expiry))
    or expiry - now > tonumber(ARGV[7]) then
    return {'INVALID_EXPIRY'}
end
local stored_session = redis.call('HGET', KEYS[7], 'session_id')
if stored_session and (stored_session ~= ARGV[2]
    or redis.call('HGET', KEYS[7], 'revision') ~= ARGV[3]) then
    redis.call('DEL', KEYS[4], KEYS[5], KEYS[7])
end
local previous_sequence = redis.call('HGET', KEYS[7], 'wire_sequence')
if previous_sequence then
    local delta = (tonumber(ARGV[4]) - tonumber(previous_sequence) + 65536) % 65536
    if delta == 0 or delta > 32768 then return {'NON_MONOTONIC'} end
end
if redis.call('HGET', KEYS[7], 'cursor') == ARGV[11] then return {'SEQUENCE_OVERFLOW'} end
local count, bytes, deadline = frame_stats(KEYS[4], now, tonumber(ARGV[9]))
if count < 0 then
    redis.call('DEL', KEYS[4], KEYS[5])
    count, bytes, deadline = 0, 0, 0
end
while count >= tonumber(ARGV[9]) or bytes + #ARGV[5] > tonumber(ARGV[10]) do
    local oldest = redis.call('ZPOPMIN', KEYS[4], 1)
    if #oldest == 0 then break end
    local _, _, removed_bytes = frame_parts(oldest[1])
    count = count - 1
    bytes = math.max(0, bytes - (removed_bytes or 0))
end
local cursor = redis.call('HINCRBY', KEYS[7], 'cursor', 1)
local cursor_token = string.rep('0', 19 - #tostring(cursor)) .. cursor
local sequence_token = string.rep('0', 5 - #ARGV[4]) .. ARGV[4]
local member = cursor_token .. ':' .. ARGV[6] .. ':' .. sequence_token .. ':' .. ARGV[5]
redis.call('ZADD', KEYS[4], 0, member)
count = count + 1
bytes = bytes + #ARGV[5]
deadline = math.max(deadline, expiry)
expire_frames(KEYS[4], KEYS[5], count, bytes, deadline)
redis.call('HSET', KEYS[7], 'wire_sequence', ARGV[4], 'session_id', ARGV[2], 'revision', ARGV[3])
redis.call('PEXPIREAT', KEYS[7], deadline)
local result = {'OK', tostring(cursor)}
for _, spectator in ipairs(redis.call('ZRANGE', KEYS[6], 0, -1)) do
    local relation_key = ARGV[18] .. spectator .. ':host'
    local spectator_session_id = redis.call('HGET', relation_key, 'spectator_session_id')
    local spectator_revision = redis.call('HGET', relation_key, 'spectator_revision')
    local relation_expiry = redis.call('HGET', relation_key, 'expires_at')
    local spectator_session_key = ARGV[17] .. spectator_session_id
    local spectator_expiry = redis.call('HGET', spectator_session_key, 'expires_at')
    local valid = redis.call('HGET', relation_key, 'host_account_id') == ARGV[1]
        and redis.call('HGET', relation_key, 'host_session_id') == ARGV[2]
        and redis.call('HGET', relation_key, 'host_revision') == ARGV[3]
        and relation_expiry and tonumber(relation_expiry) > now
        and redis.call('GET', ARGV[16] .. spectator .. ':session') == spectator_session_id .. '|' .. spectator_revision
        and spectator_expiry and tonumber(spectator_expiry) > now
    if valid then
        local mailbox_base = ARGV[19] .. spectator
        local packets_key = mailbox_base .. ':packets'
        local bytes_key = mailbox_base .. ':bytes'
        local sequence_key = mailbox_base .. ':sequence'
        local signal_key = mailbox_base .. ':signal:' .. spectator_session_id .. ':' .. spectator_revision
        local packet_expiry = math.min(expiry, tonumber(relation_expiry), tonumber(spectator_expiry),
            now + tonumber(ARGV[12]))
        local packet_count, packet_bytes, packet_deadline = ordered_stats(
            packets_key, now, tonumber(ARGV[13]), true)
        if packet_count >= 0 and packet_count < tonumber(ARGV[13])
            and packet_bytes + #ARGV[5] <= tonumber(ARGV[14])
            and redis.call('GET', sequence_key) ~= ARGV[11] then
            local packet_sequence = redis.call('INCR', sequence_key)
            local packet_token = string.rep('0', 19 - #tostring(packet_sequence)) .. packet_sequence
            redis.call('ZADD', packets_key, 0,
                packet_token .. ':' .. packet_expiry .. ':' .. ARGV[5])
            packet_count = packet_count + 1
            packet_bytes = packet_bytes + #ARGV[5]
            packet_deadline = math.max(packet_deadline, packet_expiry)
            expire_ordered(packets_key, bytes_key, packet_count, packet_bytes, packet_deadline)
            redis.call('PEXPIREAT', sequence_key, packet_deadline)
            redis.call('LPUSH', signal_key, packet_token)
            redis.call('LTRIM', signal_key, 0, 0)
            redis.call('PEXPIREAT', signal_key, packet_deadline)
            result[#result + 1] = spectator
        end
    else
        if redis.call('HGET', relation_key, 'host_session_id') == ARGV[2]
            and redis.call('HGET', relation_key, 'host_revision') == ARGV[3] then
            redis.call('DEL', relation_key)
        end
        redis.call('ZREM', KEYS[6], spectator)
    end
end
refresh_viewers(KEYS[6])
return result
"""
)

READ_FRAMES = (
    "-- perfcho:read-frames:v2\n"
    + _NOW
    + _SESSION
    + _FRAMES
    + """
local now = math.max(now_ms(), tonumber(ARGV[4]))
local status = session_status(KEYS[1], KEYS[2], ARGV[1], ARGV[2], ARGV[3], now)
if status ~= 'OK' then return {status} end
if redis.call('HGET', KEYS[5], 'session_id')
    and (redis.call('HGET', KEYS[5], 'session_id') ~= ARGV[2]
        or redis.call('HGET', KEYS[5], 'revision') ~= ARGV[3]) then
    return {'FENCED'}
end
local count, bytes, deadline = frame_stats(KEYS[3], now, tonumber(ARGV[7]))
if count < 0 then return {'CORRUPT'} end
expire_frames(KEYS[3], KEYS[4], count, bytes, deadline)
local all = redis.call('ZRANGE', KEYS[3], 0, -1)
local oldest = ''
local latest = ''
if #all > 0 then
    oldest = string.sub(all[1], 1, 19)
    latest = string.sub(all[#all], 1, 19)
end
local result = {'OK', oldest, latest, '0'}
local limit = tonumber(ARGV[6])
if ARGV[5] == '' then
    local first = math.max(1, #all - limit + 1)
    if #all > limit then result[4] = '1' end
    for index = first, #all do result[#result + 1] = all[index] end
else
    if #all > 0 and tonumber(ARGV[5]) < tonumber(oldest) - 1 then result[4] = '1' end
    for _, member in ipairs(all) do
        if string.sub(member, 1, 19) > ARGV[5] and #result - 4 < limit then
            result[#result + 1] = member
        end
    end
end
return result
"""
)


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
    get_spectator: AsyncScript
    list_spectators: AsyncScript
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
            get_spectator=redis.register_script(GET_SPECTATOR),
            list_spectators=redis.register_script(LIST_SPECTATORS),
            publish_frame=redis.register_script(PUBLISH_FRAME),
            read_frames=redis.register_script(READ_FRAMES),
        )
