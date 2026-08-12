import asyncio
import os
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from perfcho.infra.redis.bubbles import (
    RedisBubbleSubscription,
    RedisRealtimeBubbleBus,
    RedisRealtimePollGate,
    encode_bubble,
)
from perfcho.modules.realtime import (
    NotificationBubble,
    RealtimeBubbleBus,
    RealtimeBubbleSubscription,
    RealtimePollGate,
    SessionFence,
)


def test_redis_adapters_implement_realtime_ports() -> None:
    redis = Redis()
    try:
        bus = RedisRealtimeBubbleBus(redis, prefix="tests:bubbles")
        gate = RedisRealtimePollGate(redis, prefix="tests:bubbles")
        assert isinstance(bus, RealtimeBubbleBus)
        assert isinstance(gate, RealtimePollGate)
    finally:
        asyncio.run(redis.aclose())


async def test_subscribe_propagates_group_creation_failure() -> None:
    redis = MagicMock()
    redis.xgroup_create = AsyncMock(side_effect=ResponseError("failure"))
    bus = RedisRealtimeBubbleBus(redis, prefix="tests:bubbles")

    with pytest.raises(ResponseError, match="failure"):
        async with bus.subscribe(SessionFence(uuid.uuid7(), 1)):
            pass


async def test_subscription_reads_pending_then_new_entries_and_acknowledges_returned_entries() -> None:
    redis = MagicMock()
    pending = NotificationBubble("pending")
    new = NotificationBubble("new")
    redis.xreadgroup = AsyncMock(
        side_effect=[
            [[b"stream", [(b"0-0", {b"payload": encode_bubble(pending)})]]],
            [[b"stream", []]],
            [[b"stream", [(b"1-0", {b"payload": encode_bubble(new)})]]],
        ]
    )
    redis.xack = AsyncMock()
    subscription = RedisBubbleSubscription(redis, "stream", "group", "consumer")

    assert await subscription.receive(timeout=0) == pending
    assert await subscription.receive(timeout=0) == new
    await subscription.acknowledge()

    assert redis.xreadgroup.await_args_list[0].args[2] == {"stream": "0"}
    assert redis.xreadgroup.await_args_list[1].args[2] == {"stream": "0"}
    assert redis.xreadgroup.await_args_list[2].args[2] == {"stream": ">"}
    redis.xack.assert_awaited_once_with("stream", "group", b"0-0", b"1-0")


@pytest.mark.skipif(not os.getenv("TEST_REDIS_URL"), reason="TEST_REDIS_URL is not configured")
async def test_real_redis_stream_is_fenced_ordered_durable_and_acknowledged() -> None:
    prefix = f"tests:bubbles:{uuid.uuid7()}"
    subscriber_redis = Redis.from_url(os.environ["TEST_REDIS_URL"], decode_responses=False)
    publisher_redis = Redis.from_url(os.environ["TEST_REDIS_URL"], decode_responses=False)
    fence = SessionFence(uuid.uuid7(), 1)
    other_revision = SessionFence(fence.session_id, 2)
    subscriber = RedisRealtimeBubbleBus(subscriber_redis, prefix=prefix)
    publisher = RedisRealtimeBubbleBus(publisher_redis, prefix=prefix)
    try:
        assert await publisher.publish(fence, NotificationBubble("queued")) == 1
        async with subscriber.subscribe(fence) as subscription:
            assert isinstance(subscription, RealtimeBubbleSubscription)
            assert await subscription.receive(timeout=1) == NotificationBubble("queued")
            await subscription.acknowledge()
            assert await publisher.publish(other_revision, NotificationBubble("isolated")) == 1
            await publisher.publish(fence, NotificationBubble("one"))
            await publisher.publish(fence, NotificationBubble("two"))
            assert await subscription.receive(timeout=1) == NotificationBubble("one")
            assert await subscription.receive(timeout=1) == NotificationBubble("two")
            await subscription.acknowledge()
            assert await subscription.receive(timeout=0.01) is None
            second_fence = SessionFence(uuid.uuid7(), 1)
            async with subscriber.subscribe(second_fence) as second_subscription:
                bubble = NotificationBubble("fanout")
                assert await publisher.publish_many((fence, second_fence), bubble) == 2
                assert await subscription.receive(timeout=1) == bubble
                assert await second_subscription.receive(timeout=1) == bubble
                await subscription.acknowledge()
                await second_subscription.acknowledge()
        assert await publisher.publish(fence, NotificationBubble("after-close")) == 1
        async with subscriber.subscribe(fence) as resumed:
            assert await resumed.receive(timeout=1) == NotificationBubble("after-close")
            await resumed.acknowledge()
    finally:
        await subscriber_redis.aclose()
        await publisher_redis.aclose()


@pytest.mark.skipif(not os.getenv("TEST_REDIS_URL"), reason="TEST_REDIS_URL is not configured")
async def test_real_redis_poll_gate_is_owner_safe_fenced_and_expires() -> None:
    prefix = f"tests:poll-gate:{uuid.uuid7()}"
    redis_a = Redis.from_url(os.environ["TEST_REDIS_URL"], decode_responses=False)
    redis_b = Redis.from_url(os.environ["TEST_REDIS_URL"], decode_responses=False)
    account_id = 42
    fence = SessionFence(uuid.uuid7(), 7)
    stale = SessionFence(uuid.uuid7(), 1)
    gate_a = RedisRealtimePollGate(redis_a, prefix=prefix, max_ttl_seconds=1)
    gate_b = RedisRealtimePollGate(redis_b, prefix=prefix, max_ttl_seconds=1)
    owner = uuid.uuid7()
    other_owner = uuid.uuid7()
    session_key = f"{prefix}:v2:account:{account_id}:session"
    gate_key = f"{prefix}:v2:poll-gate:{account_id}"
    try:
        await redis_a.set(session_key, f"{fence.session_id}|{fence.revision}", ex=10)
        expiry = datetime.now(UTC) + timedelta(seconds=5)
        results = await asyncio.gather(
            gate_a.acquire(account_id, fence, owner, expires_at=expiry),
            gate_b.acquire(account_id, fence, other_owner, expires_at=expiry),
        )
        assert sorted(results) == [False, True]
        winner = (gate_a, owner) if results[0] else (gate_b, other_owner)
        loser = (gate_b, other_owner) if results[0] else (gate_a, owner)
        await loser[0].release(account_id, fence, loser[1])
        assert await redis_a.exists(gate_key) == 1
        await winner[0].release(account_id, fence, winner[1])
        assert await redis_a.exists(gate_key) == 0
        assert not await gate_a.acquire(account_id, stale, uuid.uuid7(), expires_at=expiry)
        assert await gate_a.acquire(account_id, fence, owner, expires_at=expiry)
        await asyncio.sleep(1.05)
        assert await gate_b.acquire(account_id, fence, other_owner, expires_at=expiry)
    finally:
        await redis_a.delete(session_key, gate_key)
        await redis_a.aclose()
        await redis_b.aclose()
