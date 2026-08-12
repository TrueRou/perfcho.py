import asyncio
import os
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import msgpack
import pytest
from redis.asyncio import Redis

from perfcho.infra.redis.bubbles import (
    RedisBubbleSubscription,
    RedisRealtimeBubbleBus,
    RedisRealtimePollGate,
    bubble_channel,
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


async def test_subscribe_closes_pubsub_when_ack_fails() -> None:
    pubsub = MagicMock()
    pubsub.subscribe = AsyncMock()
    pubsub.get_message = AsyncMock(side_effect=RuntimeError("ack failed"))
    pubsub.aclose = AsyncMock()
    redis = MagicMock()
    redis.pubsub.return_value = pubsub
    bus = RedisRealtimeBubbleBus(redis, prefix="tests:bubbles")

    with pytest.raises(RuntimeError, match="ack failed"):
        async with bus.subscribe(SessionFence(uuid.uuid7(), 1)):
            pass

    pubsub.aclose.assert_awaited_once()


async def test_subscription_closes_pubsub_when_unsubscribe_fails_and_is_idempotent() -> None:
    pubsub = MagicMock()
    pubsub.unsubscribe = AsyncMock(side_effect=RuntimeError("unsubscribe failed"))
    pubsub.aclose = AsyncMock()
    subscription = RedisBubbleSubscription(pubsub)

    with pytest.raises(RuntimeError, match="unsubscribe failed"):
        await subscription.aclose()
    await subscription.aclose()

    pubsub.unsubscribe.assert_awaited_once()
    pubsub.aclose.assert_awaited_once()


@pytest.mark.skipif(not os.getenv("TEST_REDIS_URL"), reason="TEST_REDIS_URL is not configured")
async def test_real_redis_pubsub_is_fenced_ordered_and_drops_malformed() -> None:
    prefix = f"tests:bubbles:{uuid.uuid7()}"
    subscriber_redis = Redis.from_url(os.environ["TEST_REDIS_URL"], decode_responses=False)
    publisher_redis = Redis.from_url(os.environ["TEST_REDIS_URL"], decode_responses=False)
    fence = SessionFence(uuid.uuid7(), 1)
    other_revision = SessionFence(fence.session_id, 2)
    subscriber = RedisRealtimeBubbleBus(subscriber_redis, prefix=prefix)
    publisher = RedisRealtimeBubbleBus(publisher_redis, prefix=prefix)
    try:
        assert await publisher.publish(fence, NotificationBubble("lost")) == 0
        async with subscriber.subscribe(fence) as subscription:
            assert isinstance(subscription, RealtimeBubbleSubscription)
            assert await publisher.publish(other_revision, NotificationBubble("isolated")) == 0
            await publisher_redis.publish(bubble_channel(prefix, fence), msgpack.packb({"v": 999}))
            await publisher.publish(fence, NotificationBubble("one"))
            await publisher.publish(fence, NotificationBubble("two"))
            assert await subscription.receive(timeout=1) == NotificationBubble("one")
            assert await subscription.receive(timeout=1) == NotificationBubble("two")
            assert await subscription.receive(timeout=0.01) is None
            second_fence = SessionFence(uuid.uuid7(), 1)
            async with subscriber.subscribe(second_fence) as second_subscription:
                bubble = NotificationBubble("fanout")
                assert await publisher.publish_many((fence, second_fence), bubble) == 2
                assert await subscription.receive(timeout=1) == bubble
                assert await second_subscription.receive(timeout=1) == bubble
        assert await publisher.publish(fence, NotificationBubble("after-close")) == 0
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
