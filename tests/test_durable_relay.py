import asyncio
from collections.abc import Sequence
from typing import Any, cast

import pytest
from loguru import logger

from perfcho.relay import _relay_once, _run_relay_loop


class FakeRelayStore:
    def __init__(self, references: tuple[int, ...]) -> None:
        self.references = references
        self.enqueued: list[tuple[int, str, str]] = []
        self.failed: list[tuple[int, str, Exception]] = []
        self.released: list[tuple[int, str]] = []

    async def claim(self, owner: str) -> tuple[int, ...]:
        del owner
        return self.references

    async def record_enqueue_outcomes(
        self,
        outcomes: Sequence[tuple[int, str | Exception]],
        owner: str,
    ) -> None:
        for reference, outcome in outcomes:
            if isinstance(outcome, Exception):
                self.failed.append((reference, owner, outcome))
            else:
                self.enqueued.append((reference, owner, outcome))

    async def release(self, references: Sequence[int], owner: str) -> None:
        self.released.extend((reference, owner) for reference in references)


@pytest.mark.asyncio
async def test_relay_records_each_enqueue_outcome_and_continues_batch() -> None:
    store = FakeRelayStore((1, 2))
    records: list[dict[str, Any]] = []
    sink_id = logger.add(lambda message: records.append(cast(dict[str, Any], message.record)))

    async def enqueue(reference: int) -> str:
        if reference == 1:
            raise RuntimeError("broker unavailable")
        return "task-2"

    try:
        result = await _relay_once(store, "tests:owner", enqueue, relay="tests")
    finally:
        logger.remove(sink_id)

    assert (result.claimed, result.enqueued, result.enqueue_failed) == (2, 1, 1)
    assert [(reference, owner) for reference, owner, _ in store.failed] == [(1, "tests:owner")]
    assert store.enqueued == [(2, "tests:owner", "task-2")]
    enqueue_failure = next(record for record in records if record["extra"]["event"] == "runtime.relay.enqueue_failed")
    assert enqueue_failure["level"].name == "WARNING"
    assert enqueue_failure["extra"]["error_type"] == "RuntimeError"
    assert enqueue_failure["exception"].value.args == ("broker unavailable",)


@pytest.mark.asyncio
async def test_relay_bounds_concurrent_broker_enqueues() -> None:
    store = FakeRelayStore((1, 2, 3, 4, 5))
    active = 0
    maximum_active = 0

    async def enqueue(reference: int) -> str:
        nonlocal active, maximum_active
        active += 1
        maximum_active = max(maximum_active, active)
        await asyncio.sleep(0)
        active -= 1
        return f"task-{reference}"

    result = await _relay_once(store, "tests:owner", enqueue, enqueue_concurrency=2)

    assert (result.claimed, result.enqueued, result.enqueue_failed) == (5, 5, 0)
    assert maximum_active == 2


@pytest.mark.asyncio
async def test_relay_cancellation_releases_only_unattempted_batch_tail() -> None:
    store = FakeRelayStore((1, 2, 3))

    async def enqueue(reference: int) -> str:
        del reference
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await _relay_once(store, "tests:owner", enqueue)

    assert store.released == [(2, "tests:owner"), (3, "tests:owner")]
    assert store.enqueued == []
    assert store.failed == []


@pytest.mark.asyncio
async def test_relay_logs_lifecycle_and_only_nonempty_batch_aggregates() -> None:
    class CancellingStore(FakeRelayStore):
        async def claim(self, owner: str) -> tuple[int, ...]:
            if self.references:
                references = self.references
                self.references = ()
                return references
            raise asyncio.CancelledError

    store = CancellingStore((1,))
    records: list[dict[str, Any]] = []
    sink_id = logger.add(lambda message: records.append(cast(dict[str, Any], message.record)))

    async def enqueue(reference: int) -> str:
        return f"task-{reference}"

    try:
        with pytest.raises(asyncio.CancelledError):
            wakeup = asyncio.Event()
            wakeup.set()
            await _run_relay_loop(
                "tests",
                store,
                "tests:owner",
                enqueue,
                wakeup=wakeup,
                poll_interval=0.01,
                debounce_seconds=0.001,
            )
    finally:
        logger.remove(sink_id)

    relay_events = [record["extra"]["event"] for record in records if record["extra"].get("relay") == "tests"]
    assert relay_events == [
        "runtime.relay.started",
        "runtime.relay.batch",
        "runtime.relay.stopped",
    ]
    batch = next(record for record in records if record["extra"]["event"] == "runtime.relay.batch")
    assert (batch["extra"]["claimed"], batch["extra"]["enqueued"], batch["extra"]["enqueue_failed"]) == (1, 1, 0)
