import asyncio

import pytest

from perfcho.worker import _relay_once


class FakeRelayStore:
    def __init__(self, references: tuple[int, ...]) -> None:
        self.references = references
        self.enqueued: list[tuple[int, str, str]] = []
        self.failed: list[tuple[int, str, Exception]] = []
        self.released: list[tuple[int, str]] = []

    async def claim(self, owner: str) -> tuple[int, ...]:
        del owner
        return self.references

    async def mark_enqueued(self, reference: int, owner: str, broker_task_id: str) -> None:
        self.enqueued.append((reference, owner, broker_task_id))

    async def mark_enqueue_failed(self, reference: int, owner: str, error: Exception) -> None:
        self.failed.append((reference, owner, error))

    async def release(self, reference: int, owner: str) -> None:
        self.released.append((reference, owner))


@pytest.mark.asyncio
async def test_relay_records_each_enqueue_outcome_and_continues_batch() -> None:
    store = FakeRelayStore((1, 2))

    async def enqueue(reference: int) -> str:
        if reference == 1:
            raise RuntimeError("broker unavailable")
        return "task-2"

    assert await _relay_once(store, "tests:owner", enqueue) == 2
    assert [(reference, owner) for reference, owner, _ in store.failed] == [(1, "tests:owner")]
    assert store.enqueued == [(2, "tests:owner", "task-2")]


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
