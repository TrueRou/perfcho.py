import hashlib
from collections.abc import Mapping
from types import TracebackType

import pytest

from perfcho.infra.s3 import S3ObjectStorage


class FakeBody:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.offset = 0
        self.closed = False

    async def read(self, amount: int) -> bytes:
        chunk = self.content[self.offset : self.offset + amount]
        self.offset += len(chunk)
        return chunk

    def close(self) -> None:
        self.closed = True


class FakeClient:
    def __init__(self, body: FakeBody) -> None:
        self.body = body
        self.put_calls: list[dict[str, object]] = []
        self.get_calls: list[dict[str, object]] = []
        self.delete_calls: list[dict[str, object]] = []

    async def put_object(self, **kwargs: object) -> Mapping[str, object]:
        self.put_calls.append(kwargs)
        return {"ETag": '"put-etag"'}

    async def get_object(self, **kwargs: object) -> Mapping[str, object]:
        self.get_calls.append(kwargs)
        return {
            "Body": self.body,
            "ContentLength": len(self.body.content),
            "ContentType": "application/x-osu-beatmap",
            "Metadata": {"sha256": hashlib.sha256(self.body.content).hexdigest()},
            "ETag": '"get-etag"',
        }

    async def delete_object(self, **kwargs: object) -> object:
        self.delete_calls.append(kwargs)
        return {}


class FakeClientContext:
    def __init__(self, client: FakeClient) -> None:
        self.client = client

    async def __aenter__(self) -> FakeClient:
        return self.client

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback


class FakeSession:
    def __init__(self, client: FakeClient) -> None:
        self.fake_client = client
        self.client_calls: list[tuple[str, dict[str, object]]] = []

    def client(self, service_name: str, **kwargs: object) -> FakeClientContext:
        self.client_calls.append((service_name, kwargs))
        return FakeClientContext(self.fake_client)


def object_storage(content: bytes = b"abcdefg") -> tuple[S3ObjectStorage, FakeClient, FakeBody]:
    body = FakeBody(content)
    client = FakeClient(body)
    storage = S3ObjectStorage(
        bucket="perfcho",
        region="us-east-1",
        endpoint_url="http://minio.test",
        access_key="access",
        secret_key="secret",
        addressing_style="path",
        chunk_size=3,
        session=FakeSession(client),
    )
    return storage, client, body


@pytest.mark.asyncio
async def test_s3_put_stream_and_delete_preserve_verified_metadata() -> None:
    storage, client, body = object_storage()
    digest = hashlib.sha256(body.content).digest()

    stored = await storage.put(
        "beatmaps/200/map.osu",
        body.content,
        media_type="application/x-osu-beatmap",
        expected_sha256=digest,
    )
    async with storage.open("beatmaps/200/map.osu") as stream:
        chunks = [chunk async for chunk in stream.iter_chunks()]
        opened = stream.metadata
    await storage.delete("beatmaps/200/map.osu")

    assert stored.sha256 == digest
    assert stored.etag == "put-etag"
    assert client.put_calls[0]["Metadata"] == {"sha256": digest.hex()}
    assert chunks == [b"abc", b"def", b"g"]
    assert opened.sha256 == digest
    assert opened.etag == "get-etag"
    assert body.closed
    assert client.delete_calls == [{"Bucket": "perfcho", "Key": "beatmaps/200/map.osu"}]


@pytest.mark.asyncio
async def test_s3_rejects_unsafe_keys_and_digest_mismatch_before_io() -> None:
    storage, client, _ = object_storage()

    with pytest.raises(ValueError, match="key"):
        await storage.put("../map.osu", b"content", media_type="application/octet-stream")
    with pytest.raises(ValueError, match="sha256"):
        await storage.put(
            "beatmaps/map.osu",
            b"content",
            media_type="application/octet-stream",
            expected_sha256=b"x" * 32,
        )

    assert client.put_calls == []
