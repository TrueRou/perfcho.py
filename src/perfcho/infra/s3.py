"""Implement provider-neutral object storage with an S3-compatible backend."""

from __future__ import annotations

import hashlib
import inspect
from collections.abc import AsyncIterator, Awaitable, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Protocol, cast

import aioboto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from perfcho.infra.settings import Settings
from perfcho.modules.common import ObjectStream, ObjectUnavailable, StoredObject


class _StreamingBody(Protocol):
    async def read(self, amount: int) -> bytes: ...

    def close(self) -> Awaitable[None] | None: ...


class _S3Client(Protocol):
    async def put_object(self, **kwargs: object) -> Mapping[str, object]: ...

    async def get_object(self, **kwargs: object) -> Mapping[str, object]: ...

    async def delete_object(self, **kwargs: object) -> object: ...


class _S3Session(Protocol):
    def client(self, service_name: str, **kwargs: object) -> AbstractAsyncContextManager[_S3Client]: ...


class _S3ObjectStream:
    def __init__(self, body: _StreamingBody, metadata: StoredObject, chunk_size: int) -> None:
        self._body = body
        self._metadata = metadata
        self._chunk_size = chunk_size

    @property
    def metadata(self) -> StoredObject:
        return self._metadata

    async def iter_chunks(self) -> AsyncIterator[bytes]:
        while chunk := await self._body.read(self._chunk_size):
            yield bytes(chunk)


class S3ObjectStorage:
    """Store immutable objects in one configured S3-compatible bucket."""

    def __init__(
        self,
        *,
        bucket: str,
        region: str,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        addressing_style: str,
        chunk_size: int,
        session: _S3Session | None = None,
    ) -> None:
        """Bind bucket configuration without opening network resources."""
        if not bucket or chunk_size < 1:
            raise ValueError("S3 bucket and chunk size must be configured")
        self._bucket = bucket
        self._chunk_size = chunk_size
        self._session = session or cast(_S3Session, aioboto3.Session())
        self._client_options = {
            "region_name": region,
            "endpoint_url": endpoint_url,
            "aws_access_key_id": access_key,
            "aws_secret_access_key": secret_key,
            "config": Config(
                s3={"addressing_style": addressing_style},
                retries={"mode": "standard", "max_attempts": 3},
            ),
        }

    @classmethod
    def from_settings(cls, settings: Settings) -> S3ObjectStorage:
        """Construct an adapter from validated process settings."""
        return cls(
            bucket=settings.s3_bucket,
            region=settings.s3_region,
            endpoint_url=settings.s3_endpoint_url,
            access_key=settings.s3_access_key.get_secret_value(),
            secret_key=settings.s3_secret_key.get_secret_value(),
            addressing_style=settings.s3_addressing_style,
            chunk_size=settings.object_stream_chunk_size,
        )

    async def put(
        self,
        storage_key: str,
        content: bytes,
        *,
        media_type: str,
        expected_sha256: bytes | None = None,
    ) -> StoredObject:
        """Write one object after validating its key and expected digest."""
        key = _validate_storage_key(storage_key)
        if not media_type or len(media_type) > 127:
            raise ValueError("object media type is invalid")
        digest = hashlib.sha256(content).digest()
        if expected_sha256 is not None and digest != expected_sha256:
            raise ValueError("object sha256 does not match expected digest")
        try:
            async with self._client() as client:
                result = await client.put_object(
                    Bucket=self._bucket,
                    Key=key,
                    Body=content,
                    ContentLength=len(content),
                    ContentType=media_type,
                    Metadata={"sha256": digest.hex()},
                )
        except (BotoCoreError, ClientError) as error:
            raise ObjectUnavailable("object storage write failed") from error
        return StoredObject(key, len(content), media_type, digest, _etag(result))

    @asynccontextmanager
    async def open(self, storage_key: str) -> AsyncIterator[ObjectStream]:
        """Open and close one provider response body around streamed iteration."""
        key = _validate_storage_key(storage_key)
        try:
            async with self._client() as client:
                response = await client.get_object(Bucket=self._bucket, Key=key)
                body = cast(_StreamingBody, response["Body"])
                size_value = response.get("ContentLength", 0)
                if isinstance(size_value, bool) or not isinstance(size_value, int | str):
                    raise ValueError("object storage returned an invalid content length")
                media_type_value = response.get("ContentType")
                metadata = StoredObject(
                    storage_key=key,
                    size_bytes=int(size_value),
                    media_type=media_type_value if isinstance(media_type_value, str) else "application/octet-stream",
                    sha256=_metadata_digest(response.get("Metadata")),
                    etag=_etag(response),
                )
                try:
                    yield _S3ObjectStream(body, metadata, self._chunk_size)
                finally:
                    closed = body.close()
                    if inspect.isawaitable(closed):
                        await closed
        except ObjectUnavailable:
            raise
        except (BotoCoreError, ClientError, KeyError, TypeError, ValueError) as error:
            raise ObjectUnavailable("object storage read failed") from error

    async def delete(self, storage_key: str) -> None:
        """Idempotently delete one provider object."""
        key = _validate_storage_key(storage_key)
        try:
            async with self._client() as client:
                await client.delete_object(Bucket=self._bucket, Key=key)
        except (BotoCoreError, ClientError) as error:
            raise ObjectUnavailable("object storage delete failed") from error

    def _client(self) -> AbstractAsyncContextManager[_S3Client]:
        return self._session.client("s3", **self._client_options)


def _validate_storage_key(value: str) -> str:
    if (
        not value
        or len(value) > 512
        or value.startswith("/")
        or "\\" in value
        or "//" in value
        or any(character < " " for character in value)
        or any(part in {"", ".", ".."} for part in value.split("/"))
    ):
        raise ValueError("object storage key is invalid")
    return value


def _metadata_digest(value: object) -> bytes | None:
    if not isinstance(value, Mapping):
        return None
    encoded = value.get("sha256")
    if not isinstance(encoded, str):
        return None
    try:
        digest = bytes.fromhex(encoded)
    except ValueError:
        return None
    return digest if len(digest) == 32 else None


def _etag(response: object) -> str | None:
    if not isinstance(response, Mapping):
        return None
    value = response.get("ETag")
    return value.strip('"') if isinstance(value, str) else None
