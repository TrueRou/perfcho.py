"""Fetch and normalize authoritative beatmap content from the osu! API."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal

import httpx

from perfcho.infra.logging import duration_ms, log_event
from perfcho.infra.settings import Settings
from perfcho.modules.content import (
    BeatmapNotFound,
    BeatmapsetNotFound,
    UpstreamBeatmapsetSnapshot,
    UpstreamBeatmapSnapshot,
    UpstreamContentUnavailable,
)


class BanchoUpstreamContentSource:
    """Normalize official osu! metadata and bounded beatmap file responses."""

    def __init__(
        self,
        *,
        api_base_url: str,
        token_url: str,
        client_id: int,
        client_secret: str,
        beatmap_file_base_url: str,
        max_beatmap_file_bytes: int,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        """Bind API credentials and optional process-owned HTTP transport."""
        if not api_base_url or not token_url or not beatmap_file_base_url or max_beatmap_file_bytes < 1:
            raise ValueError("osu! upstream endpoints and file limit must be configured")
        self._api_base_url = api_base_url.rstrip("/")
        self._token_url = token_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._beatmap_file_base_url = beatmap_file_base_url.rstrip("/")
        self._max_beatmap_file_bytes = max_beatmap_file_bytes
        self._client = client or httpx.AsyncClient(timeout=httpx.Timeout(30.0), follow_redirects=True)
        self._owns_client = client is None
        self._token: str | None = None
        self._token_expires_at = 0.0
        self._token_lock = asyncio.Lock()

    @classmethod
    def from_settings(
        cls,
        settings: Settings,
        *,
        client: httpx.AsyncClient | None = None,
    ) -> BanchoUpstreamContentSource:
        """Construct the official source adapter from validated settings."""
        return cls(
            api_base_url=settings.osu_api_base_url,
            token_url=settings.osu_oauth_token_url,
            client_id=settings.osu_api_client_id,
            client_secret=settings.osu_api_client_secret.get_secret_value(),
            beatmap_file_base_url=settings.upstream_beatmap_file_base_url,
            max_beatmap_file_bytes=settings.upstream_beatmap_file_max_bytes,
            client=client,
        )

    async def aclose(self) -> None:
        """Close the internally owned HTTP client."""
        if self._owns_client:
            await self._client.aclose()

    async def fetch_beatmapset(self, external_beatmapset_id: int) -> UpstreamBeatmapsetSnapshot:
        """Fetch and strictly normalize one complete official beatmapset."""
        started_ns = time.monotonic_ns()
        if external_beatmapset_id < 1:
            raise ValueError("beatmapset ID must be positive")
        response = await self._authorized_get(f"{self._api_base_url}/beatmapsets/{external_beatmapset_id}")
        if response.status_code == 404:
            _log_upstream_result("beatmapset", started_ns, status_code=404, outcome="not_found")
            raise BeatmapsetNotFound("upstream beatmapset was not found")
        if response.status_code != 200:
            _log_upstream_result("beatmapset", started_ns, status_code=response.status_code, outcome="failed")
            raise UpstreamContentUnavailable("upstream beatmapset request failed")
        try:
            payload = _mapping(response.json(), "beatmapset")
            result = _parse_beatmapset(
                payload,
                etag=response.headers.get("etag"),
                last_modified=response.headers.get("last-modified"),
            )
        except (TypeError, ValueError, KeyError) as error:
            _log_upstream_result(
                "beatmapset",
                started_ns,
                status_code=response.status_code,
                outcome="invalid_response",
                exception=error,
                error_type=type(error).__name__,
            )
            raise UpstreamContentUnavailable("upstream beatmapset response is invalid") from error
        _log_upstream_result(
            "beatmapset",
            started_ns,
            status_code=response.status_code,
            outcome="success",
            item_count=len(result.beatmaps),
        )
        return result

    async def lookup_beatmapset_id(self, checksum: str, file_name: str) -> int:
        """Resolve a set from a current beatmap checksum, falling back to its Stable filename."""
        try:
            digest = bytes.fromhex(checksum)
        except ValueError as error:
            raise ValueError("beatmap checksum must be hexadecimal") from error
        if len(checksum) != 32 or len(digest) != 16:
            raise ValueError("beatmap checksum must contain 32 hexadecimal characters")
        if not file_name or len(file_name) > 255:
            raise ValueError("beatmap filename is invalid")

        started_ns = time.monotonic_ns()
        for selector in ({"checksum": checksum.lower()}, {"filename": file_name}):
            response = await self._authorized_get(f"{self._api_base_url}/beatmaps/lookup", params=selector)
            if response.status_code == 404:
                continue
            if response.status_code != 200:
                _log_upstream_result("beatmap_lookup", started_ns, status_code=response.status_code, outcome="failed")
                raise UpstreamContentUnavailable("upstream beatmap lookup request failed")
            try:
                beatmapset_id = _integer(_mapping(response.json(), "beatmap"), "beatmapset_id")
            except (TypeError, ValueError, KeyError) as error:
                _log_upstream_result(
                    "beatmap_lookup",
                    started_ns,
                    status_code=response.status_code,
                    outcome="invalid_response",
                    exception=error,
                    error_type=type(error).__name__,
                )
                raise UpstreamContentUnavailable("upstream beatmap lookup response is invalid") from error
            if beatmapset_id < 1:
                raise UpstreamContentUnavailable("upstream beatmap lookup response is invalid")
            _log_upstream_result("beatmap_lookup", started_ns, status_code=200, outcome="success")
            return beatmapset_id

        _log_upstream_result("beatmap_lookup", started_ns, status_code=404, outcome="not_found")
        raise BeatmapNotFound("upstream beatmap was not found")

    async def fetch_beatmap_file(self, external_beatmap_id: int) -> bytes:
        """Stream one official .osu body into a strictly bounded buffer."""
        started_ns = time.monotonic_ns()
        if external_beatmap_id < 1:
            raise ValueError("beatmap ID must be positive")
        try:
            async with self._client.stream(
                "GET",
                f"{self._beatmap_file_base_url}/{external_beatmap_id}",
            ) as response:
                if response.status_code == 404:
                    _log_upstream_result("beatmap_file", started_ns, status_code=404, outcome="not_found")
                    raise BeatmapNotFound("upstream beatmap file was not found")
                if response.status_code != 200:
                    _log_upstream_result(
                        "beatmap_file",
                        started_ns,
                        status_code=response.status_code,
                        outcome="failed",
                    )
                    raise UpstreamContentUnavailable("upstream beatmap file request failed")
                content_length = response.headers.get("content-length")
                if content_length is not None and int(content_length) > self._max_beatmap_file_bytes:
                    raise UpstreamContentUnavailable("upstream beatmap file exceeds the configured limit")
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    if len(body) + len(chunk) > self._max_beatmap_file_bytes:
                        raise UpstreamContentUnavailable("upstream beatmap file exceeds the configured limit")
                    body.extend(chunk)
        except (httpx.HTTPError, ValueError) as error:
            _log_upstream_result(
                "beatmap_file",
                started_ns,
                outcome="failed",
                exception=error,
                error_type=type(error).__name__,
            )
            raise UpstreamContentUnavailable("upstream beatmap file request failed") from error
        _log_upstream_result(
            "beatmap_file",
            started_ns,
            status_code=200,
            outcome="success",
            size_bytes=len(body),
        )
        return bytes(body)

    async def _authorized_get(self, url: str, *, params: Mapping[str, str] | None = None) -> httpx.Response:
        token = await self._access_token()
        try:
            response = await self._client.get(url, headers={"Authorization": f"Bearer {token}"}, params=params)
            if response.status_code != 401:
                return response
            self._token = None
            token = await self._access_token()
            return await self._client.get(url, headers={"Authorization": f"Bearer {token}"}, params=params)
        except httpx.HTTPError as error:
            raise UpstreamContentUnavailable("upstream beatmapset request failed") from error

    async def _access_token(self) -> str:
        if self._token is not None and time.monotonic() < self._token_expires_at:
            return self._token
        async with self._token_lock:
            if self._token is not None and time.monotonic() < self._token_expires_at:
                return self._token
            if self._client_id < 1 or not self._client_secret:
                raise UpstreamContentUnavailable("osu! API credentials are not configured")
            try:
                response = await self._client.post(
                    self._token_url,
                    json={
                        "client_id": self._client_id,
                        "client_secret": self._client_secret,
                        "grant_type": "client_credentials",
                        "scope": "public",
                    },
                )
            except httpx.HTTPError as error:
                log_event(
                    "WARNING",
                    "upstream.osu.oauth.failed",
                    exception=error,
                    error_type=type(error).__name__,
                )
                raise UpstreamContentUnavailable("osu! OAuth token request failed") from error
            if response.status_code != 200:
                log_event("WARNING", "upstream.osu.oauth.failed", status_code=response.status_code)
                raise UpstreamContentUnavailable("osu! OAuth token request failed")
            try:
                payload = _mapping(response.json(), "OAuth token")
                token = _string(payload, "access_token")
                expires_in = _integer(payload, "expires_in")
            except (TypeError, ValueError, KeyError) as error:
                log_event(
                    "ERROR",
                    "upstream.osu.oauth.invalid_response",
                    exception=error,
                    error_type=type(error).__name__,
                )
                raise UpstreamContentUnavailable("osu! OAuth token response is invalid") from error
            self._token = token
            self._token_expires_at = time.monotonic() + max(1, expires_in - 30)
            return token


def _log_upstream_result(
    operation: str,
    started_ns: int,
    *,
    outcome: str,
    status_code: int | None = None,
    exception: BaseException | None = None,
    error_type: str | None = None,
    item_count: int | None = None,
    size_bytes: int | None = None,
) -> None:
    """Emit allow-listed upstream result fields without content metadata."""
    level = "DEBUG" if outcome in {"success", "not_found"} else "WARNING"
    log_event(
        level,
        "upstream.osu.request.completed",
        exception=exception,
        operation=operation,
        outcome=outcome,
        status_code=status_code,
        error_type=error_type,
        item_count=item_count,
        size_bytes=size_bytes,
        duration_ms=duration_ms(started_ns),
    )


def _parse_beatmapset(
    payload: Mapping[str, object],
    *,
    etag: str | None,
    last_modified: str | None,
) -> UpstreamBeatmapsetSnapshot:
    beatmapset_id = _integer(payload, "id")
    artist = _string(payload, "artist")
    title = _string(payload, "title")
    creator = _string(payload, "creator")
    beatmap_values = payload.get("beatmaps")
    if not isinstance(beatmap_values, list):
        raise TypeError("beatmaps must be an array")
    has_video = _boolean(payload, "video", default=False)
    beatmaps = tuple(
        _parse_beatmap(
            _mapping(value, "beatmap"),
            beatmapset_id=beatmapset_id,
            artist=artist,
            title=title,
            creator=creator,
            has_video=has_video,
        )
        for value in beatmap_values
    )
    description_value = payload.get("description")
    description = None
    if isinstance(description_value, Mapping):
        candidate = description_value.get("description")
        description = candidate if isinstance(candidate, str) else None
    availability = _optional_mapping(payload.get("availability"))
    available = not _boolean(availability, "download_disabled", default=False)
    return UpstreamBeatmapsetSnapshot(
        source_code="osu",
        external_beatmapset_id=beatmapset_id,
        creator_external_id=_optional_integer(payload.get("user_id")),
        creator_name=creator,
        artist=artist,
        artist_unicode=_optional_string(payload.get("artist_unicode")),
        title=title,
        title_unicode=_optional_string(payload.get("title_unicode")),
        source_text=_optional_string(payload.get("source")),
        tags=_string(payload, "tags", default=""),
        genre_id=_nested_identifier(payload.get("genre")),
        language_id=_nested_identifier(payload.get("language")),
        description=description,
        status=_string(payload, "status"),
        submitted_at=_optional_datetime(payload.get("submitted_date")),
        ranked_at=_optional_datetime(payload.get("ranked_date")),
        last_updated_at=_datetime(payload, "last_updated"),
        available=available,
        nsfw=_boolean(payload, "nsfw", default=False),
        beatmaps=beatmaps,
        etag=etag,
        last_modified=last_modified,
    )


def _parse_beatmap(
    payload: Mapping[str, object],
    *,
    beatmapset_id: int,
    artist: str,
    title: str,
    creator: str,
    has_video: bool,
) -> UpstreamBeatmapSnapshot:
    if _integer(payload, "beatmapset_id") != beatmapset_id:
        raise ValueError("beatmap belongs to a different beatmapset")
    difficulty_name = _string(payload, "version")
    file_name = _beatmap_file_name(artist, title, creator, difficulty_name)
    checksum = bytes.fromhex(_string(payload, "checksum"))
    return UpstreamBeatmapSnapshot(
        external_beatmap_id=_integer(payload, "id"),
        md5=checksum,
        file_name=file_name,
        difficulty_name=difficulty_name,
        ruleset=_string(payload, "mode"),
        status=_string(payload, "status"),
        source_updated_at=_datetime(payload, "last_updated"),
        total_length_ms=_integer(payload, "total_length") * 1000,
        drain_length_ms=_integer(payload, "hit_length") * 1000,
        bpm=_decimal(payload, "bpm"),
        circle_size=_decimal(payload, "cs"),
        overall_difficulty=_decimal(payload, "accuracy"),
        approach_rate=_decimal(payload, "ar"),
        health_drain=_decimal(payload, "drain"),
        circle_count=_integer(payload, "count_circles"),
        slider_count=_integer(payload, "count_sliders"),
        spinner_count=_integer(payload, "count_spinners"),
        max_combo=_integer(payload, "max_combo", default=0),
        star_rating=_decimal(payload, "difficulty_rating"),
        has_storyboard=False,
        has_video=has_video,
    )


def _beatmap_file_name(artist: str, title: str, creator: str, difficulty_name: str) -> str:
    raw = f"{artist} - {title} ({creator}) [{difficulty_name}].osu"
    forbidden = '<>:"/\\|?*'
    clean = "".join("_" if character in forbidden or character < " " else character for character in raw).strip()
    if len(clean) <= 255:
        return clean
    return f"{clean[:251]}.osu"


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be an object")
    return value


def _optional_mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _string(payload: Mapping[str, object], key: str, *, default: str | None = None) -> str:
    value = payload.get(key, default)
    if not isinstance(value, str):
        raise TypeError(f"{key} must be a string")
    return value


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _integer(payload: Mapping[str, object], key: str, *, default: int | None = None) -> int:
    value = payload.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{key} must be an integer")
    return value


def _optional_integer(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _decimal(payload: Mapping[str, object], key: str) -> Decimal:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int | float | str):
        raise TypeError(f"{key} must be numeric")
    return Decimal(str(value))


def _boolean(payload: Mapping[str, object], key: str, *, default: bool) -> bool:
    value = payload.get(key, default)
    if not isinstance(value, bool):
        raise TypeError(f"{key} must be boolean")
    return value


def _datetime(payload: Mapping[str, object], key: str) -> datetime:
    value = payload.get(key)
    result = _optional_datetime(value)
    if result is None:
        raise TypeError(f"{key} must be a timezone-aware datetime")
    return result


def _optional_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError("datetime must be a string")
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return result


def _nested_identifier(value: object) -> int | None:
    mapping = _optional_mapping(value)
    return _optional_integer(mapping.get("id"))
