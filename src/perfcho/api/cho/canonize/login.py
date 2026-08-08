"""Parse the newline-delimited Stable login request contract."""

from dataclasses import dataclass


class StableLoginParseError(ValueError):
    """Reject a malformed Stable login body without leaking parser internals."""


@dataclass(frozen=True, slots=True)
class ParsedStableLogin:
    """Carry validated protocol fields into the canonical identity command."""

    identifier: str
    password_token: str
    client_version: str
    utc_offset: int
    display_city: bool
    device_components: tuple[tuple[str, str], ...]
    private_messages_from_friends_only: bool


def parse_stable_login(body: bytes, *, expected_build: str) -> ParsedStableLogin:
    """Parse one latest-Stable login body with strict bounded field counts."""
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as error:
        raise StableLoginParseError("login body must be UTF-8") from error

    lines = text.rstrip("\n").split("\n")
    if len(lines) != 3:
        raise StableLoginParseError("login body must contain exactly three lines")
    identifier, password_token, client_line = lines
    if not identifier or len(identifier) > 254:
        raise StableLoginParseError("login identifier is invalid")
    if len(password_token) != 32:
        raise StableLoginParseError("password token is invalid")

    fields = client_line.split("|")
    if len(fields) != 5:
        raise StableLoginParseError("client information must contain five fields")
    client_version, utc_offset_text, display_city_text, client_hashes, private_messages_text = fields
    if client_version != expected_build:
        raise StableLoginParseError("unsupported Stable build")
    try:
        utc_offset = int(utc_offset_text)
    except ValueError as error:
        raise StableLoginParseError("UTC offset is invalid") from error
    if not -24 <= utc_offset <= 24:
        raise StableLoginParseError("UTC offset is outside the Stable wire range")
    if display_city_text not in {"0", "1"} or private_messages_text not in {"0", "1"}:
        raise StableLoginParseError("client boolean field is invalid")

    hash_values = client_hashes.removesuffix(":").split(":")
    if len(hash_values) != 5:
        raise StableLoginParseError("client hash field must contain five components")
    component_names = ("path", "adapters", "adapters_md5", "uninstall", "disk")
    components = tuple((name, value) for name, value in zip(component_names, hash_values, strict=True) if value)
    if not components:
        raise StableLoginParseError("at least one client device component is required")

    return ParsedStableLogin(
        identifier=identifier,
        password_token=password_token,
        client_version=client_version,
        utc_offset=utc_offset,
        display_city=display_city_text == "1",
        device_components=components,
        private_messages_from_friends_only=private_messages_text == "1",
    )
