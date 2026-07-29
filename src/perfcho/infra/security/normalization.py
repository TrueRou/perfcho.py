"""Normalize account identifiers without protocol-specific dependencies."""

import unicodedata

_STABLE_NAME_MIN_LENGTH = 2
_STABLE_NAME_MAX_LENGTH = 15
_STABLE_NAME_KEY_MAX_LENGTH = 32
_EMAIL_MAX_LENGTH = 254
_EMAIL_LOCAL_PART_MAX_LENGTH = 64
_STABLE_NAME_PUNCTUATION = frozenset("_[]-")


def normalize_stable_name(name: str) -> str:
    """Validate a Stable-compatible display name and return its canonical key."""
    normalized = unicodedata.normalize("NFKC", name)

    if not _STABLE_NAME_MIN_LENGTH <= len(normalized) <= _STABLE_NAME_MAX_LENGTH:
        raise ValueError("Stable names must contain between 2 and 15 characters after NFKC normalization")
    if not all(
        character.isalnum() or character.isspace() or character in _STABLE_NAME_PUNCTUATION for character in normalized
    ):
        raise ValueError(
            "Stable names may only contain letters, numbers, whitespace, underscores, brackets, and hyphens"
        )
    if not any(character.isalnum() for character in normalized):
        raise ValueError("Stable names must contain at least one letter or number")
    if "_" in normalized and any(character.isspace() for character in normalized):
        raise ValueError("Stable names may contain underscores or whitespace, but not both")

    key = "".join("_" if character.isspace() else character for character in normalized.casefold())
    if len(key) > _STABLE_NAME_KEY_MAX_LENGTH:
        raise ValueError("normalized Stable names must not exceed 32 characters")
    return key


def normalize_email(email: str) -> str:
    """Trim and lowercase an email without applying provider-specific aliases."""
    normalized = email.strip()
    if not normalized or len(normalized) > _EMAIL_MAX_LENGTH:
        raise ValueError("email must contain at most 254 characters")
    if normalized.count("@") != 1:
        raise ValueError("email must contain exactly one @ separator")

    local_part, domain = normalized.split("@")
    if not local_part or len(local_part) > _EMAIL_LOCAL_PART_MAX_LENGTH or not domain:
        raise ValueError("email must contain non-empty local and domain parts")
    if any(character.isspace() or unicodedata.category(character).startswith("C") for character in normalized):
        raise ValueError("email must not contain whitespace or control characters")

    return normalized.lower()
