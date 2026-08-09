"""Convert legacy scalar values into canonical perfcho representations."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import orjson

from perfcho.infra.db.enums import BeatmapStatus, Ruleset, ScoreboardVariant, ScoreGrade
from perfcho.modules.scoring.mods import parse_legacy_mods

_SCOREBOARD_IDS = {0: 1, 1: 2, 2: 3, 3: 4, 4: 5, 5: 6, 6: 7, 8: 8}
_RULESETS = {0: Ruleset.OSU, 1: Ruleset.TAIKO, 2: Ruleset.FRUITS, 3: Ruleset.MANIA}
_VARIANTS = {
    0: ScoreboardVariant.VANILLA,
    1: ScoreboardVariant.VANILLA,
    2: ScoreboardVariant.VANILLA,
    3: ScoreboardVariant.VANILLA,
    4: ScoreboardVariant.RELAX,
    5: ScoreboardVariant.RELAX,
    6: ScoreboardVariant.RELAX,
    8: ScoreboardVariant.AUTOPILOT,
}
_BEATMAP_STATUSES = {
    -1: BeatmapStatus.GRAVEYARD,
    0: BeatmapStatus.PENDING,
    1: BeatmapStatus.PENDING,
    2: BeatmapStatus.RANKED,
    3: BeatmapStatus.APPROVED,
    4: BeatmapStatus.QUALIFIED,
    5: BeatmapStatus.LOVED,
}


def aware_datetime(value: object, timezone: ZoneInfo, *, fallback: datetime) -> datetime:
    """Interpret a MySQL DATETIME in the configured legacy server timezone."""
    if not isinstance(value, datetime):
        return fallback
    if value.tzinfo is None or value.utcoffset() is None:
        value = value.replace(tzinfo=timezone)
    return value.astimezone(UTC)


def unix_datetime(value: object, *, fallback: datetime) -> datetime:
    """Convert nonzero Unix seconds to UTC and use a deterministic fallback otherwise."""
    if isinstance(value, bool) or not isinstance(value, int | float) or value <= 0:
        return fallback
    try:
        return datetime.fromtimestamp(value, tz=UTC)
    except OverflowError, OSError, ValueError:
        return fallback


def source_ruleset(mode: object) -> Ruleset:
    """Map a vanilla or variant bancho mode to its canonical ruleset."""
    mode_id = bounded_integer(mode, "mode", minimum=0, maximum=8)
    try:
        return _RULESETS[mode_id % 4]
    except KeyError as error:
        raise ValueError(f"unsupported bancho mode: {mode_id}") from error


def scoreboard(mode: object) -> tuple[int, Ruleset, ScoreboardVariant]:
    """Map one valid bancho mode to the deterministic bootstrap scoreboard."""
    mode_id = bounded_integer(mode, "mode", minimum=0, maximum=8)
    try:
        return _SCOREBOARD_IDS[mode_id], source_ruleset(mode_id), _VARIANTS[mode_id]
    except KeyError as error:
        raise ValueError(f"unsupported bancho mode: {mode_id}") from error


def beatmap_status(value: object) -> BeatmapStatus:
    """Map bancho's integer rank status, treating update-available as pending."""
    status = bounded_integer(value, "beatmap status", minimum=-1, maximum=5)
    return _BEATMAP_STATUSES[status]


def mod_set(mode: object, legacy_bits: object) -> tuple[int, list[dict[str, object]], bytes, int]:
    """Canonicalize legacy bits while requiring their variant to match the score mode."""
    scoreboard_id, _, expected_variant = scoreboard(mode)
    bits = bounded_integer(legacy_bits, "mods", minimum=0, maximum=2_147_483_647)
    mods, parsed_variant = parse_legacy_mods(bits)
    if parsed_variant.value != expected_variant.value:
        raise ValueError("score mode and assistance mod bits disagree")
    canonical = [
        mod.as_json() for mod in sorted(mods, key=lambda item: item.acronym) if mod.acronym not in {"RX", "AP"}
    ]
    digest = canonical_json_digest(canonical)
    return scoreboard_id, canonical, digest, bits


def canonical_json_digest(value: object) -> bytes:
    """Hash compact, sorted JSON using perfcho's persisted identity rule."""
    encoded = orjson.dumps(
        value,
        option=orjson.OPT_SORT_KEYS,
    )
    return hashlib.sha256(encoded).digest()


def normalized_accuracy(value: object) -> Decimal:
    """Convert a legacy percentage into perfcho's zero-to-one ratio."""
    accuracy = decimal_value(value, "accuracy") / Decimal(100)
    if not Decimal(0) <= accuracy <= Decimal(1):
        raise ValueError("accuracy is outside 0..100 percent")
    return accuracy.quantize(Decimal("0.000000001"))


def score_grade(value: object, *, passed: bool) -> ScoreGrade:
    """Normalize legacy grade strings, using F for failed submissions."""
    if not passed:
        return ScoreGrade.F
    candidate = str(value).upper()
    try:
        return ScoreGrade(candidate)
    except ValueError:
        if candidate == "N":
            return ScoreGrade.F
        raise


def play_styles(value: object) -> list[str]:
    """Expand bancho's Stable play-style flags into canonical labels."""
    bits = bounded_integer(value, "play style", minimum=0, maximum=15)
    names = ((1, "mouse"), (2, "keyboard"), (4, "tablet"), (8, "touch"))
    return [name for bit, name in names if bits & bit]


def bounded_integer(value: object, name: str, *, minimum: int, maximum: int) -> int:
    """Return an integer within inclusive migration bounds."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


def decimal_value(value: object, name: str) -> Decimal:
    """Convert a finite database numeric value without binary-float arithmetic."""
    try:
        result = Decimal(str(value))
    except Exception as error:
        raise ValueError(f"{name} must be numeric") from error
    if not result.is_finite():
        raise ValueError(f"{name} must be finite")
    return result


def json_object(value: object) -> dict[str, object]:
    """Decode a MySQL JSON object while rejecting arrays and scalar values."""
    if value is None:
        return {}
    decoded = orjson.loads(value) if isinstance(value, str) else value
    if not isinstance(decoded, dict):
        raise ValueError("legacy JSON value must be an object")
    return decoded
