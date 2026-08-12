"""Define immutable multi-formula performance inputs and results."""

import math
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from types import MappingProxyType

from perfcho.modules.common.models import JsonValue
from perfcho.modules.scoring.models import CanonicalMod, Ruleset, ScoreSubmission

_CALCULATION_QUANTUM = Decimal("0.00001")


@dataclass(frozen=True, slots=True)
class PerformanceCalculationInput:
    """Provide immutable score, release, and beatmap facts to one calculator."""

    score_id: int
    account_id: int
    formula_id: uuid.UUID
    formula_code: str
    calculator: str
    release_id: uuid.UUID
    release_version: str
    release_configuration: Mapping[str, JsonValue]
    difficulty_formula_id: uuid.UUID
    difficulty_formula_code: str
    difficulty_release_id: uuid.UUID
    difficulty_release_version: str
    difficulty_release_configuration: Mapping[str, JsonValue]
    input_digest: bytes
    beatmap_revision_id: int
    beatmap_sha256: bytes
    beatmap_storage_key: str
    ruleset: Ruleset
    mods_digest: bytes
    mods: tuple[CanonicalMod, ...]
    source: str
    score: ScoreSubmission

    def __post_init__(self) -> None:
        """Validate identities, digests, and recursively immutable configuration."""
        if self.score_id < 1 or self.beatmap_revision_id < 1:
            raise ValueError("performance calculation identifiers must be positive")
        if (
            not self.formula_code
            or not self.calculator
            or not self.release_version
            or not self.difficulty_formula_code
            or not self.difficulty_release_version
            or not self.beatmap_storage_key
        ):
            raise ValueError("performance calculation release metadata must be non-empty")
        if len(self.input_digest) != 32 or len(self.beatmap_sha256) != 32 or len(self.mods_digest) != 32:
            raise ValueError("performance calculation digests must contain 32 bytes")
        configuration = _freeze_json(dict(self.release_configuration))
        difficulty_configuration = _freeze_json(dict(self.difficulty_release_configuration))
        if not isinstance(configuration, Mapping) or not isinstance(difficulty_configuration, Mapping):
            raise ValueError("release configuration must be a JSON object")
        object.__setattr__(self, "release_configuration", configuration)
        object.__setattr__(self, "difficulty_release_configuration", difficulty_configuration)
        object.__setattr__(self, "mods", tuple(self.mods))

    def digest_payload(self) -> dict[str, object]:
        """Return algorithm inputs without transport or retry identities."""
        return {
            "schema_version": 1,
            "formula_id": str(self.formula_id),
            "formula_code": self.formula_code,
            "calculator": self.calculator,
            "release_id": str(self.release_id),
            "release_version": self.release_version,
            "release_configuration": _thaw_json(self.release_configuration),
            "difficulty_formula_id": str(self.difficulty_formula_id),
            "difficulty_formula_code": self.difficulty_formula_code,
            "difficulty_release_id": str(self.difficulty_release_id),
            "difficulty_release_version": self.difficulty_release_version,
            "difficulty_release_configuration": _thaw_json(self.difficulty_release_configuration),
            "beatmap_revision_id": self.beatmap_revision_id,
            "beatmap_sha256": self.beatmap_sha256.hex(),
            "ruleset": self.ruleset.value,
            "mods_digest": self.mods_digest.hex(),
            "mods": [mod.as_json() for mod in self.mods],
            "client_family": self.source,
            "score": {
                "total_score": self.score.total_score,
                "classic_score": self.score.classic_score,
                "accuracy": str(self.score.accuracy),
                "max_combo": self.score.max_combo,
                "outcome": self.score.outcome.value,
                "hits": [
                    {
                        "hit_result": statistic.hit_result,
                        "actual": statistic.actual,
                        "maximum": statistic.maximum,
                    }
                    for statistic in self.score.hits
                ],
            },
        }


@dataclass(frozen=True, slots=True)
class DifficultyCalculationResult:
    """Describe difficulty attributes returned with one PP calculation."""

    star_rating: Decimal
    max_combo: int
    attributes: Mapping[str, JsonValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Require finite nonnegative difficulty values and immutable details."""
        star_rating = _decimal(self.star_rating).quantize(_CALCULATION_QUANTUM)
        if star_rating < 0 or isinstance(self.max_combo, bool) or self.max_combo < 0:
            raise ValueError("difficulty values must be nonnegative")
        attributes = _freeze_json(dict(self.attributes))
        if not isinstance(attributes, Mapping):
            raise ValueError("difficulty attributes must be a JSON object")
        object.__setattr__(self, "star_rating", star_rating)
        object.__setattr__(self, "attributes", attributes)


@dataclass(frozen=True, slots=True)
class PerformanceResult:
    """Describe deterministic difficulty and PP output from one calculator."""

    pp: Decimal
    difficulty: DifficultyCalculationResult
    breakdown: Mapping[str, JsonValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Require nonnegative finite PP and immutable JSON details."""
        pp = _decimal(self.pp).quantize(_CALCULATION_QUANTUM)
        if pp < 0:
            raise ValueError("performance pp must be nonnegative")
        breakdown = _freeze_json(dict(self.breakdown))
        if not isinstance(breakdown, Mapping):
            raise ValueError("performance breakdown must be a JSON object")
        object.__setattr__(self, "pp", pp)
        object.__setattr__(self, "breakdown", breakdown)


@dataclass(frozen=True, slots=True)
class PerformanceCompletion:
    """Return persisted formula identities needed to publish completion."""

    score_id: int
    account_id: int
    ruleset: Ruleset
    formula_id: uuid.UUID
    formula_code: str
    release_id: uuid.UUID
    pp: Decimal
    output_digest: bytes


@dataclass(frozen=True, slots=True)
class ScorePerformanceView:
    """Expose one Formula-owned PP result without persistence entities."""

    score_id: int
    formula_id: uuid.UUID
    formula_code: str
    formula_name: str
    calculator: str
    release_id: uuid.UUID
    ruleset: Ruleset
    release_version: str
    release_active: bool
    pp: Decimal
    breakdown: Mapping[str, JsonValue]

    def __post_init__(self) -> None:
        """Validate bounded display metadata and freeze the result breakdown."""
        if self.score_id < 1 or not self.formula_code or not self.formula_name or not self.calculator:
            raise ValueError("score performance identity metadata is invalid")
        if not self.release_version:
            raise ValueError("score performance release version is empty")
        pp = _decimal(self.pp).quantize(_CALCULATION_QUANTUM)
        if pp < 0:
            raise ValueError("score performance pp must be nonnegative")
        breakdown = _freeze_json(dict(self.breakdown))
        if not isinstance(breakdown, Mapping):
            raise ValueError("score performance breakdown must be a JSON object")
        object.__setattr__(self, "pp", pp)
        object.__setattr__(self, "breakdown", breakdown)


def thaw_json_mapping(value: Mapping[str, JsonValue]) -> dict[str, object]:
    """Return mutable JSON for infrastructure persistence."""
    thawed = _thaw_json(value)
    if not isinstance(thawed, dict):
        raise TypeError("expected a JSON object")
    return thawed


def _decimal(value: Decimal | int | float | str) -> Decimal:
    decimal = value if isinstance(value, Decimal) else Decimal(str(value))
    if not decimal.is_finite():
        raise ValueError("numeric values must be finite")
    return decimal


def _freeze_json(value: object) -> object:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("JSON numbers must be finite")
        return value
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            raise ValueError("JSON object keys must be strings")
        return MappingProxyType({key: _freeze_json(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json(item) for item in value)
    raise ValueError(f"unsupported JSON value: {type(value).__name__}")


def _thaw_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value
