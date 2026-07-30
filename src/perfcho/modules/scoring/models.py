"""Define immutable canonical score commands, facts, and results."""

import math
import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from types import MappingProxyType

from perfcho.modules.common.models import CommandMeta, JsonValue

_MOD_ACRONYM = re.compile(r"^[A-Z0-9]{1,8}$")
_CALCULATION_QUANTUM = Decimal("0.00001")


class Ruleset(StrEnum):
    """Identify one canonical gameplay ruleset."""

    OSU = "osu"
    TAIKO = "taiko"
    FRUITS = "fruits"
    MANIA = "mania"


class ScoreboardVariant(StrEnum):
    """Identify the gameplay assistance separated into scoreboards."""

    VANILLA = "vanilla"
    RELAX = "relax"
    AUTOPILOT = "autopilot"


class ClientFamily(StrEnum):
    """Identify the protocol family which supplied score evidence."""

    STABLE = "stable"
    LAZER = "lazer"
    WEB = "web"
    API = "api"


class ScoreOutcome(StrEnum):
    """Describe how a play ended."""

    ABANDONED = "abandoned"
    FAILED = "failed"
    PASSED = "passed"


class ScoreGrade(StrEnum):
    """Represent the canonical displayed score grade."""

    N = "N"
    F = "F"
    D = "D"
    C = "C"
    B = "B"
    A = "A"
    S = "S"
    SH = "SH"
    X = "X"
    XH = "XH"


@dataclass(frozen=True, slots=True)
class CanonicalMod:
    """Store one Lazer-facing mod and its immutable settings."""

    acronym: str
    settings: Mapping[str, JsonValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Normalize acronyms and recursively freeze JSON settings."""
        acronym = self.acronym.strip().upper()
        if not _MOD_ACRONYM.fullmatch(acronym):
            raise ValueError("mod acronym must contain 1-8 uppercase ASCII letters or digits")
        object.__setattr__(self, "acronym", acronym)
        frozen = _freeze_json(dict(self.settings))
        if not isinstance(frozen, Mapping):
            raise ValueError("mod settings must be a JSON object")
        object.__setattr__(self, "settings", frozen)

    def as_json(self) -> dict[str, object]:
        """Return the canonical persistence representation."""
        result: dict[str, object] = {"acronym": self.acronym}
        if self.settings:
            result["settings"] = _thaw_json(self.settings)
        return result


@dataclass(frozen=True, slots=True)
class HitStatistic:
    """Store one extensible canonical hit-result count."""

    hit_result: str
    actual: int
    maximum: int | None = None

    def __post_init__(self) -> None:
        """Require a normalized name and coherent nonnegative counts."""
        name = self.hit_result.strip().lower()
        if not name or len(name) > 32 or not all(character.isalnum() or character == "_" for character in name):
            raise ValueError("hit_result must be a non-empty snake-case name")
        if isinstance(self.actual, bool) or self.actual < 0:
            raise ValueError("hit statistic actual count must be nonnegative")
        if self.maximum is not None and (isinstance(self.maximum, bool) or self.maximum < self.actual):
            raise ValueError("hit statistic maximum must not be smaller than actual")
        object.__setattr__(self, "hit_result", name)


@dataclass(frozen=True, slots=True)
class BeatmapReference:
    """Select a current beatmap revision by logical ID, MD5, or both."""

    beatmap_id: int | None = None
    md5: bytes | None = None

    def __post_init__(self) -> None:
        """Require at least one well-formed stable content identity."""
        if self.beatmap_id is None and self.md5 is None:
            raise ValueError("beatmap reference requires beatmap_id or md5")
        if self.beatmap_id is not None and (isinstance(self.beatmap_id, bool) or self.beatmap_id < 1):
            raise ValueError("beatmap_id must be positive")
        if self.md5 is not None and len(self.md5) != 16:
            raise ValueError("beatmap md5 must contain 16 bytes")


@dataclass(frozen=True, slots=True)
class PlayAttemptSubmission:
    """Describe the protocol-level play attempt being completed."""

    idempotency_key: str
    started_at: datetime
    ended_at: datetime
    progress: Decimal
    client_metadata: Mapping[str, JsonValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate the attempt's independent idempotency and timing facts."""
        if not self.idempotency_key or len(self.idempotency_key) > 128:
            raise ValueError("attempt idempotency_key must contain 1-128 characters")
        _require_aware(self.started_at, "attempt started_at")
        _require_aware(self.ended_at, "attempt ended_at")
        if self.ended_at < self.started_at:
            raise ValueError("attempt ended_at must not precede started_at")
        progress = _decimal(self.progress)
        if progress < 0 or progress > 1:
            raise ValueError("attempt progress must be between zero and one")
        metadata = _freeze_json(dict(self.client_metadata))
        if not isinstance(metadata, Mapping):
            raise ValueError("client_metadata must be a JSON object")
        object.__setattr__(self, "progress", progress)
        object.__setattr__(self, "client_metadata", metadata)


@dataclass(frozen=True, slots=True)
class ScoreSubmission:
    """Carry normalized score totals, outcome, grade, and hit facts."""

    total_score: int
    classic_score: int
    accuracy: Decimal
    max_combo: int
    grade: ScoreGrade
    outcome: ScoreOutcome
    perfect: bool
    hits: tuple[HitStatistic, ...]
    client_flags: int = 0
    online_checksum: bytes | None = None

    def __post_init__(self) -> None:
        """Validate scalar ranges before ruleset-aware structural validation."""
        if any(
            isinstance(value, bool) or value < 0 for value in (self.total_score, self.classic_score, self.max_combo)
        ):
            raise ValueError("score totals and max_combo must be nonnegative integers")
        if isinstance(self.client_flags, bool) or self.client_flags < 0:
            raise ValueError("client_flags must be nonnegative")
        accuracy = _decimal(self.accuracy)
        if accuracy < 0 or accuracy > 1:
            raise ValueError("accuracy must be a ratio between zero and one")
        hits = tuple(self.hits)
        names = [statistic.hit_result for statistic in hits]
        if not hits or len(names) != len(set(names)):
            raise ValueError("hit statistics must be non-empty and unique by hit_result")
        if self.online_checksum is not None and len(self.online_checksum) != 16:
            raise ValueError("online_checksum must contain 16 bytes")
        object.__setattr__(self, "accuracy", accuracy)
        object.__setattr__(self, "hits", hits)


@dataclass(frozen=True, slots=True)
class StagedReplayManifest:
    """Reference a replay object already staged in authoritative storage."""

    format: str
    sha256: bytes
    size_bytes: int
    storage_key: str
    client_version: str | None = None

    def __post_init__(self) -> None:
        """Reject incomplete or unsafe object manifests."""
        if not self.format.strip() or len(self.format) > 32:
            raise ValueError("replay format must contain 1-32 characters")
        if len(self.sha256) != 32:
            raise ValueError("replay sha256 must contain 32 bytes")
        if isinstance(self.size_bytes, bool) or self.size_bytes < 0:
            raise ValueError("replay size_bytes must be nonnegative")
        if not self.storage_key or self.storage_key != self.storage_key.strip() or len(self.storage_key) > 512:
            raise ValueError("replay storage_key must be non-empty and trimmed")
        if self.client_version is not None and len(self.client_version) > 64:
            raise ValueError("replay client_version must not exceed 64 characters")


@dataclass(frozen=True, slots=True)
class ScoreAttestation:
    """Carry normalized client and integrity evidence for one score."""

    client_family: ClientFamily
    client_version: str
    verification_state: str
    client_flags: int = 0
    checksum: bytes | None = None
    client_integrity_digest: bytes | None = None
    evidence: Mapping[str, JsonValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate bounded evidence and recursively freeze its JSON value."""
        if not self.client_version or len(self.client_version) > 64:
            raise ValueError("attestation client_version must contain 1-64 characters")
        if self.verification_state not in {"pending", "verified"}:
            raise ValueError("attestation verification_state must be pending or verified")
        if isinstance(self.client_flags, bool) or self.client_flags < 0:
            raise ValueError("attestation client_flags must be nonnegative")
        if self.checksum is not None and len(self.checksum) not in {16, 32}:
            raise ValueError("attestation checksum must contain 16 or 32 bytes")
        if self.client_integrity_digest is not None and len(self.client_integrity_digest) != 32:
            raise ValueError("client_integrity_digest must contain 32 bytes")
        evidence = _freeze_json(dict(self.evidence))
        if not isinstance(evidence, Mapping):
            raise ValueError("attestation evidence must be a JSON object")
        object.__setattr__(self, "evidence", evidence)


@dataclass(frozen=True, slots=True)
class MultiplayerSubmissionContext:
    """Reference one pre-authorized multiplayer score opportunity."""

    attempt_id: uuid.UUID
    token_digest: bytes

    def __post_init__(self) -> None:
        """Require an HMAC-only authorization proof."""
        if len(self.token_digest) != 32:
            raise ValueError("multiplayer token_digest must contain 32 bytes")


@dataclass(frozen=True, slots=True)
class AcceptScore:
    """Request canonical acceptance of one Stable or Lazer score."""

    meta: CommandMeta
    beatmap: BeatmapReference
    ruleset: Ruleset
    variant: ScoreboardVariant
    mods: tuple[CanonicalMod, ...]
    attempt: PlayAttemptSubmission
    score: ScoreSubmission
    replay: StagedReplayManifest
    attestation: ScoreAttestation
    multiplayer: MultiplayerSubmissionContext | None = None

    def __post_init__(self) -> None:
        """Freeze the mod sequence at the command boundary."""
        object.__setattr__(self, "mods", tuple(self.mods))


@dataclass(frozen=True, slots=True)
class BeatmapRevisionInfo:
    """Return immutable current content facts to score acceptance."""

    beatmap_id: int
    revision_id: int
    ruleset: Ruleset
    status: str
    object_count: int
    max_combo: int


@dataclass(frozen=True, slots=True)
class ScoreboardInfo:
    """Return one active canonical scoreboard."""

    scoreboard_id: int
    code: str
    ruleset: Ruleset
    variant: ScoreboardVariant


@dataclass(frozen=True, slots=True)
class NormalizedModSet:
    """Carry deterministic mod JSON, digest, and Stable query bits."""

    mods: tuple[CanonicalMod, ...]
    canonical: tuple[dict[str, object], ...]
    canonical_digest: bytes
    legacy_bits: int


@dataclass(frozen=True, slots=True)
class ModSetInfo:
    """Return a persisted canonical mod-set identity."""

    mod_set_id: int
    scoreboard_id: int
    canonical: tuple[dict[str, object], ...]
    canonical_digest: bytes
    legacy_bits: int


@dataclass(frozen=True, slots=True)
class AccountSubmissionContext:
    """Return authoritative account facts relevant to score acceptance."""

    account_id: int
    country_code: str | None


@dataclass(frozen=True, slots=True)
class ValidatedScore:
    """Return ruleset-derived score values used by persistence and events."""

    accuracy: Decimal
    grade: ScoreGrade
    total_hits: int


@dataclass(frozen=True, slots=True)
class PerformanceCalculationInput:
    """Provide immutable score, release, and beatmap facts to one calculator."""

    job_id: uuid.UUID
    score_id: int
    attempt_count: int
    formula_id: uuid.UUID
    formula_code: str
    calculator: str
    release_id: uuid.UUID
    release_version: str
    artifact_digest: bytes
    release_configuration: Mapping[str, JsonValue]
    difficulty_formula_id: uuid.UUID
    difficulty_formula_code: str
    difficulty_release_id: uuid.UUID
    difficulty_release_version: str
    difficulty_artifact_digest: bytes
    difficulty_release_configuration: Mapping[str, JsonValue]
    input_digest: bytes
    beatmap_revision_id: int
    beatmap_sha256: bytes
    beatmap_storage_key: str
    scoreboard: ScoreboardInfo
    mod_set_id: int
    mods: tuple[CanonicalMod, ...]
    client_family: ClientFamily
    score: ScoreSubmission

    def __post_init__(self) -> None:
        """Validate identities, digests, and recursively immutable configuration."""
        if self.score_id < 1 or self.attempt_count < 1 or self.beatmap_revision_id < 1 or self.mod_set_id < 1:
            raise ValueError("performance calculation identifiers and attempt count must be positive")
        if (
            not self.formula_code
            or not self.calculator
            or not self.release_version
            or not self.difficulty_formula_code
            or not self.difficulty_release_version
            or not self.beatmap_storage_key
        ):
            raise ValueError("performance calculation release metadata must be non-empty")
        if (
            len(self.artifact_digest) != 32
            or len(self.difficulty_artifact_digest) != 32
            or len(self.input_digest) != 32
            or len(self.beatmap_sha256) != 32
        ):
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
            "artifact_digest": self.artifact_digest.hex(),
            "release_configuration": _thaw_json(self.release_configuration),
            "difficulty_formula_id": str(self.difficulty_formula_id),
            "difficulty_formula_code": self.difficulty_formula_code,
            "difficulty_release_id": str(self.difficulty_release_id),
            "difficulty_release_version": self.difficulty_release_version,
            "difficulty_artifact_digest": self.difficulty_artifact_digest.hex(),
            "difficulty_release_configuration": _thaw_json(self.difficulty_release_configuration),
            "beatmap_revision_id": self.beatmap_revision_id,
            "beatmap_sha256": self.beatmap_sha256.hex(),
            "ruleset": self.scoreboard.ruleset.value,
            "variant": self.scoreboard.variant.value,
            "mod_set_id": self.mod_set_id,
            "mods": [mod.as_json() for mod in self.mods],
            "client_family": self.client_family.value,
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
class AcceptedScoreResult:
    """Return stable identities from an accepted score transaction."""

    attempt_id: uuid.UUID
    score_id: int
    beatmap_id: int
    beatmap_revision_id: int
    scoreboard_id: int
    mod_set_id: int
    outcome: ScoreOutcome


@dataclass(frozen=True, slots=True)
class PlayAttemptRecord:
    """Carry a fully resolved play attempt into persistence."""

    attempt_id: uuid.UUID
    account_id: int
    beatmap_id: int
    beatmap_revision_id: int
    scoreboard_id: int
    mod_set_id: int
    protocol: ClientFamily
    submission: PlayAttemptSubmission
    outcome: ScoreOutcome


@dataclass(frozen=True, slots=True)
class AttemptClaim:
    """Describe a new/open attempt or its already accepted score."""

    attempt_id: uuid.UUID
    prior_result: AcceptedScoreResult | None = None


@dataclass(frozen=True, slots=True)
class ScoreAcceptanceRecord:
    """Carry all validated immutable score facts into one repository write."""

    attempt_id: uuid.UUID
    account_id: int
    revision: BeatmapRevisionInfo
    scoreboard: ScoreboardInfo
    mod_set: ModSetInfo
    attempt: PlayAttemptSubmission
    score: ScoreSubmission
    replay: StagedReplayManifest
    attestation: ScoreAttestation
    validated: ValidatedScore
    processed_at: datetime


@dataclass(frozen=True, slots=True)
class PerformanceCompletion:
    """Return persisted formula identities needed to publish completion."""

    score_id: int
    scoreboard_id: int
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


@dataclass(frozen=True, slots=True)
class AcceptanceClaim:
    """Describe a new command receipt or its exact completed result."""

    prior_result: AcceptedScoreResult | None = None


@dataclass(frozen=True, slots=True)
class ReplayReference:
    """Describe one ready replay object and its score ownership."""

    score_id: int
    owner_account_id: int
    ruleset: Ruleset
    storage_key: str
    size_bytes: int
    format: str

    def __post_init__(self) -> None:
        """Require usable score and object identities."""
        if self.score_id < 1 or self.owner_account_id < 1:
            raise ValueError("replay reference identifiers must be positive")
        if not self.storage_key or self.size_bytes < 0 or not self.format:
            raise ValueError("replay reference object metadata is invalid")


@dataclass(frozen=True, slots=True)
class LeaderboardScoreView:
    """Describe one Stable-compatible projected leaderboard score."""

    score_id: int
    account_id: int
    display_name: str
    metric_value: Decimal
    max_combo: int
    n50: int
    n100: int
    n300: int
    nmiss: int
    nkatu: int
    ngeki: int
    perfect: bool
    legacy_mod_bits: int
    rank: int
    ended_at: datetime
    has_replay: bool


@dataclass(frozen=True, slots=True)
class LeaderboardPage:
    """Return a bounded projected leaderboard and optional personal best."""

    scores: tuple[LeaderboardScoreView, ...]
    personal_best: LeaderboardScoreView | None


@dataclass(frozen=True, slots=True)
class BeatmapGradeView:
    """Describe the grade of one projected vanilla personal best."""

    beatmap_id: int
    ruleset: Ruleset
    grade: ScoreGrade


@dataclass(frozen=True, slots=True)
class AccountStatsView:
    """Describe Stable-compatible score totals without protocol fields."""

    ranked_score: int
    accuracy: Decimal
    play_count: int
    total_score: int
    global_rank: int
    performance: int = 0

    def __post_init__(self) -> None:
        """Require non-negative totals and normalized accuracy."""
        if min(self.ranked_score, self.play_count, self.total_score, self.global_rank, self.performance) < 0:
            raise ValueError("account statistics must be non-negative")
        if not Decimal(0) <= self.accuracy <= Decimal(1):
            raise ValueError("account statistics accuracy must be between zero and one")


@dataclass(frozen=True, slots=True)
class AnticheatAnalysisRequested:
    """Define the future anti-cheat event contract without implementing a consumer."""

    source_event_id: uuid.UUID
    score_id: int
    account_id: int
    replay_sha256: bytes
    schema_version: int = 1


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


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


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
