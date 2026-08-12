"""Validate canonical score structure independently of protocol adapters."""

from decimal import Decimal

from perfcho.modules.scoring.errors import ScoreRejected
from perfcho.modules.scoring.models import (
    BeatmapRevisionInfo,
    CanonicalMod,
    PlayAttemptSubmission,
    Ruleset,
    ScoreboardVariant,
    ScoreGrade,
    ScoreOutcome,
    ScoreSubmission,
    ValidatedScore,
)

_ACCURACY_TOLERANCE = Decimal("0.0000005")
_REQUIRED_HITS = {
    Ruleset.OSU: frozenset({"great", "ok", "meh", "miss"}),
    Ruleset.TAIKO: frozenset({"great", "ok", "miss"}),
    Ruleset.FRUITS: frozenset({"great", "large_tick_hit", "small_tick_hit", "small_tick_miss", "miss"}),
    Ruleset.MANIA: frozenset({"perfect", "great", "good", "ok", "meh", "miss"}),
}


def validate_score(
    ruleset: Ruleset,
    mods: tuple[CanonicalMod, ...],
    attempt: PlayAttemptSubmission,
    score: ScoreSubmission,
    revision: BeatmapRevisionInfo,
    variant: ScoreboardVariant = ScoreboardVariant.VANILLA,
    *,
    uses_threshold_grading: bool = False,
) -> ValidatedScore:
    """Validate hits, derived accuracy, grade, outcome, progress, and full combo."""
    values = {statistic.hit_result: statistic.actual for statistic in score.hits}
    missing = _REQUIRED_HITS[ruleset] - values.keys()
    if missing:
        raise ScoreRejected(f"missing required hit statistics: {', '.join(sorted(missing))}")
    total_hits = _judged_total(ruleset, values)
    if total_hits <= 0:
        raise ScoreRejected("score must contain at least one judged hit")
    if ruleset in {Ruleset.OSU, Ruleset.TAIKO, Ruleset.MANIA}:
        if score.outcome is ScoreOutcome.PASSED and total_hits != revision.object_count:
            raise ScoreRejected("passed score hit count does not match the beatmap revision")
        if score.outcome is not ScoreOutcome.PASSED and total_hits > revision.object_count:
            raise ScoreRejected("score hit count exceeds the beatmap revision")

    accuracy = _accuracy(ruleset, values, {mod.acronym for mod in mods})
    if abs(score.accuracy - accuracy) > _ACCURACY_TOLERANCE:
        raise ScoreRejected("submitted accuracy does not match hit statistics")

    if score.outcome is ScoreOutcome.PASSED:
        if attempt.progress != 1:
            raise ScoreRejected("passed scores must report complete attempt progress")
        expected_grade = (
            _threshold_passed_grade(ruleset, values, accuracy, mods)
            if uses_threshold_grading
            else _passed_grade(ruleset, values, accuracy, mods)
        )
    elif score.outcome is ScoreOutcome.FAILED:
        expected_grade = ScoreGrade.F
    else:
        expected_grade = ScoreGrade.N
        if attempt.progress == 1:
            raise ScoreRejected("abandoned scores cannot report complete progress")
    if score.grade is not expected_grade:
        raise ScoreRejected(f"grade {score.grade} does not match derived grade {expected_grade}")

    misses = values["miss"] + values.get("small_tick_miss", 0) + values.get("large_tick_miss", 0)
    if variant is ScoreboardVariant.VANILLA and revision.max_combo > 0 and score.max_combo > revision.max_combo:
        raise ScoreRejected("score combo exceeds the beatmap revision")
    if score.perfect:
        if score.outcome is not ScoreOutcome.PASSED or misses != 0:
            raise ScoreRejected("perfect scores must pass without misses")
        if variant is ScoreboardVariant.VANILLA and revision.max_combo > 0 and score.max_combo != revision.max_combo:
            raise ScoreRejected("perfect score combo does not match the beatmap revision")
    return ValidatedScore(accuracy, expected_grade, total_hits)


def _threshold_passed_grade(
    ruleset: Ruleset,
    values: dict[str, int],
    accuracy: Decimal,
    mods: tuple[CanonicalMod, ...],
) -> ScoreGrade:
    """Apply the supplied threshold-based grade policy."""
    if accuracy == 1:
        grade = ScoreGrade.X
    elif accuracy >= Decimal("0.95"):
        grade = ScoreGrade.S
    elif accuracy >= Decimal("0.90"):
        grade = ScoreGrade.A
    elif accuracy >= Decimal("0.80"):
        grade = ScoreGrade.B
    elif accuracy >= Decimal("0.70"):
        grade = ScoreGrade.C
    else:
        grade = ScoreGrade.D

    if ruleset in {Ruleset.OSU, Ruleset.TAIKO} and grade in {ScoreGrade.S, ScoreGrade.X} and values["miss"] > 0:
        grade = ScoreGrade.A
    elif ruleset is Ruleset.MANIA and grade is ScoreGrade.S:
        imperfect = sum(values[name] for name in ("good", "ok", "meh", "miss"))
        if imperfect == 0:
            grade = ScoreGrade.X

    silver = bool({mod.acronym for mod in mods} & {"HD", "FL", "FI"})
    if silver and grade is ScoreGrade.X:
        return ScoreGrade.XH
    if silver and grade is ScoreGrade.S:
        return ScoreGrade.SH
    return grade


def _judged_total(ruleset: Ruleset, values: dict[str, int]) -> int:
    names: tuple[str, ...]
    if ruleset is Ruleset.OSU:
        names = ("great", "ok", "meh", "miss")
    elif ruleset is Ruleset.TAIKO:
        names = ("great", "ok", "miss")
    elif ruleset is Ruleset.FRUITS:
        names = ("great", "large_tick_hit", "small_tick_hit", "small_tick_miss", "large_tick_miss", "miss")
    else:
        names = ("perfect", "great", "good", "ok", "meh", "miss")
    return sum(values.get(name, 0) for name in names)


def _accuracy(ruleset: Ruleset, values: dict[str, int], mods: set[str]) -> Decimal:
    total = Decimal(_judged_total(ruleset, values))
    numerator: Decimal
    if ruleset is Ruleset.OSU:
        numerator = Decimal(300 * values["great"] + 100 * values["ok"] + 50 * values["meh"])
        denominator = 300 * total
    elif ruleset is Ruleset.TAIKO:
        numerator = Decimal(values["great"]) + Decimal("0.5") * values["ok"]
        denominator = total
    elif ruleset is Ruleset.FRUITS:
        numerator = Decimal(values["great"] + values["large_tick_hit"] + values["small_tick_hit"])
        denominator = total
    elif "SV2" in mods:
        numerator = Decimal(
            305 * values["perfect"]
            + 300 * values["great"]
            + 200 * values["good"]
            + 100 * values["ok"]
            + 50 * values["meh"]
        )
        denominator = 305 * total
    else:
        numerator = Decimal(
            300 * (values["perfect"] + values["great"]) + 200 * values["good"] + 100 * values["ok"] + 50 * values["meh"]
        )
        denominator = 300 * total
    return Decimal(numerator) / Decimal(denominator)


def _passed_grade(
    ruleset: Ruleset,
    values: dict[str, int],
    accuracy: Decimal,
    mods: tuple[CanonicalMod, ...],
) -> ScoreGrade:
    if ruleset is Ruleset.OSU:
        total = Decimal(_judged_total(ruleset, values))
        great_ratio = Decimal(values["great"]) / total
        meh_ratio = Decimal(values["meh"]) / total
        if accuracy == 1:
            grade = ScoreGrade.X
        elif great_ratio > Decimal("0.9") and meh_ratio < Decimal("0.01") and values["miss"] == 0:
            grade = ScoreGrade.S
        elif (great_ratio > Decimal("0.8") and values["miss"] == 0) or great_ratio > Decimal("0.9"):
            grade = ScoreGrade.A
        elif (great_ratio > Decimal("0.7") and values["miss"] == 0) or great_ratio > Decimal("0.8"):
            grade = ScoreGrade.B
        elif great_ratio > Decimal("0.6"):
            grade = ScoreGrade.C
        else:
            grade = ScoreGrade.D
    else:
        thresholds = {
            Ruleset.TAIKO: (Decimal("0.95"), Decimal("0.90"), Decimal("0.80"), Decimal("0.70")),
            Ruleset.FRUITS: (Decimal("0.98"), Decimal("0.94"), Decimal("0.90"), Decimal("0.85")),
            Ruleset.MANIA: (Decimal("0.95"), Decimal("0.90"), Decimal("0.80"), Decimal("0.70")),
        }[ruleset]
        if accuracy == 1:
            grade = ScoreGrade.X
        elif accuracy > thresholds[0]:
            grade = ScoreGrade.S
        elif accuracy > thresholds[1]:
            grade = ScoreGrade.A
        elif accuracy > thresholds[2]:
            grade = ScoreGrade.B
        elif accuracy > thresholds[3]:
            grade = ScoreGrade.C
        else:
            grade = ScoreGrade.D
    silver = bool({mod.acronym for mod in mods} & {"HD", "FL", "FI"})
    if silver and grade is ScoreGrade.X:
        return ScoreGrade.XH
    if silver and grade is ScoreGrade.S:
        return ScoreGrade.SH
    return grade
