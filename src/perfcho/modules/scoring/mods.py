"""Normalize structured mods into deterministic canonical identities."""

import hashlib
from collections.abc import Iterable

import orjson

from perfcho.modules.scoring.errors import ScoreRejected
from perfcho.modules.scoring.models import CanonicalMod, NormalizedModSet, Ruleset, ScoreboardVariant

_KEY_MODS = frozenset({"1K", "2K", "3K", "4K", "5K", "6K", "7K", "8K", "9K", "CO"})
_INCOMPATIBLE_GROUPS = (
    frozenset({"EZ", "HR"}),
    frozenset({"HT", "DT", "NC"}),
    frozenset({"NF", "SD", "PF"}),
)


def canonical_json_digest(value: object) -> bytes:
    """Hash compact sorted JSON exactly as the scoring bootstrap does."""
    encoded = orjson.dumps(
        value,
        option=orjson.OPT_SORT_KEYS,
    )
    return hashlib.sha256(encoded).digest()


def normalize_mods(
    ruleset: Ruleset,
    variant: ScoreboardVariant,
    mods: Iterable[CanonicalMod],
) -> NormalizedModSet:
    """Canonicalize ordering, assistance variants, compatibility, and JSON."""
    by_acronym: dict[str, CanonicalMod] = {}
    for mod in mods:
        if mod.acronym in by_acronym:
            raise ScoreRejected(f"duplicate mod acronym: {mod.acronym}")
        by_acronym[mod.acronym] = mod

    assistance = {name for name in ("RX", "AP") if name in by_acronym}
    if len(assistance) > 1:
        raise ScoreRejected("relax and autopilot cannot be combined")
    expected_assistance = {
        ScoreboardVariant.VANILLA: None,
        ScoreboardVariant.RELAX: "RX",
        ScoreboardVariant.AUTOPILOT: "AP",
    }[variant]
    supplied_assistance = next(iter(assistance), None)
    if supplied_assistance is not None and supplied_assistance != expected_assistance:
        raise ScoreRejected("assistance mod conflicts with the selected scoreboard variant")
    if supplied_assistance is not None:
        by_acronym.pop(supplied_assistance)

    if variant is ScoreboardVariant.RELAX and ruleset is Ruleset.MANIA:
        raise ScoreRejected("mania has no relax scoreboard")
    if variant is ScoreboardVariant.AUTOPILOT and ruleset is not Ruleset.OSU:
        raise ScoreRejected("autopilot is only supported by osu")
    if {"AT", "CN"} & by_acronym.keys():
        raise ScoreRejected("automatic play mods cannot submit scores")

    acronyms = frozenset(by_acronym)
    for group in _INCOMPATIBLE_GROUPS:
        selected = group & acronyms
        if len(selected) > 1:
            raise ScoreRejected(f"incompatible mods: {', '.join(sorted(selected))}")
    selected_keys = _KEY_MODS & acronyms
    if ruleset is not Ruleset.MANIA and selected_keys:
        raise ScoreRejected("key-count mods are only valid for mania")
    if len(selected_keys) > 1:
        raise ScoreRejected("multiple key-count mods cannot be combined")

    ordered = tuple(sorted(by_acronym.values(), key=lambda mod: (mod.acronym, _settings_sort_key(mod))))
    canonical = tuple(mod.as_json() for mod in ordered)
    return NormalizedModSet(
        canonical=canonical,
        mods_acronyms=frozenset(by_acronym),
        digest=canonical_json_digest(canonical),
    )


def _settings_sort_key(mod: CanonicalMod) -> str:
    return orjson.dumps(mod.as_json().get("settings", {}), option=orjson.OPT_SORT_KEYS).decode()
