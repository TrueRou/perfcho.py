"""Normalize structured mods into deterministic canonical identities."""

import hashlib
import json
from collections.abc import Iterable

from perfcho.modules.scoring.errors import ScoreRejected
from perfcho.modules.scoring.models import CanonicalMod, NormalizedModSet, Ruleset, ScoreboardVariant

LEGACY_MOD_BITS = {
    "NF": 1 << 0,
    "EZ": 1 << 1,
    "TD": 1 << 2,
    "HD": 1 << 3,
    "HR": 1 << 4,
    "SD": 1 << 5,
    "DT": 1 << 6,
    "RX": 1 << 7,
    "HT": 1 << 8,
    "NC": 1 << 9,
    "FL": 1 << 10,
    "AT": 1 << 11,
    "SO": 1 << 12,
    "AP": 1 << 13,
    "PF": 1 << 14,
    "4K": 1 << 15,
    "5K": 1 << 16,
    "6K": 1 << 17,
    "7K": 1 << 18,
    "8K": 1 << 19,
    "FI": 1 << 20,
    "RD": 1 << 21,
    "CN": 1 << 22,
    "TP": 1 << 23,
    "9K": 1 << 24,
    "CO": 1 << 25,
    "1K": 1 << 26,
    "3K": 1 << 27,
    "2K": 1 << 28,
    "SV2": 1 << 29,
    "MR": 1 << 30,
}
_KNOWN_LEGACY_MOD_MASK = sum(LEGACY_MOD_BITS.values())

_KEY_MODS = frozenset({"1K", "2K", "3K", "4K", "5K", "6K", "7K", "8K", "9K", "CO"})
_INCOMPATIBLE_GROUPS = (
    frozenset({"EZ", "HR"}),
    frozenset({"HT", "DT", "NC"}),
    frozenset({"NF", "SD", "PF"}),
)


def canonical_json_digest(value: object) -> bytes:
    """Hash compact sorted JSON exactly as the scoring bootstrap does."""
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()
    return hashlib.sha256(encoded).digest()


def normalize_mods(
    ruleset: Ruleset,
    variant: ScoreboardVariant,
    mods: Iterable[CanonicalMod],
) -> NormalizedModSet:
    """Canonicalize ordering, assistance variants, compatibility, JSON, and bits."""
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
    legacy_bits = 0
    for mod in ordered:
        legacy_bits |= LEGACY_MOD_BITS.get(mod.acronym, 0)
    if variant is ScoreboardVariant.RELAX:
        legacy_bits |= LEGACY_MOD_BITS["RX"]
    elif variant is ScoreboardVariant.AUTOPILOT:
        legacy_bits |= LEGACY_MOD_BITS["AP"]
    if legacy_bits & LEGACY_MOD_BITS["NC"]:
        legacy_bits |= LEGACY_MOD_BITS["DT"]
    if legacy_bits & LEGACY_MOD_BITS["PF"]:
        legacy_bits |= LEGACY_MOD_BITS["SD"]
    return NormalizedModSet(ordered, canonical, canonical_json_digest(canonical), legacy_bits)


def parse_legacy_mods(legacy_bits: int) -> tuple[tuple[CanonicalMod, ...], ScoreboardVariant]:
    """Convert bounded legacy bit flags into canonical mods and a scoreboard variant."""
    if isinstance(legacy_bits, bool) or not isinstance(legacy_bits, int) or legacy_bits < 0:
        raise ValueError("legacy mod bits must be a non-negative integer")
    if legacy_bits & ~_KNOWN_LEGACY_MOD_MASK:
        raise ValueError("legacy mod bits contain unknown flags")
    if legacy_bits & LEGACY_MOD_BITS["RX"] and legacy_bits & LEGACY_MOD_BITS["AP"]:
        raise ValueError("relax and autopilot cannot be combined")
    variant = (
        ScoreboardVariant.AUTOPILOT
        if legacy_bits & LEGACY_MOD_BITS["AP"]
        else ScoreboardVariant.RELAX
        if legacy_bits & LEGACY_MOD_BITS["RX"]
        else ScoreboardVariant.VANILLA
    )
    mods: list[CanonicalMod] = []
    for acronym, bit in LEGACY_MOD_BITS.items():
        if not legacy_bits & bit:
            continue
        if acronym in {"RX", "AP"}:
            mods.append(CanonicalMod(acronym))
        elif (acronym == "DT" and legacy_bits & LEGACY_MOD_BITS["NC"]) or (
            acronym == "SD" and legacy_bits & LEGACY_MOD_BITS["PF"]
        ):
            continue
        else:
            mods.append(CanonicalMod(acronym))
    return tuple(mods), variant


def _settings_sort_key(mod: CanonicalMod) -> str:
    return json.dumps(mod.as_json().get("settings", {}), sort_keys=True, separators=(",", ":"), allow_nan=False)
