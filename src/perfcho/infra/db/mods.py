"""Project canonical mods into deterministic persistence and compatibility values."""

import hashlib
from collections.abc import Iterable

import orjson

from perfcho.modules.scoring.models import CanonicalMod, ScoreboardVariant

_COMPATIBILITY_BITS = {
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


def canonical_mods_details(mods: Iterable[CanonicalMod]) -> list[dict[str, object]]:
    """Return canonical mod JSON sorted by unique acronym."""
    by_acronym = _canonical_mods_by_acronym(mods)
    return [by_acronym[acronym].as_json() for acronym in sorted(by_acronym)]


def canonical_mods_acronyms(mods: Iterable[CanonicalMod]) -> list[str]:
    """Return sorted unique canonical mod acronyms."""
    return sorted(_canonical_mods_by_acronym(mods))


def canonical_mods_digest(mods: Iterable[CanonicalMod]) -> bytes:
    """Hash compact canonical mod JSON with recursively sorted object keys."""
    details = canonical_mods_details(mods)
    return hashlib.sha256(orjson.dumps(details, option=orjson.OPT_SORT_KEYS)).digest()


def project_scoreboard_variant(mods: Iterable[CanonicalMod]) -> ScoreboardVariant:
    """Project assistance mods into the retained Stable scoreboard variant."""
    acronyms = set(canonical_mods_acronyms(mods))
    if "RX" in acronyms:
        return ScoreboardVariant.RELAX
    if "AP" in acronyms:
        return ScoreboardVariant.AUTOPILOT
    return ScoreboardVariant.VANILLA


def project_legacy_mod_bits(mods: Iterable[CanonicalMod]) -> int:
    """Project canonical mods into the retained Stable compatibility bitset."""
    canonical_mods = tuple(mods)
    _canonical_mods_by_acronym(canonical_mods)
    bits = 0
    for mod in canonical_mods:
        bits |= _COMPATIBILITY_BITS.get(mod.acronym, 0)
    if bits & _COMPATIBILITY_BITS["NC"]:
        bits |= _COMPATIBILITY_BITS["DT"]
    if bits & _COMPATIBILITY_BITS["PF"]:
        bits |= _COMPATIBILITY_BITS["SD"]
    return bits


def _canonical_mods_by_acronym(mods: Iterable[CanonicalMod]) -> dict[str, CanonicalMod]:
    by_acronym: dict[str, CanonicalMod] = {}
    for mod in mods:
        if mod.acronym in by_acronym:
            raise ValueError(f"duplicate mod acronym: {mod.acronym}")
        by_acronym[mod.acronym] = mod
    if "RX" in by_acronym and "AP" in by_acronym:
        raise ValueError("relax and autopilot cannot be combined")
    return by_acronym
