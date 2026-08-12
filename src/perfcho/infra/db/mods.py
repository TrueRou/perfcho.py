"""Project canonical mod sets into database compatibility columns."""

from collections.abc import Iterable

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


def project_legacy_mod_bits(mods: Iterable[CanonicalMod], variant: ScoreboardVariant) -> int:
    """Populate the retained database bitset from canonical gameplay values."""
    bits = 0
    for mod in mods:
        bits |= _COMPATIBILITY_BITS.get(mod.acronym, 0)
    if variant is ScoreboardVariant.RELAX:
        bits |= _COMPATIBILITY_BITS["RX"]
    elif variant is ScoreboardVariant.AUTOPILOT:
        bits |= _COMPATIBILITY_BITS["AP"]
    if bits & _COMPATIBILITY_BITS["NC"]:
        bits |= _COMPATIBILITY_BITS["DT"]
    if bits & _COMPATIBILITY_BITS["PF"]:
        bits |= _COMPATIBILITY_BITS["SD"]
    return bits
