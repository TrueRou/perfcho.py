"""Define the beatmap ranking status state machine."""

from perfcho.infra.db.enums import BeatmapStatus

# Legal ranking state machine transitions, keyed by source status.
_TRANSITIONS: dict[BeatmapStatus, frozenset[BeatmapStatus]] = {
    BeatmapStatus.GRAVEYARD: frozenset({BeatmapStatus.PENDING, BeatmapStatus.WIP}),
    BeatmapStatus.WIP: frozenset({BeatmapStatus.PENDING, BeatmapStatus.GRAVEYARD}),
    BeatmapStatus.PENDING: frozenset({BeatmapStatus.WIP, BeatmapStatus.QUALIFIED, BeatmapStatus.GRAVEYARD}),
    BeatmapStatus.QUALIFIED: frozenset({BeatmapStatus.RANKED, BeatmapStatus.PENDING}),
    BeatmapStatus.RANKED: frozenset({BeatmapStatus.LOVED, BeatmapStatus.GRAVEYARD}),
    BeatmapStatus.LOVED: frozenset({BeatmapStatus.RANKED}),
    # `approved` is a historical synonym for `ranked` and has no local command targets.
    BeatmapStatus.APPROVED: frozenset(),
}


def is_valid_transition(current: str, target: str) -> bool:
    """Return whether a ranking transition is permitted."""
    try:
        return BeatmapStatus(target) in _TRANSITIONS[BeatmapStatus(current)]
    except ValueError:
        return False
