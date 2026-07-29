"""Define expected score-acceptance failures."""

from perfcho.modules.common.errors import InputRejected, ResourceConflict, ResourceNotFound


class ScoreRejected(InputRejected):
    """Reject gameplay facts that are structurally inconsistent."""

    code = "score_rejected"


class BeatmapRevisionNotFound(ResourceNotFound):
    """Indicate that no current immutable revision matches the submission."""

    code = "beatmap_revision_not_found"


class ScoreboardUnavailable(ResourceNotFound):
    """Indicate that the requested canonical scoreboard is not active."""

    code = "scoreboard_unavailable"


class AttemptIdempotencyConflict(ResourceConflict):
    """Reject reuse of a play-attempt key for different dimensions."""

    code = "attempt_idempotency_conflict"


class MultiplayerContextRejected(InputRejected):
    """Reject an invalid, expired, or dimension-mismatched multiplayer attempt."""

    code = "multiplayer_context_rejected"


class ReplayNotFound(ResourceNotFound):
    """Indicate that a score has no ready authoritative replay object."""

    code = "replay_not_found"
