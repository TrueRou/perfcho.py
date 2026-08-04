"""Provide a safe registry for deterministic achievement evaluators."""

from collections.abc import Callable, Mapping

from perfcho.modules.social.models import AchievementEvaluationDefinition, ScoreAchievementContext

AchievementEvaluator = Callable[[AchievementEvaluationDefinition, ScoreAchievementContext], Mapping[str, object] | None]


class AchievementEvaluatorRegistry:
    """Dispatch only explicitly registered evaluator code and versions."""

    def __init__(self, evaluators: Mapping[tuple[str, int], AchievementEvaluator] | None = None) -> None:
        """Store the explicit evaluator allowlist."""
        self._evaluators = dict(evaluators or {})

    def evaluate(
        self,
        definition: AchievementEvaluationDefinition,
        context: ScoreAchievementContext,
    ) -> Mapping[str, object] | None:
        """Return evaluator evidence, or None for unknown behavior."""
        evaluator = self._evaluators.get((definition.evaluator_code, definition.evaluator_version))
        return evaluator(definition, context) if evaluator is not None else None


def score_total_at_least(
    definition: AchievementEvaluationDefinition,
    context: ScoreAchievementContext,
) -> Mapping[str, object] | None:
    """Unlock when a passed score reaches an explicitly configured total score."""
    minimum = definition.parameters.get("minimum")
    if isinstance(minimum, bool) or not isinstance(minimum, int) or minimum < 0:
        return None
    if context.outcome != "passed" or context.total_score < minimum:
        return None
    return {
        "evaluator_code": definition.evaluator_code,
        "evaluator_version": definition.evaluator_version,
        "minimum": minimum,
        "total_score": context.total_score,
    }


def default_achievement_evaluator_registry() -> AchievementEvaluatorRegistry:
    """Return the intentionally small built-in evaluator set."""
    return AchievementEvaluatorRegistry({("score_total_at_least", 1): score_total_at_least})
