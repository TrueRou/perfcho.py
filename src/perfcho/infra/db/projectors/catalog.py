"""Compose the complete catalog of database outbox projectors."""

from collections.abc import Awaitable, Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from types import MappingProxyType

from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.models.events import OutboxEvent
from perfcho.infra.db.projectors import (
    account,
    community,
    content,
    identity,
    management,
    multiplayer,
    performance,
    ranking,
    scoring_stats,
    social,
)

type ConsumerHandler = Callable[[AsyncSession, OutboxEvent, str], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class ConsumerRegistration:
    """Bind one versioned consumer name to accepted event types."""

    name: str
    event_types: frozenset[str]
    handler: ConsumerHandler


class ConsumerCatalog(Mapping[str, ConsumerRegistration]):
    """Provide an immutable, validated registry of outbox consumers."""

    def __init__(self, registrations: Iterable[ConsumerRegistration]) -> None:
        """Validate and index all explicitly composed consumers."""
        indexed: dict[str, ConsumerRegistration] = {}
        for registration in registrations:
            if not registration.name:
                raise ValueError("Outbox consumer names must not be empty")
            if not registration.event_types:
                raise ValueError(f"Outbox consumer must accept at least one event type: {registration.name}")
            if registration.name in indexed:
                raise ValueError(f"Outbox consumer is already registered: {registration.name}")
            indexed[registration.name] = registration
        self._registrations = MappingProxyType(indexed)

    def __getitem__(self, name: str) -> ConsumerRegistration:
        """Return the registration for one consumer name."""
        return self._registrations[name]

    def __iter__(self) -> Iterator[str]:
        """Iterate registered consumer names."""
        return iter(self._registrations)

    def __len__(self) -> int:
        """Return the number of registered consumers."""
        return len(self._registrations)


def build_consumer_catalog(
    performance_handler: ConsumerHandler = performance.unconfigured_projector,
) -> ConsumerCatalog:
    """Compose all consumers with the runtime-owned Performance handler."""
    return ConsumerCatalog(
        (
            ConsumerRegistration(account.CONSUMER_NAME, account.EVENT_TYPES, account.project_account_event),
            ConsumerRegistration(identity.CONSUMER_NAME, identity.EVENT_TYPES, identity.project_identity_event),
            ConsumerRegistration(content.CONSUMER_NAME, content.EVENT_TYPES, content.project_content_event),
            ConsumerRegistration(social.SOCIAL_CONSUMER_NAME, social.SOCIAL_EVENT_TYPES, social.project_social_event),
            ConsumerRegistration(
                social.ACHIEVEMENT_CONSUMER_NAME,
                social.ACHIEVEMENT_EVENT_TYPES,
                social.project_achievement_event,
            ),
            ConsumerRegistration(
                community.COMMUNITY_CONSUMER_NAME,
                community.COMMUNITY_EVENT_TYPES,
                community.project_community_event,
            ),
            ConsumerRegistration(
                community.MESSAGE_CONSUMER_NAME,
                community.MESSAGE_EVENT_TYPES,
                community.project_community_message,
            ),
            ConsumerRegistration(performance.CONSUMER_NAME, performance.EVENT_TYPES, performance_handler),
            ConsumerRegistration(ranking.CONSUMER_NAME, ranking.EVENT_TYPES, ranking.project_accepted_score),
            ConsumerRegistration(
                scoring_stats.CONSUMER_NAME,
                scoring_stats.EVENT_TYPES,
                scoring_stats.project_scoring_stats,
            ),
            ConsumerRegistration(
                multiplayer.CONSUMER_NAME,
                multiplayer.EVENT_TYPES,
                multiplayer.project_multiplayer_results,
            ),
            ConsumerRegistration(
                management.AUTHORIZATION_CONSUMER_NAME,
                management.AUTHORIZATION_EVENT_TYPES,
                management.project_authorization_event,
            ),
            ConsumerRegistration(
                management.MODERATION_CONSUMER_NAME,
                management.MODERATION_EVENT_TYPES,
                management.project_moderation_event,
            ),
        )
    )


DEFAULT_CONSUMER_CATALOG = build_consumer_catalog()
