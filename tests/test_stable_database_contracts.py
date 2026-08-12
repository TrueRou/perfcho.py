from collections import Counter
from collections.abc import Iterable
from typing import Any

import pytest
from sqlalchemy import (
    CheckConstraint,
    Column,
    ForeignKeyConstraint,
    Index,
    Table,
    UniqueConstraint,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import configure_mappers
from sqlalchemy.schema import CreateIndex, CreateTable
from sqlalchemy.sql.schema import ColumnCollectionConstraint

import perfcho.infra.db.models  # noqa: F401 - register canonical metadata.
from perfcho.infra.db import DbBase
from perfcho.infra.db import engine as infra_db
from perfcho.infra.db.models.events import OutboxDelivery
from perfcho.infra.db.repositories.outbox import append_outbox_event
from perfcho.modules.common.models import PendingEvent


def _table(name: str) -> Table:
    return DbBase.metadata.tables[name]


def _constraint(
    table_name: str,
    name: str,
    constraint_type: type[ColumnCollectionConstraint],
) -> ColumnCollectionConstraint:
    return next(
        constraint
        for constraint in _table(table_name).constraints
        if isinstance(constraint, constraint_type) and constraint.name == name
    )


def _index(table_name: str, name: str) -> Index:
    return next(index for index in _table(table_name).indexes if index.name == name)


def _column_names(items: Iterable[Column[Any]]) -> tuple[str, ...]:
    return tuple(item.name for item in items)


def test_metadata_preserves_the_reviewed_table_inventory() -> None:
    assert Counter(table.schema for table in DbBase.metadata.tables.values()) == {
        "audit": 1,
        "authz": 7,
        "community": 11,
        "content": 15,
        "core": 8,
        "event": 4,
        "iam": 18,
        "moderation": 8,
        "multiplayer": 29,
        "scoring": 20,
        "social": 8,
        "system": 2,
    }
    assert {
        "iam.auth_token_family",
        "iam.auth_challenge_scope",
        "scoring.play_attempt_token",
        "system.command_receipt",
    } <= set(DbBase.metadata.tables)
    assert "system.server_settings" not in DbBase.metadata.tables


def test_postgresql_ddl_and_mappers_are_coherent() -> None:
    configure_mappers()
    dialect = postgresql.dialect()

    for table in DbBase.metadata.sorted_tables:
        assert str(CreateTable(table).compile(dialect=dialect))
        for index in table.indexes:
            assert str(CreateIndex(index).compile(dialect=dialect))


def test_lazer_solo_score_tokens_are_numeric_and_bound_to_authoritative_dimensions() -> None:
    tokens = _table("scoring.play_attempt_token")
    assert tokens.c.id.type.python_type is int
    assert not tokens.c.account_id.nullable
    assert not tokens.c.beatmap_id.nullable
    assert not tokens.c.beatmap_revision_id.nullable
    assert not tokens.c.ruleset.nullable
    assert not tokens.c.protocol.nullable
    assert tokens.c.score_id.nullable
    assert "token_digest" not in tokens.c
    assert "token_prefix" not in tokens.c


def test_stable_identity_lifecycle_contracts() -> None:
    credentials = _table("iam.password_credential")
    algorithm_pepper = _constraint(
        "iam.password_credential",
        "ck_password_credential_algorithm_pepper_consistency",
        CheckConstraint,
    )
    assert isinstance(algorithm_pepper, CheckConstraint)
    algorithm_pepper_sql = str(algorithm_pepper.sqltext)
    assert credentials.c.pepper_version.nullable
    assert "algorithm = 'argon2id'" in algorithm_pepper_sql
    assert "pepper_version IS NOT NULL" in algorithm_pepper_sql
    assert "algorithm = 'bcrypt_md5'" in algorithm_pepper_sql
    assert "pepper_version IS NULL" in algorithm_pepper_sql

    sessions = _table("iam.auth_session")
    assert {"client_variant", "session_class", "last_activity_at"} <= set(sessions.c.keys())
    assert not sessions.c.last_activity_at.nullable
    activity_period = _constraint(
        "iam.auth_session",
        "ck_auth_session_activity_period",
        CheckConstraint,
    )
    assert isinstance(activity_period, CheckConstraint)
    assert "last_activity_at >= created_at" in str(activity_period.sqltext)
    active_client = _index("iam.auth_session", "uq_auth_session_active_client_account")
    assert active_client.unique
    assert _column_names(active_client.columns) == ("account_id",)
    assert "oauth_client_id IS NULL" in str(active_client.dialect_options["postgresql"]["where"])

    tokens = _table("iam.auth_token")
    assert {"family_id", "rotation_number"} <= set(tokens.c.keys())
    assert _column_names(
        _constraint("iam.auth_token", "uq_auth_token_family_rotation", UniqueConstraint).columns
    ) == ("family_id", "rotation_number")
    assert _column_names(
        _constraint("iam.auth_token", "fk_auth_token_family_session_account", ForeignKeyConstraint).columns
    ) == ("family_id", "session_id", "account_id")
    assert _column_names(
        _constraint("iam.auth_token", "fk_auth_token_parent_family", ForeignKeyConstraint).columns
    ) == ("parent_token_id", "family_id")
    assert "redirect_uri" in _table("iam.auth_challenge").c
    assert "last_accepted_counter" in _table("iam.totp_factor").c


def test_content_and_scoring_dimensions_are_unambiguous() -> None:
    revisions = _table("content.beatmap_revision")
    assert "file_name_key" in revisions.c
    assert _column_names(
        _constraint("content.beatmap_revision", "uq_beatmap_revision_md5", UniqueConstraint).columns
    ) == ("md5",)

    attempts = _table("scoring.play_attempt")
    assert {"beatmap_id", "ruleset", "mods_details", "mods_acronyms", "mods_digest"} <= set(attempts.c.keys())
    assert "scoreboard_id" not in attempts.c
    assert "mod_set_id" not in attempts.c
    revision_fk = _constraint("scoring.play_attempt", "fk_play_attempt_revision_beatmap", ForeignKeyConstraint)
    assert _column_names(revision_fk.columns) == ("beatmap_revision_id", "beatmap_id")

    scores = _table("scoring.score")
    assert {"ruleset", "mods_details", "mods_acronyms", "mods_digest"} <= set(scores.c.keys())
    assert "scoreboard_id" not in scores.c
    assert "mod_set_id" not in scores.c
    score_attempt_fk = _constraint("scoring.score", "fk_score_attempt_dimensions", ForeignKeyConstraint)
    assert _column_names(score_attempt_fk.columns) == (
        "attempt_id",
        "account_id",
        "beatmap_id",
        "beatmap_revision_id",
        "ruleset",
        "mods_details",
        "mods_acronyms",
        "mods_digest",
    )

    difficulty = _table("scoring.beatmap_difficulty_attribute")
    assert {"ruleset", "mods_digest"} <= set(difficulty.c.keys())
    assert "scoreboard_id" not in difficulty.c
    assert "mod_set_id" not in difficulty.c

    ranking_policy = _table("scoring.ranking_policy")
    assert set(ranking_policy.c.keys()) == {"id", "code", "ruleset", "active", "configuration"}

    attestation = _table("scoring.score_attestation")
    assert "client_integrity_digest" in attestation.c
    assert "patcher_digest" not in attestation.c

    ratings = _table("content.rating_vote")
    assert "beatmap_id" in ratings.c
    assert "beatmap_revision_id" not in ratings.c
    assert _index("content.rating_vote", "uq_rating_vote_beatmap_account").unique

    replays = _table("scoring.replay")
    unique_columns = {
        _column_names(constraint.columns)
        for constraint in replays.constraints
        if isinstance(constraint, UniqueConstraint)
    }
    assert ("sha256",) not in unique_columns
    assert ("storage_key",) not in unique_columns


def test_stable_read_paths_have_ordered_indexes() -> None:
    message_index = _index("community.message", "ix_message_channel_id_desc")
    assert str(message_index.expressions[-1]).endswith("DESC")
    assert _index("community.direct_conversation", "ix_direct_conversation_low_account")
    assert _index("community.direct_conversation", "ix_direct_conversation_high_account")
    assert _column_names(_index("community.channel_user_state", "ix_channel_user_state_account").columns)[:1] == (
        "account_id",
    )

    assert _column_names(_index("scoring.play_attempt", "ix_play_attempt_ruleset_mods").columns) == (
        "ruleset",
        "mods_digest",
    )
    assert _column_names(_index("scoring.score", "ix_score_revision_ruleset").columns) == (
        "beatmap_revision_id",
        "ruleset",
        "id",
    )
    assert _column_names(_index("scoring.score", "ix_score_beatmap_ruleset_mods").columns) == (
        "beatmap_id",
        "ruleset",
        "mods_digest",
        "id",
    )
    assert _column_names(
        _index("scoring.beatmap_difficulty_attribute", "ix_difficulty_attributes_release").columns
    ) == ("release_id", "ruleset", "mods_digest")


def test_replies_multiplayer_events_and_outbox_positions_have_integrity() -> None:
    reply_fk = _constraint("community.message", "fk_message_reply_same_channel", ForeignKeyConstraint)
    assert _column_names(reply_fk.columns) == ("channel_id", "reply_to_id")

    rooms = _table("multiplayer.room")
    assert {"password_verifier", "password_prefix"} <= set(rooms.c.keys())
    assert any(
        isinstance(constraint, CheckConstraint) and constraint.name == "ck_room_public_id_range"
        for constraint in rooms.constraints
    )
    public_id = _index("multiplayer.room", "uq_room_active_public_id")
    assert public_id.unique
    assert "status IN ('open', 'started')" in str(public_id.dialect_options["postgresql"]["where"])

    multiplayer_events = _table("multiplayer.event")
    assert "command_id" in multiplayer_events.c
    assert _column_names(
        next(
            constraint
            for constraint in multiplayer_events.constraints
            if isinstance(constraint, UniqueConstraint) and _column_names(constraint.columns) == ("command_id",)
        ).columns
    ) == ("command_id",)
    assert _constraint("multiplayer.event", "uq_multiplayer_events_room_version", UniqueConstraint)

    deliveries = _table("event.outbox_delivery")
    assert not deliveries.c.source_position.nullable
    assert "status" in deliveries.c
    assert "available_at" in deliveries.c
    outbox_events = _table("event.outbox_event")
    assert "available_at" not in outbox_events.c
    assert outbox_events.c.trace_id.nullable
    assert _index("event.outbox_event", "ix_outbox_event_trace_id")
    assert _constraint("event.outbox_delivery", "fk_outbox_delivery_event_position", ForeignKeyConstraint)
    assert _index("event.outbox_delivery", "ix_outbox_delivery_consumer_position")


def test_anticheat_findings_are_deduplicated_and_reviewable() -> None:
    findings = _table("moderation.anticheat_finding")
    assert {
        "finding_digest",
        "reviewed_by_id",
        "reviewed_at",
        "review_outcome",
        "review_notes",
    } <= set(findings.c.keys())
    assert _constraint("moderation.anticheat_finding", "uq_anticheat_finding_run_digest", UniqueConstraint)


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_outbox_delivery_copies_immutable_source_position(postgres_database_url: str) -> None:
    db_engine = await infra_db.create_engine()
    session_factory = infra_db.create_session_factory(db_engine)
    try:
        async with session_factory.begin() as session:
            event = await append_outbox_event(
                session,
                PendingEvent(
                    aggregate_type="test",
                    aggregate_id="stable-contract",
                    event_type="tests.stable-contract.v1",
                    schema_version=1,
                    payload={},
                    consumers=("tests.stable-contract.v1",),
                    partition_key="default",
                ),
            )

        async with session_factory() as session:
            delivery = await session.get(
                OutboxDelivery,
                {"event_id": event.id, "consumer": "tests.stable-contract.v1"},
            )
            assert delivery is not None
            assert delivery.source_position == event.position
    finally:
        await db_engine.dispose()
