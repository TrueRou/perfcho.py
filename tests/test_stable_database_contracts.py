from collections.abc import Iterable
from typing import Any

import pytest
from sqlalchemy import (
    CheckConstraint,
    Column,
    ForeignKeyConstraint,
    Index,
    Numeric,
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


def test_metadata_preserves_baseline_and_adds_only_lifecycle_tables() -> None:
    assert len(DbBase.metadata.tables) >= 133
    assert {
        "iam.auth_token_families",
        "iam.auth_challenge_scopes",
        "scoring.play_attempt_tokens",
        "system.command_receipts",
    } <= set(DbBase.metadata.tables)


def test_postgresql_ddl_and_mappers_are_coherent() -> None:
    configure_mappers()
    dialect = postgresql.dialect()

    for table in DbBase.metadata.sorted_tables:
        assert str(CreateTable(table).compile(dialect=dialect))
        for index in table.indexes:
            assert str(CreateIndex(index).compile(dialect=dialect))


def test_stable_identity_lifecycle_contracts() -> None:
    credentials = _table("iam.password_credentials")
    algorithm_pepper = _constraint(
        "iam.password_credentials",
        "ck_password_credentials_algorithm_pepper_consistency",
        CheckConstraint,
    )
    assert isinstance(algorithm_pepper, CheckConstraint)
    algorithm_pepper_sql = str(algorithm_pepper.sqltext)
    assert credentials.c.pepper_version.nullable
    assert "algorithm = 'argon2id'" in algorithm_pepper_sql
    assert "pepper_version IS NOT NULL" in algorithm_pepper_sql
    assert "algorithm = 'bcrypt_md5'" in algorithm_pepper_sql
    assert "pepper_version IS NULL" in algorithm_pepper_sql

    sessions = _table("iam.auth_sessions")
    assert {"client_variant", "session_class", "last_activity_at"} <= set(sessions.c.keys())
    assert not sessions.c.last_activity_at.nullable
    activity_period = _constraint(
        "iam.auth_sessions",
        "ck_auth_sessions_activity_period",
        CheckConstraint,
    )
    assert isinstance(activity_period, CheckConstraint)
    assert "last_activity_at >= created_at" in str(activity_period.sqltext)
    active_stable = _index("iam.auth_sessions", "uq_auth_sessions_active_normal_stable_account")
    assert active_stable.unique
    assert _column_names(active_stable.columns) == ("account_id",)
    assert "client_family = 'stable'" in str(active_stable.dialect_options["postgresql"]["where"])

    tokens = _table("iam.auth_tokens")
    assert {"family_id", "rotation_number"} <= set(tokens.c.keys())
    assert _column_names(
        _constraint("iam.auth_tokens", "uq_auth_tokens_family_rotation", UniqueConstraint).columns
    ) == ("family_id", "rotation_number")
    assert _column_names(
        _constraint("iam.auth_tokens", "fk_auth_tokens_family_session_account", ForeignKeyConstraint).columns
    ) == ("family_id", "session_id", "account_id")
    assert _column_names(
        _constraint("iam.auth_tokens", "fk_auth_tokens_parent_family", ForeignKeyConstraint).columns
    ) == ("parent_token_id", "family_id")
    assert "redirect_uri" in _table("iam.auth_challenges").c
    assert "last_accepted_counter" in _table("iam.totp_factors").c


def test_content_and_scoring_dimensions_are_unambiguous() -> None:
    revisions = _table("content.beatmap_revisions")
    assert "file_name_key" in revisions.c
    assert _column_names(
        _constraint("content.beatmap_revisions", "uq_beatmap_revisions_md5", UniqueConstraint).columns
    ) == ("md5",)

    attempts = _table("scoring.play_attempts")
    assert "beatmap_id" in attempts.c
    revision_fk = _constraint("scoring.play_attempts", "fk_play_attempts_revision_beatmap", ForeignKeyConstraint)
    mods_fk = _constraint("scoring.play_attempts", "fk_play_attempts_mod_set_scoreboard", ForeignKeyConstraint)
    assert _column_names(revision_fk.columns) == ("beatmap_revision_id", "beatmap_id")
    assert _column_names(mods_fk.columns) == ("mod_set_id", "scoreboard_id")

    attestation = _table("scoring.score_attestations")
    assert "client_integrity_digest" in attestation.c
    assert "patcher_digest" not in attestation.c

    ratings = _table("content.rating_votes")
    assert "beatmap_id" in ratings.c
    assert "beatmap_revision_id" not in ratings.c
    assert _index("content.rating_votes", "uq_rating_votes_beatmap_account").unique

    leaderboard = _table("scoring.leaderboard_entries")
    assert isinstance(leaderboard.c.metric_value.type, Numeric)
    assert isinstance(leaderboard.c.tie_break_value.type, Numeric)
    assert leaderboard.c.metric_value.type.precision == 30
    assert leaderboard.c.tie_break_value.type.precision == 30

    replays = _table("scoring.replays")
    unique_columns = {
        _column_names(constraint.columns)
        for constraint in replays.constraints
        if isinstance(constraint, UniqueConstraint)
    }
    assert ("sha256",) not in unique_columns
    assert ("storage_key",) not in unique_columns


def test_stable_read_paths_have_ordered_indexes() -> None:
    message_index = _index("community.messages", "ix_messages_channel_id_desc")
    assert str(message_index.expressions[-1]).endswith("DESC")
    assert _index("community.direct_conversations", "ix_direct_conversations_low_account")
    assert _index("community.direct_conversations", "ix_direct_conversations_high_account")
    assert _column_names(_index("community.channel_user_states", "ix_channel_user_states_account").columns)[:1] == (
        "account_id",
    )

    leaderboard_index = _index("scoring.leaderboard_entries", "ix_leaderboard_entries_rank")
    leaderboard_sql = str(CreateIndex(leaderboard_index).compile(dialect=postgresql.dialect()))
    assert "metric_value DESC" in leaderboard_sql
    assert "tie_break_value DESC" in leaderboard_sql
    assert _index("scoring.leaderboard_entries", "ix_leaderboard_entries_country_rank")


def test_replies_multiplayer_events_and_outbox_positions_have_integrity() -> None:
    reply_fk = _constraint("community.messages", "fk_messages_reply_same_channel", ForeignKeyConstraint)
    assert _column_names(reply_fk.columns) == ("channel_id", "reply_to_id")

    rooms = _table("multiplayer.rooms")
    assert {"password_verifier", "password_prefix"} <= set(rooms.c.keys())
    assert any(
        isinstance(constraint, CheckConstraint) and constraint.name == "ck_rooms_stable_public_id_range"
        for constraint in rooms.constraints
    )
    public_id = _index("multiplayer.rooms", "uq_rooms_active_public_id")
    assert public_id.unique
    assert "status IN ('open', 'started')" in str(public_id.dialect_options["postgresql"]["where"])

    multiplayer_events = _table("multiplayer.events")
    assert "command_id" in multiplayer_events.c
    assert _column_names(
        next(
            constraint
            for constraint in multiplayer_events.constraints
            if isinstance(constraint, UniqueConstraint) and _column_names(constraint.columns) == ("command_id",)
        ).columns
    ) == ("command_id",)
    assert _constraint("multiplayer.events", "uq_multiplayer_events_room_version", UniqueConstraint)

    deliveries = _table("events.outbox_deliveries")
    assert not deliveries.c.source_position.nullable
    assert "status" in deliveries.c
    assert "available_at" in deliveries.c
    assert "available_at" not in _table("events.outbox_events").c
    assert _constraint("events.outbox_deliveries", "fk_outbox_deliveries_event_position", ForeignKeyConstraint)
    assert _index("events.outbox_deliveries", "ix_outbox_deliveries_consumer_position")


def test_anticheat_findings_are_deduplicated_and_reviewable() -> None:
    findings = _table("moderation.anticheat_findings")
    assert {
        "finding_digest",
        "reviewed_by_id",
        "reviewed_at",
        "review_outcome",
        "review_notes",
    } <= set(findings.c.keys())
    assert _constraint("moderation.anticheat_findings", "uq_anticheat_findings_run_digest", UniqueConstraint)


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
