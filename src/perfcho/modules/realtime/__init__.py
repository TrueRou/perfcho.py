"""Expose protocol-neutral ephemeral realtime state contracts."""

from perfcho.modules.realtime.errors import (
    InvalidFrame,
    MailboxOverflow,
    PollLeaseConflict,
    PresenceCapacityReached,
    RealtimeSessionFenced,
    RealtimeSessionNotFound,
    SpectatorHostOffline,
)
from perfcho.modules.realtime.models import (
    MAX_FRAME_SEQUENCE,
    MAX_REVISION,
    MAX_SEQUENCE,
    MailboxBatch,
    MailboxPacket,
    PresenceSnapshot,
    RealtimeSession,
    SessionFence,
    SpectatorAttachment,
    SpectatorFrame,
    SpectatorFramePublish,
    SpectatorFrameWindow,
    SpectatorRelation,
)
from perfcho.modules.realtime.ports import RealtimeRepository

__all__ = (
    "MAX_FRAME_SEQUENCE",
    "MAX_REVISION",
    "MAX_SEQUENCE",
    "InvalidFrame",
    "MailboxBatch",
    "MailboxOverflow",
    "MailboxPacket",
    "PollLeaseConflict",
    "PresenceCapacityReached",
    "PresenceSnapshot",
    "RealtimeRepository",
    "RealtimeSession",
    "RealtimeSessionFenced",
    "RealtimeSessionNotFound",
    "SessionFence",
    "SpectatorAttachment",
    "SpectatorFrame",
    "SpectatorFramePublish",
    "SpectatorFrameWindow",
    "SpectatorHostOffline",
    "SpectatorRelation",
)
