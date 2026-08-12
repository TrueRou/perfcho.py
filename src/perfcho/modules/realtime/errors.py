"""Define expected protocol-neutral realtime state errors."""

from perfcho.modules.common.errors import InputRejected, ResourceConflict, ResourceNotFound


class RealtimeSessionNotFound(ResourceNotFound):
    """Indicate that a realtime session is absent or expired."""

    code = "realtime_session_not_found"


class RealtimeSessionFenced(ResourceConflict):
    """Reject work from a superseded realtime session revision."""

    code = "realtime_session_fenced"


class PresenceCapacityReached(ResourceConflict):
    """Reject a presence claim when the bounded online index is full."""

    code = "presence_capacity_reached"


class SpectatorHostOffline(ResourceConflict):
    """Reject attachment or frame work while the requested host is offline."""

    code = "spectator_host_offline"


class InvalidFrame(InputRejected):
    """Reject malformed or out-of-window spectator frame data."""

    code = "invalid_frame"
