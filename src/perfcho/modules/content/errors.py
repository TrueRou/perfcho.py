"""Define expected canonical content failures."""

from perfcho.modules.common.errors import DependencyUnavailable, InputRejected, ResourceNotFound


class ContentInputRejected(InputRejected):
    """Reject invalid content query or command input."""

    code = "content_input_rejected"


class BeatmapNotFound(ResourceNotFound):
    """Indicate that a beatmap revision cannot be resolved."""

    code = "beatmap_not_found"


class BeatmapsetNotFound(ResourceNotFound):
    """Indicate that a beatmapset cannot be resolved."""

    code = "beatmapset_not_found"


class UpstreamContentUnavailable(DependencyUnavailable):
    """Indicate that authoritative upstream metadata or files cannot be fetched."""

    code = "upstream_content_unavailable"
