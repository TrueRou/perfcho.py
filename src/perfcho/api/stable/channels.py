"""Adapt canonical community channels to Stable channel names."""

from perfcho.modules.community import ChannelSelector, ChannelView, CommunityInputRejected


def parse_stable_channel_selector(value: str) -> ChannelSelector:
    """Parse a Stable channel target into a canonical name selector."""
    if not isinstance(value, str):
        raise CommunityInputRejected("invalid Stable channel name")
    normalized = value.strip().casefold()
    if normalized.startswith("#"):
        normalized = normalized[1:]
    if normalized.startswith("#") or not 1 <= len(normalized) <= 99:
        raise CommunityInputRejected("invalid Stable channel name")
    return ChannelSelector(name=normalized)


def stable_channel_name(channel: ChannelView) -> str:
    """Render a canonical channel name for the Stable wire protocol."""
    name = channel.name.strip()
    if not name or name.startswith("#"):
        raise ValueError("canonical channel name must not contain a Stable prefix")
    return f"#{name}"
