"""Compose osu!lazer SignalR hubs on top of canonical realtime services."""

from perfcho.api.signalr.hubs import MetadataHub, MultiplayerHub, SpectatorHub, build_signalr_apps, register_signalr

__all__ = (
    "MetadataHub",
    "MultiplayerHub",
    "SpectatorHub",
    "build_signalr_apps",
    "register_signalr",
)
