"""Resolve Stable client addresses through explicitly trusted reverse proxies."""

from collections.abc import Sequence
from ipaddress import ip_address, ip_network

from fastapi import Request


def resolve_client_ip(request: Request, trusted_proxy_cidrs: Sequence[str]) -> str:
    """Return one canonical address, accepting proxy headers only from trusted peers."""
    peer_value = request.client.host if request.client is not None else "127.0.0.1"
    try:
        peer = ip_address(peer_value)
    except ValueError:
        return "127.0.0.1"

    trusted = False
    for cidr in trusted_proxy_cidrs:
        try:
            if peer in ip_network(cidr, strict=True):
                trusted = True
                break
        except ValueError:
            continue
    if not trusted:
        return str(peer)

    for header_name in ("CF-Connecting-IP", "X-Real-IP"):
        values = request.headers.getlist(header_name)
        if not values:
            continue
        if len(values) != 1:
            return str(peer)
        try:
            forwarded = ip_address(values[0])
        except ValueError:
            return str(peer)
        return str(forwarded)
    return str(peer)
