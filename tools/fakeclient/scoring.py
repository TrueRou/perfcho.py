"""Build and submit one valid Stable score through osu.py's HTTP session."""

import hashlib
import re
from base64 import b64encode
from datetime import UTC, datetime

from perfcho.infra.security.rijndael import Rijndael256Cbc
from tools.fakeclient.client import FakeClient, FakeClientError

_SCORE_ID = re.compile(r"(?:^|\|)onlineScoreId:(\d+)(?:\||$)")
_OSU_VERSION = "20260711"


def submit_score(
    client: FakeClient,
    *,
    beatmap_md5: str,
    replay: bytes,
    ended_at: datetime | None = None,
) -> int:
    """Submit a perfect three-object play and return its public score ID."""
    if len(replay) < 24:
        raise ValueError("replay fixture must contain at least 24 bytes")
    ended = ended_at or datetime.now(UTC)
    timestamp = ended.strftime("%y%m%d%H%M%S")
    username = client.game.username
    client_hash = "fakeclient-integrity-hash"
    fields = [
        beatmap_md5,
        f"{username} ",
        "",
        "3",
        "0",
        "0",
        "0",
        "0",
        "0",
        "900",
        "3",
        "True",
        "X",
        "0",
        "True",
        "0",
        timestamp,
        _OSU_VERSION,
        "0",
    ]
    checksum_payload = (
        f"chickenmcnuggets3o1500smustard00uu{beatmap_md5}3True{username}900X0QTrue0"
        f"{_OSU_VERSION}{timestamp}{client_hash}"
    )
    fields[2] = hashlib.md5(checksum_payload.encode(), usedforsecurity=False).hexdigest()
    iv = b"fakeclient-score-iv".ljust(32, b"-")
    cipher = Rijndael256Cbc(
        key=f"osu!-scoreburgr---------{_OSU_VERSION}".encode(),
        iv=iv,
    )
    encrypted_score = b64encode(cipher.encrypt(":".join(fields).encode())).decode()
    encrypted_client_hash = b64encode(cipher.encrypt(client_hash.encode())).decode()
    files = [
        ("score", (None, encrypted_score)),
        ("score", ("fakeclient.osr", replay, "application/octet-stream")),
        ("x", (None, "0")),
        ("ft", (None, "0")),
        ("st", (None, "4000")),
        ("pass", (None, client.game.password_hash)),
        ("osuver", (None, _OSU_VERSION)),
        ("s", (None, encrypted_client_hash)),
        ("iv", (None, b64encode(iv).decode())),
        ("bmk", (None, beatmap_md5)),
        ("sbk", (None, "")),
        ("c1", (None, "fakeclient-uninstall|fakeclient-disk")),
    ]
    response = client.game.api.session.post(
        f"{client.game.api.url}/web/osu-submit-modular-selector.php",
        files=files,
        timeout=client.timeout,
    )
    match = _SCORE_ID.search(response.text)
    if response.status_code != 200 or match is None:
        raise FakeClientError(f"Stable score submission failed: HTTP {response.status_code} {response.text!r}")
    return int(match.group(1))
