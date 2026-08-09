"""Serve deterministic public assets used to verify perfcho forwarding."""

import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import orjson

from tools.fakeclient.fixtures import BEATMAPSET_ID

_PNG = b"\x89PNG\r\n\x1a\n" + b"fakeclient-avatar"
_JPEG = b"\xff\xd8\xff\xe0" + b"fakeclient-thumbnail" + b"\xff\xd9"
_MP3 = b"ID3" + b"fakeclient-preview"
_OSZ = b"PK\x03\x04" + b"fakeclient-osz"


class FixtureHandler(BaseHTTPRequestHandler):
    """Return fixed assets without reading or writing object storage."""

    def do_GET(self) -> None:
        """Serve one deterministic response selected by request path."""
        path = self.path.partition("?")[0]
        if path == "/web/osu-getseasonal.php":
            self._send(orjson.dumps(["http://127.0.0.1/seasonal.jpg"]), "application/json")
        elif path == "/menu-content.json":
            self._send(orjson.dumps({"images": []}), "application/json")
        elif path.startswith("/thumb/"):
            self._send(_JPEG, "image/jpeg")
        elif path.startswith("/preview/"):
            self._send(_MP3, "audio/mpeg")
        elif path in {f"/d/{BEATMAPSET_ID}", f"/d/{BEATMAPSET_ID}n"}:
            self._send(_OSZ, "application/x-osu-beatmap-archive")
        elif path.removeprefix("/").isdigit():
            self._send(_PNG, "image/png")
        else:
            self.send_error(404)

    def _send(self, content: bytes, media_type: str) -> None:
        self.send_response(200)
        self.send_header("Content-Type", media_type)
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def log_message(self, format: str, *args: object) -> None:
        """Suppress noisy fixture-server access logging."""


def main() -> int:
    """Run the bounded local fixture server until terminated."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18081)
    args = parser.parse_args()
    server = ThreadingHTTPServer((args.host, args.port), FixtureHandler)
    try:
        server.serve_forever()
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
