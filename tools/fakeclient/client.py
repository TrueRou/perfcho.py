"""Adapt osu.py to a local perfcho endpoint without bypassing HTTP."""

from __future__ import annotations

import queue
import threading
import time
from collections import defaultdict
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from urllib.parse import urlsplit, urlunsplit

import requests
from osu import Game
from osu.bancho.client import BanchoClient
from osu.bancho.connector_http import HttpBanchoConnector
from osu.bancho.constants import ClientPackets, ServerPackets
from osu.bancho.streams import StreamOut
from requests.adapters import HTTPAdapter

OSU_PY_VERSION = "1.5.4"
OSU_PY_COMMIT = "31a51dc323ae151fe711bb0cb22bd266abdaa500"
STABLE_VERSION_NUMBER = 20260711.1
STABLE_EXECUTABLE_HASH = "0" * 32


class FakeClientError(RuntimeError):
    """Report an externally observable fake-client failure."""


class LocalEndpointAdapter(HTTPAdapter):
    """Route every osu.py asset and Web request to one local HTTP origin."""

    def __init__(self, base_url: str) -> None:
        """Bind all rewritten requests to one validated HTTP origin."""
        super().__init__()
        target = urlsplit(base_url)
        if target.scheme not in {"http", "https"} or not target.netloc or target.path not in {"", "/"}:
            raise ValueError("base_url must be an HTTP origin without a path")
        self._target = target

    def send(
        self,
        request: requests.PreparedRequest,
        stream: bool = False,
        timeout: float | tuple[float | None, float | None] | None = None,
        verify: bool | str = True,
        cert: str | tuple[str, str] | None = None,
        proxies: dict[str, str] | None = None,
    ) -> requests.Response:
        """Rewrite only the origin while preserving the Stable route and query."""
        if not isinstance(request.url, str):
            raise FakeClientError("requests prepared a non-text URL")
        source = urlsplit(request.url)
        if source.hostname in {"127.0.0.1", "localhost"}:
            return super().send(
                request,
                stream=stream,
                timeout=timeout,
                verify=verify,
                cert=cert,
                proxies=proxies,
            )
        request.url = urlunsplit((self._target.scheme, self._target.netloc, source.path, source.query, ""))
        request.headers["Host"] = source.netloc
        return super().send(
            request,
            stream=stream,
            timeout=timeout,
            verify=verify,
            cert=cert,
            proxies=proxies,
        )


class LocalBanchoConnector(HttpBanchoConnector):
    """Use osu.py's HTTP Bancho implementation against an explicit origin."""

    def __init__(self, base_url: str, timeout: float) -> None:
        """Bind the connector to one explicit HTTP origin and timeout."""
        super().__init__(domain="perfcho.invalid")
        target = urlsplit(base_url)
        if target.scheme not in {"http", "https"} or not target.netloc or target.path not in {"", "/"}:
            raise ValueError("base_url must be an HTTP origin without a path")
        self._base_url = urlunsplit((target.scheme, target.netloc, "", "", ""))
        self._timeout = timeout

    def bind(self, bancho: BanchoClient) -> None:
        """Bind the normal connector state, then replace its generated osu! domain."""
        super().bind(bancho)
        self.url = self._base_url
        self.session.headers["Host"] = "c.perfcho.invalid"

    def connect(self) -> None:
        """Perform the osu.py login request with a bounded network timeout."""
        data = f"{self.game.username}\n{self.game.password_hash}\n{self.game.client}\n"
        response = self.session.post(self.url, data=data, timeout=self._timeout)
        if not response.ok:
            self.bancho.connected = False
            self.bancho.retry = False
            raise FakeClientError(f"Bancho login returned HTTP {response.status_code}")
        token = response.headers.get("cho-token")
        if not token:
            self.bancho.connected = False
            self.bancho.retry = False
            self.game.packets.data_received(response.content, self.game)
            raise FakeClientError("Bancho login did not return a cho-token")
        self.bancho.connected = True
        self.token = token
        self.session.headers["osu-token"] = token
        self.game.packets.data_received(response.content, self.game)

    def receive(self) -> None:
        """Flush queued packets with a bounded network timeout."""
        if not self.bancho.connected:
            return
        if self.queue.empty():
            self.bancho.ping_count += 1
            self.bancho.ping()
            return
        packets: list[bytes] = []
        while not self.queue.empty():
            packets.append(self.queue.get())
        response = self.session.post(self.url, data=b"".join(packets), timeout=self._timeout)
        if not response.ok:
            self.bancho.connected = False
            self.bancho.retry = False
            raise FakeClientError(f"Bancho poll returned HTTP {response.status_code}")
        self.bancho.fast_read = False
        self.game.packets.data_received(response.content, self.game)
        self.bancho.last_action = time.time()


@dataclass(frozen=True, slots=True)
class ClientEvent:
    """Capture one decoded osu.py server event."""

    packet: ServerPackets
    arguments: tuple[object, ...]


class EventProbe:
    """Collect decoded osu.py events in packet-specific FIFO queues."""

    def __init__(self, game: Game) -> None:
        """Register one event collector for every packet decoded by osu.py."""
        self._queues: defaultdict[ServerPackets, queue.Queue[ClientEvent]] = defaultdict(queue.Queue)
        for packet in ServerPackets:
            game.events.register(packet)(self._handler(packet))

    def _handler(self, packet: ServerPackets) -> Callable[..., None]:
        def capture(*arguments: object) -> None:
            self._queues[packet].put(ClientEvent(packet, arguments))

        return capture

    def pop(self, packet: ServerPackets) -> ClientEvent | None:
        """Return one already received event without waiting."""
        try:
            return self._queues[packet].get_nowait()
        except queue.Empty:
            return None


class FakeClient:
    """Own one ordinary osu.py Stable client connected to perfcho."""

    def __init__(
        self,
        username: str,
        password: str,
        base_url: str,
        *,
        timeout: float = 5.0,
        disable_logging: bool = True,
    ) -> None:
        """Create an ordinary Stable client whose I/O is restricted to base_url."""
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.game = Game(
            username=username,
            password=password,
            server="perfcho.invalid",
            version=STABLE_VERSION_NUMBER,
            executable_hash=STABLE_EXECUTABLE_HASH,
            tournament=False,
            force_linux_emulation=True,
            disable_logging=disable_logging,
        )
        self._adapter = LocalEndpointAdapter(self.base_url)
        self.game.api.session.mount("http://", self._adapter)
        self.game.api.session.mount("https://", self._adapter)
        self.game.api.session.trust_env = False
        self.game.session.mount("http://", self._adapter)
        self.game.session.mount("https://", self._adapter)
        self.game.session.trust_env = False
        self.connector = LocalBanchoConnector(self.base_url, timeout)
        self.connector.session.trust_env = False
        self.game.bancho.set_connector(self.connector)
        self.events = EventProbe(self.game)
        self._closed = False
        self._lock = threading.Lock()

    @property
    def connected(self) -> bool:
        """Return whether osu.py still considers the Bancho session connected."""
        return self.game.bancho.connected

    def connect(self) -> None:
        """Run the normal Web probe and Bancho login sequence."""
        if not self.game.api.connect():
            raise FakeClientError("Stable Web connectivity probe rejected the client")
        self.game.bancho.connect()
        if not self.connected or self.game.bancho.user_id < 1:
            raise FakeClientError("osu.py did not establish a usable Bancho identity")

    def poll(self) -> None:
        """Execute one real Bancho HTTP poll."""
        with self._lock:
            self.connector.receive()

    def wait_for(self, packet: ServerPackets, *, timeout: float | None = None) -> ClientEvent:
        """Poll until a decoded event arrives or the bounded timeout expires."""
        deadline = time.monotonic() + (self.timeout if timeout is None else timeout)
        while time.monotonic() < deadline:
            if event := self.events.pop(packet):
                return event
            self.poll()
            if event := self.events.pop(packet):
                return event
            time.sleep(0.01)
        raise FakeClientError(f"timed out waiting for {packet.name}")

    def request_all_presences(self) -> None:
        """Request the bounded online presence index used by Stable."""
        self.game.bancho.enqueue(ClientPackets.USER_PRESENCE_REQUEST_ALL, (0).to_bytes(4, "little", signed=True))

    def set_away_message(self, message: str) -> None:
        """Set the current session's Stable away reply."""
        stream = StreamOut()
        stream.string(self.game.username)
        stream.string(message)
        stream.string(self.game.username)
        stream.s32(self.game.bancho.user_id)
        self.game.bancho.enqueue(ClientPackets.SET_AWAY_MESSAGE, stream.get())

    def set_block_non_friend_dms(self, enabled: bool) -> None:
        """Set the authoritative Stable direct-message preference."""
        self.game.bancho.enqueue(
            ClientPackets.TOGGLE_BLOCK_NON_FRIEND_DMS,
            int(enabled).to_bytes(4, "little", signed=True),
        )

    def iter_download(self, beatmapset_id: int, *, no_video: bool = False) -> Iterator[bytes]:
        """Require osu.py Direct download to return a usable byte iterator."""
        result = self.game.api.download_osz(beatmapset_id, no_video=no_video)
        if result is None:
            raise FakeClientError("osu.py Direct download failed")
        return result

    def close(self) -> None:
        """Send the normal Stable logout packet and release osu.py resources."""
        if self._closed:
            return
        self._closed = True
        try:
            self.game.bancho.exit()
        finally:
            self.game.api.session.close()
            self.game.session.close()
            self.game.events.executor.shutdown(wait=True, cancel_futures=True)
            self.game.tasks.executor.shutdown(wait=True, cancel_futures=True)

    def __enter__(self) -> FakeClient:
        """Connect the client for a managed scenario."""
        self.connect()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        """Close the managed client regardless of scenario outcome."""
        self.close()
