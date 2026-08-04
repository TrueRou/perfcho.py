"""Manage isolated infrastructure and real perfcho process roles for E2E."""

from __future__ import annotations

import os
import signal
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

import requests

from tools.fakeclient.client import FakeClientError

_ROOT = Path(__file__).resolve().parents[2]
_COMPOSE = Path(__file__).with_name("compose.yaml")


@dataclass(slots=True)
class ManagedRuntime:
    """Own one isolated Compose project and its perfcho child processes."""

    artifacts: Path
    project_name: str = "perfcho-fakeclient-e2e"
    api_port: int = 18080
    api_process: subprocess.Popen[bytes] | None = None
    worker_process: subprocess.Popen[bytes] | None = None
    upstream_process: subprocess.Popen[bytes] | None = None

    @property
    def base_url(self) -> str:
        """Return the externally reachable API origin."""
        return f"http://127.0.0.1:{self.api_port}"

    def environment(self) -> dict[str, str]:
        """Return a complete isolated runtime environment."""
        return {
            **os.environ,
            "DATABASE_URL": "postgresql+asyncpg://perfcho:perfcho@127.0.0.1:57432/perfcho",
            "REDIS_STATE_URL": "redis://127.0.0.1:57379/0",
            "REDIS_STATE_PREFIX": "perfcho:fakeclient:e2e",
            "TASKIQ_BROKER_URL": "redis://127.0.0.1:57379/1",
            "TASKIQ_QUEUE_NAME": "perfcho:fakeclient:tasks",
            "TASKIQ_CONSUMER_GROUP": "perfcho:fakeclient:workers",
            "S3_ENDPOINT_URL": "http://127.0.0.1:59100",
            "S3_ACCESS_KEY": "perfcho",
            "S3_SECRET_KEY": "perfcho-fakeclient",
            "S3_BUCKET": "perfcho",
            "S3_ADDRESSING_STYLE": "path",
            "STABLE_BEATMAP_DOWNLOAD_BASE_URL": "http://127.0.0.1:18081/d",
            "STABLE_AVATAR_BASE_URL": "http://127.0.0.1:18081",
            "STABLE_BEATMAP_ASSET_BASE_URL": "http://127.0.0.1:18081",
            "STABLE_SEASONAL_URL": "http://127.0.0.1:18081/web/osu-getseasonal.php",
            "STABLE_MENU_CONTENT_URL": "http://127.0.0.1:18081/menu-content.json",
            "OSU_API_CLIENT_ID": "0",
            "OSU_API_CLIENT_SECRET": "",
            "LOG_LEVEL": "INFO",
        }

    def start(self) -> None:
        """Start isolated dependencies, API, seed data, and Worker."""
        self.artifacts.mkdir(parents=True, exist_ok=True)
        self._compose("up", "-d", "--wait", "postgres", "redis", "minio")
        self._compose("run", "--rm", "minio-init")
        env = self.environment()
        self.upstream_process = self._spawn(
            ("uv", "run", "python", "-m", "tools.fakeclient.upstream", "--port", "18081"),
            env,
            "upstream.log",
        )
        self.api_process = self._spawn(
            (
                "uv",
                "run",
                "uvicorn",
                "perfcho.main:asgi_app",
                "--host",
                "127.0.0.1",
                "--port",
                str(self.api_port),
            ),
            env,
            "api.log",
        )
        self._wait_ready()
        subprocess.run(
            ("uv", "run", "python", "-m", "tools.fakeclient.fixtures"),
            cwd=_ROOT,
            env=env,
            check=True,
            timeout=60,
        )
        self.worker_process = self._spawn(
            ("uv", "run", "taskiq", "worker", "perfcho.worker:broker", "--ack-type", "when_executed"),
            env,
            "worker.log",
        )

    def stop(self) -> None:
        """Stop only owned child processes and the exact Compose project."""
        for process in (self.worker_process, self.api_process, self.upstream_process):
            if process is None or process.poll() is not None:
                continue
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=5)
        self._compose("down", "--volumes", "--remove-orphans", check=False)

    def _spawn(self, command: tuple[str, ...], env: dict[str, str], log_name: str) -> subprocess.Popen[bytes]:
        log = (self.artifacts / log_name).open("wb")
        return subprocess.Popen(
            command,
            cwd=_ROOT,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    def _wait_ready(self) -> None:
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            if self.api_process is not None and self.api_process.poll() is not None:
                raise FakeClientError("perfcho API exited before readiness")
            try:
                response = requests.get(f"{self.base_url}/", timeout=1)
                if response.status_code == 200:
                    return
            except requests.RequestException:
                pass
            time.sleep(0.2)
        raise FakeClientError("perfcho API did not become ready")

    def _compose(self, *arguments: str, check: bool = True) -> None:
        subprocess.run(
            ("docker", "compose", "-p", self.project_name, "-f", str(_COMPOSE), *arguments),
            cwd=_ROOT,
            check=check,
            timeout=120,
        )

    def __enter__(self) -> ManagedRuntime:
        """Start all managed resources."""
        try:
            self.start()
        except BaseException:
            self.stop()
            raise
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        """Clean up all managed resources."""
        self.stop()
