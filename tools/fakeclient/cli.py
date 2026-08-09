"""Expose a small black-box smoke command for an already running perfcho API."""

import argparse
from collections.abc import Sequence
from dataclasses import asdict
from pathlib import Path

import orjson

from tools.fakeclient.client import OSU_PY_COMMIT, OSU_PY_VERSION, FakeClient
from tools.fakeclient.runtime import ManagedRuntime
from tools.fakeclient.scenarios import run_full_suite


def build_parser() -> argparse.ArgumentParser:
    """Build the fakeclient command parser."""
    parser = argparse.ArgumentParser(description="Drive perfcho through osu.py over real HTTP")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="start isolated infrastructure and run the full E2E suite")
    run.add_argument("--artifacts", type=Path, default=Path(".fakeclient/latest"))
    run.add_argument("--timeout", type=float, default=8.0)
    smoke = commands.add_parser("smoke", help="connect one account to an already running API")
    smoke.add_argument("--base-url", default="http://127.0.0.1:8000")
    smoke.add_argument("--username", required=True)
    smoke.add_argument("--password", required=True)
    smoke.add_argument("--timeout", type=float, default=5.0)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run a managed full suite or one attached-client smoke check."""
    args = build_parser().parse_args(argv)
    if args.command == "run":
        with ManagedRuntime(args.artifacts) as runtime:
            results = run_full_suite(runtime.base_url, timeout=args.timeout)
        print(orjson.dumps([asdict(result) for result in results], option=orjson.OPT_SORT_KEYS).decode())
        return 0
    with FakeClient(args.username, args.password, args.base_url, timeout=args.timeout) as client:
        client.game.bancho.request_status()
        player = client.game.bancho.player
        result = {
            "osu_py_version": OSU_PY_VERSION,
            "osu_py_commit": OSU_PY_COMMIT,
            "account_id": player.id,
            "username": player.name,
            "protocol": client.game.bancho.protocol,
            "playcount": player.playcount,
        }
        print(orjson.dumps(result, option=orjson.OPT_SORT_KEYS).decode())
    return 0
