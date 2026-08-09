"""Execute multi-client black-box scenarios through osu.py public APIs."""

import hashlib
import time
from collections.abc import Callable
from dataclasses import dataclass

import requests
from osu.api.constants import CommentTarget, RankingType
from osu.bancho.constants import (
    ButtonState,
    MatchTeamType,
    Mods,
    PresenceFilter,
    ReplayAction,
    ServerPackets,
    StatusAction,
)
from osu.objects import Match, ReplayFrame, ScoreFrame

from tools.fakeclient.client import FakeClient, FakeClientError
from tools.fakeclient.fixtures import _DATA_PATH, BEATMAP_FILE_NAME, BEATMAP_ID, BEATMAPSET_ID
from tools.fakeclient.scoring import submit_score


@dataclass(frozen=True, slots=True)
class ScenarioResult:
    """Describe one completed black-box scenario."""

    name: str
    duration_seconds: float


def register_account(base_url: str, username: str, email: str, password: str, *, timeout: float) -> None:
    """Register an account through the public Stable registration endpoint."""
    response = requests.post(
        f"{base_url.rstrip('/')}/users",
        data={
            "user[username]": username,
            "user[user_email]": email,
            "user[password]": password,
            "check": "0",
        },
        headers={"User-Agent": "osu!"},
        timeout=timeout,
    )
    if response.status_code != 200 or response.content != b"ok":
        raise FakeClientError(f"registration for {username} failed with HTTP {response.status_code}")


def _pump_until(client: FakeClient, predicate: Callable[[], bool], *, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        client.poll()
        if predicate():
            return
        time.sleep(0.01)
    raise FakeClientError("timed out waiting for client state")


def run_full_suite(base_url: str, *, timeout: float = 8.0) -> tuple[ScenarioResult, ...]:
    """Run ordinary Stable login, social, spectator, multiplayer, and Web flows."""
    password = "perfcho-e2e-password"
    register_account(base_url, "fakeclient-a", "fakeclient-a@example.test", password, timeout=timeout)
    register_account(base_url, "fakeclient-b", "fakeclient-b@example.test", password, timeout=timeout)
    results: list[ScenarioResult] = []
    started = time.monotonic()
    with (
        FakeClient("fakeclient-a", password, base_url, timeout=timeout) as first,
        FakeClient("fakeclient-b", password, base_url, timeout=timeout) as second,
    ):
        results.append(ScenarioResult("login", time.monotonic() - started))

        social_started = time.monotonic()
        _pump_until(first, lambda: first.game.bancho.players.by_name("fakeclient-b") is not None, timeout=timeout)
        peer = first.game.bancho.players.by_name("fakeclient-b")
        if peer is None:
            raise FakeClientError("first client did not load the second player's presence")
        peer.add_friend()
        peer.request_presence()
        peer.request_stats()
        first.game.bancho.request_status()
        first.game.bancho.players.request_updates(PresenceFilter.Friends)
        first.request_all_presences()
        second.set_away_message("fakeclient away")
        second.set_block_non_friend_dms(False)
        first.game.bancho.status.action = StatusAction.Playing
        first.game.bancho.status.text = "Perfcho - Perfcho E2E [Normal]"
        first.game.bancho.status.checksum = hashlib.md5(_DATA_PATH.read_bytes(), usedforsecurity=False).hexdigest()
        first.game.bancho.status.beatmap_id = BEATMAP_ID
        first.game.bancho.update_status()
        peer.send_message("fakeclient private message")
        second.wait_for(ServerPackets.SEND_MESSAGE, timeout=timeout)
        first_channel = first.game.bancho.channels.get("#osu")
        second_channel = second.game.bancho.channels.get("#osu")
        if first_channel is None or second_channel is None or not first_channel.joined or not second_channel.joined:
            raise FakeClientError("clients did not autojoin #osu")
        first_channel.send_message("fakeclient public message")
        second.wait_for(ServerPackets.SEND_MESSAGE, timeout=timeout)
        first_channel.leave()
        first_channel.join()
        rejoined_channel = first.game.bancho.channels.get("#osu")
        if rejoined_channel is None or not rejoined_channel.joined:
            raise FakeClientError("osu.py channel rejoin failed")
        second_friends = second.game.api.get_friends()
        if second_friends != [1]:
            diagnostic = second.game.api.session.get(
                f"{second.game.api.url}/web/osu-getfriends.php",
                params={"u": second.game.username, "h": second.game.password_hash},
                timeout=timeout,
            )
            raise FakeClientError(
                f"second client's Web friend list was {second_friends!r}; "
                f"diagnostic HTTP {diagnostic.status_code} body={diagnostic.text!r}"
            )
        peer.remove_friend()
        results.append(ScenarioResult("social-and-chat", time.monotonic() - social_started))

        spectator_started = time.monotonic()
        host = second.game.bancho.players.by_name("fakeclient-a")
        if host is None:
            raise FakeClientError("spectator did not load host presence")
        second.game.bancho.start_spectating(host)
        first.wait_for(ServerPackets.SPECTATOR_JOINED, timeout=timeout)
        second.game.bancho.cant_spectate()
        first.poll()
        frame = ReplayFrame(ButtonState.Left1, 100, 128.0, 192.0)
        score_frame = ScoreFrame(100, 0, 1, 0, 0, 0, 0, 0, 300, 1, 1, True, 255, 0)
        first.game.bancho.send_frames(ReplayAction.Standard, [frame], score_frame)
        second.wait_for(ServerPackets.SPECTATE_FRAMES, timeout=timeout)
        second.game.bancho.stop_spectating()
        first.wait_for(ServerPackets.SPECTATOR_LEFT, timeout=timeout)
        results.append(ScenarioResult("spectator", time.monotonic() - spectator_started))

        multiplayer_started = time.monotonic()
        fixture = _DATA_PATH.read_bytes()
        checksum = hashlib.md5(fixture, usedforsecurity=False).hexdigest()
        match = Match(
            name="perfcho fakeclient room",
            beatmap_text="Perfcho - Perfcho E2E [Normal]",
            beatmap_id=BEATMAP_ID,
            beatmap_checksum=checksum,
            host_id=first.game.bancho.user_id,
        )
        first.game.bancho.join_lobby()
        second.game.bancho.join_lobby()
        first.game.bancho.create_match(match)
        if first.game.bancho.match is None:
            raise FakeClientError("host did not receive match join success")
        first.game.bancho.match.invite(second.game.bancho.user_id)
        second.wait_for(ServerPackets.MATCH_INVITE, timeout=timeout)
        second.game.bancho.join_match(first.game.bancho.match.id)
        if second.game.bancho.match is None:
            raise FakeClientError("guest did not join the multiplayer room")
        second.game.bancho.match.change_slot(2)
        first.game.bancho.match.lock_slot(15)
        first.game.bancho.match.lock_slot(15)
        second.game.bancho.match.no_beatmap()
        second.game.bancho.match.has_beatmap()
        first.game.bancho.match.change_mods(Mods.Hidden)
        first.game.bancho.match.team_type = MatchTeamType.TeamVs
        first.game.bancho.match.change_settings()
        first.game.bancho.match.change_team()
        second.game.bancho.match.change_team()
        first.game.bancho.match.change_password("fakeclient-room-password")
        first.game.bancho.match.change_password("")
        guest_slot = next(
            index
            for index, slot in enumerate(first.game.bancho.match.slots)
            if slot.player_id == second.game.bancho.user_id
        )
        host_slot = next(
            index
            for index, slot in enumerate(first.game.bancho.match.slots)
            if slot.player_id == first.game.bancho.user_id
        )
        first.game.bancho.match.transfer_host(guest_slot)
        second.poll()
        if second.game.bancho.match is None:
            raise FakeClientError("guest lost multiplayer state during host transfer")
        second.game.bancho.match.transfer_host(host_slot)
        first.poll()
        second.game.bancho.match.ready()
        second.game.bancho.match.not_ready()
        second.game.bancho.match.ready()
        first.poll()
        first.game.bancho.match.start()
        second.wait_for(ServerPackets.MATCH_START, timeout=timeout)
        second.game.bancho.match.load_complete()
        first.game.bancho.match.load_complete()
        live_frame = ScoreFrame(2000, 0, 2, 0, 0, 0, 0, 0, 600, 2, 2, True, 255, 0)
        second.game.bancho.match.send_score(live_frame)
        first.wait_for(ServerPackets.MATCH_SCORE_UPDATE, timeout=timeout)
        second.game.bancho.match.skip()
        first.game.bancho.match.skip()
        second.game.bancho.match.fail()
        first.game.bancho.match.complete()
        second.game.bancho.leave_match()
        first.game.bancho.leave_match()
        first.game.bancho.leave_lobby()
        second.game.bancho.leave_lobby()
        results.append(ScenarioResult("multiplayer", time.monotonic() - multiplayer_started))

        web_started = time.monotonic()
        updates = first.game.api.check_updates()
        if not updates or updates[0].get("filename") != "osu!.exe":
            raise FakeClientError("osu.py update check did not receive a valid manifest")
        if first.game.api.get_backgrounds() is None or first.game.api.get_menu_content() is None:
            raise FakeClientError("osu.py public metadata forwarding failed")
        search = first.game.api.search_beatmapsets("Perfcho")
        if not search or search[0].set_id != BEATMAPSET_ID:
            raise FakeClientError("osu.py Direct search did not return the fixture")
        if "Added favourite" not in first.game.api.add_favourite(BEATMAPSET_ID):
            raise FakeClientError("osu.py favourite write failed")
        if BEATMAPSET_ID not in first.game.api.get_favourites():
            raise FakeClientError("osu.py favourite read failed")
        rating = first.game.api.get_star_rating(BEATMAP_ID)
        if rating < 0:
            raise FakeClientError("osu.py difficulty query returned an invalid value")
        first.game.api.post_comment(
            "fakeclient map comment",
            1000,
            target=CommentTarget.Map,
            beatmap_id=BEATMAP_ID,
        )
        comments = first.game.api.get_comments(beatmap_id=BEATMAP_ID)
        if not comments or comments[-1].text != "fakeclient map comment":
            raise FakeClientError("osu.py comment round trip failed")
        first.game.api.post_comment(
            "fakeclient song comment",
            500,
            target=CommentTarget.Song,
            set_id=BEATMAPSET_ID,
        )
        if not first.game.api.get_comments(set_id=BEATMAPSET_ID):
            raise FakeClientError("osu.py beatmapset comment round trip failed")
        if not first.game.bancho.player.avatar():
            raise FakeClientError("osu.py avatar forwarding failed")
        if not first.game.api.get_beatmap_thumbnail(BEATMAPSET_ID):
            raise FakeClientError("osu.py thumbnail forwarding failed")
        if not first.game.api.get_beatmap_preview(BEATMAPSET_ID):
            raise FakeClientError("osu.py preview forwarding failed")
        map_response = first.game.api.session.get(
            f"{first.game.api.url}/web/maps/{BEATMAP_FILE_NAME}",
            timeout=timeout,
        )
        if map_response.content != fixture:
            raise FakeClientError("Stable beatmap file stream did not match the synchronized fixture")
        leaderboard = first.game.api.get_scores(checksum, BEATMAP_FILE_NAME, BEATMAPSET_ID)
        if leaderboard is None or leaderboard.beatmap_id != BEATMAP_ID:
            raise FakeClientError("osu.py leaderboard query failed")
        for ranking_type in (RankingType.SelectedMod, RankingType.Friends, RankingType.Country):
            page = first.game.api.get_scores(
                checksum,
                BEATMAP_FILE_NAME,
                BEATMAPSET_ID,
                mods=Mods.NoMod,
                rank_type=ranking_type,
            )
            if page is None or page.beatmap_id != BEATMAP_ID:
                raise FakeClientError(f"osu.py {ranking_type.name} leaderboard query failed")
        rate_params: dict[str, str | int] = {
            "u": first.game.username,
            "p": first.game.password_hash,
            "c": checksum,
            "v": 9,
        }
        rate = first.game.api.session.get(
            f"{first.game.api.url}/web/osu-rate.php",
            params=rate_params,
            timeout=timeout,
        )
        if not rate.ok or not rate.text.startswith("alreadyvoted"):
            raise FakeClientError("Stable beatmap rating write failed")
        info = first.game.api.session.post(
            f"{first.game.api.url}/web/osu-getbeatmapinfo.php",
            params={"u": first.game.username, "h": first.game.password_hash},
            json={"Filenames": [BEATMAP_FILE_NAME], "Ids": [str(BEATMAP_ID)]},
            timeout=timeout,
        )
        if not info.ok or checksum not in info.text:
            raise FakeClientError("Stable beatmap info query failed")
        direct_set_params: dict[str, str | int] = {
            "u": first.game.username,
            "h": first.game.password_hash,
            "s": BEATMAPSET_ID,
        }
        direct_set = first.game.api.session.get(
            f"{first.game.api.url}/web/osu-search-set.php",
            params=direct_set_params,
            timeout=timeout,
        )
        if not direct_set.ok or str(BEATMAPSET_ID) not in direct_set.text:
            raise FakeClientError("Stable Direct set query failed")
        downloaded = b"".join(first.iter_download(BEATMAPSET_ID))
        if not downloaded:
            raise FakeClientError("osu.py Direct download returned no bytes")
        results.append(ScenarioResult("stable-web", time.monotonic() - web_started))

        scoring_started = time.monotonic()
        replay = b"fakeclient-replay".ljust(32, b"-")
        score_id = submit_score(first, beatmap_md5=checksum, replay=replay)
        deadline = time.monotonic() + timeout
        projected = None
        while time.monotonic() < deadline:
            projected = first.game.api.get_scores(checksum, BEATMAP_FILE_NAME, BEATMAPSET_ID)
            if projected is not None and any(score.id == score_id for score in projected.scores):
                break
            time.sleep(0.2)
        if projected is None or not any(score.id == score_id for score in projected.scores):
            raise FakeClientError("submitted score was not projected to the Stable leaderboard")
        downloaded_replay = second.game.api.get_replay(score_id)
        if downloaded_replay != replay:
            raise FakeClientError("osu.py replay download did not match the submitted artifact")
        projected_score = next(score for score in projected.scores if score.id == score_id)
        if projected_score.get_replay(second.game) != replay:
            raise FakeClientError("osu.py Score replay helper failed")
        first.game.api.post_comment(
            "fakeclient replay comment",
            250,
            target=CommentTarget.Replay,
            replay_id=score_id,
        )
        if not projected_score.get_comments(first.game):
            raise FakeClientError("osu.py Score comment helper failed")
        mark_read = second.game.api.session.get(
            f"{second.game.api.url}/web/osu-markasread.php",
            params={
                "u": second.game.username,
                "h": second.game.password_hash,
                "channel": first.game.username,
            },
            timeout=timeout,
        )
        if not mark_read.ok:
            raise FakeClientError("Stable direct-message read cursor update failed")
        results.append(ScenarioResult("score-and-replay", time.monotonic() - scoring_started))
    return tuple(results)
