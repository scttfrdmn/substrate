"""Unit tests for the deterministic-run settings (#1598) — no substrate binary required."""

from __future__ import annotations

import datetime
import json
import subprocess
from unittest.mock import MagicMock, patch

import pytest

from pytest_substrate import DEFAULT_EPOCH, DEFAULT_SEED
from pytest_substrate.plugin import _requested_run, _resolve_run
from pytest_substrate.server import SubstrateServer


def _json_response(payload: dict) -> MagicMock:
    mock = MagicMock()
    mock.__enter__ = lambda s: s
    mock.__exit__ = MagicMock(return_value=False)
    mock.read.return_value = json.dumps(payload).encode()
    return mock


def _launched_env(server: SubstrateServer) -> dict[str, str]:
    """Start ``server`` with Popen and the health wait mocked, and return the env it was given."""
    with (
        patch.object(SubstrateServer, "_find_binary", return_value="/bin/substrate"),
        patch("subprocess.Popen", return_value=MagicMock(spec=subprocess.Popen)) as popen,
        patch.object(server, "_wait_healthy"),
    ):
        server.start()
    return popen.call_args.kwargs["env"]


class TestLaunchEnvironment:
    def test_seeds_by_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("SUBSTRATE_RUN_EPOCH", raising=False)
        env = _launched_env(SubstrateServer())
        assert env["SUBSTRATE_RUN_SEED"] == str(DEFAULT_SEED)
        assert "SUBSTRATE_RUN_EPOCH" not in env

    def test_seed_zero_is_passed_so_the_server_is_unseeded(self) -> None:
        assert _launched_env(SubstrateServer(seed=0))["SUBSTRATE_RUN_SEED"] == "0"

    def test_epoch_pins_the_clock(self) -> None:
        env = _launched_env(SubstrateServer(epoch="2026-03-04T05:06:07Z"))
        assert env["SUBSTRATE_RUN_EPOCH"] == "2026-03-04T05:06:07Z"

    def test_a_datetime_epoch_is_rendered_in_utc(self) -> None:
        when = datetime.datetime(2026, 3, 4, 6, 6, 7, tzinfo=datetime.timezone(datetime.timedelta(hours=1)))
        env = _launched_env(SubstrateServer(epoch=when))
        assert env["SUBSTRATE_RUN_EPOCH"] == "2026-03-04T05:06:07.000000Z"

    def test_the_callers_environment_is_inherited(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SUBSTRATE_LOG_LEVEL", "debug")
        assert _launched_env(SubstrateServer())["SUBSTRATE_LOG_LEVEL"] == "debug"


class TestRunControl:
    def test_run_settings_reads_the_endpoint(self) -> None:
        server = SubstrateServer()
        reply = {"seed": 1, "epoch": DEFAULT_EPOCH, "drawn": 3}
        with patch("urllib.request.urlopen", return_value=_json_response(reply)) as urlopen:
            assert server.run_settings() == reply
        assert urlopen.call_args[0][0] == f"{server.url}/v1/control/run"

    @pytest.mark.parametrize(
        ("kwargs", "body"),
        [
            ({"seed": 7}, {"seed": 7}),
            ({"epoch": "2026-01-01T00:00:00Z"}, {"epoch": "2026-01-01T00:00:00Z"}),
            ({"epoch": None}, {"epoch": ""}),
            ({"seed": 0, "epoch": None}, {"seed": 0, "epoch": ""}),
            ({}, {}),
        ],
    )
    def test_set_run_sends_only_what_was_passed(self, kwargs: dict, body: dict) -> None:
        server = SubstrateServer()
        with patch("urllib.request.urlopen", return_value=_json_response({"seed": 0, "drawn": 0})) as urlopen:
            server.set_run(**kwargs)
        req = urlopen.call_args[0][0]
        assert req.full_url == f"{server.url}/v1/control/run"
        assert req.get_method() == "POST"
        assert json.loads(req.data) == body

    def test_freeze_time_sends_frozen_and_the_instant(self) -> None:
        server = SubstrateServer()
        reply = {"simulated_time": "2026-01-01T00:00:00Z", "scale": 1, "frozen": True}
        with patch("urllib.request.urlopen", return_value=_json_response(reply)) as urlopen:
            got = server.freeze_time("2026-01-01T00:00:00Z")
        assert json.loads(urlopen.call_args[0][0].data) == {"frozen": True, "time": "2026-01-01T00:00:00Z"}
        assert got == datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)

    def test_unfreeze_time_sends_frozen_false(self) -> None:
        server = SubstrateServer()
        reply = {"simulated_time": "2026-01-01T00:00:00Z", "scale": 1, "frozen": False}
        with patch("urllib.request.urlopen", return_value=_json_response(reply)) as urlopen:
            server.unfreeze_time()
        assert json.loads(urlopen.call_args[0][0].data) == {"frozen": False}


class TestRunSettingsResolution:
    @pytest.mark.parametrize(
        ("settings", "want"),
        [
            ({}, (None, None)),
            ({"deterministic": True}, (None, DEFAULT_EPOCH)),
            ({"deterministic": True, "epoch": "2026-05-05T00:00:00Z"}, (None, "2026-05-05T00:00:00Z")),
            ({"seed": "9"}, (9, None)),
            ({"deterministic": False}, (None, None)),
        ],
    )
    def test_resolve(self, settings: dict, want: tuple) -> None:
        assert _resolve_run(settings) == want

    def test_marker_then_indirect_parameter(self) -> None:
        marker = MagicMock(kwargs={"seed": 3, "deterministic": False})
        node = MagicMock()
        node.get_closest_marker.return_value = marker
        request = MagicMock(node=node, param={"deterministic": True})
        assert _requested_run(request) == {"seed": 3, "deterministic": True}

    def test_a_bare_bool_parameter_means_deterministic(self) -> None:
        node = MagicMock()
        node.get_closest_marker.return_value = None
        assert _requested_run(MagicMock(node=node, param=True)) == {"deterministic": True}
