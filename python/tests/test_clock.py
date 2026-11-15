"""Unit tests for SubstrateServer's clock helpers (#1518) — no substrate binary required."""

from __future__ import annotations

import datetime
import json
from unittest.mock import MagicMock, patch

import pytest

from pytest_substrate.server import SubstrateServer, _parse_rfc3339

UTC = datetime.UTC


def _clock_response(simulated: str) -> MagicMock:
    """A urlopen() result whose body is GET /v1/control/time's shape."""
    mock = MagicMock()
    mock.__enter__ = lambda s: s
    mock.__exit__ = MagicMock(return_value=False)
    mock.read.return_value = json.dumps({"simulated_time": simulated, "scale": 1}).encode()
    return mock


class TestParseRFC3339:
    @pytest.mark.parametrize(
        ("raw", "want"),
        [
            ("2026-11-16T12:00:00Z", datetime.datetime(2026, 11, 16, 12, tzinfo=UTC)),
            # Go renders nanoseconds; Python keeps microseconds.
            ("2026-11-16T12:00:00.123456789Z", datetime.datetime(2026, 11, 16, 12, 0, 0, 123456, tzinfo=UTC)),
            ("2026-11-16T12:00:00.5Z", datetime.datetime(2026, 11, 16, 12, 0, 0, 500000, tzinfo=UTC)),
            ("2026-11-16T13:00:00+01:00", datetime.datetime(2026, 11, 16, 12, tzinfo=UTC)),
        ],
    )
    def test_parses_what_the_clock_endpoint_reports(self, raw: str, want: datetime.datetime) -> None:
        assert _parse_rfc3339(raw) == want


class TestSetTime:
    def test_posts_the_instant_to_the_clock_endpoint(self) -> None:
        server = SubstrateServer()
        with patch("urllib.request.urlopen", return_value=_clock_response("2026-11-16T12:00:00Z")) as mock_open:
            got = server.set_time(datetime.datetime(2026, 11, 16, 12, tzinfo=UTC))

        req = mock_open.call_args[0][0]
        assert req.full_url == f"{server.url}/v1/control/time"
        assert req.get_method() == "POST"
        assert json.loads(req.data) == {"time": "2026-11-16T12:00:00.000000Z"}
        assert got == datetime.datetime(2026, 11, 16, 12, tzinfo=UTC)

    def test_a_naive_datetime_is_utc_and_a_string_is_sent_as_is(self) -> None:
        server = SubstrateServer()
        with patch("urllib.request.urlopen", return_value=_clock_response("2026-11-16T12:00:00Z")) as mock_open:
            server.set_time(datetime.datetime(2026, 11, 16, 12))
            server.set_time("2026-11-16T12:00:00Z")
        sent = [json.loads(call[0][0].data)["time"] for call in mock_open.call_args_list]
        assert sent == ["2026-11-16T12:00:00.000000Z", "2026-11-16T12:00:00Z"]


class TestAdvanceTime:
    def test_reads_the_clock_and_sets_it_forward(self) -> None:
        server = SubstrateServer()
        responses = [_clock_response("2026-11-16T12:00:00Z"), _clock_response("2026-11-16T13:00:05Z")]
        with patch("urllib.request.urlopen", side_effect=responses) as mock_open:
            got = server.advance_time(hours=1, seconds=5)

        get_url = mock_open.call_args_list[0][0][0]
        assert get_url == f"{server.url}/v1/control/time"
        posted = mock_open.call_args_list[1][0][0]
        assert json.loads(posted.data) == {"time": "2026-11-16T13:00:05.000000Z"}
        assert got == datetime.datetime(2026, 11, 16, 13, 0, 5, tzinfo=UTC)

    def test_refuses_a_backward_move_and_mixed_arguments(self) -> None:
        server = SubstrateServer()
        with pytest.raises(ValueError):
            server.advance_time(datetime.timedelta(seconds=-1))
        with pytest.raises(TypeError):
            server.advance_time(datetime.timedelta(hours=1), minutes=5)
