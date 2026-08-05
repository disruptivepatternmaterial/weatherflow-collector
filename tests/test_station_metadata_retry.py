# test_station_metadata_retry.py

"""
Tests for StationMetadataManager startup retry / fallback logic.

Covers the fix for the 2026-08-04 boot-race incident: the collector started
before the network was up, the single startup metadata fetch failed with a
ConnectionError, and all rows were written with hub-only tags until a manual
restart.

Run from the repository root:

    python3 -m pytest tests/ -v

No network access is required; the WeatherFlow API fetch is mocked.
"""

import copy
import os
import sys
import textwrap
from unittest import mock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

# config.py requires the API token at import time; no network calls are made
os.environ.setdefault("WEATHERFLOW_COLLECTOR_API_TOKEN", "test-token")

import utils.utils as utils  # noqa: E402
from station_metadata_manager import StationMetadataManager  # noqa: E402


SAMPLE_API_RESPONSE = {
    "stations": [
        {
            "station_id": 12345,
            "name": "Bowman Mtn",
            "latitude": 40.0,
            "longitude": -105.0,
            "timezone": "America/Los_Angeles",
            "station_meta": {"elevation": 1000.0},
            "devices": [
                {
                    "device_id": 111,
                    "device_type": "HB",
                    "serial_number": "HB-00145924",
                    "device_meta": {"name": "Hub"},
                },
                {
                    "device_id": 222,
                    "device_type": "ST",
                    "serial_number": "ST-00163656",
                    "device_meta": {"name": "Tempest"},
                },
            ],
        }
    ]
}


class FakeClock:
    """Deterministic replacement for time.monotonic / time.sleep."""

    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


@pytest.fixture
def manager(tmp_path):
    utils.StationMetadataSingleton().load_metadata({})
    mgr = StationMetadataManager()
    mgr.config_file = str(tmp_path / "conf" / "weatherflow_station.conf")
    yield mgr
    utils.StationMetadataSingleton().load_metadata({})


def test_run_retries_with_backoff_until_fetch_succeeds(manager):
    """Transient ConnectionError at boot must not leave the singleton empty."""
    manager.fetch_station_metadata = mock.Mock(
        side_effect=[None, None, SAMPLE_API_RESPONSE]
    )

    with mock.patch("time.sleep") as mock_sleep:
        manager.run()

    assert manager.fetch_station_metadata.call_count == 3
    # Exponential backoff: 5 s, then 10 s
    assert [call.args[0] for call in mock_sleep.call_args_list] == [5, 10]

    metadata = utils.StationMetadataSingleton().get_metadata()
    assert 12345 in metadata
    assert metadata[12345]["name"] == "Bowman Mtn"

    # Atomic write: config file exists, no tmp file left behind
    assert os.path.exists(manager.config_file)
    assert not os.path.exists(f"{manager.config_file}.tmp")


def test_run_falls_back_to_cached_config_file(manager):
    """After the startup window, last-good config file supplies the tags."""
    # Simulate a previous successful run having written the config file
    manager.process_metadata(SAMPLE_API_RESPONSE)
    manager.create_config_file()
    manager.station_metadata = {}
    utils.StationMetadataSingleton().load_metadata({})

    # Cache exists, so run() uses the short cached window; exhaust it at once
    manager.STARTUP_RETRY_CACHED_WINDOW = 0
    manager.fetch_station_metadata = mock.Mock(return_value=None)

    with mock.patch.object(manager, "_start_background_refresh") as mock_bg:
        manager.run()

    mock_bg.assert_called_once()

    metadata = utils.StationMetadataSingleton().get_metadata()
    assert 12345 in metadata
    assert metadata[12345]["station_name"] == "Bowman Mtn"
    serials = {d["serial_number"] for d in metadata[12345]["devices"]}
    assert serials == {"HB-00145924", "ST-00163656"}

    # The lookup used by the data processor to build tags must resolve
    station, device = utils.get_station_and_device_config_by_serial_number(
        "ST-00163656"
    )
    assert station is not None
    assert device["serial_number"] == "ST-00163656"


def test_run_without_cache_starts_background_refresh(manager):
    """No cache + API down: singleton stays empty but retries continue."""
    manager.STARTUP_RETRY_WINDOW = 0
    manager.fetch_station_metadata = mock.Mock(return_value=None)

    with mock.patch.object(manager, "_start_background_refresh") as mock_bg:
        manager.run()

    mock_bg.assert_called_once()
    assert utils.StationMetadataSingleton().get_metadata() == {}


def test_background_refresh_loads_singleton_when_api_recovers(manager):
    """A late background fetch success must fix tags for subsequent rows."""
    manager.fetch_station_metadata = mock.Mock(
        side_effect=[None, SAMPLE_API_RESPONSE]
    )

    with mock.patch("time.sleep") as mock_sleep:
        manager._background_refresh()  # returns once the fetch succeeds

    assert manager.fetch_station_metadata.call_count == 2
    assert [call.args[0] for call in mock_sleep.call_args_list] == [5, 10]

    # Handlers read the singleton live per event, so this is sufficient
    # for later rows to carry correct serial/station tags
    station, device = utils.get_station_and_device_config_by_serial_number(
        "ST-00163656"
    )
    assert station is not None
    assert station["name"] == "Bowman Mtn"


def test_backoff_delay_is_capped(manager):
    """Backoff doubles from 5 s and caps at 5 minutes."""
    failures = 10
    manager.fetch_station_metadata = mock.Mock(
        side_effect=[None] * failures + [SAMPLE_API_RESPONSE]
    )

    with mock.patch("time.sleep") as mock_sleep:
        manager._background_refresh()

    delays = [call.args[0] for call in mock_sleep.call_args_list]
    assert delays == [5, 10, 20, 40, 80, 160, 300, 300, 300, 300, 300]


def test_background_refresh_never_mutates_published_dict(manager):
    """The published singleton dict must never be mutated in place."""
    # Publish last-good cached metadata first (as the fallback path would)
    manager.process_metadata(SAMPLE_API_RESPONSE)
    manager.create_config_file()
    manager.station_metadata = {}
    assert manager.load_metadata_from_config_file()

    singleton = utils.StationMetadataSingleton()
    singleton.load_metadata(manager.station_metadata)
    published = singleton.get_metadata()
    snapshot = copy.deepcopy(published)

    # Background refresh succeeds with fresh (richer) metadata
    manager.fetch_station_metadata = mock.Mock(return_value=SAMPLE_API_RESPONSE)
    with mock.patch("time.sleep"):
        manager._background_refresh()

    refreshed = singleton.get_metadata()
    assert refreshed is not published  # new dict published atomically
    assert published == snapshot  # old dict untouched
    assert refreshed[12345]["latitude"] == 40.0  # fresh data present


def test_run_with_cache_uses_short_startup_window(manager):
    """A valid cache must not burn the full 10-minute window before fallback."""
    manager.process_metadata(SAMPLE_API_RESPONSE)
    manager.create_config_file()
    manager.station_metadata = {}
    utils.StationMetadataSingleton().load_metadata({})

    manager.fetch_station_metadata = mock.Mock(return_value=None)
    clock = FakeClock()

    with mock.patch("time.monotonic", clock.monotonic), mock.patch(
        "time.sleep", clock.sleep
    ), mock.patch.object(manager, "_start_background_refresh") as mock_bg:
        manager.run()

    mock_bg.assert_called_once()
    assert clock.now <= manager.STARTUP_RETRY_CACHED_WINDOW  # ~15 s, not 10 min
    assert manager.fetch_station_metadata.call_count <= 3

    metadata = utils.StationMetadataSingleton().get_metadata()
    assert 12345 in metadata  # cache was loaded


def test_run_without_cache_does_not_overshoot_window(manager):
    """Sleeps are clamped so the startup window is ~10 min, not ~15."""
    manager.fetch_station_metadata = mock.Mock(return_value=None)
    clock = FakeClock()

    with mock.patch("time.monotonic", clock.monotonic), mock.patch(
        "time.sleep", clock.sleep
    ), mock.patch.object(manager, "_start_background_refresh"):
        manager.run()

    assert clock.now == manager.STARTUP_RETRY_WINDOW  # exactly 600 s, no overshoot


def test_fallback_skips_malformed_config_sections(manager):
    """One bad section is skipped with a warning; the rest is recovered."""
    os.makedirs(os.path.dirname(manager.config_file), exist_ok=True)
    with open(manager.config_file, "w") as f:
        f.write(
            textwrap.dedent(
                """\
                [General]
                api_token = test-token

                [12345]
                enabled = True
                name = Bowman Mtn

                [Device_222_12345]
                enabled = True
                device_id = 222
                device_type = ST
                serial_number = ST-00163656
                name = Tempest

                [Device_999_12345]
                enabled = notabool
                device_id = Unknown
                device_type = HB
                serial_number = HB-BAD
                name = Bad
                """
            )
        )

    assert manager.load_metadata_from_config_file() is True
    devices = manager.station_metadata[12345]["devices"]
    assert {d["serial_number"] for d in devices} == {"ST-00163656"}
    assert manager.station_metadata[12345]["station_name"] == "Bowman Mtn"
