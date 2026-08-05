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

import os
import sys
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


def test_run_falls_back_to_cached_config_file(manager):
    """After the startup window, last-good config file supplies the tags."""
    # Simulate a previous successful run having written the config file
    manager.process_metadata(SAMPLE_API_RESPONSE)
    manager.create_config_file()
    manager.station_metadata = {}
    utils.StationMetadataSingleton().load_metadata({})

    manager.STARTUP_RETRY_WINDOW = 0  # exhaust the blocking window immediately
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
