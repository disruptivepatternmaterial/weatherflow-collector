# test_unknown_station_location.py

"""
Unknown station location must stay None from where it is created onward.

Before this, missing metadata produced latitude 0.0, longitude 0.0 and
elevation 0.0. Those landed in the message tags, and with elevation 0 the
calculated sea level pressure equalled the station pressure.

Run from the repository root:

    python3 -m pytest tests/ -v

No network or database is required.
"""

import asyncio
import os
import sys
import textwrap
from unittest import mock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

# config.py requires the API token at import time; no network calls are made
os.environ.setdefault("WEATHERFLOW_COLLECTOR_API_TOKEN", "test-token")

import utils.utils as utils  # noqa: E402
from handlers.rest_forecasts import RESTForecastsHandler  # noqa: E402
from handlers.rest_observations_device import RESTObservationsDeviceHandler  # noqa: E402
from handlers.rest_observations_station import RESTObservationsStationHandler  # noqa: E402
from station_metadata_manager import StationMetadataManager  # noqa: E402
from utils.calculate_weather_metrics import CalculateWeatherMetrics  # noqa: E402

READING = {"air_temperature": 12.0, "relative_humidity": 70.0,
           "station_pressure": 1000.0, "wind_avg": 1.5}


@pytest.fixture
def manager(tmp_path):
    utils.StationMetadataSingleton().load_metadata({})
    mgr = StationMetadataManager()
    mgr.config_file = str(tmp_path / "conf" / "weatherflow_station.conf")
    yield mgr
    utils.StationMetadataSingleton().load_metadata({})


# --- where the location is created --------------------------------------------

def test_api_station_without_location_is_none(manager):
    manager.process_metadata({"stations": [
        {"station_id": 12345, "name": "Test", "devices": []},
    ]})
    station = manager.station_metadata[12345]

    assert station["latitude"] is None
    assert station["longitude"] is None
    assert station["elevation"] is None


def test_api_station_location_is_kept(manager):
    manager.process_metadata({"stations": [
        {"station_id": 12345, "name": "Test", "latitude": 40.0,
         "longitude": -105.0, "station_meta": {"elevation": 0.0}, "devices": []},
    ]})
    station = manager.station_metadata[12345]

    assert station["latitude"] == 40.0
    assert station["longitude"] == -105.0
    assert station["elevation"] == 0.0


def test_config_file_fallback_location_is_none(manager):
    os.makedirs(os.path.dirname(manager.config_file), exist_ok=True)
    with open(manager.config_file, "w") as f:
        f.write(textwrap.dedent("""\
            [12345]
            enabled = True
            name = Test

            [Device_222_12345]
            enabled = True
            device_id = 222
            device_type = ST
            serial_number = ST-00000001
            name = Tempest
            """))

    assert manager.load_metadata_from_config_file() is True
    station = manager.station_metadata[12345]

    assert station["latitude"] is None
    assert station["longitude"] is None
    assert station["elevation"] is None


# --- no sea level pressure without an elevation ---------------------------------

def test_no_sea_level_pressure_without_elevation():
    got = CalculateWeatherMetrics.calculate_weather_metrics(dict(READING, elevation=None))
    assert "calculated_sea_level_pressure" not in got


def test_sea_level_pressure_with_a_real_elevation():
    got = CalculateWeatherMetrics.calculate_weather_metrics(dict(READING, elevation=1000.0))
    assert got["calculated_sea_level_pressure"] > READING["station_pressure"]


def _published_fields(handler_cls, full_data, call):
    event_manager = mock.Mock()
    event_manager.publish = mock.AsyncMock()
    handler = handler_cls(event_manager)
    asyncio.run(getattr(handler, call)(full_data))
    measurements = {"weatherflow_obs", "weatherflow_forecast_current"}
    published = [
        c.args[1] for c in event_manager.publish.call_args_list
        if c.args[0] == "influxdb_storage_event"
        and c.args[1].get("measurement") in measurements
    ]
    assert len(published) == 1
    return published[0]["fields"]


def _device_obs(station_info):
    obs = [1787887750, 0.5, READING["wind_avg"], 2.0, 180, 3,
           READING["station_pressure"], READING["air_temperature"],
           READING["relative_humidity"]]
    return {"metadata": {"station_id": 12345},
            "data": {"obs": [obs], "summary": {}, "source": "db"},
            "station_info": station_info, "device_info": {}}


def _station_obs(station_info):
    return {"metadata": {"station_id": 12345},
            "data": dict(READING, timestamp=1787887750),
            "station_info": station_info}


def _current_conditions(station_info):
    return {"metadata": {"station_id": 12345},
            "data": {"current_conditions": dict(READING, time=1787887750)},
            "station_info": station_info}


REST_PATHS = [
    (RESTObservationsDeviceHandler, _device_obs, "process_data"),
    (RESTObservationsStationHandler, _station_obs, "process_data"),
    (RESTForecastsHandler, _current_conditions, "handle_current_conditions"),
]


@pytest.mark.parametrize("handler_cls, build, call", REST_PATHS)
@pytest.mark.parametrize("station_info", [{}, {"station_elevation": None}])
def test_rest_paths_calculate_no_sea_level_pressure_without_elevation(
    handler_cls, build, call, station_info
):
    fields = _published_fields(handler_cls, build(station_info), call)
    assert "calculated_sea_level_pressure" not in fields
    assert "calculated_dew_point" in fields


@pytest.mark.parametrize("handler_cls, build, call", REST_PATHS)
def test_rest_paths_calculate_sea_level_pressure_with_elevation(handler_cls, build, call):
    fields = _published_fields(handler_cls, build({"station_elevation": 1000.0}), call)
    assert fields["calculated_sea_level_pressure"] > READING["station_pressure"]
