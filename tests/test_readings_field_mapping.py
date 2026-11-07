# test_readings_field_mapping.py

"""
Tests for the payload-to-column mapping used by weather.readings.

Covers the rapid_wind defect: every weatherflow_rapid_wind row ever written had
wind_speed_mps NULL, from all three collectors, because rapid_wind reports the
instantaneous speed as `wind_speed` while FIELD_TO_COLUMN only knew `wind_avg`.
The direction mapped fine, so the rows looked populated at a glance.

Run from the repository root:

    python3 -m pytest tests/ -v

No database is required; only the pure row-building helper is exercised.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

# config.py requires the API token at import time; no network calls are made
os.environ.setdefault("WEATHERFLOW_COLLECTOR_API_TOKEN", "test-token")

import utils.utils as utils  # noqa: E402
from storage.timescaledb import (  # noqa: E402
    FIELD_TO_COLUMN,
    READING_COLUMNS,
    _build_readings_row,
)


def row_as_dict(measurement, fields, tags=None, timestamp=1787887750):
    """Build a readings row and label it by column so assertions read clearly."""
    row = _build_readings_row(measurement, tags or {}, fields, timestamp)
    assert len(row) == len(READING_COLUMNS), (
        "row width %d does not match the %d columns it is inserted into"
        % (len(row), len(READING_COLUMNS))
    )
    return dict(zip(READING_COLUMNS, row))


# --- the defect itself -------------------------------------------------------

def test_rapid_wind_records_its_speed():
    """A rapid_wind payload must land a speed, not just a direction."""
    fields = {"timestamp": 1787887750, "wind_speed": 0.66, "wind_direction": 280}
    got = row_as_dict("weatherflow_rapid_wind", fields)

    assert got["wind_speed_mps"] == pytest.approx(0.66)
    assert got["wind_direction_deg"] == 280


def test_rapid_wind_speed_of_zero_is_stored_not_dropped():
    """Dead calm is a real reading. Falsy values must survive the mapping."""
    fields = {"timestamp": 1787887750, "wind_speed": 0.0, "wind_direction": 0}
    got = row_as_dict("weatherflow_rapid_wind", fields)

    assert got["wind_speed_mps"] == pytest.approx(0.0)
    assert got["wind_speed_mps"] is not None
    assert got["wind_direction_deg"] == 0


def test_rapid_wind_speed_arriving_as_a_string_is_coerced():
    """The websocket feed is JSON, so guard against a numeric string."""
    fields = {"timestamp": 1787887750, "wind_speed": "2.41", "wind_direction": "55"}
    got = row_as_dict("weatherflow_rapid_wind", fields)

    assert got["wind_speed_mps"] == pytest.approx(2.41)
    assert got["wind_direction_deg"] == 55


# --- the mapping the fix must not disturb ------------------------------------

def test_obs_still_maps_wind_avg():
    """obs_st reports wind_avg; that path worked and must keep working."""
    fields = {
        "timestamp": 1787887750,
        "wind_avg": 3.2,
        "wind_gust": 5.1,
        "wind_lull": 1.4,
        "wind_direction": 190,
        "air_temperature": 20.0,
    }
    got = row_as_dict("weatherflow_obs", fields)

    assert got["wind_speed_mps"] == pytest.approx(3.2)
    assert got["wind_gust_mps"] == pytest.approx(5.1)
    assert got["wind_lull_mps"] == pytest.approx(1.4)
    assert got["wind_direction_deg"] == 190
    assert got["temperature_c"] == pytest.approx(20.0)
    assert "temperature_f" not in got


def test_both_wind_field_names_target_the_same_column():
    """
    wind_avg and wind_speed are two names for one column, so a payload carrying
    both would have one silently overwrite the other. No WeatherFlow message
    does that today; this pins the assumption so it is noticed if one ever does.
    """
    assert FIELD_TO_COLUMN["wind_avg"] == "wind_speed_mps"
    assert FIELD_TO_COLUMN["wind_speed"] == "wind_speed_mps"


def test_a_missing_wind_speed_stays_null():
    """Absent is still absent; the fix must not invent a zero."""
    got = row_as_dict("weatherflow_rapid_wind", {"timestamp": 1787887750,
                                                 "wind_direction": 280})
    assert got["wind_speed_mps"] is None


# --- dew point ---------------------------------------------------------------

def test_obs_stores_calculated_dew_point():
    """The station sends `calculated_dew_point`; `_buck` is a different formula."""
    fields = {
        "timestamp": 1787887750,
        "air_temperature": 11.2,
        "calculated_dew_point": 8.9,
        "calculated_dew_point_buck": 8.7,
    }
    got = row_as_dict("weatherflow_obs", fields)

    assert got["dew_point_c"] == pytest.approx(8.9)


def test_rapid_wind_has_no_dew_point():
    got = row_as_dict("weatherflow_rapid_wind", {"timestamp": 1787887750,
                                                 "wind_speed": 0.66,
                                                 "wind_direction": 280})
    assert got["dew_point_c"] is None


# --- today's rain ------------------------------------------------------------

def test_obs_stores_todays_rain():
    got = row_as_dict("weatherflow_obs", {"timestamp": 1787887750,
                                          "local_daily_rain_accumulation": 6.809263})
    assert got["precip_accum_local_day_mm"] == pytest.approx(6.809263)


def test_todays_rain_of_zero_is_stored_not_dropped():
    """A dry day is a real reading, not a missing one."""
    got = row_as_dict("weatherflow_obs", {"timestamp": 1787887750,
                                          "local_daily_rain_accumulation": 0.0})
    assert got["precip_accum_local_day_mm"] is not None
    assert got["precip_accum_local_day_mm"] == 0.0


def test_missing_todays_rain_stays_null():
    got = row_as_dict("weatherflow_obs", {"timestamp": 1787887750,
                                          "air_temperature": 11.2})
    assert got["precip_accum_local_day_mm"] is None


# --- other calculated values -------------------------------------------------

def test_udp_obs_stores_calculated_values():
    """The station's direct reading only has the collector's calculated values."""
    fields = {
        "timestamp": 1787887750,
        "air_temperature": 12.9,
        "calculated_sea_level_pressure": 1016.2,
        "calculated_heat_index": 12.9,
        "calculated_wind_chill": 12.4,
    }
    got = row_as_dict("weatherflow_obs", fields)

    assert got["sea_level_pressure_hpa"] == pytest.approx(1016.2)
    assert got["heat_index_c"] == pytest.approx(12.9)
    assert got["wind_chill_c"] == pytest.approx(12.4)


def test_web_service_values_win_over_calculated_ones():
    """REST rows carry both; the value WeatherFlow reports is the one stored."""
    fields = {
        "timestamp": 1787887750,
        "heat_index": 9.9,
        "calculated_heat_index": 9.94,
        "wind_chill": 8.7,
        "calculated_wind_chill": 9.9,
        "sea_level_pressure": 1015.0,
        "calculated_sea_level_pressure": 1016.2,
    }
    got = row_as_dict("weatherflow_obs", fields)

    assert got["heat_index_c"] == pytest.approx(9.9)
    assert got["wind_chill_c"] == pytest.approx(8.7)
    assert got["sea_level_pressure_hpa"] == pytest.approx(1015.0)


def test_rapid_wind_gets_none_of_the_obs_values():
    got = row_as_dict("weatherflow_rapid_wind", {"timestamp": 1787887750,
                                                 "wind_speed": 0.66,
                                                 "wind_direction": 280})
    for column in ("precip_accum_local_day_mm", "sea_level_pressure_hpa",
                   "heat_index_c", "wind_chill_c"):
        assert got[column] is None


# --- station location ----------------------------------------------------------

STATION_TAGS = {
    "serial_number": "ST-00000001",
    "station_latitude": 40.0,
    "station_longitude": -105.0,
    "station_elevation": 1000.0,
}


@pytest.mark.parametrize("measurement", ["weatherflow_obs", "weatherflow_rapid_wind"])
def test_station_location_tags_fill_their_columns(measurement):
    got = row_as_dict(measurement, {"timestamp": 1787887750, "wind_speed": 0.66},
                      tags=STATION_TAGS)

    assert got["latitude"] == pytest.approx(40.0)
    assert got["longitude"] == pytest.approx(-105.0)
    assert got["elevation_m"] == pytest.approx(1000.0)


def test_unknown_station_location_stays_null():
    tags = {"serial_number": "ST-00000001", "station_latitude": None,
            "station_longitude": None, "station_elevation": None}
    got = row_as_dict("weatherflow_obs", {"timestamp": 1787887750}, tags=tags)

    assert got["latitude"] is None
    assert got["longitude"] is None
    assert got["elevation_m"] is None


def test_missing_station_location_tags_stay_null():
    got = row_as_dict("weatherflow_obs", {"timestamp": 1787887750},
                      tags={"serial_number": "ST-00000001"})

    assert got["latitude"] is None
    assert got["longitude"] is None
    assert got["elevation_m"] is None


def test_station_elevation_of_zero_is_stored_not_dropped():
    """A station at sea level is a real reading."""
    tags = dict(STATION_TAGS, station_elevation=0.0)
    got = row_as_dict("weatherflow_obs", {"timestamp": 1787887750}, tags=tags)

    assert got["elevation_m"] is not None
    assert got["elevation_m"] == 0.0


def test_station_location_that_is_not_a_number_is_not_stored():
    tags = dict(STATION_TAGS, station_latitude="40.0", station_longitude=True)
    got = row_as_dict("weatherflow_obs", {"timestamp": 1787887750}, tags=tags)

    assert got["latitude"] is None
    assert got["longitude"] is None
    assert got["elevation_m"] == pytest.approx(1000.0)


def test_strike_position_fields_are_not_the_station_location():
    fields = {"timestamp": 1787887750, "latitude": 41.5, "longitude": -104.2}
    got = row_as_dict("weatherflow_geo_strike", fields,
                      tags={"serial_number": "ST-00000001"})

    assert got["latitude"] is None
    assert got["longitude"] is None


# --- the normaliser backing it ----------------------------------------------

def test_normalizer_treats_wind_speed_like_wind_avg():
    normalized = utils.normalize_fields({"wind_avg": "3.2", "wind_speed": "0.66"})
    assert isinstance(normalized["wind_avg"], float)
    assert isinstance(normalized["wind_speed"], float)
    assert normalized["wind_speed"] == pytest.approx(0.66)
