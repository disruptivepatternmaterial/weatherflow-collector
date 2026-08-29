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
    assert got["temperature_f"] == pytest.approx(68.0)


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


# --- the normaliser backing it ----------------------------------------------

def test_normalizer_treats_wind_speed_like_wind_avg():
    normalized = utils.normalize_fields({"wind_avg": "3.2", "wind_speed": "0.66"})
    assert isinstance(normalized["wind_avg"], float)
    assert isinstance(normalized["wind_speed"], float)
    assert normalized["wind_speed"] == pytest.approx(0.66)
