"""
TimescaleDB Storage Backend for WeatherFlow Collector

Two-table strategy:
  - weather.readings           — observation data with typed columns
  - weather.weatherflow_raw    — ALL data (forecasts, device/hub status,
                                 system metrics, observations) as JSONB

Every event goes to weatherflow_raw.  Observations additionally get
column-mapped into weather.readings.
"""

import asyncio
import json
import time
import traceback
from datetime import datetime, timezone

import psycopg
import psycopg_pool

import config
import logger
import utils.utils as utils
from storage.timescaledb_schema import ensure_schema

logger_ts = logger.get_module_logger(__name__ + ".TimescaleDBStorage")

# --- observation filter (weather.readings) ---

OBSERVATION_MEASUREMENTS = {
    "weatherflow_obs",
    "weatherflow_rapid_wind",
    "weatherflow_evt_strike",
    "weatherflow_evt_precip",
    "weatherflow_geo_strike",
    "obs_st",
    "obs_sky",
    "obs_air",
    "obs_tempest",
    "rapid_wind",
    "evt_strike",
    "evt_precip",
}

NON_OBSERVATION_PREFIXES = (
    "weatherflow_forecast",
    "weatherflow_stats",
    "weatherflow_system",
    "weatherflow_device_status",
    "weatherflow_hub_status",
)

# --- field mapping for weather.readings ---

FIELD_TO_COLUMN = {
    "air_temperature": "temperature_c",
    "relative_humidity": "humidity_pct",
    "station_pressure": "pressure_hpa",
    "sea_level_pressure": "sea_level_pressure_hpa",
    "wind_avg": "wind_speed_mps",
    "wind_direction": "wind_direction_deg",
    "wind_gust": "wind_gust_mps",
    "wind_lull": "wind_lull_mps",
    "uv": "uvi",
    "dew_point": "dew_point_c",
    "solar_radiation": "solar_radiation_wm2",
    "illuminance": "illuminance_lux",
    "lightning_strike_count": "lightning_strike_count",
    "lightning_strike_avg_distance": "lightning_strike_avg_distance_km",
    "precip": "precipitation_mm",
    "rain_accumulated": "precipitation_mm",
    "feels_like": "feels_like_c",
    "heat_index": "heat_index_c",
    "wind_chill": "wind_chill_c",
    "battery": "battery_voltage_v",
    "precip_accum_local_day": "precip_accum_local_day_mm",
    "precip_accum_local_yesterday": "precip_accum_local_yesterday_mm",
    "firmware_revision": "firmware_version",
}

READING_COLUMNS = [
    "time", "device_id", "device_name", "source", "measurement",
    "temperature_c", "temperature_f", "humidity_pct", "pressure_hpa",
    "sea_level_pressure_hpa", "wind_speed_mps", "wind_direction_deg",
    "wind_gust_mps", "wind_lull_mps", "uvi", "dew_point_c",
    "solar_radiation_wm2", "illuminance_lux", "lightning_strike_count",
    "lightning_strike_avg_distance_km", "precipitation_mm",
    "precip_accum_local_day_mm", "precip_accum_local_yesterday_mm",
    "feels_like_c", "heat_index_c", "wind_chill_c",
    "battery_voltage_v", "firmware_version", "raw_payload",
]

_COLS_CSV = ", ".join(READING_COLUMNS)
_PLACEHOLDERS = ", ".join(["%s"] * len(READING_COLUMNS))
INSERT_READINGS_SQL = f"INSERT INTO weather.readings ({_COLS_CSV}) VALUES ({_PLACEHOLDERS})"

INSERT_RAW_SQL = """
INSERT INTO weather.weatherflow_raw
    (ts, measurement, device_id, station_id, collector_type, tags, fields)
VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)
"""


def _ts_to_datetime(timestamp):
    if timestamp is None:
        return datetime.now(timezone.utc)
    return datetime.fromtimestamp(int(timestamp), tz=timezone.utc)


def _extract_device_id(tags):
    return (
        tags.get("serial_number")
        or tags.get("device_id")
        or tags.get("hub_sn")
        or ""
    )


def _build_raw_row(measurement, tags, fields, timestamp):
    """Build a tuple for weather.weatherflow_raw."""
    sorted_tags = dict(sorted(tags.items()))
    return (
        _ts_to_datetime(timestamp),
        measurement,
        _extract_device_id(sorted_tags),
        sorted_tags.get("station_id", ""),
        sorted_tags.get("collector_type", ""),
        json.dumps(sorted_tags),
        json.dumps(fields),
    )


def _build_readings_row(measurement, tags, fields, timestamp):
    """Build a tuple for weather.readings (observations only)."""
    fields = utils.normalize_fields(fields)
    sorted_tags = dict(sorted(tags.items()))

    device_id = _extract_device_id(sorted_tags)
    device_name = sorted_tags.get("station_name", "")

    mapped = {}
    for wf_field, column in FIELD_TO_COLUMN.items():
        if wf_field in fields and fields[wf_field] is not None:
            mapped[column] = fields[wf_field]

    temp_c = mapped.get("temperature_c")
    if temp_c is not None:
        mapped["temperature_f"] = round(float(temp_c) * 9.0 / 5.0 + 32.0, 2)

    return (
        _ts_to_datetime(timestamp),
        device_id,
        device_name,
        "weatherflow",
        measurement,
        mapped.get("temperature_c"),
        mapped.get("temperature_f"),
        mapped.get("humidity_pct"),
        mapped.get("pressure_hpa"),
        mapped.get("sea_level_pressure_hpa"),
        mapped.get("wind_speed_mps"),
        mapped.get("wind_direction_deg"),
        mapped.get("wind_gust_mps"),
        mapped.get("wind_lull_mps"),
        mapped.get("uvi"),
        mapped.get("dew_point_c"),
        mapped.get("solar_radiation_wm2"),
        mapped.get("illuminance_lux"),
        mapped.get("lightning_strike_count"),
        mapped.get("lightning_strike_avg_distance_km"),
        mapped.get("precipitation_mm"),
        mapped.get("precip_accum_local_day_mm"),
        mapped.get("precip_accum_local_yesterday_mm"),
        mapped.get("feels_like_c"),
        mapped.get("heat_index_c"),
        mapped.get("wind_chill_c"),
        mapped.get("battery_voltage_v"),
        str(mapped["firmware_version"]) if mapped.get("firmware_version") is not None else None,
        json.dumps({"tags": sorted_tags, "fields": fields}),
    )


class TimescaleDBStorage:
    def __init__(self, event_manager):
        self.event_manager = event_manager
        self.module_name = "timescaledb_handler"
        self.collector_type = "timescaledb_handler"
        self.metrics_by_client_type = {}
        self.pool = None
        self._ready = False

    async def initialize(self):
        conninfo = (
            f"host={config.WEATHERFLOW_COLLECTOR_TIMESCALEDB_HOST} "
            f"port={config.WEATHERFLOW_COLLECTOR_TIMESCALEDB_PORT} "
            f"dbname={config.WEATHERFLOW_COLLECTOR_TIMESCALEDB_DATABASE} "
            f"user={config.WEATHERFLOW_COLLECTOR_TIMESCALEDB_USER} "
            f"password={config.WEATHERFLOW_COLLECTOR_TIMESCALEDB_PASSWORD} "
            f"sslmode={config.WEATHERFLOW_COLLECTOR_TIMESCALEDB_SSLMODE}"
        )

        self.pool = psycopg_pool.AsyncConnectionPool(
            conninfo=conninfo,
            min_size=config.WEATHERFLOW_COLLECTOR_TIMESCALEDB_POOL_MIN,
            max_size=config.WEATHERFLOW_COLLECTOR_TIMESCALEDB_POOL_MAX,
            open=False,
        )
        await self.pool.open()
        logger_ts.info("TimescaleDB connection pool opened")

        if config.WEATHERFLOW_COLLECTOR_TIMESCALEDB_SCHEMA_AUTO_CREATE:
            await ensure_schema(self.pool)

        self._ready = True
        logger_ts.info("TimescaleDB storage ready")

    def _is_observation(self, measurement):
        if not measurement:
            return False
        for prefix in NON_OBSERVATION_PREFIXES:
            if measurement.startswith(prefix):
                return False
        if measurement in OBSERVATION_MEASUREMENTS:
            return True
        if measurement.startswith("obs_") or measurement.startswith("weatherflow_obs"):
            return True
        return False

    async def _write_raw(self, measurement, tags, fields, timestamp):
        """Write one row to weatherflow_raw (all measurement types)."""
        row = _build_raw_row(measurement, tags, fields, timestamp)
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(INSERT_RAW_SQL, row)
            await conn.commit()

    async def _write_raw_batch(self, batch):
        """Write a batch to weatherflow_raw."""
        rows = []
        for measurement, tags, fields, timestamp in batch:
            if not isinstance(tags, dict) or not isinstance(fields, dict):
                continue
            rows.append(_build_raw_row(measurement, tags, fields, timestamp))
        if not rows:
            return
        batch_size = config.WEATHERFLOW_COLLECTOR_TIMESCALEDB_BATCH_SIZE
        for i in range(0, len(rows), batch_size):
            chunk = rows[i : i + batch_size]
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.executemany(INSERT_RAW_SQL, chunk)
                await conn.commit()

    async def save_data(self, measurement, tags, fields, timestamp=None):
        await self._write_raw(measurement, tags, fields, timestamp)

        if not self._is_observation(measurement):
            return

        row = _build_readings_row(measurement, tags, fields, timestamp)
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(INSERT_READINGS_SQL, row)
            await conn.commit()

    async def save_batch_data(self, batch):
        if not batch:
            return

        await self._write_raw_batch(batch)

        obs_rows = []
        for measurement, tags, fields, timestamp in batch:
            if not isinstance(tags, dict) or not isinstance(fields, dict):
                continue
            if self._is_observation(measurement):
                obs_rows.append(_build_readings_row(measurement, tags, fields, timestamp))

        if not obs_rows:
            return

        batch_size = config.WEATHERFLOW_COLLECTOR_TIMESCALEDB_BATCH_SIZE
        for i in range(0, len(obs_rows), batch_size):
            chunk = obs_rows[i : i + batch_size]
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.executemany(INSERT_READINGS_SQL, chunk)
                await conn.commit()

    async def update(self, data):
        if not self._ready:
            logger_ts.warning("TimescaleDB storage not ready, dropping event")
            return

        request_start = time.time()
        metadata = data.get("metadata", {})
        collector_type = metadata.get("collector_type", "unknown")
        metric_name = f"update_{collector_type}"

        try:
            data_type = data.get("data_type", "single")
            if data_type == "batch":
                await self.save_batch_data(data.get("batch_data", []))
            else:
                measurement = data.get("measurement")
                tags = data.get("tags", {})
                fields = data.get("fields", {})
                timestamp = data.get("timestamp")
                await self.save_data(measurement, tags, fields, timestamp)

        except Exception as e:
            tb = traceback.format_exc()
            logger_ts.error(f"Error writing to TimescaleDB: {e}\n{tb}")
            if collector_type not in self.metrics_by_client_type:
                self.metrics_by_client_type[collector_type] = {
                    "request_count": 0,
                    "error_count": 0,
                }
            self.metrics_by_client_type[collector_type]["error_count"] += 1

        if collector_type not in self.metrics_by_client_type:
            self.metrics_by_client_type[collector_type] = {
                "request_count": 0,
                "error_count": 0,
            }
        self.metrics_by_client_type[collector_type]["request_count"] += 1

        duration = time.time() - request_start
        await utils.async_publish_metrics(
            self.event_manager,
            metric_name=metric_name,
            module_name=self.module_name,
            rate=self.metrics_by_client_type[collector_type]["request_count"],
            errors=self.metrics_by_client_type[collector_type]["error_count"],
            duration=duration,
        )

    async def close(self):
        if self.pool:
            await self.pool.close()
            logger_ts.info("TimescaleDB connection pool closed")
