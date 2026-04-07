"""
TimescaleDB Storage Backend for WeatherFlow Collector

Writes observation data into the existing ``weather.readings`` hypertable,
mapping WeatherFlow field names to the table's column names.  Non-observation
measurements (forecasts, device/hub status, system metrics) are skipped with
a debug log.

The full raw fields dict is stored in the ``raw_payload`` JSONB column so
nothing is lost even if a column mapping is missing.
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

# Measurements that represent actual weather observations.
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

# WeatherFlow field name → weather.readings column name.
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

# All columns in weather.readings that we may populate (order matters for SQL).
READING_COLUMNS = [
    "time",
    "device_id",
    "device_name",
    "source",
    "measurement",
    "temperature_c",
    "temperature_f",
    "humidity_pct",
    "pressure_hpa",
    "sea_level_pressure_hpa",
    "wind_speed_mps",
    "wind_direction_deg",
    "wind_gust_mps",
    "wind_lull_mps",
    "uvi",
    "dew_point_c",
    "solar_radiation_wm2",
    "illuminance_lux",
    "lightning_strike_count",
    "lightning_strike_avg_distance_km",
    "precipitation_mm",
    "precip_accum_local_day_mm",
    "precip_accum_local_yesterday_mm",
    "feels_like_c",
    "heat_index_c",
    "wind_chill_c",
    "battery_voltage_v",
    "firmware_version",
    "raw_payload",
]

_COLS_CSV = ", ".join(READING_COLUMNS)
_PLACEHOLDERS = ", ".join(["%s"] * len(READING_COLUMNS))
INSERT_SQL = f"INSERT INTO weather.readings ({_COLS_CSV}) VALUES ({_PLACEHOLDERS})"


def _ts_to_datetime(timestamp):
    if timestamp is None:
        return datetime.now(timezone.utc)
    return datetime.fromtimestamp(int(timestamp), tz=timezone.utc)


def _extract_device_id(tags):
    """Pick the best device identifier from the tags dict."""
    return (
        tags.get("serial_number")
        or tags.get("device_id")
        or tags.get("hub_sn")
        or ""
    )


def _build_row(measurement, tags, fields, timestamp):
    """Map a single event into a tuple matching READING_COLUMNS order."""
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
        _ts_to_datetime(timestamp),                          # time
        device_id,                                           # device_id
        device_name,                                         # device_name
        "weatherflow",                                       # source
        measurement,                                         # measurement
        mapped.get("temperature_c"),                         # temperature_c
        mapped.get("temperature_f"),                         # temperature_f
        mapped.get("humidity_pct"),                          # humidity_pct
        mapped.get("pressure_hpa"),                          # pressure_hpa
        mapped.get("sea_level_pressure_hpa"),                # sea_level_pressure_hpa
        mapped.get("wind_speed_mps"),                        # wind_speed_mps
        mapped.get("wind_direction_deg"),                    # wind_direction_deg
        mapped.get("wind_gust_mps"),                         # wind_gust_mps
        mapped.get("wind_lull_mps"),                         # wind_lull_mps
        mapped.get("uvi"),                                   # uvi
        mapped.get("dew_point_c"),                           # dew_point_c
        mapped.get("solar_radiation_wm2"),                   # solar_radiation_wm2
        mapped.get("illuminance_lux"),                       # illuminance_lux
        mapped.get("lightning_strike_count"),                 # lightning_strike_count
        mapped.get("lightning_strike_avg_distance_km"),       # lightning_strike_avg_distance_km
        mapped.get("precipitation_mm"),                      # precipitation_mm
        mapped.get("precip_accum_local_day_mm"),             # precip_accum_local_day_mm
        mapped.get("precip_accum_local_yesterday_mm"),       # precip_accum_local_yesterday_mm
        mapped.get("feels_like_c"),                          # feels_like_c
        mapped.get("heat_index_c"),                          # heat_index_c
        mapped.get("wind_chill_c"),                          # wind_chill_c
        mapped.get("battery_voltage_v"),                     # battery_voltage_v
        str(mapped["firmware_version"]) if mapped.get("firmware_version") is not None else None,
        json.dumps({"tags": sorted_tags, "fields": fields}), # raw_payload
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
        """Open the connection pool and run schema migrations."""
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

    SKIP_PREFIXES = (
        "weatherflow_forecast",
        "weatherflow_stats",
        "weatherflow_system",
    )

    def _is_observation(self, measurement):
        if not measurement:
            return False
        for prefix in self.SKIP_PREFIXES:
            if measurement.startswith(prefix):
                return False
        if measurement in OBSERVATION_MEASUREMENTS:
            return True
        if measurement.startswith("obs_") or measurement.startswith("weatherflow_"):
            return True
        return False

    async def save_data(self, measurement, tags, fields, timestamp=None):
        if not self._is_observation(measurement):
            logger_ts.debug(f"Skipping non-observation measurement: {measurement}")
            return

        row = _build_row(measurement, tags, fields, timestamp)
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(INSERT_SQL, row)
            await conn.commit()
        logger_ts.debug(f"Wrote 1 row for {measurement}")

    async def save_batch_data(self, batch):
        if not batch:
            return

        rows = []
        for measurement, tags, fields, timestamp in batch:
            if not isinstance(tags, dict) or not isinstance(fields, dict):
                logger_ts.error(f"Invalid batch item skipped: measurement={measurement}")
                continue
            if not self._is_observation(measurement):
                continue
            rows.append(_build_row(measurement, tags, fields, timestamp))

        if not rows:
            return

        batch_size = config.WEATHERFLOW_COLLECTOR_TIMESCALEDB_BATCH_SIZE
        for i in range(0, len(rows), batch_size):
            chunk = rows[i : i + batch_size]
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.executemany(INSERT_SQL, chunk)
                await conn.commit()

        logger_ts.debug(f"Wrote {len(rows)} observation rows in batch")

    async def update(self, data):
        """Event handler called by EventManager for ``influxdb_storage_event``."""
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
