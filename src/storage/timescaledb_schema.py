"""
TimescaleDB Schema Migrations for WeatherFlow Collector

1. Adds WeatherFlow-specific columns to the existing weather.readings
   hypertable (observations only).
2. Creates weather.weatherflow_raw — a generic JSONB hypertable for ALL
   data (forecasts, device/hub status, system metrics, and a raw copy
   of observations).
"""

import config
import logger

logger_schema = logger.get_module_logger(__name__ + ".TimescaleDBSchema")

ALTER_COLUMNS = [
    "ALTER TABLE weather.readings ADD COLUMN IF NOT EXISTS solar_radiation_wm2 DOUBLE PRECISION;",
    "ALTER TABLE weather.readings ADD COLUMN IF NOT EXISTS illuminance_lux INT;",
    "ALTER TABLE weather.readings ADD COLUMN IF NOT EXISTS lightning_strike_count INT;",
    "ALTER TABLE weather.readings ADD COLUMN IF NOT EXISTS lightning_strike_avg_distance_km DOUBLE PRECISION;",
    "ALTER TABLE weather.readings ADD COLUMN IF NOT EXISTS precipitation_mm DOUBLE PRECISION;",
    "ALTER TABLE weather.readings ADD COLUMN IF NOT EXISTS wind_lull_mps DOUBLE PRECISION;",
    "ALTER TABLE weather.readings ADD COLUMN IF NOT EXISTS feels_like_c DOUBLE PRECISION;",
    "ALTER TABLE weather.readings ADD COLUMN IF NOT EXISTS heat_index_c DOUBLE PRECISION;",
    "ALTER TABLE weather.readings ADD COLUMN IF NOT EXISTS wind_chill_c DOUBLE PRECISION;",
    "ALTER TABLE weather.readings ADD COLUMN IF NOT EXISTS precip_accum_local_day_mm DOUBLE PRECISION;",
    "ALTER TABLE weather.readings ADD COLUMN IF NOT EXISTS precip_accum_local_yesterday_mm DOUBLE PRECISION;",
    "ALTER TABLE weather.readings ADD COLUMN IF NOT EXISTS measurement TEXT;",
]

RAW_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS weather.weatherflow_raw (
    ts              TIMESTAMPTZ     NOT NULL,
    measurement     TEXT            NOT NULL,
    device_id       TEXT,
    station_id      TEXT,
    collector_type  TEXT,
    tags            JSONB           NOT NULL DEFAULT '{}',
    fields          JSONB           NOT NULL DEFAULT '{}'
);
"""

RAW_HYPERTABLE_DDL = """
SELECT create_hypertable(
    'weather.weatherflow_raw', 'ts',
    if_not_exists => TRUE,
    migrate_data  => TRUE
);
"""

RAW_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_wfraw_measurement_ts ON weather.weatherflow_raw (measurement, ts DESC);",
    "CREATE INDEX IF NOT EXISTS idx_wfraw_device_ts ON weather.weatherflow_raw (device_id, ts DESC);",
]


async def ensure_schema(pool):
    """Add missing columns to weather.readings and create weather.weatherflow_raw."""
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            for ddl in ALTER_COLUMNS:
                try:
                    await cur.execute(ddl)
                except Exception as e:
                    logger_schema.warning(f"Column migration skipped: {e}")
            logger_schema.info("weather.readings columns verified")

            logger_schema.info("Creating weather.weatherflow_raw table")
            await cur.execute(RAW_TABLE_DDL)
            await cur.execute(RAW_HYPERTABLE_DDL)
            for idx in RAW_INDEXES:
                await cur.execute(idx)
            logger_schema.info("weather.weatherflow_raw hypertable verified")

        await conn.commit()
    logger_schema.info("Schema migration complete")
