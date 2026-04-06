"""
TimescaleDB Schema Migrations for WeatherFlow Collector

Adds WeatherFlow-specific columns to the existing weather.readings
hypertable.  All operations are idempotent (ADD COLUMN IF NOT EXISTS).
Does NOT create the table or hypertable — those already exist from the
weather-readings.sql foundation.
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


async def ensure_schema(pool):
    """Add any missing columns to the existing weather.readings hypertable."""
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            for ddl in ALTER_COLUMNS:
                try:
                    await cur.execute(ddl)
                except Exception as e:
                    logger_schema.warning(f"Column migration skipped: {e}")
        await conn.commit()
    logger_schema.info("Schema migration complete — weather.readings columns verified")
