-- Phase 2: raw hourly weather storage (Open-Meteo historical archive).
-- Idempotent: runs on first container init via docker-entrypoint-initdb.d
-- (alphabetically after bootstrap.sql, which creates the schemas) and can
-- be re-applied any time with `make bootstrap`.

CREATE TABLE IF NOT EXISTS raw_weather.hourly (
    location_key            text        NOT NULL,
    latitude                numeric     NOT NULL,
    longitude               numeric     NOT NULL,
    weather_timestamp       timestamptz NOT NULL,
    temperature_c           numeric,
    apparent_temperature_c  numeric,
    relative_humidity_pct   numeric,
    wind_speed_kph          numeric,
    payload                 jsonb       NOT NULL,
    fetched_at              timestamptz NOT NULL,
    UNIQUE (location_key, weather_timestamp)
);

COMMENT ON TABLE raw_weather.hourly IS
    'One row per location-hour from the Open-Meteo archive; doubles as the fetch cache (the UNIQUE key is checked before requesting)';
COMMENT ON COLUMN raw_weather.hourly.location_key IS
    'Normalized ~1.1 km cell per D7: ''{lat_2dp}_{lon_2dp}''';
COMMENT ON COLUMN raw_weather.hourly.latitude IS
    'Latitude rounded to 2 decimal places per D7 (matches location_key, not the API-echoed grid point)';
COMMENT ON COLUMN raw_weather.hourly.longitude IS
    'Longitude rounded to 2 decimal places per D7 (matches location_key, not the API-echoed grid point)';
COMMENT ON COLUMN raw_weather.hourly.weather_timestamp IS
    'UTC start of the observation hour (requested and matched in UTC)';
COMMENT ON COLUMN raw_weather.hourly.temperature_c IS
    'NULL means the archive had no data for this hour (missing, never zero); re-requested by later incremental syncs';
COMMENT ON COLUMN raw_weather.hourly.payload IS
    'Original per-hour slice of the API response plus its hourly_units, values unmodified';

-- The UNIQUE constraint indexes (location_key, weather_timestamp); Phase 3
-- staging also scans by observation time:
CREATE INDEX IF NOT EXISTS hourly_weather_timestamp_idx
    ON raw_weather.hourly (weather_timestamp);

-- Phase C3 (D29): wind direction joins the hourly variable set. The
-- repo's first in-place column migration — ADD COLUMN IF NOT EXISTS
-- keeps this file idempotent for both fresh container inits (the ALTER
-- runs right after the CREATE above) and `make bootstrap` re-applies
-- against a populated warehouse (existing rows keep their keys and get
-- a NULL backfilled by `make reconcile-weather`, never by the DDL).
ALTER TABLE raw_weather.hourly
    ADD COLUMN IF NOT EXISTS wind_direction_deg numeric;

COMMENT ON COLUMN raw_weather.hourly.wind_direction_deg IS
    'Wind direction at 10 m, degrees, meteorological FROM convention (D29): 0 legitimately means north — a value, never a stand-in for missing; NULL means the archive had no direction for this hour';
