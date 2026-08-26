with source as (

    select * from {{ source('raw_weather', 'hourly') }}

)

select
    location_key,
    latitude,
    longitude,
    weather_timestamp,
    temperature_c,
    round(temperature_c * 9.0 / 5.0 + 32, 1)          as temperature_f,
    apparent_temperature_c,
    round(apparent_temperature_c * 9.0 / 5.0 + 32, 1) as apparent_temperature_f,
    relative_humidity_pct,
    wind_speed_kph,
    round(wind_speed_kph / 1.609344, 1)               as wind_speed_mph,
    -- D29 (C3): meteorological FROM convention; 0 = north is a value,
    -- NULL = missing, never zero. Deliberately NOT part of
    -- has_measurements below: direction never decides whether an hour
    -- counts as an observation (the running side is untouched by C3).
    wind_direction_deg,
    -- False marks the explicit "archive had no data" rows (all-NULL
    -- measurements); downstream matching must never treat them as data.
    (
        temperature_c is not null
        or apparent_temperature_c is not null
        or relative_humidity_pct is not null
        or wind_speed_kph is not null
    )                                                 as has_measurements,
    fetched_at
from source
