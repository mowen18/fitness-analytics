-- Core projection of int_segment_efforts (v2.0 Phase C2): the
-- effort-level contract, one row per Strava segment effort on a D23
-- ride. All computation happens upstream; this is a pure
-- explicit-column surface so marts read core only. VirtualRide efforts
-- are KEPT here, flagged is_virtual_ride — D28 holds them out of
-- marts, not out of core; the holdout is pinned by
-- assert_segment_trend_excludes_virtual_efforts.
with efforts as (

    select * from {{ ref('int_segment_efforts') }}

)

select
    effort_id,
    activity_id,
    segment_id,
    segment_name,
    segment_distance_m,
    average_grade_pct,
    city,
    state,
    elapsed_time_s,
    moving_time_s,
    effort_distance_m,
    speed_mph,
    start_date_utc,
    start_date_local,
    average_hr_bpm,
    average_cadence_rpm,
    pr_rank,
    is_virtual_ride,
    sport_type,
    is_indoor,
    week_start_date,
    ride_is_valid,
    ride_exclusion_reason,
    temperature_c,
    temperature_f,
    apparent_temperature_c,
    apparent_temperature_f,
    relative_humidity_pct,
    wind_speed_kph,
    wind_speed_mph,
    weather_match_minutes,
    weather_available,
    -- D30 geometry and effort-hour wind (C3). Sign convention, pinned:
    -- positive headwind_mph = headwind, negative = tailwind; crosswind
    -- is the unsigned perpendicular component. Wind is matched at the
    -- parent ride's start cell to the EFFORT's start time (60-minute
    -- rule); winding efforts keep their computed values, flagged.
    bearing_deg,
    sinuosity,
    winding_segment,
    effort_wind_speed_mph,
    effort_wind_direction_deg,
    effort_wind_match_minutes,
    headwind_mph,
    crosswind_mph,
    fetched_at
from efforts
