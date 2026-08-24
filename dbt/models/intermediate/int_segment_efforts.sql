{#
  One row per Strava segment effort on a D23 ride (v2.0 Phase C2, D24):
  the effort joined to its segment's identity and its parent ride's
  context. The INNER join to int_ride_measures applies the D23 ride
  grain by construction — raw efforts whose parent activity is outside
  the grain (or leaves it later, e.g. a sport_type edited in Strava
  after the detail fetch) simply have no parent row and drop out: a
  grain rule, not an exclusion needing a reason.

  No D25 gating here: parent-ride validity travels as ride_is_valid /
  ride_exclusion_reason context, displayed downstream and never a
  filter — a ride failing the sanity checks is a data-error ride, so
  its effort times are suspect but stay visible.
#}

with efforts as (

    select * from {{ ref('stg_strava__segment_efforts') }}

),

segments as (

    select * from {{ ref('stg_strava__segments') }}

),

rides as (

    select * from {{ ref('int_ride_measures') }}

)

select
    efforts.effort_id,
    efforts.activity_id,
    efforts.segment_id,
    segments.segment_name,
    segments.distance_m    as segment_distance_m,
    segments.average_grade_pct,
    segments.city,
    segments.state,
    efforts.elapsed_time_s,
    efforts.moving_time_s,
    efforts.start_date_utc,
    efforts.start_date_local,
    efforts.average_hr_bpm,
    efforts.average_cadence_rpm,
    efforts.pr_rank,
    -- D28's mart holdout is VIRTUAL, not indoor: a trainer-flagged
    -- outdoor ride type still contributes efforts to marts; VirtualRide
    -- never does.
    rides.sport_type = 'VirtualRide' as is_virtual_ride,
    rides.sport_type,
    rides.is_indoor,
    rides.week_start_date,
    rides.is_valid         as ride_is_valid,
    rides.exclusion_reason as ride_exclusion_reason,
    -- Parent-ride weather, matched at the ride's start hour and cell.
    -- Names kept verbatim so C3's effort-hour wind match can add its
    -- own effort_* columns beside them without renames.
    rides.temperature_c,
    rides.temperature_f,
    rides.apparent_temperature_c,
    rides.apparent_temperature_f,
    rides.relative_humidity_pct,
    rides.wind_speed_kph,
    rides.wind_speed_mph,
    rides.weather_match_minutes,
    rides.weather_available,
    efforts.fetched_at
from efforts
inner join rides using (activity_id)
left join segments using (segment_id)
