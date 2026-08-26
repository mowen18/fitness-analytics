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

),

geometry as (

    select * from {{ ref('int_segment_geometry') }}

),

observations as (

    -- Qualifying = carries at least one measurement, exactly as the
    -- ride match defines it (the explicit all-NULL "archive had no
    -- data" rows must never win). Direction-less observations DO
    -- qualify: D30 wants the NEAREST observation, and shopping farther
    -- afield for a direction-carrying hour would attach wind that
    -- misdescribes the effort's hour (the v1.7 no-fallback doctrine).
    select * from {{ ref('stg_weather__hourly') }}
    where has_measurements

),

effort_wind as (

    -- D30 effort-hour wind match: the nearest qualifying observation
    -- at the PARENT RIDE's start cell (the documented spatial caveat —
    -- wind is not observed at the segment's own location) to the
    -- EFFORT's start time. The efforts-rides join here is only the
    -- match-key projection (effort_id, start time, cell); the D23
    -- grain stays encoded once in the model's final join, which alone
    -- decides the output rows. Indoor parents have a NULL location_key
    -- by construction (C1), so their efforts never match wind. Mirrors
    -- int_rides_with_weather's closeness ranking with one deliberate
    -- deviation: the secondary sort key makes equidistant ties
    -- deterministic — the ride model's unspecified tie order is
    -- untouchable (its output is frozen), a new model's is not.
    select
        efforts.effort_id,
        observations.wind_speed_mph,
        observations.wind_direction_deg,
        round(
            abs(extract(epoch from (observations.weather_timestamp - efforts.start_date_utc)))
            / 60.0
        )::integer as effort_wind_match_minutes,
        row_number() over (
            partition by efforts.effort_id
            order by
                abs(extract(epoch from (observations.weather_timestamp - efforts.start_date_utc))),
                observations.weather_timestamp
        ) as closeness_rank
    from efforts
    inner join rides using (activity_id)
    inner join observations using (location_key)

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
    efforts.distance_m as effort_distance_m,
    -- Effort speed divides by ELAPSED time: Strava ranks and displays
    -- segments on total elapsed time (moving-time speed applies to
    -- activities, not segments), and elapsed is D28's primary series.
    -- NULL when the payload has no distance or elapsed time is zero
    -- (guard the division at the source) — never zero.
    case
        when efforts.distance_m is not null and efforts.elapsed_time_s > 0
            then round(
                (efforts.distance_m * 3600 / (efforts.elapsed_time_s * 1609.344))::numeric, 1
            )
    end as speed_mph,
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
    -- D30 geometry and effort-hour wind (C3). Values only inside the
    -- 60-minute rule; the match distance itself stays as context (the
    -- weather_match_minutes precedent). headwind_mph sign convention,
    -- pinned: POSITIVE = HEADWIND — wind_speed_mph *
    -- cos(radians(wind_direction_deg - bearing_deg)) is positive when
    -- the meteorological FROM direction opposes travel along the
    -- segment's straight-line bearing; negative = tailwind. Crosswind
    -- is the unsigned perpendicular component. NULL whenever the match
    -- misses the 60-minute rule, direction is missing (never zero), or
    -- the segment has no bearing; computed and flagged, not nulled,
    -- when the segment is winding.
    geometry.bearing_deg,
    geometry.sinuosity,
    geometry.winding_segment,
    case
        when effort_wind.effort_wind_match_minutes <= 60
            then effort_wind.wind_speed_mph
    end as effort_wind_speed_mph,
    case
        when effort_wind.effort_wind_match_minutes <= 60
            then effort_wind.wind_direction_deg
    end as effort_wind_direction_deg,
    effort_wind.effort_wind_match_minutes,
    case
        when effort_wind.effort_wind_match_minutes <= 60
            and effort_wind.wind_speed_mph is not null
            and effort_wind.wind_direction_deg is not null
            and geometry.bearing_deg is not null
            then round(
                (
                    effort_wind.wind_speed_mph
                    * cos(radians(effort_wind.wind_direction_deg - geometry.bearing_deg))
                )::numeric, 1
            )
    end as headwind_mph,
    case
        when effort_wind.effort_wind_match_minutes <= 60
            and effort_wind.wind_speed_mph is not null
            and effort_wind.wind_direction_deg is not null
            and geometry.bearing_deg is not null
            then round(
                (
                    effort_wind.wind_speed_mph
                    * abs(sin(radians(effort_wind.wind_direction_deg - geometry.bearing_deg)))
                )::numeric, 1
            )
    end as crosswind_mph,
    efforts.fetched_at
from efforts
inner join rides using (activity_id)
left join segments using (segment_id)
left join geometry using (segment_id)
left join effort_wind
    on effort_wind.effort_id = efforts.effort_id
    and effort_wind.closeness_rank = 1
