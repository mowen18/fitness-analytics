with source as (

    select * from {{ source('raw_strava', 'segment_efforts') }}

)

select
    effort_id,
    activity_id,
    segment_id,
    (payload ->> 'elapsed_time')::integer       as elapsed_time_s,
    (payload ->> 'moving_time')::integer        as moving_time_s,
    -- The effort's own recorded distance. Absent key stays NULL —
    -- speed is derived downstream and missing stays missing.
    (payload ->> 'distance')::numeric           as distance_m,
    (payload ->> 'start_date')::timestamptz     as start_date_utc,
    -- Strava sends start_date_local with a literal 'Z' even though it is
    -- local wall-clock time; ::timestamp deliberately drops that bogus
    -- zone marker instead of treating the value as UTC.
    (payload ->> 'start_date_local')::timestamp as start_date_local,
    -- Absent when the device recorded no HR for the effort: the missing
    -- key stays NULL, never 0.
    (payload ->> 'average_heartrate')::numeric  as average_hr_bpm,
    (payload ->> 'average_cadence')::numeric    as average_cadence_rpm,
    -- Top-3 personal-record rank when Strava assigns one; NULL means
    -- "not a PR", never 0.
    (payload ->> 'pr_rank')::integer            as pr_rank,
    fetched_at
from source
