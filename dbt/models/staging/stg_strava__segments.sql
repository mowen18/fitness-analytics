with source as (

    select * from {{ source('raw_strava', 'segments') }}

)

select
    segment_id,
    payload ->> 'name'                         as segment_name,
    (payload ->> 'distance')::numeric          as distance_m,
    (payload ->> 'average_grade')::numeric     as average_grade_pct,
    (payload ->> 'maximum_grade')::numeric     as maximum_grade_pct,
    payload ->> 'city'                         as city,
    payload ->> 'state'                        as state,
    -- Segment endpoints stay staging-only fuel for the C3 bearing and
    -- sinuosity work; they never surface in core, marts, or the app.
    (payload -> 'start_latlng' ->> 0)::numeric as start_latitude,
    (payload -> 'start_latlng' ->> 1)::numeric as start_longitude,
    (payload -> 'end_latlng' ->> 0)::numeric   as end_latitude,
    (payload -> 'end_latlng' ->> 1)::numeric   as end_longitude,
    fetched_at
from source
