with runs as (

    -- Rides also land in raw_strava.streams (D31); this view is runs only
    -- (the filter documented in staging YAML; v2.4 owner exception).
    select activity_id
    from {{ ref('stg_strava__activities') }}
    where sport_type in ('Run', 'TrailRun', 'VirtualRun')

),

successful_streams as (

    select activity_id, payload, sample_count
    from {{ source('raw_strava', 'streams') }}
    inner join runs using (activity_id)
    where ingestion_status = 'success'

)

select
    streams.activity_id,
    sample.idx                                                as sample_index,
    (streams.payload -> 'time' -> 'data' ->> sample.idx)::integer as elapsed_s,
    (streams.payload -> 'heartrate' -> 'data' ->> sample.idx)::numeric as hr_bpm,
    (streams.payload -> 'velocity_smooth' -> 'data' ->> sample.idx)::numeric
                                                              as velocity_m_per_s,
    coalesce(
        (streams.payload -> 'moving' -> 'data' ->> sample.idx)::boolean, false
    )                                                         as is_moving,
    (streams.payload -> 'grade_smooth' -> 'data' ->> sample.idx)::numeric as grade_pct,
    -- Instrument-sanity validity, matching the staging HR bounds
    -- (25-250 bpm) plus a physical velocity ceiling (12 m/s is beyond
    -- world-record sprint pace). Invalid samples are excluded from
    -- drift halves; heavy invalidity then fails the coverage check
    -- rather than poisoning the averages.
    (
        (streams.payload -> 'heartrate' -> 'data' ->> sample.idx) is not null
        and (streams.payload -> 'heartrate' -> 'data' ->> sample.idx)::numeric
            between 25 and 250
        and (streams.payload -> 'velocity_smooth' -> 'data' ->> sample.idx) is not null
        and (streams.payload -> 'velocity_smooth' -> 'data' ->> sample.idx)::numeric
            between 0 and 12
    )                                                         as is_valid_sample
from successful_streams streams
cross join lateral
    generate_series(0, streams.sample_count - 1) as sample(idx)
