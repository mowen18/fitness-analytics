-- Activity-grain stream-fetch state (one row per run's raw_strava.streams
-- row): feeds fct_drift_candidates' stream-availability exclusion rungs
-- without core reading the raw source. raw_strava.streams is the only
-- source readable outside staging, from intermediate models ONLY (Revision
-- v1.2 follow-up); run payloads are deliberately unstaged. Rides also land
-- in that table (D31); this view is runs only (the filter documented in
-- staging YAML; v2.4 owner exception).
with runs as (

    select activity_id
    from {{ ref('stg_strava__activities') }}
    where sport_type in ('Run', 'TrailRun', 'VirtualRun')

)

select
    activity_id,
    ingestion_status,
    (payload ? 'time' and payload ? 'heartrate' and payload ? 'velocity_smooth')
        as has_required_arrays
from {{ source('raw_strava', 'streams') }}
inner join runs using (activity_id)
