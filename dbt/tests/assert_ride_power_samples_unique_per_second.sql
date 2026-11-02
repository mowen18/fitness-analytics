-- stg_strava__ride_power_samples' grain is ride x sample second
-- (composite — the single-column unique test cannot express it).
select activity_id, t_sec, count(*) as row_count
from {{ ref('stg_strava__ride_power_samples') }}
group by activity_id, t_sec
having count(*) > 1
