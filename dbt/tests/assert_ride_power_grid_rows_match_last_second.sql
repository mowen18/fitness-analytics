-- D32: t_sec runs from 0 to the ride's last t_sec with no hole, so rows
-- per ride = last t_sec + 1, and the last second is the last sample's.
with grid as (

    select
        activity_id,
        count(*)   as row_count,
        min(t_sec) as first_t_sec,
        max(t_sec) as last_t_sec
    from {{ ref('int_ride_power_grid') }}
    group by activity_id

),

samples as (

    select activity_id, max(t_sec) as last_sample_t_sec
    from {{ ref('stg_strava__ride_power_samples') }}
    group by activity_id

)

select grid.*, samples.last_sample_t_sec
from grid
full join samples using (activity_id)
where grid.row_count is distinct from grid.last_t_sec + 1
   or grid.first_t_sec is distinct from 0
   or grid.last_t_sec is distinct from samples.last_sample_t_sec
