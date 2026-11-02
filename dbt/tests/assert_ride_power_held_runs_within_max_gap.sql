-- D32: no held run is longer than power_hold_max_gap_s. Consecutive held
-- seconds share t_sec minus their row number.
with held as (

    select
        activity_id,
        t_sec,
        t_sec - row_number() over (partition by activity_id order by t_sec) as run_key
    from {{ ref('int_ride_power_grid') }}
    where fill_reason = 'held'

)

select activity_id, run_key, count(*) as run_length_s
from held
group by activity_id, run_key
having count(*) > {{ var('power_hold_max_gap_s') }}
