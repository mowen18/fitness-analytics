-- int_ride_power_grid's grain is ride x second (composite — the
-- single-column unique test cannot express it).
select activity_id, t_sec, count(*) as row_count
from {{ ref('int_ride_power_grid') }}
group by activity_id, t_sec
having count(*) > 1
