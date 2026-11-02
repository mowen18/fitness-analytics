-- D32: every grid row has exactly one fill_reason, so per-ride counts sum
-- to the ride's grid row count, and every grid ride has a coverage row.
with grid as (

    select activity_id, count(*) as grid_row_count
    from {{ ref('int_ride_power_grid') }}
    group by activity_id

)

select coverage.*, grid.grid_row_count
from {{ ref('int_ride_power_coverage') }} coverage
full join grid using (activity_id)
where coverage.observed_s + coverage.held_s + coverage.stopped_s + coverage.missing_s
        is distinct from grid.grid_row_count
   or coverage.total_s is distinct from grid.grid_row_count
