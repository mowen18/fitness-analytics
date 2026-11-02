{#
  D32 per-ride power coverage: seconds by fill_reason and the share of
  non-stopped seconds with a usable watts value. Stopped seconds leave
  the denominator because a stopped rider has no power to measure. No
  threshold in this phase. Coverage is NULL when total - stopped is 0.
#}

with counts as (

    select
        activity_id,
        count(*)                                        as total_s,
        count(*) filter (where fill_reason = 'observed') as observed_s,
        count(*) filter (where fill_reason = 'held')     as held_s,
        count(*) filter (where fill_reason = 'stopped')  as stopped_s,
        count(*) filter (where fill_reason = 'missing')  as missing_s
    from {{ ref('int_ride_power_grid') }}
    group by activity_id

)

select
    *,
    round(
        (observed_s + held_s)::numeric / nullif(total_s - stopped_s, 0),
        4
    ) as coverage
from counts
