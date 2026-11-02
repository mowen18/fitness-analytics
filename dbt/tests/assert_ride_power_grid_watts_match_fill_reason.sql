-- D32: observed rows carry the received value unmodified; held rows are
-- the only filled values; stopped and missing rows carry no power.
select activity_id, t_sec, watts_observed, watts, fill_reason
from {{ ref('int_ride_power_grid') }}
where (fill_reason = 'observed'
        and (watts_observed is null or watts is distinct from watts_observed))
   or (fill_reason = 'held' and (watts_observed is not null or watts is null))
   or (fill_reason in ('stopped', 'missing') and (watts_observed is not null or watts is not null))
