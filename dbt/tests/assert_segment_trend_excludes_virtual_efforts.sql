-- D28: VirtualRide efforts are flagged in core and NEVER reach the
-- segment mart. Returns any mart row whose effort is virtual-flagged
-- in fct_segment_efforts (proven red by injection before first green;
-- the built-in tests cannot express a cross-relation holdout).
select trend.effort_id, trend.segment_id
from {{ ref('mart_segment_trend') }} trend
inner join {{ ref('fct_segment_efforts') }} efforts using (effort_id)
where efforts.is_virtual_ride
