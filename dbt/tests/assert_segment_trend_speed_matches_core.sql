-- The mart's speed_mph is a pure projection of fct_segment_efforts —
-- the mart cannot recompute it (effort_distance_m deliberately stays
-- core-only) — so this effort-grain lockstep join is what keeps the
-- displayed value and the core encoding from drifting apart (the
-- assert_segment_trend_wind_matches_core archetype).
select
    trend.effort_id,
    trend.speed_mph as trend_speed_mph,
    core.speed_mph as core_speed_mph
from {{ ref('mart_segment_trend') }} trend
inner join {{ ref('fct_segment_efforts') }} core using (effort_id)
where trend.speed_mph is distinct from core.speed_mph
