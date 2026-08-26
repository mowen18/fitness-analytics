-- C3: the mart's three wind columns (headwind_mph, crosswind_mph,
-- winding_segment — the exact set the phase adds, amended D19) are
-- pure projections of fct_segment_efforts. The mart cannot recompute
-- them — direction and bearing deliberately stay core-only — so this
-- effort-grain lockstep join (the assert_band_weekly_matches_
-- segment_mart archetype) is what keeps the displayed values and the
-- core encoding from drifting apart.
select
    trend.effort_id,
    trend.headwind_mph as trend_headwind_mph,
    core.headwind_mph as core_headwind_mph
from {{ ref('mart_segment_trend') }} trend
inner join {{ ref('fct_segment_efforts') }} core using (effort_id)
where
    trend.headwind_mph is distinct from core.headwind_mph
    or trend.crosswind_mph is distinct from core.crosswind_mph
    or trend.winding_segment is distinct from core.winding_segment
