-- D28: the sufficiency flag must equal the comparison it claims to
-- encode under whatever segment_trend_min_efforts currently is — the
-- built-in tests cannot compare a flag against a var-driven predicate.
select segment_id, effort_count, is_sufficient
from {{ ref('mart_segment_trend') }}
where is_sufficient != (effort_count >= {{ var('segment_trend_min_efforts') }})
