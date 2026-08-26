-- Effort speed must equal effort_distance_m over ELAPSED time in mph
-- (the Strava segment convention — segments rank and display on total
-- elapsed time — and D28's primary series), recomputed from the
-- relation's own columns so the stored value and the formula can
-- never drift apart. IS DISTINCT FROM also pins the NULL rule: speed
-- is NULL exactly when distance is missing or elapsed time is not
-- positive — never zero.
select effort_id, effort_distance_m, elapsed_time_s, speed_mph
from {{ ref('fct_segment_efforts') }}
where speed_mph is distinct from (
    case
        when effort_distance_m is not null and elapsed_time_s > 0
            then round((effort_distance_m * 3600 / (elapsed_time_s * 1609.344))::numeric, 1)
    end
)
