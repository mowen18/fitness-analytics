-- D30: headwind_mph must equal effort_wind_speed_mph *
-- cos(radians(effort_wind_direction_deg - bearing_deg)) — POSITIVE =
-- HEADWIND, the pinned sign convention — and be NULL exactly when an
-- input is missing (direction missing is never zero; the effort-wind
-- columns are already NULL beyond the 60-minute rule, so the input
-- guard covers that rung too). Crosswind: same inputs, unsigned sine.
-- Recomputed from the relation's own columns so the stored values and
-- the formula can never drift apart.
select effort_id, headwind_mph, crosswind_mph
from {{ ref('fct_segment_efforts') }}
where
    headwind_mph is distinct from (
        case
            when effort_wind_speed_mph is not null
                and effort_wind_direction_deg is not null
                and bearing_deg is not null
                then round(
                    (
                        effort_wind_speed_mph
                        * cos(radians(effort_wind_direction_deg - bearing_deg))
                    )::numeric, 1
                )
        end
    )
    or crosswind_mph is distinct from (
        case
            when effort_wind_speed_mph is not null
                and effort_wind_direction_deg is not null
                and bearing_deg is not null
                then round(
                    (
                        effort_wind_speed_mph
                        * abs(sin(radians(effort_wind_direction_deg - bearing_deg)))
                    )::numeric, 1
                )
        end
    )
