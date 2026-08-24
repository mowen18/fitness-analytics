-- D28: short_segment must equal "per-segment median elapsed time
-- strictly under short_segment_seconds", recomputed here from the
-- mart's own rows so the flag and the statistic can never drift apart.
with medians as (

    select
        segment_id,
        percentile_cont(0.5) within group (order by elapsed_time_s)
            as median_elapsed_s
    from {{ ref('mart_segment_trend') }}
    group by segment_id

)

select trend.segment_id, medians.median_elapsed_s, trend.short_segment
from {{ ref('mart_segment_trend') }} trend
inner join medians using (segment_id)
where trend.short_segment != (medians.median_elapsed_s < {{ var('short_segment_seconds') }})
