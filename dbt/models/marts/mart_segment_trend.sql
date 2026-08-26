{#
  D28 trend at Strava-segment x effort grain — the mart the Cycling
  segments view reads, and the only cycling-segment relation on the D19
  allow-list, so every number the view displays must live here. Primary
  series: elapsed time; moving time secondary. VirtualRide efforts are
  held out (kept in core, flagged) and reported per segment as
  virtual_effort_count so the view can caption the exclusion.

  effort_count, effort_seq, the rolling median, and the cumulative best
  all cover NON-virtual efforts only: sufficiency is the minimum sample
  of the DISPLAYED statistic (the D12 spirit; recorded in marts.yml).
  Parent-ride validity (ride_is_valid / ride_exclusion_reason) and the
  short_segment noise flag are displayed caveats, never filters.
  Static column names per v1.3: segment_rolling_effort_window changes
  the window, never the interface.
#}

with included as (

    -- D28: virtual efforts never reach segment marts.
    select * from {{ ref('fct_segment_efforts') }}
    where not is_virtual_ride

),

sequenced as (

    select
        *,
        row_number() over (
            partition by segment_id
            order by start_date_utc, effort_id
        ) as effort_seq
    from included

),

segment_stats as (

    select
        segment_id,
        count(*) as effort_count,
        percentile_cont(0.5) within group (order by elapsed_time_s)
            as median_elapsed_s
    from included
    group by segment_id

),

virtual_counts as (

    -- Counted from core so the excluded efforts stay reportable
    -- without the app touching anything off the allow-list.
    select
        segment_id,
        count(*) as virtual_effort_count
    from {{ ref('fct_segment_efforts') }}
    where is_virtual_ride
    group by segment_id

),

rolling as (

    -- Rolling median over the last segment_rolling_effort_window
    -- efforts per segment. percentile_cont is not a window function,
    -- so the window is an explicit self-join + group (the
    -- mart_efficiency_trend pattern) — row-based here rather than
    -- calendar-based because D28's window is "last 5 efforts".
    select
        this.segment_id,
        this.effort_seq,
        percentile_cont(0.5) within group (order by w.elapsed_time_s)
                 as rolling_median_elapsed_s,
        count(*) as rolling_effort_count
    from sequenced this
    inner join sequenced w
        on w.segment_id = this.segment_id
        and w.effort_seq
            between this.effort_seq - ({{ var('segment_rolling_effort_window') }} - 1)
            and this.effort_seq
    group by this.segment_id, this.effort_seq

)

select
    sequenced.segment_id,
    sequenced.segment_name,
    sequenced.segment_distance_m,
    sequenced.average_grade_pct,
    sequenced.city,
    sequenced.state,
    sequenced.effort_id,
    sequenced.effort_seq,
    sequenced.activity_id,
    sequenced.start_date_local,
    sequenced.elapsed_time_s,
    sequenced.moving_time_s,
    -- Effort speed over ELAPSED time (the Strava segment convention;
    -- D28's primary series) — displayed context, projected from core
    -- and pinned by assert_segment_trend_speed_matches_core.
    sequenced.speed_mph,
    sequenced.average_hr_bpm,
    sequenced.average_cadence_rpm,
    sequenced.pr_rank,
    round(rolling.rolling_median_elapsed_s::numeric, 1) as rolling_median_elapsed_s,
    rolling.rolling_effort_count,
    -- The cumulative best CAN use a plain window function — min() is a
    -- true window aggregate, unlike percentile_cont above — so the two
    -- sibling statistics deliberately use different mechanisms.
    min(sequenced.elapsed_time_s) over (
        partition by sequenced.segment_id
        order by sequenced.effort_seq
        rows between unbounded preceding and current row
    ) as best_elapsed_s,
    segment_stats.effort_count,
    segment_stats.effort_count >= {{ var('segment_trend_min_efforts') }}
        as is_sufficient,
    -- Short-segment noise flag (displayed, never a filter): at medians
    -- under short_segment_seconds, ±1 s GPS sampling is material.
    segment_stats.median_elapsed_s < {{ var('short_segment_seconds') }}
        as short_segment,
    coalesce(virtual_counts.virtual_effort_count, 0) as virtual_effort_count,
    sequenced.ride_is_valid,
    sequenced.ride_exclusion_reason,
    sequenced.weather_available,
    sequenced.temperature_f,
    sequenced.apparent_temperature_f,
    sequenced.relative_humidity_pct,
    sequenced.wind_speed_mph,
    -- C3 (D30), the exact three columns the phase adds — no allow-list
    -- change rides on this (amended D19). Sign convention, pinned:
    -- positive headwind_mph = headwind, negative = tailwind; crosswind
    -- unsigned. NULL when direction, the 60-minute effort-hour match,
    -- or the segment bearing is missing; winding_segment is a
    -- displayed caveat (straight-line bearing misdescribes the
    -- course), never a filter. Pinned to core by
    -- assert_segment_trend_wind_matches_core.
    sequenced.headwind_mph,
    sequenced.crosswind_mph,
    sequenced.winding_segment
from sequenced
left join segment_stats using (segment_id)
left join virtual_counts using (segment_id)
left join rolling
    on rolling.segment_id = sequenced.segment_id
    and rolling.effort_seq = sequenced.effort_seq
