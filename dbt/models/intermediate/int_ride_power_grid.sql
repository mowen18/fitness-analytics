{#
  D32 one-second power grid: one row per real-power ride per second, 0 to
  the ride's last t_sec. watts_observed is exactly as received; watts is
  what downstream metrics read. Coasting is a real 0 W and stays observed;
  NULL means missing, never zero.

  Postgres 17 has no IGNORE NULLS, so the fill uses the running-count
  trick: count(watts_observed) only advances on an observed row, so each
  observed row and the NULL seconds after it share one observed_so_far.
  Group 0 is the NULL seconds before the first observed row.

  The fill rule is first match wins, in the D32 order. A NULL distance on
  a bounding row makes speed NULL, so the stopped comparison is not true
  and the run stays missing — never assumed stopped.
#}

with samples as (

    select * from {{ ref('stg_strava__ride_power_samples') }}

),

ride_seconds as (

    select
        activity_id,
        generate_series(0, max(t_sec)) as t_sec
    from samples
    group by activity_id

),

grid as (

    select
        ride_seconds.activity_id,
        ride_seconds.t_sec,
        samples.watts       as watts_observed,
        samples.distance_m,
        count(samples.watts) over (
            partition by ride_seconds.activity_id
            order by ride_seconds.t_sec
        )                   as observed_so_far
    from ride_seconds
    left join samples using (activity_id, t_sec)

),

bounds as (

    -- Bounding rows have non-NULL watts_observed. Each group's observed
    -- row is the run's prior bound; the next observed row is its next.
    select
        activity_id,
        observed_so_far,
        watts_observed                   as prior_watts,
        t_sec                            as prior_t_sec,
        distance_m                       as prior_distance_m,
        lead(t_sec) over bounding        as next_t_sec,
        lead(distance_m) over bounding   as next_distance_m
    from grid
    where watts_observed is not null
    window bounding as (partition by activity_id order by t_sec)

),

null_runs as (

    select
        activity_id,
        observed_so_far,
        count(*) as run_length_s
    from grid
    where watts_observed is null
    group by activity_id, observed_so_far

),

classified as (

    select
        grid.activity_id,
        grid.t_sec,
        grid.watts_observed,
        grid.distance_m,
        bounds.prior_watts,
        case
            when grid.watts_observed is not null
                then 'observed'
            when null_runs.run_length_s <= {{ var('power_hold_max_gap_s') }}
                and bounds.prior_t_sec is not null
                then 'held'
            when bounds.prior_t_sec is null or bounds.next_t_sec is null
                then 'missing'
            when (bounds.next_distance_m - bounds.prior_distance_m)
                / (bounds.next_t_sec - bounds.prior_t_sec)
                < {{ var('power_stopped_speed_mps') }}
                then 'stopped'
            else 'missing'
        end as fill_reason
    from grid
    left join bounds using (activity_id, observed_so_far)
    left join null_runs using (activity_id, observed_so_far)

)

select
    activity_id,
    t_sec,
    watts_observed,
    case
        when fill_reason = 'observed' then watts_observed
        when fill_reason = 'held' then prior_watts
    end as watts,
    fill_reason,
    distance_m
from classified
