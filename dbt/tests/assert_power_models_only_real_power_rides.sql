-- D27 as amended by v2.4: no estimated-watts row, real-power run, or
-- e-bike may reach a power model. Checked against the source, so this
-- pin cannot drift with the staging filter it guards.
with real_power_rides as (

    select activity_id
    from {{ source('raw_strava', 'activities') }}
    where activity_type in (
        {%- for ride_type in var('ride_sport_types') %}
        '{{ ride_type }}'{% if not loop.last %},{% endif %}
        {%- endfor %}
    )
      and coalesce((payload ->> 'device_watts')::boolean, false)

),

power_model_rows as (

    select activity_id, 'stg_strava__ride_power_samples' as model
    from {{ ref('stg_strava__ride_power_samples') }}

    union all

    select activity_id, 'int_ride_power_grid' as model
    from {{ ref('int_ride_power_grid') }}

    union all

    select activity_id, 'int_ride_power_coverage' as model
    from {{ ref('int_ride_power_coverage') }}

)

select power_model_rows.activity_id, power_model_rows.model
from power_model_rows
left join real_power_rides using (activity_id)
where real_power_rides.activity_id is null
