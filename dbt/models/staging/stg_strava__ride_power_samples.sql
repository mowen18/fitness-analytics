{#
  D32 ride power samples: one row per stream sample of a ride with real
  power. Power is trusted only when device_watts is literally true
  (D27 as amended by v2.4); false and an absent key both stay out, and
  ride_sport_types keeps real-power runs and e-bikes out. Run payloads
  stay unstaged (Revision v1.2 follow-up); this model is the recorded
  v2.4 exception.

  rows from() walks the parallel arrays once instead of indexing every
  sample. jsonb_array_elements_text turns JSON null into SQL NULL, and an
  absent watts key pads NULL, so a ride whose payload has no watts array
  stays visible instead of vanishing. Watts are never modified here:
  NULL means missing, and a real 0 W stays 0.
#}

with streams as (

    select activity_id, payload
    from {{ source('raw_strava', 'streams') }}
    where ingestion_status = 'success'

),

power_rides as (

    select activity_id
    from {{ source('raw_strava', 'activities') }}
    where activity_type in (
        {%- for ride_type in var('ride_sport_types') %}
        '{{ ride_type }}'{% if not loop.last %},{% endif %}
        {%- endfor %}
    )
      and coalesce((payload ->> 'device_watts')::boolean, false)

)

select
    streams.activity_id,
    sample.t_sec::integer      as t_sec,
    sample.watts::numeric      as watts,
    sample.distance_m::numeric as distance_m
from streams
inner join power_rides using (activity_id)
cross join lateral rows from (
    jsonb_array_elements_text(streams.payload -> 'time' -> 'data'),
    jsonb_array_elements_text(streams.payload -> 'watts' -> 'data'),
    jsonb_array_elements_text(streams.payload -> 'distance' -> 'data')
) as sample (t_sec, watts, distance_m)
