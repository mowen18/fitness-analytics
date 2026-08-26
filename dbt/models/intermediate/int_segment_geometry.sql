{#
  D30 segment geometry, one row per Strava segment (v2.0 Phase C3):
  the great-circle initial bearing from the segment's start to its end
  coordinates, the haversine straight-line distance, sinuosity
  (segment distance / straight line), and the winding flag.

  winding_segment is strictly ABOVE winding_sinuosity_max (D30's word;
  the short_segment strict-< precedent mirrored): a sinuosity of
  exactly the bound is NOT winding. The flag is a displayed caveat
  downstream — the straight-line bearing misdescribes a winding
  course — never a filter. Unknown geometry (missing endpoints, or
  identical endpoints where bearing is undefined and the sinuosity
  division impossible) stays NULL, never false: missing is not
  "not winding". The bearing guard also matters because Postgres
  answers atan2(0, 0) with 0, which would silently claim due north
  for a loop segment.

  Sinuosity is rounded to 4 dp so the exact-boundary comparison is
  decidable; bearing_deg to 1 dp (the derived-value convention).
  straight_line_m stays unrounded and intermediate-only.
#}

with segments as (

    select * from {{ ref('stg_strava__segments') }}

),

measured as (

    select
        segment_id,
        distance_m,
        start_latitude,
        start_longitude,
        end_latitude,
        end_longitude,
        case
            when start_latitude is not null and start_longitude is not null
                and end_latitude is not null and end_longitude is not null
                then {{ haversine_meters(
                    'start_latitude', 'start_longitude',
                    'end_latitude', 'end_longitude') }}
        end as straight_line_m
    from segments

),

derived as (

    select
        segment_id,
        distance_m,
        case
            when straight_line_m > 0
                then round(({{ bearing_degrees(
                    'start_latitude', 'start_longitude',
                    'end_latitude', 'end_longitude') }})::numeric, 1)
        end as bearing_deg,
        straight_line_m,
        case
            when straight_line_m > 0 and distance_m is not null
                then round((distance_m / straight_line_m)::numeric, 4)
        end as sinuosity
    from measured

)

select
    segment_id,
    distance_m,
    bearing_deg,
    straight_line_m,
    sinuosity,
    case
        when sinuosity is not null
            then sinuosity > {{ var('winding_sinuosity_max') }}
    end as winding_segment
from derived
