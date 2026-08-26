{#
  Great-circle INITIAL bearing in degrees from the first coordinate
  pair toward the second, rendered once for the D30 segment-geometry
  work: 0 = north, 90 = east, range [0, 360). Callers pass
  numeric-valued expressions in degrees. Identical points make atan2's
  arguments (0, 0), which Postgres answers with 0 — silently "due
  north" — so callers MUST guard on a positive straight-line distance
  before trusting this value; the macro deliberately does not hide
  that (the guard belongs with the caller's NULL semantics).
#}
{% macro bearing_degrees(lat1, lon1, lat2, lon2) -%}
mod(
        (degrees(atan2(
            sin(radians(({{ lon2 }}) - ({{ lon1 }}))) * cos(radians({{ lat2 }})),
            cos(radians({{ lat1 }})) * sin(radians({{ lat2 }}))
                - sin(radians({{ lat1 }})) * cos(radians({{ lat2 }}))
                    * cos(radians(({{ lon2 }}) - ({{ lon1 }})))
        )) + 360)::numeric,
        360
    )
{%- endmacro %}
