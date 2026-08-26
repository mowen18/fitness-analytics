{#
  Great-circle (haversine) distance in meters between two coordinate
  pairs, rendered once for the D30 segment-geometry work. Earth radius
  is pinned here and only here: 6371008.8 m (the IUGG mean radius) —
  tests hand-compute expectations with the same constant. Callers pass
  numeric-valued expressions in degrees; the least(1.0, ...) clamp
  keeps floating-point noise on antipodal-ish inputs out of asin's
  domain. NULL endpoints propagate to a NULL result — callers guard
  where NULL must mean something specific.
#}
{% macro haversine_meters(lat1, lon1, lat2, lon2) -%}
2 * 6371008.8 * asin(least(1.0, sqrt(
        sin(radians(({{ lat2 }}) - ({{ lat1 }})) / 2) ^ 2
        + cos(radians({{ lat1 }})) * cos(radians({{ lat2 }}))
            * sin(radians(({{ lon2 }}) - ({{ lon1 }})) / 2) ^ 2
    )))
{%- endmacro %}
