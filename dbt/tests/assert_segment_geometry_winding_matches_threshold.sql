-- D30: winding_segment must equal "sinuosity strictly above
-- winding_sinuosity_max", recomputed from the relation's own rows with
-- the same var the model renders, so the flag and the threshold can
-- never drift apart under --vars overrides. IS DISTINCT FROM also pins
-- the NULL rule: unknown geometry carries a NULL flag (missing is not
-- "not winding"), and a NULL sinuosity with a non-NULL flag — either
-- way around — is a violation.
select segment_id, sinuosity, winding_segment
from {{ ref('int_segment_geometry') }}
where winding_segment is distinct from (sinuosity > {{ var('winding_sinuosity_max') }})
