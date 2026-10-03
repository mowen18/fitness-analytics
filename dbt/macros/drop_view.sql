{#
  Run cascading view drops one at a time (deadlock fix, 2026-10-03).

  dbt replaces a view by swapping in a new one, then dropping the old
  one ("<name>__dbt_backup") with CASCADE. The cascade also drops every
  old view downstream of it. Since C3, int_segment_efforts reads
  stg_weather__hourly directly and stg_strava__activities through
  int_rides_with_weather -> int_ride_measures, so the two staging
  cascades reach the same views in different orders. When both drops
  start at the same moment, each holds a lock the other needs and
  Postgres aborts one with "deadlock detected".

  The advisory lock makes every view drop wait for the one before it.
  Both statements travel in one query message, which Postgres runs as
  one transaction, so the lock is released as soon as the drop ends.
  The lock is per database. This overrides dbt-postgres's
  postgres__drop_view (same drop statement, lock added); pinned by
  test_view_drops_take_the_cascade_lock.
#}
{% macro postgres__drop_view(relation) -%}
    select pg_advisory_xact_lock(hashtext('running_analytics.drop_view_cascade'));
    drop view if exists {{ relation }} cascade
{%- endmacro %}
