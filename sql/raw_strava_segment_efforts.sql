-- Phase C2 (D24): segment-effort ingestion via per-ride detail fetches.
-- One status row per attempted fetch (activity_details, mirroring
-- raw_strava.streams), plus the extracted efforts and their segments.
-- Idempotent: applied on first container init via docker-entrypoint-initdb.d
-- (alphabetically after raw_strava.sql) and re-appliable with `make bootstrap`.

CREATE TABLE IF NOT EXISTS raw_strava.activity_details (
    activity_id      bigint       PRIMARY KEY,
    payload          jsonb        NOT NULL,
    effort_count     integer,
    fetched_at       timestamptz  NOT NULL,
    ingestion_status text         NOT NULL
        CHECK (ingestion_status IN ('success', 'failed', 'unavailable')),
    error_message    text         NULL
);

COMMENT ON TABLE raw_strava.activity_details IS
    'One row per attempted detail fetch (GET /activities/{id}?include_all_efforts=true). success = detail stored and efforts extracted; unavailable = the activity no longer exists on Strava (terminal); failed = transient error, retried by the next run. Absent row = not yet attempted — the status rows are the resumability mechanism (D24); there is no sync watermark.';
COMMENT ON COLUMN raw_strava.activity_details.payload IS
    'Full DetailedActivity response for success; {} for failed/unavailable (NOT NULL so "no payload" is always explicit, never ambiguous). Estimated average_watts/kilojoules live ONLY here — never modeled downstream (D27).';
COMMENT ON COLUMN raw_strava.activity_details.effort_count IS
    'Number of segment efforts in the detail for success rows; NULL otherwise (missing, not zero)';
COMMENT ON COLUMN raw_strava.activity_details.error_message IS
    'Actionable failure detail for failed/unavailable rows; never contains tokens';

CREATE INDEX IF NOT EXISTS activity_details_ingestion_status_idx
    ON raw_strava.activity_details (ingestion_status);

CREATE TABLE IF NOT EXISTS raw_strava.segment_efforts (
    effort_id   bigint       PRIMARY KEY,
    activity_id bigint       NOT NULL,
    segment_id  bigint       NOT NULL,
    payload     jsonb        NOT NULL,
    fetched_at  timestamptz  NOT NULL
);

COMMENT ON TABLE raw_strava.segment_efforts IS
    'One row per segment effort, extracted from the parent activity detail (D24). Upserted by effort_id: re-fetching a ride replaces its efforts in place, so re-runs never duplicate.';
COMMENT ON COLUMN raw_strava.segment_efforts.payload IS
    'Full DetailedSegmentEffort object, including the nested segment summary';

CREATE INDEX IF NOT EXISTS segment_efforts_activity_id_idx
    ON raw_strava.segment_efforts (activity_id);
CREATE INDEX IF NOT EXISTS segment_efforts_segment_id_idx
    ON raw_strava.segment_efforts (segment_id);

CREATE TABLE IF NOT EXISTS raw_strava.segments (
    segment_id bigint       PRIMARY KEY,
    payload    jsonb        NOT NULL,
    fetched_at timestamptz  NOT NULL
);

COMMENT ON TABLE raw_strava.segments IS
    'One row per Strava segment, upserted from the summary nested in each effort (D24). Shared across activities; last write wins — the newest fetch''s summary replaces the stored payload.';
