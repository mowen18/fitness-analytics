"""Command-line interface. Never prints or logs token values."""

import logging
import sys
from datetime import UTC, datetime
from urllib.parse import parse_qs, urlparse

import click
import psycopg

from running_pipeline import (
    activity_ingestion,
    coordinate_ingestion,
    segment_ingestion,
    stream_ingestion,
    weather_ingestion,
)
from running_pipeline.config import load_settings
from running_pipeline.database import get_connection
from running_pipeline.strava_client import (
    DEFAULT_SCOPE,
    StravaApiError,
    StravaAuthError,
    StravaClient,
)
from running_pipeline.weather_client import WeatherClient


@click.group()
def cli():
    """Running Analytics Pipeline."""


@cli.command()
def athlete():
    """Fetch and print the authenticated athlete's profile."""
    client = StravaClient(load_settings())
    try:
        profile = client.get_athlete()
    except StravaAuthError as exc:
        raise click.ClickException(
            f"{exc}\nIf the refresh token is invalid or under-scoped, run "
            "`running-pipeline authorize` to re-authorize."
        ) from exc

    name = f"{profile.get('firstname', '')} {profile.get('lastname', '')}".strip()
    location = ", ".join(
        part for part in (profile.get("city"), profile.get("state"), profile.get("country")) if part
    )
    click.echo(f"Athlete id:  {profile.get('id')}")
    click.echo(f"Name:        {name or '<none>'}")
    click.echo(f"Username:    {profile.get('username') or '<none>'}")
    click.echo(f"Location:    {location or '<none>'}")
    click.echo(f"Created:     {profile.get('created_at') or '<unknown>'}")
    click.echo(f"Followers:   {profile.get('follower_count', '<hidden>')}")
    click.echo(f"Following:   {profile.get('friend_count', '<hidden>')}")


@cli.command("sync-activities")
@click.option(
    "--full",
    is_flag=True,
    help="Full reconciliation: re-fetch everything from SYNC_START_DATE, ignoring the watermark.",
)
def sync_activities(full: bool):
    """Sync Strava activities into raw_strava.activities (idempotent)."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )
    settings = load_settings()
    client = StravaClient(settings)
    try:
        with get_connection(settings) as conn:
            report = activity_ingestion.sync_activities(settings, client, conn, full=full)
    except StravaAuthError as exc:
        raise click.ClickException(
            f"{exc}\nIf the refresh token is invalid or under-scoped, run "
            "`running-pipeline authorize` to re-authorize."
        ) from exc
    except StravaApiError as exc:
        raise click.ClickException(str(exc)) from exc
    except psycopg.OperationalError as exc:
        raise click.ClickException(
            f"Could not reach Postgres: {exc}\nStart it with `make up`."
        ) from exc

    outcome = "stopped early at the rate limit" if report.stopped_early else "complete"
    click.echo(
        f"Sync {outcome}: pages={report.pages} received={report.received} "
        f"inserted={report.inserted} updated={report.updated} skipped={report.skipped}"
    )
    if report.stopped_early:
        click.echo("Committed pages were kept; re-run later to finish.")
        sys.exit(3)


@cli.command("sync-streams")
def sync_streams():
    """Backfill activity streams for fetch-eligible runs (resumable)."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )
    settings = load_settings()
    client = StravaClient(settings)
    try:
        with get_connection(settings) as conn:
            report = stream_ingestion.sync_streams(settings, client, conn)
    except StravaAuthError as exc:
        raise click.ClickException(
            f"{exc}\nIf the refresh token is invalid or under-scoped, run "
            "`running-pipeline authorize` to re-authorize."
        ) from exc
    except psycopg.OperationalError as exc:
        raise click.ClickException(
            f"Could not reach Postgres: {exc}\nStart it with `make up`."
        ) from exc

    outcome = "stopped early at the rate limit" if report.stopped_early else "complete"
    click.echo(
        f"Stream backfill {outcome}: eligible={report.eligible} "
        f"succeeded={report.succeeded} unavailable={report.unavailable} "
        f"failed={report.failed} last_processed="
        f"{report.last_processed_id if report.last_processed_id is not None else 'none'}"
    )
    if report.stopped_early:
        click.echo("Committed rows were kept; re-run later to resume.")
        sys.exit(3)


@cli.command("sync-segment-efforts")
def sync_segment_efforts():
    """Backfill segment efforts for D23 rides via detail fetches (resumable)."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )
    settings = load_settings()
    client = StravaClient(settings)
    try:
        with get_connection(settings) as conn:
            report = segment_ingestion.sync_segment_efforts(settings, client, conn)
    except StravaAuthError as exc:
        raise click.ClickException(
            f"{exc}\nIf the refresh token is invalid or under-scoped, run "
            "`running-pipeline authorize` to re-authorize."
        ) from exc
    except psycopg.OperationalError as exc:
        raise click.ClickException(
            f"Could not reach Postgres: {exc}\nStart it with `make up`."
        ) from exc

    outcome = "stopped early at the rate limit" if report.stopped_early else "complete"
    click.echo(
        f"Segment-effort backfill {outcome}: eligible={report.eligible} "
        f"succeeded={report.succeeded} unavailable={report.unavailable} "
        f"failed={report.failed} efforts_stored={report.efforts_stored} "
        f"last_processed="
        f"{report.last_processed_id if report.last_processed_id is not None else 'none'}"
    )
    if report.stopped_early:
        click.echo("Committed rows were kept; re-run later to resume.")
        sys.exit(3)


@cli.command("backfill-coordinates")
def backfill_coordinates():
    """Resolve run-start coordinates (payload, else detail polyline)."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )
    settings = load_settings()
    client = StravaClient(settings)
    try:
        with get_connection(settings) as conn:
            report = coordinate_ingestion.backfill_coordinates(settings, client, conn)
    except StravaAuthError as exc:
        raise click.ClickException(
            f"{exc}\nIf the refresh token is invalid or under-scoped, run "
            "`running-pipeline authorize` to re-authorize."
        ) from exc
    except psycopg.OperationalError as exc:
        raise click.ClickException(
            f"Could not reach Postgres: {exc}\nStart it with `make up`."
        ) from exc

    outcome = "stopped early at the rate limit" if report.stopped_early else "complete"
    click.echo(
        f"Coordinate backfill {outcome}: harvested={report.harvested} "
        f"candidates={report.candidates} decoded={report.decoded} "
        f"unavailable={report.unavailable} failed={report.failed}"
    )
    if report.stopped_early:
        click.echo("Committed rows were kept; re-run later to resume.")
        sys.exit(3)


@cli.command("sync-weather")
@click.option(
    "--full",
    is_flag=True,
    help="Re-fetch weather even for already-cached hours (the archive occasionally revises data).",
)
def sync_weather(full: bool):
    """Attach hourly weather to outdoor runs (idempotent; the table is the cache)."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )
    settings = load_settings()
    client = WeatherClient(settings)
    try:
        with get_connection(settings) as conn:
            report = weather_ingestion.sync_weather(settings, client, conn, full=full)
    except psycopg.OperationalError as exc:
        raise click.ClickException(
            f"Could not reach Postgres: {exc}\nStart it with `make up`."
        ) from exc

    outcome = "stopped early at the request limit" if report.stopped_early else "complete"
    click.echo(
        f"Weather sync {outcome}: eligible_runs={report.eligible_runs} "
        f"runs_without_location={report.runs_without_location} "
        f"hours_needed={report.hours_needed} hours_cached={report.hours_cached} "
        f"requests={report.requests_made} inserted={report.inserted} "
        f"updated={report.updated} skipped={report.skipped} "
        f"failed_batches={report.failed_batches} "
        f"hours_still_missing={report.hours_still_missing}"
    )
    if report.stopped_early:
        click.echo("Committed batches were kept; re-run later to fetch the rest.")
        sys.exit(3)


@cli.command()
@click.option("--scope", default=DEFAULT_SCOPE, show_default=True, help="OAuth scopes to request.")
@click.option(
    "--code",
    default=None,
    help="Authorization code, or the full redirect URL it came from. "
    "Skips the interactive prompt (needed in non-TTY shells).",
)
def authorize(scope: str, code: str | None):
    """One-time browser authorization (initial setup or re-scoping)."""
    settings = load_settings()
    client = StravaClient(settings)

    if code is None:
        click.echo("1. Open this URL in your browser and click Authorize:\n")
        click.echo(f"   {client.authorize_url(scope)}\n")
        click.echo(
            "2. You will be redirected to a localhost URL that fails to load —\n"
            "   that is expected. Copy the value of the `code` parameter from\n"
            "   the address bar (or copy the whole URL).\n"
        )
        code = click.prompt("3. Paste the authorization code or redirect URL")

    try:
        tokens = client.exchange_code(_extract_code(code))
    except StravaAuthError as exc:
        raise click.ClickException(str(exc)) from exc

    expires = datetime.fromtimestamp(tokens.expires_at, tz=UTC).isoformat()
    click.echo(f"\nAuthorized. Tokens saved to {settings.token_file} (access token")
    click.echo(f"expires {expires}). The STRAVA_REFRESH_TOKEN line in .env is now")
    click.echo("stale by design; the token file is authoritative from here on.")


def _extract_code(raw: str) -> str:
    """Accept either the bare authorization code or the full redirect URL."""
    raw = raw.strip()
    if "code=" not in raw:
        return raw
    query = urlparse(raw).query or raw.split("?", 1)[-1]
    values = parse_qs(query).get("code", [])
    if not values:
        raise click.ClickException("Could not find a `code` parameter in the pasted value.")
    return values[0]


if __name__ == "__main__":
    sys.exit(cli())
