"""Strava API client: OAuth token refresh, athlete profile, and paginated
activity retrieval with bounded retries and rate-limit awareness.

Strava rotates the refresh token on every refresh; the newest one is
persisted to a gitignored local file immediately, before any other call,
because losing it forces a full browser re-authorization.

Token values must never appear in logs, exceptions, or reprs.
"""

import json
import os
import tempfile
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode

import requests

from running_pipeline.config import Settings

AUTHORIZE_URL = "https://www.strava.com/oauth/authorize"
TOKEN_URL = "https://www.strava.com/oauth/token"
API_BASE = "https://www.strava.com/api/v3"

DEFAULT_SCOPE = "read,activity:read_all"
# Refresh a bit before the reported expiry to avoid using a token that
# dies mid-request.
EXPIRY_MARGIN_SECONDS = 60
REQUEST_TIMEOUT_SECONDS = 30

ACTIVITIES_PER_PAGE_MAX = 200  # Strava's documented maximum
# Stream types for drift analysis (plan Phase 5): elapsed seconds, HR,
# smoothed velocity, moving flag, grade.
STREAM_TYPES = ("time", "heartrate", "velocity_smooth", "moving", "grade_smooth")
# Backoff between retries of transient failures (connection errors, 5xx);
# one initial attempt plus one retry per entry.
TRANSIENT_RETRY_BACKOFF_SECONDS = (1, 2, 4)
# Stop cleanly once usage reaches this fraction of any reported limit.
RATE_LIMIT_STOP_FRACTION = 0.9


class StravaAuthError(RuntimeError):
    """Auth/token exchange failed. Message is safe to print (no tokens)."""


class StravaApiError(RuntimeError):
    """API request failed (after bounded retries when transient). No tokens."""


class RateLimitStop(RuntimeError):
    """Stop ingestion cleanly: keep committed work, don't advance the watermark."""


class RateLimitExceeded(RateLimitStop):
    """Strava returned 429; a limit is already exhausted."""


class RateLimitApproaching(RateLimitStop):
    """Usage crossed RATE_LIMIT_STOP_FRACTION of a reported limit."""


@dataclass
class RateLimitStatus:
    window: str  # "read" or "overall"
    short_usage: int
    short_limit: int
    daily_usage: int
    daily_limit: int

    def approaching(self, fraction: float = RATE_LIMIT_STOP_FRACTION) -> bool:
        return (
            self.short_usage >= fraction * self.short_limit
            or self.daily_usage >= fraction * self.daily_limit
        )

    def describe(self) -> str:
        return (
            f"{self.window} 15-min {self.short_usage}/{self.short_limit}, "
            f"daily {self.daily_usage}/{self.daily_limit}"
        )


@dataclass
class TokenSet:
    access_token: str
    refresh_token: str
    expires_at: int  # unix epoch seconds

    def is_fresh(self, now: float | None = None) -> bool:
        now = time.time() if now is None else now
        return self.expires_at > now + EXPIRY_MARGIN_SECONDS


class TokenStore:
    """Persists the token trio outside version control (0600 perms)."""

    def __init__(self, path: Path):
        self.path = path

    def load(self) -> TokenSet | None:
        if not self.path.exists():
            return None
        data = json.loads(self.path.read_text())
        return TokenSet(
            access_token=data["access_token"],
            refresh_token=data["refresh_token"],
            expires_at=int(data["expires_at"]),
        )

    def save(self, tokens: TokenSet) -> None:
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        payload = json.dumps(
            {
                "access_token": tokens.access_token,
                "refresh_token": tokens.refresh_token,
                "expires_at": tokens.expires_at,
            },
            indent=2,
        )
        # Atomic replace: a crash mid-write must never lose the previous
        # (still-valid) rotated refresh token.
        fd, tmp_path = tempfile.mkstemp(dir=self.path.parent, prefix=".tokens-")
        try:
            with os.fdopen(fd, "w") as tmp_file:
                tmp_file.write(payload)
            os.chmod(tmp_path, 0o600)
            os.replace(tmp_path, self.path)
        except BaseException:
            Path(tmp_path).unlink(missing_ok=True)
            raise


class StravaClient:
    def __init__(
        self,
        settings: Settings,
        store: TokenStore | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self._settings = settings
        self._store = store or TokenStore(settings.token_file)
        self._session = requests.Session()
        self._sleep = sleep  # injectable so tests don't wait out backoffs
        # Usage from the most recent API response; lets single-request
        # callers (stream backfill) stop cleanly between requests the way
        # iter_activity_pages stops between pages.
        self.last_rate_limit_statuses: list[RateLimitStatus] = []

    # ── OAuth ─────────────────────────────────────────────────────────

    def authorize_url(self, scope: str = DEFAULT_SCOPE) -> str:
        """URL for the one-time browser authorization (initial or re-auth)."""
        params = urlencode(
            {
                "client_id": self._settings.strava_client_id,
                "redirect_uri": "http://localhost/exchange_token",
                "response_type": "code",
                "approval_prompt": "auto",
                "scope": scope,
            }
        )
        return f"{AUTHORIZE_URL}?{params}"

    def exchange_code(self, code: str) -> TokenSet:
        """Exchange an authorization code for tokens and persist them."""
        return self._token_request(grant_type="authorization_code", code=code)

    def refresh_access_token(self) -> TokenSet:
        """Refresh using the newest known refresh token and persist the rotation."""
        stored = self._store.load()
        if stored is not None:
            refresh_token = stored.refresh_token
        elif self._settings.strava_refresh_token is not None:
            refresh_token = self._settings.strava_refresh_token.get_secret_value()
        else:
            raise StravaAuthError(
                "No refresh token available: token file "
                f"{self._store.path} does not exist and STRAVA_REFRESH_TOKEN "
                "is unset. Run `running-pipeline authorize` first."
            )
        return self._token_request(grant_type="refresh_token", refresh_token=refresh_token)

    def get_access_token(self) -> str:
        """Reuse the stored access token while fresh; refresh otherwise."""
        stored = self._store.load()
        if stored is not None and stored.is_fresh():
            return stored.access_token
        return self.refresh_access_token().access_token

    def _token_request(self, **grant_fields: str) -> TokenSet:
        response = self._session.post(
            TOKEN_URL,
            data={
                "client_id": self._settings.strava_client_id,
                "client_secret": self._settings.strava_client_secret.get_secret_value(),
                **grant_fields,
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        if response.status_code != 200:
            # Strava error bodies for the token endpoint describe the failed
            # field (e.g. invalid refresh_token) without echoing secrets.
            raise StravaAuthError(
                f"Strava token request failed (HTTP {response.status_code}): "
                f"{_safe_error_body(response)}"
            )
        data = response.json()
        tokens = TokenSet(
            access_token=data["access_token"],
            refresh_token=data["refresh_token"],
            expires_at=int(data["expires_at"]),
        )
        # Persist before returning: the old refresh token is already dead.
        self._store.save(tokens)
        return tokens

    # ── API ───────────────────────────────────────────────────────────

    def get_athlete(self) -> dict:
        return self._api_get("/athlete").json()

    def iter_activity_pages(
        self, after: datetime, per_page: int = ACTIVITIES_PER_PAGE_MAX
    ) -> Iterator[list[dict]]:
        """Yield pages of SummaryActivity dicts started after `after`, oldest-first.

        Stops on the first short page. When rate-limit usage crosses
        RATE_LIMIT_STOP_FRACTION, the current page is still yielded and
        RateLimitApproaching is raised before the next fetch, so callers
        keep completed work and a later run resumes from its unadvanced
        watermark.
        """
        if after.tzinfo is None:
            raise ValueError("after must be timezone-aware (UTC) to become a Strava epoch")
        page_number = 1
        while True:
            response = self._api_get(
                "/athlete/activities",
                params={"after": int(after.timestamp()), "per_page": per_page, "page": page_number},
            )
            activities = response.json()
            if activities:
                yield activities
            if len(activities) < per_page:
                return  # final page: the sync is complete even at high usage
            triggered = [s for s in _rate_limit_statuses(response.headers) if s.approaching()]
            if triggered:
                raise RateLimitApproaching(
                    "Stopping before the Strava rate limit: "
                    + "; ".join(status.describe() for status in triggered)
                )
            page_number += 1

    def get_activity_detail(
        self, activity_id: int, include_all_efforts: bool = False
    ) -> dict | None:
        """DetailedActivity for one activity; None when it no longer exists.

        Needed for coordinate resolution: when the map-privacy setting
        strips start_latlng from every payload, the detail response still
        carries the encoded route in map.polyline. Segment ingestion (D24)
        passes include_all_efforts=True so the response carries every
        effort, not just the athlete's notable ones.
        """
        params = {"include_all_efforts": "true"} if include_all_efforts else None
        response = self._api_get(f"/activities/{activity_id}", params=params, none_on_404=True)
        return None if response is None else response.json()

    def get_activity_streams(
        self, activity_id: int, keys: tuple[str, ...] = STREAM_TYPES
    ) -> dict | None:
        """Streams for one activity, keyed by type; None when Strava has none.

        A 404 means the activity has no stream data (manual entries and
        some app-synced activities never get streams) — that is a
        terminal "unavailable", not an error.
        """
        response = self._api_get(
            f"/activities/{activity_id}/streams",
            params={"keys": ",".join(keys), "key_by_type": "true"},
            none_on_404=True,
        )
        return None if response is None else response.json()

    def rate_limit_approaching(self) -> list[RateLimitStatus]:
        """Statuses from the last response that crossed the stop fraction."""
        return [s for s in self.last_rate_limit_statuses if s.approaching()]

    def _api_get(
        self, path: str, params: dict | None = None, *, none_on_404: bool = False
    ) -> requests.Response | None:
        """GET an API path with auth, bounded transient retries, and 429 handling.

        401 forces one token refresh then one retry (the access token may
        have been revoked server-side even though it looks fresh locally);
        connection errors and 5xx retry with backoff; 429 and the remaining
        4xx never retry.
        """
        access_token = self.get_access_token()
        refreshed = False
        remaining_backoff = list(TRANSIENT_RETRY_BACKOFF_SECONDS)
        max_attempts = 1 + len(TRANSIENT_RETRY_BACKOFF_SECONDS)
        while True:
            try:
                response = self._session.get(
                    f"{API_BASE}{path}",
                    params=params,
                    headers={"Authorization": f"Bearer {access_token}"},
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )
            except (requests.ConnectionError, requests.Timeout) as exc:
                if not remaining_backoff:
                    raise StravaApiError(
                        f"Strava API unreachable after {max_attempts} attempts "
                        f"on GET {path}: {exc.__class__.__name__}"
                    ) from exc
                self._sleep(remaining_backoff.pop(0))
                continue

            if response.status_code >= 500:
                if not remaining_backoff:
                    raise StravaApiError(
                        f"Strava API failed after {max_attempts} attempts "
                        f"(HTTP {response.status_code}) on GET {path}: "
                        f"{_safe_error_body(response)}"
                    )
                self._sleep(remaining_backoff.pop(0))
                continue
            if response.status_code == 401:
                if refreshed:
                    raise StravaAuthError(
                        f"Strava rejected the access token even after a forced refresh "
                        f"(HTTP 401 on GET {path}). The authorization may have been "
                        "revoked or re-scoped: run `running-pipeline authorize`."
                    )
                refreshed = True
                access_token = self.refresh_access_token().access_token
                continue
            if response.status_code == 429:
                statuses = _rate_limit_statuses(response.headers)
                detail = "; ".join(s.describe() for s in statuses) or "no usage headers"
                raise RateLimitExceeded(
                    f"Strava rate limit exceeded (HTTP 429 on GET {path}): {detail}"
                )
            if response.status_code == 404 and none_on_404:
                self.last_rate_limit_statuses = _rate_limit_statuses(response.headers)
                return None
            if response.status_code != 200:
                raise StravaApiError(
                    f"Strava API request failed (HTTP {response.status_code}) "
                    f"on GET {path}: {_safe_error_body(response)}"
                )
            self.last_rate_limit_statuses = _rate_limit_statuses(response.headers)
            return response


def _rate_limit_statuses(headers) -> list[RateLimitStatus]:
    """Parse Strava usage headers ("<15min>,<daily>" pairs); read limits and
    overall limits constrain independently, so both are reported when present.
    Malformed or missing headers never fail a sync — they just disable the guard.
    """
    statuses = []
    for window, prefix in (("read", "X-ReadRateLimit"), ("overall", "X-RateLimit")):
        limits = headers.get(f"{prefix}-Limit")
        usage = headers.get(f"{prefix}-Usage")
        if not limits or not usage:
            continue
        try:
            short_limit, daily_limit = (int(v) for v in limits.split(","))
            short_usage, daily_usage = (int(v) for v in usage.split(","))
        except ValueError:
            continue
        statuses.append(RateLimitStatus(window, short_usage, short_limit, daily_usage, daily_limit))
    return statuses


def _safe_error_body(response: requests.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return "<non-JSON error body>"
    # Belt and braces: token-shaped keys never make it into the message.
    if isinstance(body, dict):
        body = {k: v for k, v in body.items() if "token" not in k.lower()}
    return json.dumps(body)
