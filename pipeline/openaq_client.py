"""Thin OpenAQ v3 client: auth header, retries, rate-limit awareness, pagination, attempt reporting.

Every HTTP attempt is reported to an optional `on_attempt` callback (the audit trail). The API key
travels only in a header: it is never part of the URL that gets reported or stored.
"""
import logging
import time
from datetime import datetime, timezone

import requests

log = logging.getLogger(__name__)
BASE_URL = "https://api.openaq.org/v3"


class FetchError(RuntimeError):
    """A request that could not be completed. `outcome` is the classification of the LAST attempt."""

    def __init__(self, message, *, outcome, http_status=None, attempts=0):
        super().__init__(message)
        self.outcome = outcome
        self.http_status = http_status
        self.attempts = attempts


class OpenAQClient:
    def __init__(self, api_key: str, max_retries: int = 5, timeout: int = 60, *,
                 session=None, sleep=time.sleep, on_attempt=None):
        self.session = session or requests.Session()
        # Key travels in a header, so it never appears in the logged/stored URL.
        self.session.headers["X-API-Key"] = api_key
        self._api_key = api_key
        self.max_retries = max_retries
        self.timeout = timeout
        self._sleep = sleep
        self.on_attempt = on_attempt

    # ------------------------------------------------------------------ requests

    def get(self, path: str, params: dict | None = None, ctx: dict | None = None) -> tuple[dict, str]:
        """GET with retry. Returns (json, final_url); the url is our lineage record.
        Retries: 5xx, 429, timeouts, network errors, unparseable bodies. 4xx (other than 429) is not retried."""
        url = f"{BASE_URL}/{path.lstrip('/')}"
        shown_url = requests.Request("GET", url, params=params).prepare().url
        last = dict(outcome="NETWORK_ERROR", http_status=None, message="no attempt made")
        for attempt in range(1, self.max_retries + 1):
            started = datetime.now(timezone.utc)
            body, resp, floor = None, None, 0
            outcome, status, rows, err_type, err_msg = "SUCCESS", None, None, None, None
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
                status = resp.status_code
                if status == 429:
                    outcome, floor = "RATE_LIMITED", self._reset_seconds(resp, 30)
                elif status >= 500:
                    outcome = "HTTP_5XX"
                elif status >= 400:
                    outcome = "HTTP_4XX"
                else:
                    try:
                        body = resp.json()
                        rows = len(body.get("results", [])) if isinstance(body, dict) else None
                    except ValueError as exc:
                        outcome, err_type, err_msg = "INVALID_RESPONSE", type(exc).__name__, str(exc)
                if outcome in ("RATE_LIMITED", "HTTP_5XX", "HTTP_4XX"):
                    err_type, err_msg = f"HTTP {status}", getattr(resp, "reason", None) or f"HTTP {status}"
            except requests.Timeout as exc:
                outcome, err_type, err_msg = "TIMEOUT", type(exc).__name__, str(exc)
            except requests.RequestException as exc:
                outcome, err_type, err_msg = "NETWORK_ERROR", type(exc).__name__, str(exc)

            retryable = outcome not in ("SUCCESS", "HTTP_4XX")
            is_final = (not retryable) or attempt == self.max_retries
            backoff = None if is_final else max(floor, 2 ** attempt)
            self._emit(dict(endpoint=path, url=shown_url, ctx=ctx or {}, attempt_no=attempt,
                            started_at=started, finished_at=datetime.now(timezone.utc),
                            outcome=outcome, http_status=status, rows_returned=rows,
                            error_type=err_type, error_message=err_msg,
                            backoff_seconds=backoff, is_final=is_final))
            if outcome == "SUCCESS":
                self._respect_rate_limit(resp)
                return body, (getattr(resp, "url", None) or shown_url)
            last = dict(outcome=outcome, http_status=status, message=self._clean(err_msg))
            if not retryable:
                break
            if backoff is not None:
                log.warning("%s on %s; retry in %ss (attempt %s/%s)", outcome, path, backoff, attempt, self.max_retries)
                self._sleep(backoff)
        raise FetchError(f"giving up on {path}: {last['outcome']} (http {last['http_status']}) {last['message'] or ''}".strip(),
                         outcome=last["outcome"], http_status=last["http_status"], attempts=attempt)

    def pages(self, path: str, params: dict, limit: int = 1000, max_pages: int = 1000, ctx: dict | None = None):
        """Yield (results, url) per page until a short page signals the end.
        The API reports 'found' as '>N', so it cannot be used as a row count."""
        for page in range(1, max_pages + 1):
            body, url = self.get(path, {**params, "limit": limit, "page": page}, ctx={**(ctx or {}), "page": page})
            results = body.get("results", [])
            yield results, url
            if len(results) < limit:
                return
        raise RuntimeError(f"exceeded max_pages={max_pages} for {path}")

    # ------------------------------------------------------------------ helpers

    def _clean(self, text):
        """The client knows the key, so it scrubs it from every message it logs, stores or raises."""
        if text is None:
            return None
        text = str(text)
        return text.replace(self._api_key, "***REDACTED***") if self._api_key else text

    def _emit(self, record: dict) -> None:
        record["error_message"] = self._clean(record.get("error_message"))
        record["error_type"] = self._clean(record.get("error_type"))
        if self.on_attempt:
            self.on_attempt(record)         # an audit failure must be loud, never swallowed

    @staticmethod
    def _reset_seconds(resp, default: int) -> int:
        for h in ("Retry-After", "x-ratelimit-reset"):
            if str(resp.headers.get(h, "")).isdigit():
                return int(resp.headers[h]) + 1
        return default

    def _respect_rate_limit(self, resp) -> None:
        remaining = str(resp.headers.get("x-ratelimit-remaining", ""))
        if remaining.isdigit() and int(remaining) <= 1:
            wait = self._reset_seconds(resp, 5)
            log.info("rate limit nearly exhausted; sleeping %ss", wait)
            self._sleep(wait)
