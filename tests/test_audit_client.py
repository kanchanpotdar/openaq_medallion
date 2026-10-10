"""The HTTP client's retry behaviour and the attempt records it emits (no network, no real sleeping)."""
import requests
import pytest

from pipeline.openaq_client import FetchError, OpenAQClient
from tests.fake_openaq import BAD_JSON, FakeSession, Script, item, loc

# SYNTHETIC test credential, not a real secret: the audit tests inject it to prove it never reaches
# any log or audit table.
KEY = "SECRET-KEY-DO-NOT-LEAK-123"


def make(behaviour, **kw):
    session = FakeSession([loc(1, [7])], {7: behaviour})
    attempts, sleeps = [], []
    client = OpenAQClient(KEY, session=session, sleep=sleeps.append, on_attempt=attempts.append, **kw)
    return client, session, attempts, sleeps


def fetch(client):
    return client.get("sensors/7/measurements", {"limit": 1000}, ctx={"sensor_id": 7})


def test_success_is_one_attempt_with_row_count():
    client, _, attempts, sleeps = make([item("2026-10-02T03:00:00Z"), item("2026-10-02T03:15:00Z")])
    body, _ = fetch(client)
    assert len(body["results"]) == 2
    (a,) = attempts
    assert (a["attempt_no"], a["outcome"], a["http_status"], a["rows_returned"], a["is_final"]) == (1, "SUCCESS", 200, 2, True)
    assert a["backoff_seconds"] is None and sleeps == []


def test_http_500_is_retried_with_backoff_and_every_attempt_is_recorded():
    client, session, attempts, sleeps = make(500)
    with pytest.raises(FetchError) as e:
        fetch(client)
    assert (e.value.outcome, e.value.http_status, e.value.attempts) == ("HTTP_5XX", 500, 5)
    assert [a["attempt_no"] for a in attempts] == [1, 2, 3, 4, 5]
    assert {a["outcome"] for a in attempts} == {"HTTP_5XX"} and {a["http_status"] for a in attempts} == {500}
    assert [a["backoff_seconds"] for a in attempts] == [2, 4, 8, 16, None]      # no pointless sleep after the last try
    assert sleeps == [2, 4, 8, 16]
    assert [a["is_final"] for a in attempts] == [False, False, False, False, True]
    assert len(session.sensor_calls(7)) == 5


def test_a_transient_failure_then_success_records_both_attempts():
    client, _, attempts, sleeps = make(Script([500, 500, [item("2026-10-02T03:00:00Z")]]))
    body, _ = fetch(client)
    assert len(body["results"]) == 1
    assert [(a["attempt_no"], a["outcome"], a["is_final"]) for a in attempts] == [
        (1, "HTTP_5XX", False), (2, "HTTP_5XX", False), (3, "SUCCESS", True)]
    assert sleeps == [2, 4]


def test_rate_limiting_is_classified_and_honours_retry_after():
    from tests.fake_openaq import FakeResponse
    session = FakeSession([loc(1, [7])])
    seq = [FakeResponse(429, {}, headers={"Retry-After": "9"}), FakeResponse(200, {"results": []})]
    session.get = lambda url, params=None, timeout=None: seq.pop(0)
    attempts, sleeps = [], []
    OpenAQClient(KEY, session=session, sleep=sleeps.append, on_attempt=attempts.append).get("sensors/7/measurements")
    assert [a["outcome"] for a in attempts] == ["RATE_LIMITED", "SUCCESS"]
    assert attempts[0]["http_status"] == 429 and attempts[0]["backoff_seconds"] == 10    # Retry-After + 1
    assert sleeps == [10]


def test_timeouts_are_classified_separately_from_http_errors():
    client, _, attempts, _ = make(requests.Timeout("read timed out"))
    with pytest.raises(FetchError) as e:
        fetch(client)
    assert e.value.outcome == "TIMEOUT" and e.value.http_status is None
    assert {a["outcome"] for a in attempts} == {"TIMEOUT"} and attempts[0]["error_type"] == "Timeout"


def test_connection_errors_are_network_errors():
    client, _, attempts, _ = make(requests.ConnectionError("connection reset"))
    with pytest.raises(FetchError) as e:
        fetch(client)
    assert e.value.outcome == "NETWORK_ERROR" and len(attempts) == 5


def test_client_errors_other_than_429_are_not_retried():
    client, session, attempts, sleeps = make(404)
    with pytest.raises(FetchError) as e:
        fetch(client)
    assert (e.value.outcome, e.value.http_status, e.value.attempts) == ("HTTP_4XX", 404, 1)
    assert len(attempts) == 1 and attempts[0]["is_final"] and sleeps == []


def test_an_unparseable_body_is_retried_as_invalid_response():
    client, _, attempts, _ = make(Script([BAD_JSON, [item("2026-10-02T03:00:00Z")]]))
    fetch(client)
    assert [a["outcome"] for a in attempts] == ["INVALID_RESPONSE", "SUCCESS"]


def test_the_api_key_never_appears_in_urls_attempt_records_or_errors():
    client, session, attempts, _ = make(requests.ConnectionError(f"failed while sending header X-API-Key: {KEY}"))
    with pytest.raises(FetchError) as e:
        fetch(client)
    blob = repr(attempts) + str(e.value) + repr(session.calls)
    assert KEY not in repr(attempts) and KEY not in str(e.value)       # scrubbed by the client itself
    assert all(KEY not in a["url"] for a in attempts)                  # it travels only in a header
    assert session.headers["X-API-Key"] == KEY                         # ...which is where it belongs
    assert "***REDACTED***" in attempts[0]["error_message"]


def test_pagination_stops_on_a_short_page_and_reports_each_page():
    many = [item(f"2026-10-02T{h:02d}:00:00Z", 60, h) for h in range(5)]
    client, _, attempts, _ = make(many)
    pages = list(client.pages("sensors/7/measurements", {}, limit=2, ctx={"sensor_id": 7}))
    assert [len(r) for r, _ in pages] == [2, 2, 1]                       # a short last page ends the walk
    assert [a["ctx"]["page"] for a in attempts] == [1, 2, 3]
    assert [a["rows_returned"] for a in attempts] == [2, 2, 1]


def test_http_408_is_a_retried_timeout_not_a_final_client_error():
    """408 Request Timeout is the server giving up on a slow (deep-page) request: transient, so it is retried like
    any timeout. Found in the 90-day run, where it ended four sensors' fetches early."""
    client, _, attempts, sleeps = make(Script([408, 408, [item("2026-10-02T03:00:00Z")]]))
    body, _ = fetch(client)
    assert len(body["results"]) == 1
    assert [(a["attempt_no"], a["outcome"], a["http_status"], a["is_final"]) for a in attempts] == [
        (1, "TIMEOUT", 408, False), (2, "TIMEOUT", 408, False), (3, "SUCCESS", 200, True)]
    assert attempts[0]["error_type"] == "HTTP 408" and sleeps == [2, 4]


def test_a_408_that_never_clears_is_still_a_recorded_failure_after_all_retries():
    client, session, attempts, sleeps = make(408)
    with pytest.raises(FetchError) as e:
        fetch(client)
    assert (e.value.outcome, e.value.http_status, e.value.attempts) == ("TIMEOUT", 408, 5)
    assert [a["attempt_no"] for a in attempts] == [1, 2, 3, 4, 5] and attempts[-1]["is_final"]
    assert len(session.sensor_calls(7)) == 5 and sleeps == [2, 4, 8, 16]
