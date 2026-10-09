"""A scripted stand-in for the OpenAQ HTTP API, injected into the REAL OpenAQClient as its session.

So the real retry / classification / attempt-reporting code runs in tests; only the network is fake.
Behaviours per sensor: a list of measurement items (HTTP 200), an int (that HTTP status, always),
an exception (raised), or Script([...]) for a sequence of those, one per call (last one repeats)."""
import re
from urllib.parse import urlencode

OK_UNIT = "µg/m³"
BAD_JSON = object()


class FakeResponse:
    def __init__(self, status=200, body=None, headers=None, url="https://fake.test/", reason=""):
        self.status_code, self._body, self.headers, self.url, self.reason = status, body, headers or {}, url, reason

    def json(self):
        if self._body is BAD_JSON:
            raise ValueError("response is not JSON")
        return self._body


class Script:
    def __init__(self, steps):
        self.steps, self.i = list(steps), 0

    def next(self):
        step = self.steps[min(self.i, len(self.steps) - 1)]
        self.i += 1
        return step


class FakeSession:
    def __init__(self, locations, sensors=None):
        self.headers = {}
        self.locations = locations
        self.sensors = sensors or {}
        self.calls = []

    def get(self, url, params=None, timeout=None):
        params = dict(params or {})
        self.calls.append((url, params))
        full = url + ("?" + urlencode(params) if params else "")
        if url.endswith("/locations"):
            limit, page = int(params.get("limit", 1000)), int(params.get("page", 1))
            return FakeResponse(200, {"results": self.locations[(page - 1) * limit: page * limit]}, url=full)
        m = re.search(r"/sensors/(\d+)/measurements$", url)
        if not m:
            return FakeResponse(404, {}, url=full, reason="Not Found")
        behaviour = self.sensors.get(int(m.group(1)), [])
        if isinstance(behaviour, Script):
            behaviour = behaviour.next()
        if isinstance(behaviour, BaseException):
            raise behaviour
        if behaviour is BAD_JSON:
            return FakeResponse(200, BAD_JSON, url=full)
        if isinstance(behaviour, int):
            return FakeResponse(behaviour, {"message": "scripted"}, url=full, reason=f"scripted {behaviour}")
        limit, page = int(params.get("limit", 1000)), int(params.get("page", 1))
        return FakeResponse(200, {"results": behaviour[(page - 1) * limit: page * limit]}, url=full)

    def sensor_calls(self, sensor_id):
        return [c for c in self.calls if c[0].endswith(f"/sensors/{sensor_id}/measurements")]


def loc(location_id, sensor_ids, provider="CPCB", last="2026-10-04T12:00:00Z", tz="Asia/Kolkata",
        lat=28.6, lon=77.2):
    return {
        "id": location_id, "name": f"Location {location_id}", "locality": None, "timezone": tz,
        "country": {"id": 9, "code": "IN", "name": "India"}, "provider": {"id": 1, "name": provider},
        "isMobile": False, "isMonitor": True, "coordinates": {"latitude": lat, "longitude": lon},
        "datetimeFirst": {"utc": "2026-01-01T00:00:00Z"}, "datetimeLast": {"utc": last},
        "sensors": [{"id": sid, "name": "pm25 " + OK_UNIT,
                     "parameter": {"id": 2, "name": "pm25", "units": OK_UNIT, "displayName": "PM2.5"}}
                    for sid in sensor_ids]}


def item(start_iso, minutes=15, value=10.0):
    """One measurement as the API returns it. start_iso like '2026-10-02T03:00:00Z'."""
    from datetime import datetime, timedelta, timezone
    start = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
    end = start + timedelta(minutes=minutes)
    fmt = lambda d: d.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {"value": value, "flagInfo": {"hasFlags": False},
            "parameter": {"id": 2, "name": "pm25", "units": OK_UNIT, "displayName": None},
            "period": {"label": "raw", "interval": f"{minutes // 60:02d}:{minutes % 60:02d}:00",
                       "datetimeFrom": {"utc": fmt(start)}, "datetimeTo": {"utc": fmt(end)}},
            "coverage": {"percentComplete": 100.0}}
