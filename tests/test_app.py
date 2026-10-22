import base64
import json
import os
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

import app as m


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(m, "DATA_DIR", tmp_path)
    monkeypatch.setattr(m, "DB_PATH", tmp_path / "locations.sqlite3")
    monkeypatch.setattr(m, "SITES_PATH", tmp_path / "sites.json")
    monkeypatch.setattr(m, "USERNAME", "admin")
    monkeypatch.setattr(m, "PASSWORD", "p" * 40)
    monkeypatch.setattr(m, "INGEST_TOKEN", "t" * 48)
    monkeypatch.setattr(m, "TIMEZONE", "America/New_York")
    with TestClient(m.app) as c:
        yield c


BASIC = "Basic " + base64.b64encode(("admin:" + "p" * 40).encode()).decode()
BEARER = "Bearer " + "t" * 48
PAYLOAD = {"lat": 51.495065, "lon": -0.043945, "acc": 12, "alt": 519,
           "vel": 0, "batt": 85, "bs": 2, "tst": 1704067200, "bear": 180.5}


def post(c, payload, token=BEARER):
    return c.post("/api/location", json=payload, headers={"Authorization": token})


def test_security_ingest_and_duplicates(client):
    assert client.get("/").status_code == 401
    assert client.get("/api/locations").status_code == 401
    assert client.get("/api/location").status_code == 401
    assert client.get("/healthz").json() == {"ok": True}
    assert post(client, PAYLOAD, "Bearer bad").status_code == 401
    response = post(client, PAYLOAD)
    assert response.status_code == 200, response.text
    assert response.json()["saved"] is True
    assert post(client, PAYLOAD).json()["duplicate"] is True
    assert post(client, {**PAYLOAD, "tst": 1704067210, "device": "truck"}, BASIC).status_code == 200
    result = client.get("/api/locations", headers={"Authorization": BASIC}).json()
    assert len(result["locations"]) == 2
    assert result["locations"][0]["device"] == "truck"
    assert client.get("/", headers={"Authorization": BASIC}).status_code == 200
    assert client.get("/api/export.csv", headers={"Authorization": BASIC}).text.count("\n") == 3


def test_dashboard_address_search_is_enabled(client):
    response = client.get("/", headers={"Authorization": BASIC})
    assert response.status_code == 200
    assert 'id=\'address-search\'' in response.text
    assert 'id=\'address-results\'' in response.text
    assert "https://nominatim.openstreetmap.org" in response.headers["content-security-policy"]


def test_dashboard_date_range_and_geofence_editor_controls(client):
    response = client.get("/", headers={"Authorization": BASIC})
    assert response.status_code == 200
    assert "id='hours-range'" in response.text
    assert "id='range-start'" in response.text
    assert "id='range-end'" in response.text
    assert "id='cancel-edit'" in response.text
    assert "method: editingName ? 'PUT' : 'POST'" in response.text


def test_validation_and_optional_fields(client):
    for candidate in ({**PAYLOAD, "lat": 91}, {**PAYLOAD, "tst": 1704067200000},
                      {**PAYLOAD, "device": "../../evil"}, {**PAYLOAD, "acc": -9},
                      {**PAYLOAD, "batt": 120}, {**PAYLOAD, "bs": 99}):
        assert post(client, candidate).status_code == 422
    assert post(client, {k: v for k, v in PAYLOAD.items() if k not in ("vel", "alt", "bear")}).status_code == 200
    assert post(client, {**PAYLOAD, "tst": 1704067210, "cog": 12.2, "tid": "AB"}).status_code == 200
    locations = client.get("/api/locations", headers={"Authorization": BASIC}).json()["locations"]
    assert locations[0]["bear"] == 180.5   # 'bear' takes priority over 'cog' if both supplied
    assert locations[0]["device"] == "AB"


def test_time_report_out_of_order_and_csv(client):
    lat, lon = 43.6600, -70.2500
    m.SITES_PATH.write_text(json.dumps({"sites": [{"name": "Job A", "lat": lat, "lon": lon,
                                                        "radius_m": 180, "max_accuracy_m": 50}]}))
    # 2026-10-20 9:00-9:31 UTC (6 samples), first outside at minute 31.
    t = int(datetime(2026, 10, 20, 13, 0, tzinfo=timezone.utc).timestamp())
    for minute in (20, 0, 10, 29):
        assert post(client, {**PAYLOAD, "lat": lat, "lon": lon,
                             "tst": t + minute*60}).status_code == 200
    assert post(client, {**PAYLOAD, "lat": lat+0.01, "lon": lon,
                         "tst": t+31*60}).status_code == 200
    response = client.get('/api/hours?start_date=2026-10-20&end_date=2026-10-20',
                          headers={"Authorization": BASIC})
    assert response.status_code == 200, response.text
    report = response.json()
    assert len(report["visits"]) == 1
    assert report["totals"][0]["seconds"] == 31*60
    assert report["visits"][0]["end_reason"] == "outside_geofence"
    export = client.get('/api/hours.csv?start_date=2026-10-20&end_date=2026-10-20',
                        headers={"Authorization": BASIC})
    assert 'Job A' in export.text
    assert export.headers["content-type"].startswith("text/csv")


def test_gaps_do_not_inflate_time(client):
    m.SITES_PATH.write_text(json.dumps({"sites": [{"name": "Site", "lat": 43, "lon": -70,
                                                        "radius_m": 100}]}))
    t = 1792486800
    for minute in (0, 1, 50, 51):
        assert post(client, {**PAYLOAD, "lat": 43, "lon": -70,
                             "tst": t + minute * 60}).status_code == 200
    report = client.get('/api/hours?days=91', headers={"Authorization": BASIC})
    assert report.status_code == 200
    # Use direct function to sidestep default report dates for synthetic sample.
    points = m.get_rows(ascending=True)
    visits = m.calculate_visits(points, m.load_sites())
    assert len(visits) == 2
    assert sum(v["duration_seconds"] for v in visits) == 120


def test_rectangle_job_lifecycle_and_visit_bounds(client):
    headers = {"Authorization": BASIC}
    payload = {"name": "North Lot", "south": 43.0, "west": -70.0,
               "north": 43.01, "east": -69.99, "max_accuracy_m": 75}
    created = client.post("/api/sites", json=payload, headers=headers)
    assert created.status_code == 201, created.text
    assert created.json()["site"] == payload
    assert client.post("/api/sites", json=payload, headers=headers).status_code == 409

    sites = m.load_sites()
    assert len(sites) == 1
    points = [
        {"device": "phone", "tst": 100, "lat": 43.005, "lon": -69.995, "acc": 10},
        {"device": "phone", "tst": 160, "lat": 43.006, "lon": -69.996, "acc": 10},
        {"device": "phone", "tst": 220, "lat": 43.02, "lon": -69.995, "acc": 10},
    ]
    visits = m.calculate_visits(points, sites)
    assert len(visits) == 1
    assert visits[0]["site"] == "North Lot"
    assert visits[0]["duration_seconds"] == 120

    assert client.get("/api/sites", headers=headers).json()["sites"] == [payload]
    assert client.delete("/api/sites?name=North%20Lot", headers=headers).json() == {"ok": True}
    assert client.get("/api/sites", headers=headers).json()["sites"] == []
    assert client.delete("/api/sites?name=North%20Lot", headers=headers).status_code == 404


def test_update_site_geometry_and_name(client):
    headers = {"Authorization": BASIC}
    original = {"name": "Old Job", "south": 43.0, "west": -70.0,
                "north": 43.01, "east": -69.99}
    assert client.post("/api/sites", json=original, headers=headers).status_code == 201
    updated = {"name": "Renamed Job", "south": 43.1, "west": -70.1,
               "north": 43.2, "east": -70.0, "max_accuracy_m": 100}
    response = client.put("/api/sites?name=Old%20Job", json=updated, headers=headers)
    assert response.status_code == 200, response.text
    assert response.json()["site"] == updated
    assert client.get("/api/sites", headers=headers).json()["sites"] == [updated]
    assert client.put("/api/sites?name=Missing", json=updated, headers=headers).status_code == 404

    circle = {"name": "Circle Job", "lat": 43.5, "lon": -70.5, "radius_m": 250}
    assert client.put("/api/sites?name=Renamed%20Job", json=circle, headers=headers).status_code == 200
    assert client.get("/api/sites", headers=headers).json()["sites"] == [
        {**circle, "max_accuracy_m": 75.0}
    ]


def test_rectangle_site_validation(client):
    headers = {"Authorization": BASIC}
    for invalid in (
        {"name": "Partial", "south": 43, "west": -70},
        {"name": "Inverted", "south": 44, "west": -70, "north": 43, "east": -69},
    ):
        assert client.post("/api/sites", json=invalid, headers=headers).status_code == 422


def test_dashboard_uses_configured_sites(client):
    m.SITES_PATH.write_text(json.dumps({"sites": [{
        "name": "Saved Job", "south": 43.0, "west": -70.0,
        "north": 43.01, "east": -69.99,
    }]}))
    response = client.get("/", headers={"Authorization": BASIC})
    assert response.status_code == 200
    assert "Configured worksites</span></div>" in response.text
    assert ">1</strong><span>Configured worksites" in response.text
    assert "No jobs are loaded for this receiver" not in response.text


def test_large_payload_rejected(client):
    payload = {**PAYLOAD, "pad": "x" * 17000}
    assert post(client, payload).status_code == 413
