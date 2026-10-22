from __future__ import annotations

import base64
import csv
import hashlib
import hmac
import io
import json
import math
import os
import re
import secrets
import sqlite3
from contextlib import asynccontextmanager, contextmanager
from datetime import date, datetime, timedelta, timezone
from html import escape
from pathlib import Path
from typing import Annotated, Literal
from urllib.parse import quote
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
DB_PATH = DATA_DIR / "locations.sqlite3"
SITES_PATH = DATA_DIR / "sites.json"
USERNAME = os.environ.get("APP_USERNAME", "")
PASSWORD = os.environ.get("APP_PASSWORD", "")
INGEST_TOKEN = os.environ.get("INGEST_TOKEN", "")
TIMEZONE = os.environ.get("APP_TZ", "America/New_York")
GAP_MINUTES = int(os.environ.get("MAX_GAP_MINUTES", "20"))
EXIT_GRACE_MINUTES = int(os.environ.get("EXIT_GRACE_MINUTES", "5"))
MIN_INSIDE_SAMPLES = int(os.environ.get("MIN_INSIDE_SAMPLES", "2"))
MAX_REQUEST_BYTES = 16 * 1024
MAX_REPORT_POINTS = 250_000
DEVICE_RE = re.compile(r"^[a-zA-Z0-9_.-]{1,64}$")
USERNAME_RE = re.compile(r"^[a-zA-Z0-9_.-]{3,64}$")
PASSWORD_ITERATIONS = 310_000


class GPSFix(BaseModel):
    model_config = ConfigDict(extra="allow", allow_inf_nan=False)
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    acc: float = Field(ge=0, le=100_000)
    alt: float | None = None
    vel: float | None = Field(default=None, ge=0)
    batt: int = Field(ge=0, le=100)
    bs: int = Field(ge=0, le=3)
    tst: int = Field(ge=946684800, le=4102444800)
    bear: float | None = Field(default=None, ge=0, le=360)
    device: str | None = None


class WorkSite(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    name: str = Field(min_length=1, max_length=80)
    lat: float | None = Field(default=None, ge=-90, le=90)
    lon: float | None = Field(default=None, ge=-180, le=180)
    radius_m: float | None = Field(default=None, ge=10, le=50_000)
    south: float | None = Field(default=None, ge=-90, le=90)
    west: float | None = Field(default=None, ge=-180, le=180)
    north: float | None = Field(default=None, ge=-90, le=90)
    east: float | None = Field(default=None, ge=-180, le=180)
    max_accuracy_m: float = Field(default=75, gt=0, le=10_000)

    @model_validator(mode="after")
    def validate_geometry(self):
        bounds = (self.south, self.west, self.north, self.east)
        circle = (self.lat, self.lon, self.radius_m)
        has_bounds = any(value is not None for value in bounds)
        has_circle = any(value is not None for value in circle)
        if has_bounds and (not all(value is not None for value in bounds) or has_circle):
            raise ValueError("Provide either all rectangle bounds or a circle center and radius")
        if has_bounds and (self.south >= self.north or self.west >= self.east):
            raise ValueError("Rectangle bounds must have south < north and west < east")
        if not has_bounds and not all(value is not None for value in circle):
            raise ValueError("Provide either rectangle bounds or a circle center and radius")
        return self


class UserCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    username: str = Field(min_length=3, max_length=64, pattern=r"^[a-zA-Z0-9_.-]+$")
    password: str = Field(min_length=12, max_length=256)
    role: Literal["admin", "user"] = "user"


class UserUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    role: Literal["admin", "user"] | None = None
    password: str | None = Field(default=None, min_length=12, max_length=256)
    active: bool | None = None


class ManualTimeEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")
    date: date
    site: str = Field(min_length=1, max_length=80)
    device: str = Field(default="manual", min_length=1, max_length=64)
    start_time: str = Field(pattern=r"^\d{2}:\d{2}$")
    end_time: str = Field(pattern=r"^\d{2}:\d{2}$")
    note: str = Field(default="", max_length=300)


class RuntimeSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")
    timezone: str = Field(min_length=1, max_length=64)
    max_gap_minutes: int = Field(ge=1, le=1440)
    exit_grace_minutes: int = Field(ge=0, le=240)
    min_inside_samples: int = Field(ge=2, le=100)


@contextmanager
def connection():
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout=30000")
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def init_database():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not SITES_PATH.exists():
        SITES_PATH.write_text('{"sites": []}\n', encoding="utf-8")
    with connection() as con:
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("""
            CREATE TABLE IF NOT EXISTS locations (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              device TEXT NOT NULL,
              tst INTEGER NOT NULL,
              received_at TEXT NOT NULL,
              lat REAL NOT NULL,
              lon REAL NOT NULL,
              acc REAL NOT NULL,
              alt REAL,
              vel REAL,
              batt INTEGER NOT NULL,
              bs INTEGER NOT NULL,
              bear REAL,
              raw_json TEXT NOT NULL,
              UNIQUE (device, tst, lat, lon)
            )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_locations_tst ON locations(tst)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_locations_device_tst ON locations(device,tst)")


def check_config():
    if not USERNAME or not PASSWORD or not INGEST_TOKEN:
        raise RuntimeError("Set APP_USERNAME, APP_PASSWORD and INGEST_TOKEN in .env")
    if PASSWORD.lower().startswith(("change", "password")) or len(PASSWORD) < 20:
        raise RuntimeError("APP_PASSWORD must be a strong password (>=20 chars)")
    if INGEST_TOKEN.lower().startswith("change") or len(INGEST_TOKEN) < 32:
        raise RuntimeError("INGEST_TOKEN must be a strong token (>=32 chars)")
    if GAP_MINUTES < 1 or EXIT_GRACE_MINUTES < 0 or MIN_INSIDE_SAMPLES < 2:
        raise RuntimeError("Invalid geofence timing settings")
    try:
        ZoneInfo(TIMEZONE)
    except ZoneInfoNotFoundError as exc:
        raise RuntimeError(f"Unsupported APP_TZ: {TIMEZONE}") from exc


@asynccontextmanager
async def lifespan(_: FastAPI):
    check_config()
    init_database()
    yield


app = FastAPI(title="TimeTracker", docs_url=None, redoc_url=None,
              openapi_url=None, lifespan=lifespan)


@app.middleware("http")
async def private_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["Cache-Control"] = "no-store"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response


def _basic_ok(value: str) -> bool:
    if not value.lower().startswith("basic "):
        return False
    try:
        raw = base64.b64decode(value[6:].strip(), validate=True).decode("utf-8")
        username, password = raw.split(":", 1)
    except (ValueError, UnicodeDecodeError):
        return False
    return hmac.compare_digest(username, USERNAME) & hmac.compare_digest(password, PASSWORD) == 1


def require_read(request: Request):
    if not _basic_ok(request.headers.get("authorization", "")):
        raise HTTPException(status_code=401, detail="Authentication required",
                            headers={"WWW-Authenticate": 'Basic realm="GPS Receiver"'})


def require_ingest(request: Request):
    header = request.headers.get("authorization", "")
    token_ok = header.startswith("Bearer ") and hmac.compare_digest(header[7:], INGEST_TOKEN)
    if not (token_ok or _basic_ok(header)):
        raise HTTPException(status_code=401, detail="Invalid GPS credentials",
                            headers={"WWW-Authenticate": 'Basic realm="GPS Receiver"'})


def resolve_device(payload: dict) -> str:
    value = payload.get("device") or payload.get("device_id") or payload.get("tid") or "phone"
    if not isinstance(value, str) or not DEVICE_RE.fullmatch(value):
        raise HTTPException(status_code=422, detail="device must contain only letters, numbers, _, . or -")
    return value


def save_fix(fix: GPSFix, device: str, original: dict) -> tuple[int, bool]:
    with connection() as con:
        cursor = con.execute("""
            INSERT OR IGNORE INTO locations
              (device,tst,received_at,lat,lon,acc,alt,vel,batt,bs,bear,raw_json)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (device, fix.tst, datetime.now(timezone.utc).isoformat(),
              fix.lat, fix.lon, fix.acc, fix.alt, fix.vel, fix.batt,
              fix.bs, fix.bear, json.dumps(original, separators=(",", ":"))))
        inserted = cursor.rowcount == 1
        if inserted:
            return cursor.lastrowid, True
        row = con.execute("SELECT id FROM locations WHERE device=? AND tst=? AND lat=? AND lon=?",
                          (device, fix.tst, fix.lat, fix.lon)).fetchone()
        return int(row["id"]), False


@app.get("/healthz", include_in_schema=False)
def healthz():
    try:
        with connection() as con:
            con.execute("SELECT 1 FROM locations LIMIT 1").fetchone()
    except sqlite3.Error as exc:
        raise HTTPException(status_code=503, detail="GPS database unavailable") from exc
    return {"ok": True}


@app.get("/api/location", dependencies=[Depends(require_ingest)])
def location_ready():
    return {"ok": True, "message": "Send a JSON POST to this URL"}


@app.post("/api/location", dependencies=[Depends(require_ingest)])
async def ingest(request: Request):
    if "application/json" not in request.headers.get("content-type", "").lower():
        raise HTTPException(status_code=415, detail="Content-Type must be application/json")
    data = bytearray()
    async for chunk in request.stream():
        data.extend(chunk)
        if len(data) > MAX_REQUEST_BYTES:
            raise HTTPException(status_code=413, detail="GPS payload too large")
    try:
        original = json.loads(data)
    except (ValueError, UnicodeDecodeError):
        raise HTTPException(status_code=400, detail="Malformed JSON") from None
    if not isinstance(original, dict):
        raise HTTPException(status_code=422, detail="Expected one GPS JSON object")
    # Some GPS templates call the bearing cog; keep both compatible.
    normalized = dict(original)
    if "bear" not in normalized and "cog" in normalized:
        normalized["bear"] = normalized["cog"]
    try:
        fix = GPSFix.model_validate(normalized)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors(include_url=False)) from None
    dev = resolve_device(original)
    identifier, added = save_fix(fix, dev, original)
    return {"ok": True, "saved": added, "duplicate": not added,
            "id": identifier, "device": dev}


def get_rows(start: int | None = None, end: int | None = None,
             device: str | None = None, limit: int = 500,
             ascending: bool = False) -> list[dict]:
    clauses, args = [], []
    if start is not None:
        clauses.append("tst >= ?")
        args.append(start)
    if end is not None:
        clauses.append("tst < ?")
        args.append(end)
    if device:
        clauses.append("device = ?")
        args.append(device)
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    direction = "ASC" if ascending else "DESC"
    sql = ("SELECT id, device, tst, received_at, lat, lon, acc, alt, vel, "
           "batt, bs, bear FROM locations" + where +
           f" ORDER BY tst {direction}, id {direction} LIMIT ?")
    with connection() as con:
        return [dict(row) for row in con.execute(sql, (*args, limit)).fetchall()]


@app.get("/api/locations", dependencies=[Depends(require_read)])
def history(start: int | None = Query(default=None, ge=0),
            end: int | None = Query(default=None, ge=0),
            device: str | None = None,
            limit: int = Query(default=250, ge=1, le=5000)):
    return {"locations": get_rows(start, end, device, limit)}


CSV_FIELDS = ["id", "device", "tst", "received_at", "lat", "lon", "acc", "alt", "vel", "batt", "bs", "bear"]


def as_csv(rows, fields):
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(fields)
    yield buffer.getvalue()
    buffer.seek(0)
    buffer.truncate(0)
    for row in rows:
        # Prefix spreadsheet-formula-looking string values to avoid CSV formula injection.
        sanitized = [("'" + str(row.get(k)) if isinstance(row.get(k), str) and
                      row.get(k, "").startswith(("=", "+", "-", "@"))
                      else row.get(k, "")) for k in fields]
        writer.writerow(sanitized)
        yield buffer.getvalue()
        buffer.seek(0)
        buffer.truncate(0)


@app.get("/api/export.csv", dependencies=[Depends(require_read)])
def export_csv(start: int | None = Query(default=None, ge=0),
               end: int | None = Query(default=None, ge=0),
               device: str | None = None):
    # Streaming and bounded memory even for a large history.
    def row_iterator():
        clauses, args = [], []
        if start is not None:
            clauses.append("tst >= ?")
            args.append(start)
        if end is not None:
            clauses.append("tst < ?")
            args.append(end)
        if device:
            clauses.append("device = ?")
            args.append(device)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with connection() as con:
            for row in con.execute("SELECT " + ",".join(CSV_FIELDS) +
                                   " FROM locations" + where + " ORDER BY tst ASC,id ASC", args):
                yield dict(row)
    return StreamingResponse(as_csv(row_iterator(), CSV_FIELDS), media_type="text/csv",
                             headers={"Content-Disposition": 'attachment; filename="gps-locations.csv"',
                                      "Cache-Control": "no-store"})


def load_sites() -> list[WorkSite]:
    try:
        obj = json.loads(SITES_PATH.read_text(encoding="utf-8"))
        if not isinstance(obj, dict) or not isinstance(obj.get("sites"), list):
            raise ValueError("sites.json requires a sites array")
        sites = [WorkSite.model_validate(s) for s in obj["sites"]]
        if len({site.name for site in sites}) != len(sites):
            raise ValueError("Worksite names must be unique")
        return sites
    except (OSError, ValueError, ValidationError) as exc:
        raise HTTPException(status_code=500, detail=f"Invalid /data/sites.json: {exc}") from exc


def save_sites(sites: list[WorkSite]):
    SITES_PATH.parent.mkdir(parents=True, exist_ok=True)
    temporary = SITES_PATH.with_suffix(".json.tmp")
    temporary.write_text(json.dumps({"sites": [site.model_dump(exclude_none=True)
                                                  for site in sites]}, indent=2) + "\n",
                          encoding="utf-8")
    temporary.replace(SITES_PATH)


@app.get("/api/sites", dependencies=[Depends(require_read)])
def list_sites():
    return {"sites": [site.model_dump(exclude_none=True) for site in load_sites()]}


@app.post("/api/sites", dependencies=[Depends(require_read)], status_code=201)
def create_site(site: WorkSite):
    sites = load_sites()
    if any(existing.name == site.name for existing in sites):
        raise HTTPException(status_code=409, detail="A job with that name already exists")
    sites.append(site)
    save_sites(sites)
    return {"site": site.model_dump(exclude_none=True)}


@app.put("/api/sites", dependencies=[Depends(require_read)])
def update_site(name: str = Query(min_length=1, max_length=80), site: WorkSite = ...):
    sites = load_sites()
    index = next((i for i, existing in enumerate(sites) if existing.name == name), None)
    if index is None:
        raise HTTPException(status_code=404, detail="Job not found")
    if any(existing.name == site.name and existing.name != name for existing in sites):
        raise HTTPException(status_code=409, detail="A job with that name already exists")
    sites[index] = site
    save_sites(sites)
    return {"site": site.model_dump(exclude_none=True)}


@app.delete("/api/sites", dependencies=[Depends(require_read)])
def delete_site(name: str = Query(min_length=1, max_length=80)):
    sites = load_sites()
    remaining = [site for site in sites if site.name != name]
    if len(remaining) == len(sites):
        raise HTTPException(status_code=404, detail="Job not found")
    save_sites(remaining)
    return {"ok": True}


def distance_m(lat_a: float, lon_a: float, lat_b: float, lon_b: float) -> float:
    r = 6_371_000.0
    phi1, phi2 = math.radians(lat_a), math.radians(lat_b)
    dp = phi2 - phi1
    dl = math.radians(lon_b - lon_a)
    a = math.sin(dp / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dl / 2)**2
    return 2 * r * math.asin(min(1, math.sqrt(a)))


def calculate_visits(points: list[dict], sites: list[WorkSite]):
    visits = []
    devices = {}
    for point in points:
        devices.setdefault(point["device"], []).append(point)
    max_gap = GAP_MINUTES * 60
    exit_grace = EXIT_GRACE_MINUTES * 60
    for site in sites:
        for device, fixes in devices.items():
            start = last_inside = None
            count = 0

            def finish(end: int, reason: str):
                if start is not None and count >= MIN_INSIDE_SAMPLES and end > start:
                    visits.append({"site": site.name, "device": device,
                                   "start": start, "end": end, "duration_seconds": end-start,
                                   "end_reason": reason, "inside_samples": count})

            for p in fixes:
                if p["acc"] > site.max_accuracy_m:
                    continue
                t = p["tst"]
                if site.south is not None:
                    inside = (site.south <= p["lat"] <= site.north and
                              site.west <= p["lon"] <= site.east)
                else:
                    inside = distance_m(p["lat"], p["lon"], site.lat, site.lon) <= site.radius_m
                if start is not None and t - last_inside > max_gap:
                    finish(last_inside, "tracking_gap")
                    start = last_inside = None
                    count = 0
                if inside:
                    if start is None:
                        start = t
                        count = 0
                    # Count distinct timestamps, not duplicate fixes at one second.
                    if last_inside != t:
                        count += 1
                    last_inside = t
                elif start is not None:
                    finish(min(t, last_inside + exit_grace), "outside_geofence")
                    start = last_inside = None
                    count = 0
            if start is not None:
                finish(last_inside, "last_known_inside")
    return sorted(visits, key=lambda r: (r["start"], r["site"], r["device"]))


def reports_for_days(first: date, last: date):
    if first > last or (last-first).days > 90:
        raise HTTPException(status_code=400, detail="Use a date range of at most 91 days")
    tz = ZoneInfo(TIMEZONE)
    low = int(datetime.combine(first, datetime.min.time(), tz).timestamp())
    high = int(datetime.combine(last + timedelta(days=1), datetime.min.time(), tz).timestamp())
    points = get_rows(low, high, limit=MAX_REPORT_POINTS + 1, ascending=True)
    if len(points) > MAX_REPORT_POINTS:
        raise HTTPException(status_code=400, detail="Too many GPS points; use a shorter date range")
    sites = load_sites()
    visits = calculate_visits(points, sites)
    daily = {}
    segments = []
    for visit in visits:
        t = visit["start"]
        while t < visit["end"]:
            day = datetime.fromtimestamp(t, tz).date()
            next_midnight = int(datetime.combine(day+timedelta(days=1), datetime.min.time(), tz).timestamp())
            stop = min(visit["end"], next_midnight)
            key = (day.isoformat(), visit["site"], visit["device"])
            daily[key] = daily.get(key, 0) + stop - t
            segments.append({**visit, "date": day.isoformat(), "start": t, "end": stop,
                             "duration_seconds": stop - t,
                             "start_local": datetime.fromtimestamp(t, tz).isoformat(),
                             "end_local": datetime.fromtimestamp(stop, tz).isoformat()})
            t = stop
    totals = [{"date": d, "site": s, "device": dev, "seconds": seconds,
               "hours": round(seconds / 3600, 3)}
              for (d, s, dev), seconds in sorted(daily.items())]
    return {"timezone": TIMEZONE, "from": first.isoformat(), "through": last.isoformat(),
            "method": "Estimated from GPS; requires at least two accurate inside fixes. "
                      "Gaps and exits may undercount or overcount actual attendance.",
            "sites": [site.model_dump() for site in sites],
            "totals": totals, "visits": segments, "points_examined": len(points)}


def report_dates(start_date: date | None, end_date: date | None, days: int):
    today = datetime.now(ZoneInfo(TIMEZONE)).date()
    if start_date is None and end_date is None:
        return today - timedelta(days=days - 1), today
    return start_date or end_date or today, end_date or start_date or today


@app.get("/api/hours", dependencies=[Depends(require_read)])
def hours(start_date: date | None = None, end_date: date | None = None,
          days: int = Query(default=7, ge=1, le=91)):
    first, last = report_dates(start_date, end_date, days)
    return reports_for_days(first, last)


@app.get("/api/hours.csv", dependencies=[Depends(require_read)])
def hours_csv(start_date: date | None = None, end_date: date | None = None,
              days: int = Query(default=7, ge=1, le=91)):
    first, last = report_dates(start_date, end_date, days)
    report = reports_for_days(first, last)
    fields = ["date", "site", "device", "hours", "seconds"]
    return StreamingResponse(as_csv(report["totals"], fields), media_type="text/csv",
                             headers={"Content-Disposition": 'attachment; filename="gps-work-hours.csv"',
                                      "Cache-Control": "no-store"})


def local_time(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, ZoneInfo(TIMEZONE)).strftime("%Y-%m-%d %I:%M:%S %p")


@app.get("/", response_class=HTMLResponse, dependencies=[Depends(require_read)])
def dashboard():
    recent = get_rows(limit=100)
    today = datetime.now(ZoneInfo(TIMEZONE)).date()
    report = reports_for_days(today - timedelta(days=6), today)
    with connection() as con:
        count = con.execute("SELECT COUNT(*) FROM locations").fetchone()[0]
        last = con.execute("SELECT MAX(received_at) FROM locations").fetchone()[0]
    summed = sum(v["seconds"] for v in report["totals"])
    row_html = "".join(
        "<tr>" +
        f"<td>{escape(local_time(p['tst']))}</td>" +
        f"<td>{escape(p['device'])}</td>" +
        f"<td><a target='_blank' rel='noopener noreferrer' "
        f"href='https://www.openstreetmap.org/?mlat={p['lat']}&amp;mlon={p['lon']}#map=17/{p['lat']}/{p['lon']}'>"
        f"{p['lat']:.6f}, {p['lon']:.6f}</a></td>" +
        f"<td>{p['acc']:.0f} m</td><td>{p['batt']}%</td></tr>"
        for p in recent)
    total_html = "".join(
        f"<tr><td>{escape(t['date'])}</td><td>{escape(t['site'])}</td>"
        f"<td>{escape(t['device'])}</td><td>{t['hours']:.2f}</td></tr>"
        for t in reversed(report["totals"]))
    notice = ("<p class='hint'>No jobs are loaded for this receiver. Create one above, or check that "
              "the running container is mounted to the data folder containing your saved jobs.</p>"
              if not report["sites"] else "")
    report_start, report_end = report["from"], report["through"]
    return HTMLResponse("""<!doctype html><html lang='en'><head><meta charset='utf-8'>
<meta name='viewport' content='width=device-width,initial-scale=1'>
<meta name='referrer' content='strict-origin'><title>JMTimeTracker GPS Receiver</title>
<link rel='stylesheet' href='https://unpkg.com/leaflet@1.9.4/dist/leaflet.css'>
<style>
:root{color-scheme:dark}body{margin:0;background:#0c1220;color:#e9eef8;font:15px system-ui,Segoe UI,sans-serif}
main{max-width:1200px;margin:auto;padding:30px 22px}h1{font-size:29px;margin:0 0 7px}h2{font-size:19px}
.hint{color:#aab7cf}.cards{display:flex;gap:15px;flex-wrap:wrap;margin:24px 0}
.card{background:#172238;border:1px solid #2c3b55;border-radius:12px;padding:17px 22px;min-width:170px}
.card strong{font-size:25px;display:block}.card span{color:#a8b9d4}
section{background:#141f32;border:1px solid #2c3b55;border-radius:12px;padding:20px;margin-bottom:20px}
.scroll{overflow:auto}table{border-collapse:collapse;width:100%;min-width:600px}
th,td{text-align:left;padding:10px;border-bottom:1px solid #2b3950;white-space:nowrap}
th{color:#b4c1d7;font-size:13px}a{color:#76c8ff}nav a{display:inline-block;margin:10px 14px 0 0}
small{color:#a8b9d4}code{color:#96e6cd}
.map-layout{display:grid;grid-template-columns:minmax(0,1.7fr) minmax(260px,1fr);gap:20px}
#map{height:440px;min-height:300px;background:#d8dedb;border:1px solid #34435a;border-radius:5px;z-index:0}
.map-panel{min-width:0}.map-actions{display:flex;gap:9px;flex-wrap:wrap;margin:0 0 12px}
button,input{font:inherit;color:inherit;background:#0f1929;border:1px solid #43536d;border-radius:4px;padding:10px 12px}
button{cursor:pointer;background:#b7edda;color:#14231f;border-color:#b7edda;font-weight:650}
button:hover{background:#d1f6e8}button.secondary{background:#24334a;color:#e9eef8;border-color:#43536d}
button.danger{background:transparent;color:#ffaaa4;border-color:#704747;padding:6px 9px}
button:disabled{opacity:.5;cursor:not-allowed}input{width:100%;box-sizing:border-box}
label{display:block;color:#b4c1d7;font-size:13px;margin:13px 0 5px}
.bounds{color:#b4c1d7;font:12px ui-monospace,monospace;min-height:35px;overflow-wrap:anywhere}
.address-search{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:8px;margin:0 0 12px}
.address-search input{min-width:0}.search-results{list-style:none;padding:0;margin:0 0 12px}
.search-results li+li{margin-top:5px}.search-results button{width:100%;text-align:left;background:#24334a;color:#e9eef8;border-color:#43536d;font-weight:400}
.search-results button:hover{background:#30425d}.map-note{font-size:12px;color:#a8b9d4;margin:4px 0 12px}
.sr-only{position:absolute;width:1px;height:1px;padding:0;margin:-1px;overflow:hidden;clip:rect(0,0,0,0);white-space:nowrap;border:0}
.job-list{list-style:none;padding:0;margin:15px 0 0}.job-list li{display:flex;align-items:center;justify-content:space-between;gap:10px;padding:10px 0;border-bottom:1px solid #2b3950}
.job-actions{display:flex;gap:6px;flex-shrink:0}.job-actions button{padding:6px 9px}
.date-range{display:flex;align-items:end;gap:10px;flex-wrap:wrap;margin:0 0 12px}
.date-field{min-width:160px}.date-field label{margin-top:0}.report-status{color:#ffaaa4;min-height:20px}
.job-name{font-weight:650;overflow-wrap:anywhere}.job-shape{display:block;color:#a8b9d4;font-size:12px;margin-top:3px}
.status{min-height:22px;color:#a8b9d4;margin:10px 0 0}.status.error{color:#ffaaa4}
@media(max-width:760px){.map-layout{grid-template-columns:1fr}#map{height:55vh;min-height:320px}main{padding:22px 14px}}
</style></head><body><main><h1>Colota GPS Receiver</h1>
<p class='hint'>Private GPS collection and estimated job-site time tracking.</p>
<div class='cards'><div class='card'><strong>""" + str(count) + """</strong><span>Stored GPS points</span></div>
<div class='card'><strong>""" + str(len(report["sites"])) + """</strong><span>Configured worksites</span></div>
<div class='card'><strong id='hours-total'>""" + f"{summed / 3600:.1f}" + """ h</strong><span>Estimated on-site hours · selected dates</span></div></div>
<section><h2>New job site</h2><p class='hint'>Drag across the map to select the job boundary.</p>
<form id='address-search' class='address-search' role='search'><label class='sr-only' for='address-query'>Street address or place</label>
<input id='address-query' type='search' maxlength='200' autocomplete='street-address' placeholder='Search a street address or place' required>
<button id='address-search-button' type='submit'>Search</button></form>
<p class='map-note'>Address searches are sent to OpenStreetMap.</p><ul id='address-results' class='search-results' aria-label='Address search results'></ul>
<div class='map-layout'><div><div class='map-actions'><button id='draw-toggle' type='button' aria-pressed='false'>Select map area</button>
<button id='clear-selection' class='secondary' type='button'>Clear selection</button></div><div id='map' role='application' aria-label='Map for selecting a job boundary'></div></div>
<div class='map-panel'><form id='job-form'><label for='job-name'>Job name</label><input id='job-name' name='name' maxlength='80' required autocomplete='off' placeholder='e.g. Riverside renovation'>
<label for='geometry'>Geofence shape</label><select id='geometry' name='geometry'><option value='rectangle'>Rectangle</option><option value='circle'>Circle</option></select>
<div id='rectangle-fields'><label>Selected bounds (south, west to north, east)</label><div id='bounds' class='bounds'>No area selected</div></div>
<div id='circle-fields' hidden><label for='radius'>Radius (meters)</label><input id='radius' type='number' min='10' max='50000' step='1' value='150'>
<label for='center-lat'>Center latitude</label><input id='center-lat' type='number' min='-90' max='90' step='any'>
<label for='center-lon'>Center longitude</label><input id='center-lon' type='number' min='-180' max='180' step='any'><p class='map-note'>Click the map to set the circle center.</p></div>
<label for='accuracy'>Maximum GPS accuracy (meters)</label><input id='accuracy' name='max_accuracy_m' type='number' min='1' max='10000' step='1' value='75' required>
<div class='map-actions'><button id='save-job' type='submit' disabled>Save as new job</button><button id='cancel-edit' class='secondary' type='button' hidden>Cancel edit</button></div></form><p id='map-status' class='status' role='status' aria-live='polite'></p>
<h3>Saved jobs</h3><ul id='job-list' class='job-list'></ul></div></div></section>
<nav><a href='/api/locations'>Recent locations JSON</a><a href='/api/export.csv'>Export all GPS CSV</a>
<a id='hours-json' href='/api/hours?days=7'>Work hours JSON</a><a id='hours-csv' href='/api/hours.csv?days=7'>Work hours CSV</a></nav>
<section><h2 id='hours-heading'>Worksite hours — """ + escape(report_start) + """ to """ + escape(report_end) + """</h2>
<form id='hours-range' class='date-range'><div class='date-field'><label for='range-start'>From</label><input id='range-start' type='date' value='""" + escape(report_start) + """' required></div>
<div class='date-field'><label for='range-end'>Through</label><input id='range-end' type='date' value='""" + escape(report_end) + """' required></div>
<button type='submit'>Apply dates</button></form><p id='hours-status' class='report-status' role='status' aria-live='polite'></p>""" + notice + """
<div class='scroll'><table><thead><tr><th>Date</th><th>Site</th><th>Device</th><th>Hours (estimated)</th></tr></thead>
<tbody id='hours-rows'>""" + (total_html or "<tr><td colspan='4'>No qualifying geofence visits yet.</td></tr>") + """</tbody></table></div>
<p><small>Hours are inferred from GPS samples and are not a verified payroll timesheet.</small></p></section>
<section><h2>Latest 100 GPS points</h2><p class='hint'>Map links open only when clicked.</p>
<div class='scroll'><table><thead><tr><th>Location time</th><th>Device</th><th>Coordinates</th><th>Accuracy</th><th>Battery</th></tr></thead>
<tbody>""" + (row_html or "<tr><td colspan='5'>No GPS data received yet.</td></tr>") + """</tbody></table></div></section>
<p><small>Last receiver upload: """ + escape(last or "never") + """ · Local timezone: """ + escape(TIMEZONE) + """</small></p>
</main><script src='https://unpkg.com/leaflet@1.9.4/dist/leaflet.js'></script><script>
const map = L.map('map').setView([20, 0], 2);
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19, attribution: '&copy; OpenStreetMap contributors'
}).addTo(map);
const jobLayers = L.layerGroup().addTo(map);
let selecting = false;
let start = null;
let selection = null;
let editingName = null;
let selectionCenter = null;
const boundsLabel = document.getElementById('bounds');
const saveButton = document.getElementById('save-job');
const drawButton = document.getElementById('draw-toggle');
const status = document.getElementById('map-status');
const geometrySelect = document.getElementById('geometry');
let lastAddressSearch = 0;
function setStatus(message, error = false) {
    status.textContent = message;
    status.classList.toggle('error', error);
}
function showBounds(bounds) {
    const south = bounds.getSouth();
    const west = bounds.getWest();
    const north = bounds.getNorth();
    const east = bounds.getEast();
    boundsLabel.textContent = [south, west, north, east].map(value => value.toFixed(6)).join(', ');
    saveButton.disabled = north - south < 0.00001 || east - west < 0.00001;
}
function updateCircle(lat, lon) {
    const radius = Number(document.getElementById('radius').value);
    selectionCenter = {lat, lon};
    document.getElementById('center-lat').value = lat.toFixed(6);
    document.getElementById('center-lon').value = lon.toFixed(6);
    if (selection) map.removeLayer(selection);
    selection = L.circle([lat, lon], {radius, color: '#e46d43', weight: 2, fillOpacity: 0.16}).addTo(map);
    boundsLabel.textContent = `${lat.toFixed(6)}, ${lon.toFixed(6)} · ${radius} m radius`;
    saveButton.disabled = false;
}
function setEditing(job) {
    editingName = job.name;
    selectionCenter = null;
    document.getElementById('job-name').value = job.name;
    document.getElementById('accuracy').value = job.max_accuracy_m ?? 75;
    geometrySelect.value = job.south !== undefined ? 'rectangle' : 'circle';
    document.getElementById('rectangle-fields').hidden = geometrySelect.value !== 'rectangle';
    document.getElementById('circle-fields').hidden = geometrySelect.value !== 'circle';
    drawButton.hidden = geometrySelect.value === 'circle';
    document.getElementById('cancel-edit').hidden = false;
    saveButton.textContent = 'Save changes';
    if (geometrySelect.value === 'rectangle') {
        if (selection) map.removeLayer(selection);
        selection = L.rectangle([[job.south, job.west], [job.north, job.east]],
            {color: '#e46d43', weight: 2, fillOpacity: 0.16}).addTo(map);
        showBounds(selection.getBounds());
        map.fitBounds(selection.getBounds(), {padding: [24, 24], maxZoom: 17});
    } else {
        document.getElementById('radius').value = job.radius_m;
        updateCircle(job.lat, job.lon);
        map.setView([job.lat, job.lon], Math.max(map.getZoom(), 16));
    }
    setStatus(`Editing ${job.name}. Click the boundary on the map or adjust its values.`);
    document.getElementById('job-name').focus();
}
geometrySelect.addEventListener('change', () => {
    const isCircle = geometrySelect.value === 'circle';
    document.getElementById('rectangle-fields').hidden = isCircle;
    document.getElementById('circle-fields').hidden = !isCircle;
    drawButton.hidden = isCircle;
    if (isCircle && selection) {
        const center = selectionCenter || selection.getBounds().getCenter();
        updateCircle(center.lat, center.lng);
    } else if (!isCircle && selection && selection.getBounds) {
        const bounds = selection.getBounds();
        if (selectionCenter) {
            map.removeLayer(selection);
            selection = L.rectangle(bounds, {color: '#e46d43', weight: 2, fillOpacity: 0.16}).addTo(map);
        }
        selectionCenter = null;
        showBounds(bounds);
    }
});
document.getElementById('radius').addEventListener('input', () => {
    if (geometrySelect.value === 'circle' && selectionCenter) updateCircle(selectionCenter.lat, selectionCenter.lon);
});
for (const fieldId of ['center-lat', 'center-lon']) {
    document.getElementById(fieldId).addEventListener('change', () => {
        const lat = Number(document.getElementById('center-lat').value);
        const lon = Number(document.getElementById('center-lon').value);
        if (Number.isFinite(lat) && Number.isFinite(lon) && Math.abs(lat) <= 90 && Math.abs(lon) <= 180) {
            updateCircle(lat, lon);
            map.setView([lat, lon], Math.max(map.getZoom(), 16));
        }
    });
}
function updateSelection(point) {
    if (!start) return;
    const southWest = L.latLng(Math.min(start.lat, point.lat), Math.min(start.lng, point.lng));
    const northEast = L.latLng(Math.max(start.lat, point.lat), Math.max(start.lng, point.lng));
    const bounds = L.latLngBounds(southWest, northEast);
    if (!selection) selection = L.rectangle(bounds, {color: '#e46d43', weight: 2, fillOpacity: 0.16}).addTo(map);
    else selection.setBounds(bounds);
    showBounds(bounds);
}
document.getElementById('address-search').addEventListener('submit', async event => {
    event.preventDefault();
    const query = document.getElementById('address-query').value.trim();
    if (!query) return;
    const searchButton = document.getElementById('address-search-button');
    const resultsList = document.getElementById('address-results');
    const wait = 1000 - (Date.now() - lastAddressSearch);
    if (wait > 0) {
        setStatus('Please wait a moment before searching again.');
        return;
    }
    lastAddressSearch = Date.now();
    searchButton.disabled = true;
    resultsList.replaceChildren();
    setStatus('Searching OpenStreetMap...');
    try {
        const params = new URLSearchParams({format: 'jsonv2', limit: '5', q: query});
        const response = await fetch(`https://nominatim.openstreetmap.org/search?${params}`,
            {headers: {Accept: 'application/json'}});
        if (!response.ok) throw new Error('Address search is temporarily unavailable.');
        const results = await response.json();
        if (!results.length) {
            setStatus('No matching addresses found.', true);
            return;
        }
        results.forEach(result => {
            const item = document.createElement('li');
            const choice = document.createElement('button');
            choice.type = 'button';
            choice.textContent = result.display_name;
            choice.addEventListener('click', () => {
                const [south, north, west, east] = result.boundingbox.map(Number);
                if ([south, north, west, east].every(Number.isFinite)) {
                    map.fitBounds([[south, west], [north, east]], {maxZoom: 17, padding: [24, 24]});
                } else {
                    map.setView([Number(result.lat), Number(result.lon)], 17);
                }
                setStatus('Map centered on the selected result. Drag to outline the job site.');
            });
            item.append(choice);
            resultsList.append(item);
        });
        setStatus(`${results.length} matching places. Choose one to move the map.`);
    } catch (error) {
        setStatus(error.message || 'Address search failed. Try again.', true);
    } finally {
        searchButton.disabled = false;
    }
});
drawButton.addEventListener('click', () => {
    selecting = !selecting;
    drawButton.setAttribute('aria-pressed', String(selecting));
    drawButton.textContent = selecting ? 'Drag on map to draw' : 'Select map area';
    map.getContainer().style.cursor = selecting ? 'crosshair' : '';
    if (selecting) map.dragging.disable(); else map.dragging.enable();
    setStatus(selecting ? 'Drag from one corner to the opposite corner.' : '');
});
map.on('click', event => {
    if (geometrySelect.value === 'circle') {
        updateCircle(event.latlng.lat, event.latlng.lng);
        setStatus('Circle center selected. Adjust its radius and save.');
    }
});
map.on('mousedown', event => { if (selecting) { start = event.latlng; updateSelection(start); } });
map.on('mousemove', event => { if (start) updateSelection(event.latlng); });
map.on('mouseup', event => {
    if (!start) return;
    updateSelection(event.latlng);
    start = null;
    selecting = false;
    map.dragging.enable();
    map.getContainer().style.cursor = '';
    drawButton.setAttribute('aria-pressed', 'false');
    drawButton.textContent = 'Select map area';
    setStatus(saveButton.disabled ? 'Drag a larger area to create a job.' :
        editingName ? `Boundary updated for ${editingName}. Save your changes.` : 'Area selected. Add a job name and save.');
});
document.getElementById('clear-selection').addEventListener('click', () => {
    if (selection) map.removeLayer(selection);
    selection = null;
    start = null;
    boundsLabel.textContent = 'No area selected';
    saveButton.disabled = true;
    setStatus('Selection cleared.');
});
document.getElementById('cancel-edit').addEventListener('click', () => {
    editingName = null;
    selectionCenter = null;
    if (selection) map.removeLayer(selection);
    selection = null;
    start = null;
    document.getElementById('job-form').reset();
    document.getElementById('accuracy').value = '75';
    document.getElementById('rectangle-fields').hidden = false;
    document.getElementById('circle-fields').hidden = true;
    drawButton.hidden = false;
    boundsLabel.textContent = 'No area selected';
    saveButton.textContent = 'Save as new job';
    saveButton.disabled = true;
    document.getElementById('cancel-edit').hidden = true;
    setStatus('Edit canceled.');
});
async function loadJobs() {
    const response = await fetch('/api/sites');
    if (!response.ok) throw new Error('Could not load saved jobs.');
    const jobs = (await response.json()).sites;
    const list = document.getElementById('job-list');
    list.replaceChildren();
    jobLayers.clearLayers();
    jobs.forEach((job, index) => {
        const item = document.createElement('li');
        const detail = document.createElement('div');
        const name = document.createElement('span');
        name.className = 'job-name';
        name.textContent = job.name;
        const shape = document.createElement('span');
        shape.className = 'job-shape';
        shape.textContent = job.south !== undefined ? 'Rectangular boundary' : `${job.radius_m} m radius`;
        detail.append(name, shape);
        const edit = document.createElement('button');
        edit.type = 'button';
        edit.className = 'secondary';
        edit.textContent = 'Edit';
        edit.setAttribute('aria-label', `Edit ${job.name} geofence`);
        edit.addEventListener('click', () => setEditing(job));
        const remove = document.createElement('button');
        remove.type = 'button';
        remove.className = 'danger';
        remove.textContent = 'Delete';
        remove.setAttribute('aria-label', `Delete ${job.name}`);
        remove.addEventListener('click', async () => {
            if (!confirm(`Delete job "${job.name}"?`)) return;
            const result = await fetch(`/api/sites?name=${encodeURIComponent(job.name)}`, {method: 'DELETE'});
            if (!result.ok) { setStatus('Could not delete this job.', true); return; }
            setStatus(`Deleted ${job.name}.`);
            await loadJobs();
        });
        const actions = document.createElement('div');
        actions.className = 'job-actions';
        actions.append(edit, remove);
        item.append(detail, actions);
        list.append(item);
        const color = ['#3a9d83', '#d99a35', '#668bd3', '#ba6c9e'][index % 4];
        if (job.south !== undefined) {
            L.rectangle([[job.south, job.west], [job.north, job.east]], {color, weight: 2, fillOpacity: 0.08})
                .on('click', event => { L.DomEvent.stopPropagation(event); setEditing(job); }).addTo(jobLayers);
        } else {
            L.circle([job.lat, job.lon], {radius: job.radius_m, color, fillOpacity: 0.08})
                .on('click', event => { L.DomEvent.stopPropagation(event); setEditing(job); }).addTo(jobLayers);
        }
    });
}
document.getElementById('job-form').addEventListener('submit', async event => {
    event.preventDefault();
    if (!selection) return;
    const wasEditing = editingName !== null;
    const payload = {name: document.getElementById('job-name').value.trim(),
        max_accuracy_m: Number(document.getElementById('accuracy').value)};
    if (geometrySelect.value === 'circle') {
        if (!selectionCenter) { setStatus('Choose a map point for the circle center.', true); return; }
        payload.lat = selectionCenter.lat;
        payload.lon = selectionCenter.lon;
        payload.radius_m = Number(document.getElementById('radius').value);
    } else {
        const bounds = selection.getBounds();
        Object.assign(payload, {south: bounds.getSouth(), west: bounds.getWest(),
            north: bounds.getNorth(), east: bounds.getEast()});
    }
    saveButton.disabled = true;
    const url = editingName ? `/api/sites?name=${encodeURIComponent(editingName)}` : '/api/sites';
    const response = await fetch(url, {method: editingName ? 'PUT' : 'POST',
        headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)});
    if (!response.ok) {
        const problem = await response.json().catch(() => ({}));
        setStatus(Array.isArray(problem.detail) ? 'Check the job name and selected bounds.' : problem.detail || 'Could not save the job.', true);
        saveButton.disabled = false;
        return;
    }
    document.getElementById('job-form').reset();
    document.getElementById('accuracy').value = '75';
    editingName = null;
    selectionCenter = null;
    if (selection) map.removeLayer(selection);
    selection = null;
    boundsLabel.textContent = 'No area selected';
    saveButton.disabled = true;
    saveButton.textContent = 'Save as new job';
    document.getElementById('cancel-edit').hidden = true;
    document.getElementById('rectangle-fields').hidden = false;
    document.getElementById('circle-fields').hidden = true;
    drawButton.hidden = false;
    setStatus(wasEditing ? 'Job updated.' : 'Job saved.');
    await loadJobs();
});
document.getElementById('hours-range').addEventListener('submit', async event => {
    event.preventDefault();
    const first = document.getElementById('range-start').value;
    const last = document.getElementById('range-end').value;
    const statusLine = document.getElementById('hours-status');
    const firstDate = new Date(`${first}T00:00:00Z`);
    const lastDate = new Date(`${last}T00:00:00Z`);
    const daySpan = (lastDate - firstDate) / 86400000;
    if (!first || !last || daySpan < 0 || daySpan > 90) {
        statusLine.textContent = 'Choose a valid date range of no more than 91 days.';
        return;
    }
    statusLine.textContent = '';
    const query = new URLSearchParams({start_date: first, end_date: last});
    try {
        const response = await fetch(`/api/hours?${query}`);
        const report = await response.json();
        if (!response.ok) throw new Error(report.detail || 'Could not load the selected report.');
        const body = document.getElementById('hours-rows');
        body.replaceChildren();
        if (!report.totals.length) {
            const row = document.createElement('tr');
            const cell = document.createElement('td');
            cell.colSpan = 4;
            cell.textContent = 'No qualifying geofence visits for these dates.';
            row.append(cell);
            body.append(row);
        } else {
            report.totals.forEach(total => {
                const row = document.createElement('tr');
                for (const value of [total.date, total.site, total.device, total.hours.toFixed(2)]) {
                    const cell = document.createElement('td');
                    cell.textContent = value;
                    row.append(cell);
                }
                body.append(row);
            });
        }
        document.getElementById('hours-heading').textContent = `Worksite hours — ${report.from} to ${report.through}`;
        document.getElementById('hours-total').textContent = `${(report.totals.reduce((sum, item) => sum + item.seconds, 0) / 3600).toFixed(1)} h`;
        document.getElementById('hours-json').href = `/api/hours?${query}`;
        document.getElementById('hours-csv').href = `/api/hours.csv?${query}`;
    } catch (error) {
        statusLine.textContent = error.message || 'Could not load the selected report.';
    }
});
fetch('/api/locations?limit=1').then(response => response.ok ? response.json() : null).then(data => {
    const point = data?.locations?.[0];
    if (point) map.setView([point.lat, point.lon], 14);
}).catch(() => {});
loadJobs().catch(error => setStatus(error.message, true));
</script></body></html>""", headers={"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff",
                                                             "Content-Security-Policy": "default-src 'none'; style-src 'unsafe-inline' https://unpkg.com; script-src 'unsafe-inline' https://unpkg.com; img-src https://*.tile.openstreetmap.org https://unpkg.com data:; connect-src 'self' https://nominatim.openstreetmap.org; base-uri 'none'; form-action 'self'"})
