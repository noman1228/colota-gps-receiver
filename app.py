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
from datetime import date, datetime, time, timedelta, timezone
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
        con.execute("""
            CREATE TABLE IF NOT EXISTS users (
              username TEXT PRIMARY KEY COLLATE NOCASE,
              password_salt TEXT NOT NULL,
              password_hash TEXT NOT NULL,
              role TEXT NOT NULL CHECK(role IN ('admin','user')),
              active INTEGER NOT NULL DEFAULT 1,
              created_at TEXT NOT NULL
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS manual_times (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              date TEXT NOT NULL,
              site TEXT NOT NULL,
              device TEXT NOT NULL,
              start INTEGER NOT NULL,
              end INTEGER NOT NULL,
              note TEXT NOT NULL DEFAULT '',
              created_by TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              CHECK(end > start)
            )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_manual_times_date ON manual_times(date)")
        con.execute("""
            CREATE TABLE IF NOT EXISTS app_settings (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL
            )
        """)
        salt, password_hash = hash_password(PASSWORD)
        con.execute("""
            INSERT OR IGNORE INTO users
              (username,password_salt,password_hash,role,active,created_at)
            VALUES (?,?,?,'admin',1,?)
        """, (USERNAME, salt, password_hash, datetime.now(timezone.utc).isoformat()))
        defaults = {
            "timezone": TIMEZONE,
            "max_gap_minutes": str(GAP_MINUTES),
            "exit_grace_minutes": str(EXIT_GRACE_MINUTES),
            "min_inside_samples": str(MIN_INSIDE_SAMPLES),
        }
        con.executemany("INSERT OR IGNORE INTO app_settings(key,value) VALUES(?,?)", defaults.items())


def hash_password(password: str, salt: str | None = None) -> tuple[str, str]:
    salt = salt or secrets.token_hex(16)
    result = hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), PASSWORD_ITERATIONS)
    return salt, result.hex()


def get_runtime_settings() -> RuntimeSettings:
    with connection() as con:
        values = {row["key"]: row["value"] for row in
                  con.execute("SELECT key,value FROM app_settings")}
    try:
        settings = RuntimeSettings(
            timezone=values.get("timezone", TIMEZONE),
            max_gap_minutes=int(values.get("max_gap_minutes", GAP_MINUTES)),
            exit_grace_minutes=int(values.get("exit_grace_minutes", EXIT_GRACE_MINUTES)),
            min_inside_samples=int(values.get("min_inside_samples", MIN_INSIDE_SAMPLES)),
        )
        ZoneInfo(settings.timezone)
        return settings
    except (ValueError, ZoneInfoNotFoundError) as exc:
        raise HTTPException(status_code=500, detail="Invalid runtime settings") from exc


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


def authenticate_basic(value: str) -> dict | None:
    if not value.lower().startswith("basic "):
        return None
    try:
        raw = base64.b64decode(value[6:].strip(), validate=True).decode("utf-8")
        username, password = raw.split(":", 1)
    except (ValueError, UnicodeDecodeError):
        return None
    if not USERNAME_RE.fullmatch(username):
        return None
    with connection() as con:
        row = con.execute("SELECT * FROM users WHERE username=? COLLATE NOCASE", (username,)).fetchone()
    if row is None or not row["active"]:
        return None
    _, password_hash = hash_password(password, row["password_salt"])
    if not hmac.compare_digest(password_hash, row["password_hash"]):
        return None
    return {"username": row["username"], "role": row["role"]}


def require_user(request: Request) -> dict:
    user = authenticate_basic(request.headers.get("authorization", ""))
    if user is None:
        raise HTTPException(status_code=401, detail="Authentication required",
                            headers={"WWW-Authenticate": 'Basic realm="GPS Receiver"'})
    return user


def require_admin(user: Annotated[dict, Depends(require_user)]) -> dict:
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Administrator permission required")
    return user


def require_read(request: Request):
    user = authenticate_basic(request.headers.get("authorization", ""))
    if user is None:
        raise HTTPException(status_code=401, detail="Authentication required",
                            headers={"WWW-Authenticate": 'Basic realm="GPS Receiver"'})
    return require_admin(user)


def require_ingest(request: Request):
    header = request.headers.get("authorization", "")
    token_ok = header.startswith("Bearer ") and hmac.compare_digest(header[7:], INGEST_TOKEN)
    user = authenticate_basic(header) if not token_ok else None
    if not (token_ok or (user is not None and user["role"] == "admin")):
        raise HTTPException(status_code=401, detail="Invalid GPS credentials",
                            headers={"WWW-Authenticate": 'Basic realm="GPS Receiver"'})


@app.get("/api/users", dependencies=[Depends(require_admin)])
def list_users():
    with connection() as con:
        rows = con.execute("SELECT username,role,active,created_at FROM users ORDER BY username COLLATE NOCASE")
        return {"users": [dict(row) for row in rows]}


@app.post("/api/users", dependencies=[Depends(require_admin)], status_code=201)
def create_user(user: UserCreate):
    salt, password_hash = hash_password(user.password)
    try:
        with connection() as con:
            con.execute("""
                INSERT INTO users(username,password_salt,password_hash,role,active,created_at)
                VALUES(?,?,?,?,1,?)
            """, (user.username, salt, password_hash, user.role, datetime.now(timezone.utc).isoformat()))
    except sqlite3.IntegrityError as exc:
        raise HTTPException(status_code=409, detail="That username already exists") from exc
    return {"username": user.username, "role": user.role, "active": True}


@app.put("/api/users/{username}", dependencies=[Depends(require_admin)])
def update_user(username: str, updates: UserUpdate, request: Request):
    actor = authenticate_basic(request.headers.get("authorization", ""))
    with connection() as con:
        current = con.execute("SELECT username,role,active FROM users WHERE username=? COLLATE NOCASE",
                              (username,)).fetchone()
        if current is None:
            raise HTTPException(status_code=404, detail="User not found")
        new_role = updates.role or current["role"]
        new_active = current["active"] if updates.active is None else updates.active
        if (actor is not None and actor["username"].casefold() == current["username"].casefold() and
                (new_role != "admin" or not new_active)):
            raise HTTPException(status_code=409, detail="You cannot remove administrator access from your own account")
        if current["role"] == "admin" and (new_role != "admin" or not new_active):
            admin_count = con.execute("SELECT COUNT(*) FROM users WHERE role='admin' AND active=1").fetchone()[0]
            if admin_count <= 1:
                raise HTTPException(status_code=409, detail="At least one active administrator is required")
        if updates.password is None:
            con.execute("UPDATE users SET role=?,active=? WHERE username=?",
                        (new_role, int(new_active), current["username"]))
        else:
            salt, password_hash = hash_password(updates.password)
            con.execute("UPDATE users SET role=?,active=?,password_salt=?,password_hash=? WHERE username=?",
                        (new_role, int(new_active), salt, password_hash, current["username"]))
    return {"username": current["username"], "role": new_role, "active": bool(new_active)}


@app.delete("/api/users/{username}", dependencies=[Depends(require_admin)])
def delete_user(username: str, request: Request):
    actor = authenticate_basic(request.headers.get("authorization", ""))
    if actor is not None and actor["username"].casefold() == username.casefold():
        raise HTTPException(status_code=409, detail="You cannot delete your own account")
    with connection() as con:
        current = con.execute("SELECT username,role,active FROM users WHERE username=? COLLATE NOCASE",
                              (username,)).fetchone()
        if current is None:
            raise HTTPException(status_code=404, detail="User not found")
        if current["role"] == "admin" and current["active"]:
            admin_count = con.execute("SELECT COUNT(*) FROM users WHERE role='admin' AND active=1").fetchone()[0]
            if admin_count <= 1:
                raise HTTPException(status_code=409, detail="At least one active administrator is required")
        con.execute("DELETE FROM users WHERE username=?", (current["username"],))
    return {"ok": True}


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


def manual_entry_values(entry: ManualTimeEntry) -> tuple[int, int]:
    settings = get_runtime_settings()
    try:
        start_time = time.fromisoformat(entry.start_time)
        end_time = time.fromisoformat(entry.end_time)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="Use valid 24-hour start and end times") from exc
    zone = ZoneInfo(settings.timezone)
    start = int(datetime.combine(entry.date, start_time, zone).timestamp())
    end = int(datetime.combine(entry.date, end_time, zone).timestamp())
    if end <= start:
        raise HTTPException(status_code=422, detail="End time must be later than start time on the same date")
    return start, end


def manual_entry_response(row: sqlite3.Row) -> dict:
    settings = get_runtime_settings()
    zone = ZoneInfo(settings.timezone)
    return {"id": row["id"], "date": row["date"], "site": row["site"],
            "device": row["device"],
            "start_time": datetime.fromtimestamp(row["start"], zone).strftime("%H:%M"),
            "end_time": datetime.fromtimestamp(row["end"], zone).strftime("%H:%M"),
            "hours": round((row["end"] - row["start"]) / 3600, 2),
            "note": row["note"], "created_by": row["created_by"]}


@app.get("/api/manual-times", dependencies=[Depends(require_admin)])
def list_manual_times(start_date: date | None = None, end_date: date | None = None):
    clauses, args = [], []
    if start_date:
        clauses.append("date >= ?")
        args.append(start_date.isoformat())
    if end_date:
        clauses.append("date <= ?")
        args.append(end_date.isoformat())
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    with connection() as con:
        rows = con.execute("SELECT * FROM manual_times" + where + " ORDER BY start DESC", args).fetchall()
    return {"entries": [manual_entry_response(row) for row in rows]}


@app.post("/api/manual-times", dependencies=[Depends(require_admin)], status_code=201)
def create_manual_time(entry: ManualTimeEntry, user: Annotated[dict, Depends(require_admin)]):
    start, end = manual_entry_values(entry)
    with connection() as con:
        cursor = con.execute("""
            INSERT INTO manual_times(date,site,device,start,end,note,created_by,updated_at)
            VALUES(?,?,?,?,?,?,?,?)
        """, (entry.date.isoformat(), entry.site, entry.device, start, end, entry.note,
              user["username"], datetime.now(timezone.utc).isoformat()))
        row = con.execute("SELECT * FROM manual_times WHERE id=?", (cursor.lastrowid,)).fetchone()
    return {"entry": manual_entry_response(row)}


@app.put("/api/manual-times/{entry_id}", dependencies=[Depends(require_admin)])
def update_manual_time(entry_id: int, entry: ManualTimeEntry):
    start, end = manual_entry_values(entry)
    with connection() as con:
        cursor = con.execute("""
            UPDATE manual_times SET date=?,site=?,device=?,start=?,end=?,note=?,updated_at=? WHERE id=?
        """, (entry.date.isoformat(), entry.site, entry.device, start, end, entry.note,
              datetime.now(timezone.utc).isoformat(), entry_id))
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Manual time entry not found")
        row = con.execute("SELECT * FROM manual_times WHERE id=?", (entry_id,)).fetchone()
    return {"entry": manual_entry_response(row)}


@app.delete("/api/manual-times/{entry_id}", dependencies=[Depends(require_admin)])
def delete_manual_time(entry_id: int):
    with connection() as con:
        cursor = con.execute("DELETE FROM manual_times WHERE id=?", (entry_id,))
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Manual time entry not found")
    return {"ok": True}


@app.get("/api/settings", dependencies=[Depends(require_admin)])
def read_settings():
    return get_runtime_settings().model_dump()


@app.put("/api/settings", dependencies=[Depends(require_admin)])
def update_settings(settings: RuntimeSettings):
    try:
        ZoneInfo(settings.timezone)
    except ZoneInfoNotFoundError as exc:
        raise HTTPException(status_code=422, detail="Unknown timezone") from exc
    values = {key: str(value) for key, value in settings.model_dump().items()}
    with connection() as con:
        con.executemany("""
            INSERT INTO app_settings(key,value) VALUES(?,?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """, values.items())
    return settings.model_dump()


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
    settings = get_runtime_settings()
    max_gap = settings.max_gap_minutes * 60
    exit_grace = settings.exit_grace_minutes * 60
    for site in sites:
        for device, fixes in devices.items():
            start = last_inside = None
            count = 0

            def finish(end: int, reason: str):
                if start is not None and count >= settings.min_inside_samples and end > start:
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
    settings = get_runtime_settings()
    tz = ZoneInfo(settings.timezone)
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
    with connection() as con:
        manual_rows = con.execute("SELECT * FROM manual_times WHERE date>=? AND date<=? ORDER BY start",
                                  (first.isoformat(), last.isoformat())).fetchall()
    manual_entries = [manual_entry_response(row) for row in manual_rows]
    for row in manual_rows:
        key = (row["date"], row["site"], row["device"])
        daily[key] = daily.get(key, 0) + row["end"] - row["start"]
    totals = [{"date": d, "site": s, "device": dev, "seconds": seconds,
               "hours": round(seconds / 3600, 3)}
              for (d, s, dev), seconds in sorted(daily.items())]
    return {"timezone": settings.timezone, "from": first.isoformat(), "through": last.isoformat(),
            "method": "Estimated from GPS; requires at least two accurate inside fixes. "
                      "Gaps and exits may undercount or overcount actual attendance.",
            "sites": [site.model_dump() for site in sites],
            "totals": totals, "visits": segments, "manual_entries": manual_entries,
            "points_examined": len(points)}


def report_dates(start_date: date | None, end_date: date | None, days: int):
    today = datetime.now(ZoneInfo(get_runtime_settings().timezone)).date()
    if start_date is None and end_date is None:
        return today - timedelta(days=days - 1), today
    return start_date or end_date or today, end_date or start_date or today


@app.get("/api/hours")
def hours(start_date: date | None = None, end_date: date | None = None,
          days: int = Query(default=7, ge=1, le=91),
          user: Annotated[dict, Depends(require_user)] = None):
    first, last = report_dates(start_date, end_date, days)
    report = reports_for_days(first, last)
    if user["role"] == "user":
        return {"from": report["from"], "through": report["through"],
                "totals": [{key: row[key] for key in ("date", "hours", "seconds")}
                           for row in report["totals"]]}
    return report


@app.get("/api/hours.csv")
def hours_csv(start_date: date | None = None, end_date: date | None = None,
              days: int = Query(default=7, ge=1, le=91),
              user: Annotated[dict, Depends(require_user)] = None):
    first, last = report_dates(start_date, end_date, days)
    report = reports_for_days(first, last)
    fields = ["date", "hours", "seconds"] if user["role"] == "user" else ["date", "site", "device", "hours", "seconds"]
    return StreamingResponse(as_csv(report["totals"], fields), media_type="text/csv",
                             headers={"Content-Disposition": 'attachment; filename="gps-work-hours.csv"',
                                      "Cache-Control": "no-store"})


def local_time(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, ZoneInfo(get_runtime_settings().timezone)).strftime("%Y-%m-%d %I:%M:%S %p")


def user_dashboard(report: dict, username: str) -> HTMLResponse:
    totals = "".join(
        f"<tr><td>{escape(row['date'])}</td><td>{row['hours']:.2f}</td></tr>"
        for row in report["totals"]
    ) or "<tr><td colspan='2'>No time entries for these dates.</td></tr>"
    return HTMLResponse("""<!doctype html><html lang='en'><head><meta charset='utf-8'>
<meta name='viewport' content='width=device-width,initial-scale=1'><title>Time report</title>
<style>body{margin:0;background:#101820;color:#eef2f1;font:15px system-ui,sans-serif}main{max-width:760px;margin:48px auto;padding:24px}
h1{font-size:26px}.hint{color:#b5c2c0}section{border-top:1px solid #42504e;padding:20px 0}
form{display:flex;align-items:end;flex-wrap:wrap;gap:12px}label{display:block;font-size:13px;color:#b5c2c0;margin-bottom:5px}
input,button{font:inherit;padding:9px 11px;border:1px solid #53635f;border-radius:3px;background:#172520;color:#eef2f1}
button{background:#b8e4cd;color:#15251d;font-weight:650;cursor:pointer}table{border-collapse:collapse;width:100%;margin-top:18px}
th,td{text-align:left;padding:11px;border-bottom:1px solid #354541}#error{color:#ffaaa4;min-height:20px}
</style></head><body><main><h1>Time report</h1><p class='hint'>Signed in as """ + escape(username) + """</p>
<section><form id='date-range'><div><label for='from'>From</label><input id='from' type='date' value='""" + report["from"] + """' required></div>
<div><label for='through'>Through</label><input id='through' type='date' value='""" + report["through"] + """' required></div>
<button type='submit'>View times</button></form><p id='error' role='status' aria-live='polite'></p>
<table><thead><tr><th>Date</th><th>Hours</th></tr></thead><tbody id='rows'>""" + totals + """</tbody></table></section>
<script>document.getElementById('date-range').addEventListener('submit',async event=>{
event.preventDefault();const from=document.getElementById('from').value;const through=document.getElementById('through').value;
const span=(Date.parse(through+'T00:00:00Z')-Date.parse(from+'T00:00:00Z'))/86400000;const error=document.getElementById('error');
if(!from||!through||span<0||span>90){error.textContent='Choose a valid range of no more than 91 days.';return;}
try{const response=await fetch('/api/hours?'+new URLSearchParams({start_date:from,end_date:through}));const report=await response.json();
if(!response.ok)throw Error(report.detail||'Unable to load report.');const body=document.getElementById('rows');body.replaceChildren();
if(!report.totals.length){const row=document.createElement('tr');const cell=document.createElement('td');cell.colSpan=2;cell.textContent='No time entries for these dates.';row.append(cell);body.append(row);}
for(const total of report.totals){const row=document.createElement('tr');for(const value of [total.date,total.hours.toFixed(2)]){const cell=document.createElement('td');cell.textContent=value;row.append(cell);}body.append(row);}
error.textContent='';}catch(exception){error.textContent=exception.message;}});</script></main></body></html>""")


@app.get("/", response_class=HTMLResponse)
def dashboard(start_date: date | None = None, end_date: date | None = None,
              user: dict = Depends(require_user)):
    first, last = report_dates(start_date, end_date, 7)
    report = reports_for_days(first, last)
    if user["role"] == "user":
        safe_report = {"from": report["from"], "through": report["through"],
                       "totals": [{"date": row["date"], "hours": row["hours"]}
                                  for row in report["totals"]]}
        return user_dashboard(safe_report, user["username"])
    recent = get_rows(limit=100)
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
<section><h2>Manual time entries</h2><p class='hint'>Add or correct time records. These are included in hours reports.</p>
<form id='manual-time-form' class='date-range'><input id='manual-time-id' type='hidden'>
<div class='date-field'><label for='manual-date'>Date</label><input id='manual-date' type='date' value='""" + escape(report_end) + """' required></div>
<div class='date-field'><label for='manual-site'>Job site</label><input id='manual-site' maxlength='80' required></div>
<div class='date-field'><label for='manual-device'>Device / person</label><input id='manual-device' maxlength='64' value='manual' required></div>
<div class='date-field'><label for='manual-start'>Start</label><input id='manual-start' type='time' required></div>
<div class='date-field'><label for='manual-end'>End</label><input id='manual-end' type='time' required></div>
<div class='date-field'><label for='manual-note'>Note</label><input id='manual-note' maxlength='300'></div>
<button id='manual-save' type='submit'>Add time</button><button id='manual-cancel' class='secondary' type='button' hidden>Cancel edit</button></form>
<p id='manual-status' class='status' role='status' aria-live='polite'></p>
<div class='scroll'><table><thead><tr><th>Date</th><th>Site</th><th>Device</th><th>Start</th><th>End</th><th>Hours</th><th>Actions</th></tr></thead>
<tbody id='manual-time-rows'></tbody></table></div></section>
<section><h2>User management</h2><form id='new-user-form' class='date-range'>
<div class='date-field'><label for='new-username'>Username</label><input id='new-username' minlength='3' maxlength='64' pattern='[A-Za-z0-9_.-]+' required></div>
<div class='date-field'><label for='new-password'>Temporary password</label><input id='new-password' type='password' minlength='12' maxlength='256' required></div>
<div class='date-field'><label for='new-role'>Permission</label><select id='new-role'><option value='user'>View times only</option><option value='admin'>Administrator</option></select></div>
<button type='submit'>Add user</button></form><p id='user-status' class='status' role='status' aria-live='polite'></p>
<div class='scroll'><table><thead><tr><th>User</th><th>Role</th><th>Active</th><th>New password</th><th>Actions</th></tr></thead>
<tbody id='users-rows'></tbody></table></div></section>
<section><h2>Time-tracking settings</h2><form id='settings-form' class='date-range'>
<div class='date-field'><label for='settings-timezone'>Timezone</label><input id='settings-timezone' maxlength='64' required></div>
<div class='date-field'><label for='settings-gap'>Maximum tracking gap (minutes)</label><input id='settings-gap' type='number' min='1' max='1440' required></div>
<div class='date-field'><label for='settings-grace'>Exit grace (minutes)</label><input id='settings-grace' type='number' min='0' max='240' required></div>
<div class='date-field'><label for='settings-samples'>Required inside fixes</label><input id='settings-samples' type='number' min='2' max='100' required></div>
<button type='submit'>Save settings</button></form><p id='settings-status' class='status' role='status' aria-live='polite'></p></section>
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
const currentUsername = """ + json.dumps(user["username"]) + """;
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
let editingTimeId = null;
async function loadManualTimes() {
    const params = new URLSearchParams({start_date: document.getElementById('range-start').value,
        end_date: document.getElementById('range-end').value});
    const response = await fetch(`/api/manual-times?${params}`);
    if (!response.ok) throw new Error('Could not load manual time entries.');
    const entries = (await response.json()).entries;
    const body = document.getElementById('manual-time-rows');
    body.replaceChildren();
    for (const entry of entries) {
        const row = document.createElement('tr');
        for (const value of [entry.date, entry.site, entry.device, entry.start_time,
            entry.end_time, entry.hours.toFixed(2)]) {
            const cell = document.createElement('td');
            cell.textContent = value;
            row.append(cell);
        }
        const actions = document.createElement('td');
        const edit = document.createElement('button');
        edit.type = 'button'; edit.className = 'secondary'; edit.textContent = 'Edit';
        edit.addEventListener('click', () => {
            editingTimeId = entry.id;
            document.getElementById('manual-date').value = entry.date;
            document.getElementById('manual-site').value = entry.site;
            document.getElementById('manual-device').value = entry.device;
            document.getElementById('manual-start').value = entry.start_time;
            document.getElementById('manual-end').value = entry.end_time;
            document.getElementById('manual-note').value = entry.note;
            document.getElementById('manual-save').textContent = 'Save time';
            document.getElementById('manual-cancel').hidden = false;
        });
        const remove = document.createElement('button');
        remove.type = 'button'; remove.className = 'danger'; remove.textContent = 'Delete';
        remove.setAttribute('aria-label', `Delete time on ${entry.date} at ${entry.site}`);
        remove.addEventListener('click', async () => {
            if (!confirm(`Delete ${entry.date} time at ${entry.site}?`)) return;
            const result = await fetch(`/api/manual-times/${entry.id}`, {method: 'DELETE'});
            if (!result.ok) { setStatus('Could not delete time entry.', true); return; }
            await loadManualTimes();
            document.getElementById('hours-range').requestSubmit();
        });
        actions.append(edit, remove);
        row.append(actions);
        body.append(row);
    }
}
document.getElementById('manual-time-form').addEventListener('submit', async event => {
    event.preventDefault();
    const payload = {date: document.getElementById('manual-date').value,
        site: document.getElementById('manual-site').value.trim(),
        device: document.getElementById('manual-device').value.trim(),
        start_time: document.getElementById('manual-start').value,
        end_time: document.getElementById('manual-end').value,
        note: document.getElementById('manual-note').value.trim()};
    const editing = editingTimeId !== null;
    const response = await fetch(editing ? `/api/manual-times/${editingTimeId}` : '/api/manual-times', {
        method: editing ? 'PUT' : 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)});
    if (!response.ok) {
        const problem = await response.json().catch(() => ({}));
        document.getElementById('manual-status').textContent = problem.detail || 'Could not save time entry.';
        return;
    }
    document.getElementById('manual-status').textContent = editing ? 'Time entry updated.' : 'Time entry added.';
    editingTimeId = null;
    event.target.reset();
    document.getElementById('manual-date').value = document.getElementById('range-end').value;
    document.getElementById('manual-device').value = 'manual';
    document.getElementById('manual-save').textContent = 'Add time';
    document.getElementById('manual-cancel').hidden = true;
    await loadManualTimes();
    document.getElementById('hours-range').requestSubmit();
});
document.getElementById('manual-cancel').addEventListener('click', () => {
    editingTimeId = null;
    document.getElementById('manual-time-form').reset();
    document.getElementById('manual-date').value = document.getElementById('range-end').value;
    document.getElementById('manual-device').value = 'manual';
    document.getElementById('manual-save').textContent = 'Add time';
    document.getElementById('manual-cancel').hidden = true;
});
document.getElementById('hours-range').addEventListener('submit', () => {
    loadManualTimes().catch(error => setStatus(error.message, true));
});
async function loadUsers() {
    const response = await fetch('/api/users');
    if (!response.ok) throw new Error('Could not load users.');
    const users = (await response.json()).users;
    const body = document.getElementById('users-rows');
    body.replaceChildren();
    for (const user of users) {
        const row = document.createElement('tr');
        const username = document.createElement('td'); username.textContent = user.username; row.append(username);
        const roleCell = document.createElement('td');
        const role = document.createElement('select');
        for (const [value, label] of [['user', 'View times only'], ['admin', 'Administrator']]) {
            const option = document.createElement('option'); option.value = value; option.textContent = label; role.append(option);
        }
        role.value = user.role; role.disabled = user.username.toLowerCase() === currentUsername.toLowerCase();
        roleCell.append(role); row.append(roleCell);
        const activeCell = document.createElement('td');
        const active = document.createElement('input'); active.type = 'checkbox'; active.checked = Boolean(user.active);
        active.disabled = user.username.toLowerCase() === currentUsername.toLowerCase();
        activeCell.append(active); row.append(activeCell);
        const passwordCell = document.createElement('td');
        const password = document.createElement('input'); password.type = 'password'; password.minLength = 12; password.maxLength = 256;
        password.autocomplete = 'new-password'; password.placeholder = 'Leave blank to keep'; passwordCell.append(password); row.append(passwordCell);
        const actionCell = document.createElement('td');
        const save = document.createElement('button'); save.type = 'button'; save.textContent = 'Save';
        save.addEventListener('click', async () => {
            const changes = {role: role.value, active: active.checked};
            if (password.value) changes.password = password.value;
            const result = await fetch(`/api/users/${encodeURIComponent(user.username)}`, {
                method: 'PUT', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(changes)});
            if (!result.ok) { const problem = await result.json(); document.getElementById('user-status').textContent = problem.detail || 'Could not update user.'; return; }
            document.getElementById('user-status').textContent = `Updated ${user.username}.`;
            await loadUsers();
        });
        const remove = document.createElement('button'); remove.type = 'button'; remove.className = 'danger'; remove.textContent = 'Delete';
        remove.disabled = user.username.toLowerCase() === currentUsername.toLowerCase();
        remove.addEventListener('click', async () => {
            if (!confirm(`Delete user ${user.username}?`)) return;
            const result = await fetch(`/api/users/${encodeURIComponent(user.username)}`, {method: 'DELETE'});
            if (!result.ok) { const problem = await result.json(); document.getElementById('user-status').textContent = problem.detail || 'Could not delete user.'; return; }
            await loadUsers();
        });
        actionCell.append(save, remove); row.append(actionCell); body.append(row);
    }
}
document.getElementById('new-user-form').addEventListener('submit', async event => {
    event.preventDefault();
    const payload = {username: document.getElementById('new-username').value.trim(),
        password: document.getElementById('new-password').value, role: document.getElementById('new-role').value};
    const response = await fetch('/api/users', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)});
    if (!response.ok) { const problem = await response.json(); document.getElementById('user-status').textContent = problem.detail || 'Could not add user.'; return; }
    event.target.reset(); document.getElementById('user-status').textContent = `Added ${payload.username}.`;
    await loadUsers();
});
async function loadSettings() {
    const response = await fetch('/api/settings');
    if (!response.ok) throw new Error('Could not load settings.');
    const settings = await response.json();
    document.getElementById('settings-timezone').value = settings.timezone;
    document.getElementById('settings-gap').value = settings.max_gap_minutes;
    document.getElementById('settings-grace').value = settings.exit_grace_minutes;
    document.getElementById('settings-samples').value = settings.min_inside_samples;
}
document.getElementById('settings-form').addEventListener('submit', async event => {
    event.preventDefault();
    const payload = {timezone: document.getElementById('settings-timezone').value.trim(),
        max_gap_minutes: Number(document.getElementById('settings-gap').value),
        exit_grace_minutes: Number(document.getElementById('settings-grace').value),
        min_inside_samples: Number(document.getElementById('settings-samples').value)};
    const response = await fetch('/api/settings', {method: 'PUT', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)});
    const statusLine = document.getElementById('settings-status');
    if (!response.ok) { const problem = await response.json(); statusLine.textContent = problem.detail || 'Invalid settings.'; return; }
    statusLine.textContent = 'Settings saved.';
    document.getElementById('hours-range').requestSubmit();
});
fetch('/api/locations?limit=1').then(response => response.ok ? response.json() : null).then(data => {
    const point = data?.locations?.[0];
    if (point) map.setView([point.lat, point.lon], 14);
}).catch(() => {});
loadJobs().catch(error => setStatus(error.message, true));
loadManualTimes().catch(error => setStatus(error.message, true));
loadUsers().catch(error => document.getElementById('user-status').textContent = error.message);
loadSettings().catch(error => document.getElementById('settings-status').textContent = error.message);
</script></body></html>""", headers={"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff",
                                                             "Content-Security-Policy": "default-src 'none'; style-src 'unsafe-inline' https://unpkg.com; script-src 'unsafe-inline' https://unpkg.com; img-src https://*.tile.openstreetmap.org https://unpkg.com data:; connect-src 'self' https://nominatim.openstreetmap.org; base-uri 'none'; form-action 'self'"})
