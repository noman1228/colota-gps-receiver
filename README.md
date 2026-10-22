# Colota GPS Receiver + Site Hours

A small, self-hosted Docker receiver for [Colota](https://colota.app/): encrypted public ingress through your reverse proxy, persistent GPS storage, a private dashboard, raw-history export, and estimated job-site time from geofences.

## 1. Install on the Docker host

Copy this folder to your Linux server (e.g. `/opt/colota-gps-receiver`), then:

```bash
cd /opt/colota-gps-receiver
chmod +x setup.sh
./setup.sh
docker compose up -d --build
docker compose ps
```

`setup.sh` generates a random browser password and a separate random phone-ingest token in `.env`, creates `data/sites.json`, and restricts file permissions. **Do not upload, email, or publish the `.env` file.** The database is `/opt/colota-gps-receiver/data/locations.sqlite3` and survives container restarts and rebuilds.

Open the browser dashboard on your LAN: `http://SERVER-LAN-IP:8765/`. Use the credentials in `.env` (`APP_USERNAME`/`APP_PASSWORD`).

To see the phone token:

```bash
sed -n '/^INGEST_TOKEN=/p' .env
```

To see the browser credentials:

```bash
sed -n '/^APP_USERNAME=/p; /^APP_PASSWORD=/p' .env
```

**Do not forward port 8765 on your router.** Its local HTTP traffic is for your reverse proxy / trusted LAN only. For stronger isolation, set `BIND_IP=` in `.env` to the LAN IP of the Docker host or use a shared Docker network between the receiver and reverse proxy. If the reverse proxy runs on a separate Docker bridge network, `BIND_IP=127.0.0.1` will usually *not* work for it.

## 2. Public HTTPS through Nginx Proxy Manager

In Nginx Proxy Manager, add a Proxy Host:

| Setting | Value |
|---|---|
| Domain | `gps.yourdomain.com` (replace with your real DNS name) |
| Scheme | `http` |
| Forward Hostname / IP | Your Docker server's LAN IP (e.g., `192.168.1.249`) |
| Forward Port | `8765` |
| Block Common Exploits | Enabled |
| SSL | Request a Let's Encrypt certificate; enable Force SSL |

Create an A/AAAA or CNAME DNS record resolving the GPS hostname to your public reverse proxy address. Only **80/443** (as required for your proxy/certificate flow) should be routed externally to Nginx Proxy Manager; never forward 8765. If the domain uses the same public IP as your existing Home Assistant proxy, it can use the same 443 port—different hostnames route to different containers.

Public ingest URL:

`https://gps.yourdomain.com/api/location`

Private browser dashboard:

`https://gps.yourdomain.com/`

Keep NPM access lists / additional proxy password requirements disabled **for this host** unless Colota is configured to pass those credentials too. This app already authenticates every GPS POST and all location-read endpoints, with *separate credentials*. If you expose the dashboard publicly, Basic authentication protects it but is not equivalent to an MFA-based VPN; prefer restricting dashboard access with proxy route rules or a VPN for sensitive history. Disable caching on the proxy.

## 3. Colota setup

Go to **Settings → Connection**:

1. **Server endpoint**: `https://gps.yourdomain.com/api/location`
2. **Request format**: **Custom** (default Colota fields; POST / JSON).
3. **Authentication & Headers**: **Bearer Token**. Enter `INGEST_TOKEN` from `.env`, without the `Bearer ` prefix.
4. Optional **Custom fields**: `device` = `jason-phone`, `work-phone`, etc. Letters, numbers, underscores, hyphens, periods only. This is useful if you add more phones; otherwise records use device `phone`.
5. **Test connection** or start tracking; verify server logs and dashboard.

Colota `alt`, `vel`, and `bear` may be omitted by the phone; that's supported. The receiver accepts the `cog` alias for bearing as well. Colota queues failed syncs locally; the backend preserves the original `tst`, **not** the time of upload, when calculating shifts. It accepts concurrent requests and ignores duplicate records with the same device, timestamp, latitude, and longitude.

### Test with curl

On the Docker host, load the ingest token from `.env` and send the exact sample:

```bash
TOKEN="$(sed -n 's/^INGEST_TOKEN=//p' .env)"
curl -i -X POST 'http://127.0.0.1:8765/api/location' \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"lat":51.495065,"lon":-0.043945,"acc":12,"alt":519,"vel":0,"batt":85,"bs":2,"tst":1704067200,"bear":180.5}'
```

Expected HTTP 200 and `{"ok":true,"saved":true,"duplicate":false,...}`. Repeat the request to see `"duplicate":true`. **The sample timestamp is January 1, 2024** and will appear in historical records rather than today's history. If your `BIND_IP` is set to the server's LAN IP instead of `0.0.0.0`, use that IP in the curl URL rather than `127.0.0.1`.

On PowerShell, use `Invoke-RestMethod` with an `Authorization` header and `ConvertTo-Json`, or use `curl.exe` (not the alias).

## 4. Create job sites for estimated time tracking

Open the dashboard and use **New job site**: search a street address or place, choose a matching result to center the map, then choose **Select map area** and drag a rectangle around the job boundary. Enter a name and maximum GPS accuracy, then save. Click a saved boundary or its **Edit** button to adjust the geofence or rename the job; saved jobs can also be deleted. The hours section accepts a selectable inclusive start/end date range (up to 91 days). Changes take effect immediately; no restart is needed. You can still edit `data/sites.json` directly if preferred.

Existing circular geofences remain supported. New jobs created from the map use rectangular boundaries stored as `south`, `west`, `north`, and `east` coordinates, and time reports count fixes inside those bounds.

The map loads Leaflet from its CDN and map tiles from OpenStreetMap. Your GPS points are not uploaded to those services, but loading a map sends the map's viewed area and your IP address to the tile provider. Address searches are sent to OpenStreetMap's Nominatim service. The rest of the receiver does not require external map services.

Example circular geofence configuration (replace these example coordinates with your actual job locations):

```json
{
  "sites": [
    {"name":"Main Jobsite", "lat":44.000000, "lon":-70.000000, "radius_m":150, "max_accuracy_m":75},
    {"name":"Second Jobsite", "lat":44.100000, "lon":-70.100000, "radius_m":175, "max_accuracy_m":75}
  ]
}
```

Use radii around 100–200 m as a starting point and tune based on reception and site boundaries. For worksite attendance, a 30–60-second Colota recording interval with stationary fixes enabled is generally more practical than 5-second recording. Ensure the phone continues recording while stationary, or hours will be undercounted. Each geofence calculates sessions from the recorded GPS samples. No location data is sent to third-party services by the receiver.

**Current time-tracking rules**:

- Require **2 accurate location fixes inside** a worksite to confirm a visit.
- Ignore fixes less accurate than that site's `max_accuracy_m`.
- Visits start at the first inside fix and finish at the first outside fix, bounded by an `EXIT_GRACE_MINUTES` grace interval after the last inside fix.
- If the gap between good inside fixes exceeds `MAX_GAP_MINUTES`, stop at the last confirmed inside fix; do *not* count a long offline period as worked time.
- Unclosed visits finish at the last confirmed inside fix (no speculative hours while the phone is silent).
- Visits are computed from chronological event timestamps, so delayed and out-of-order uploads update historical reports correctly.
- Time totals are split across local calendar dates using `APP_TZ` (including daylight-saving changes).

The defaults are a **20-minute** maximum tracking gap, **5-minute** exit grace, and **2** minimum inside fixes. These can be changed in `.env` followed by `docker compose up -d`. **These are GPS-based estimates, not verified payroll/attendance records.** Your exact paid-time rules (breaks, lunch, rounding, minimum shift length, employee consent) may differ.

## 5. API and exports

Every route except `/healthz` needs authentication:

- `POST /api/location`: Bearer token or browser Basic credentials. JSON single-point location ingestion.
- `GET /api/location`: confirms readiness using ingestion credentials.
- `GET /`: dashboard; requires `APP_USERNAME` / `APP_PASSWORD` (Basic auth).
- `GET /api/locations?start=UNIX_SEC&end=UNIX_SEC&device=jason-phone&limit=250`: latest records as JSON. `end` is exclusive.
- `GET /api/sites`, `POST /api/sites`, `PUT /api/sites?name=JOB_NAME`, `DELETE /api/sites?name=JOB_NAME`: list, create, update, and delete worksite jobs; browser credentials required.
- `GET /api/export.csv`: full raw-history CSV export, optionally filtered by `start`, `end`, `device`.
- `GET /api/hours?days=7`: estimated worksite visits and daily hours as JSON.
- `GET /api/hours?start_date=2026-10-01&end_date=2026-10-21`: explicit inclusive dates; up to 91 days per query.
- `GET /api/hours.csv?days=7`: daily per-site hours CSV.
- `GET /healthz`: unprotected service health check, no location data.

Exporting history requires the browser credentials, **not** the phone token. The dashboard shows the last 100 fixes and time tracked over the last 7 days. It opens OpenStreetMap only when a point link is clicked; it does not preload external map tiles or transmit GPS history to a map service.

## Backup and maintenance

Location data contains sensitive movement history. Back up the **entire** `data` directory (including SQLite `-wal` and `-shm` files when present), or create a consistent online backup through Python's SQLite backup API. For a simple cold backup:

```bash
docker compose stop
cp -a data "/secure-backup/colota-data-$(date +%F)"
docker compose start
```

Adjust `/secure-backup` to a real, secure destination. Never put a raw database copy on a publicly shared path. There is no automatic purge/retention—data is kept until you delete it. Keep Docker and Python dependencies updated, and protect `.env` and the backups.

Use `docker compose logs --tail=100 colota-receiver` to troubleshoot. The database uses WAL and an index on device/time for concurrent Colota uploads.

## Test locally (optional)

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt pytest httpx
pytest -q
```
