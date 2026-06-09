#!/usr/bin/env python3
"""FIRESTORM FIRMS pipeline — pulls NASA FIRMS MODIS + VIIRS direct CSV API
and emits two slim JSONs the frontend can fetch like every other feed.

Why this exists: the ESRI Living Atlas FeatureServer mirror (which the
frontend used to query directly) has schema asymmetry between the MODIS
and VIIRS layers (MODIS lacks lat/lng attributes), Web-Mercator-only
geometry, and a 16k records-per-page cap that requires pagination for
VIIRS US-wide (~41k records). Eight rounds of v2_213b–v2_213h were the
result. This pipeline normalizes both sensors into the same shape and
removes every one of those quirks at the source.

Source: https://firms.modaps.eosdis.nasa.gov/api/area/csv/<MAP_KEY>/<src>/<coords>/<days>/<date?>
Auth:   free MAP_KEY (registered at firms.modaps.eosdis.nasa.gov/api/map_key);
        passed via FIRMS_MAP_KEY env var, never logged.
Output: data/firms.json   — MODIS  (Aqua + Terra), 24h US-wide
        data/viirs.json   — VIIRS  (S-NPP + NOAA-20), 24h US-wide
        data/health.json  — pipeline watchdog (consecutive_failures, status)
"""
import csv
import io
import json
import os
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

MAP_KEY = os.environ.get('FIRMS_MAP_KEY', '').strip()
if not MAP_KEY:
    sys.exit('FIRMS_MAP_KEY env var required; register at firms.modaps.eosdis.nasa.gov/api/map_key')

# US-wide bbox covering CONUS + AK + HI + PR. Identical to the
# index.html _USWIDE_ENVELOPE the frontend already trusts.
US_BBOX = '-180,15,-65,72'   # minLon,minLat,maxLon,maxLat

# Day range — FIRMS API takes 1-10 days back. 1 day matches the
# MODIS_Thermal_v1 "48h" window we used to query (close enough; FIRMS
# 1-day is rolling 24h from the run timestamp).
DAYS = 1

# Per FIRMS docs: VIIRS_SNPP_NRT (legacy S-NPP) + VIIRS_NOAA20_NRT
# (NOAA-20) are the two operational VIIRS satellites; we union them in
# the output JSON tagged by satellite. MODIS_NRT is Aqua + Terra
# combined in a single feed.
SOURCES = {
    'firms.json': ['MODIS_NRT'],                                    # MODIS Aqua + Terra
    'viirs.json': ['VIIRS_SNPP_NRT', 'VIIRS_NOAA20_NRT'],           # S-NPP + NOAA-20
}

# Hard ceilings. FIRMS API doesn't paginate but rate-limits at ~5000
# transactions per 10 minutes per key, so on a really hot day with very
# active fires this could clip — we log a warning and ship what we got.
US_RECORD_HARD_CAP = {
    'firms.json':  20_000,
    'viirs.json': 100_000,
}

API_BASE = 'https://firms.modaps.eosdis.nasa.gov/api/area/csv'
HTTP_TIMEOUT = 60


def fetch_source(source: str) -> list[dict]:
    """Pull one FIRMS source as CSV + parse into list[dict]. Returns
    [] on transient failure; raises on misconfigured request."""
    url = f'{API_BASE}/{MAP_KEY}/{source}/{US_BBOX}/{DAYS}'
    req = urllib.request.Request(url, headers={'User-Agent': 'firestorm-firms-data/1.0'})
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            body = resp.read().decode('utf-8', errors='replace')
    except urllib.error.HTTPError as e:
        # 401 = bad MAP_KEY. 429 = rate limited. 5xx = transient. We
        # don't want any of these to silently produce stale JSON, so
        # bubble the failure up to the watchdog.
        sys.stderr.write(f'[fetch_source] HTTPError {e.code} for {source}: {e.reason}\n')
        return []
    except urllib.error.URLError as e:
        sys.stderr.write(f'[fetch_source] URLError for {source}: {e.reason}\n')
        return []

    # FIRMS API returns text "Invalid MAP_KEY." literally on 200 with
    # bad key — guard against that.
    if body.startswith('Invalid '):
        sys.exit(f'[fetch_source] {source}: {body.strip()[:80]}')

    rows = []
    for r in csv.DictReader(io.StringIO(body)):
        try:
            lat = float(r.get('latitude', ''))
            lng = float(r.get('longitude', ''))
        except (TypeError, ValueError):
            continue
        if not (-90 <= lat <= 90 and -180 <= lng <= 180):
            continue
        # Normalize MODIS + VIIRS into the same shape. FRP and brightness
        # are common; bright_ti4/bright_ti5 are VIIRS-specific (the two
        # thermal bands), brightness/bright_t31 are MODIS-specific. We
        # surface a single `brightness_k` (Kelvin) field that the frontend
        # uses for the heat-color ramp, plus the raw fields for popups.
        bright = r.get('brightness') or r.get('bright_ti4') or r.get('bright_t31')
        try:
            bright_k = float(bright) if bright else None
        except ValueError:
            bright_k = None
        try:
            frp = float(r.get('frp')) if r.get('frp') else None
        except ValueError:
            frp = None
        rows.append({
            'lat': round(lat, 4),
            'lng': round(lng, 4),
            'brightness_k': bright_k,
            'frp': frp,
            'confidence': r.get('confidence') or None,
            'acq_date': r.get('acq_date') or None,
            'acq_time': r.get('acq_time') or None,
            'satellite': r.get('satellite') or source.replace('_NRT', '').replace('_SP', ''),
            'sensor': 'MODIS' if 'MODIS' in source else 'VIIRS',
            'daynight': r.get('daynight') or None,
        })
    return rows


def build_output(out_filename: str, sources: list[str]) -> tuple[list[dict], list[str]]:
    """Pull each FIRMS source for this output file, union them, return
    (combined rows, list of source labels for which fetch failed)."""
    combined: list[dict] = []
    failed: list[str] = []
    for src in sources:
        sys.stderr.write(f'[fetch] {src} ...\n')
        rows = fetch_source(src)
        if not rows:
            failed.append(src)
            continue
        sys.stderr.write(f'[fetch] {src}: {len(rows)} rows\n')
        combined.extend(rows)
    cap = US_RECORD_HARD_CAP.get(out_filename, 100_000)
    if len(combined) > cap:
        sys.stderr.write(f'[cap] {out_filename}: {len(combined)} > {cap}, truncating\n')
        combined = combined[:cap]
    return combined, failed


def write_json(path: str, payload: dict) -> None:
    tmp = path + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(payload, f, separators=(',', ':'))
    os.replace(tmp, path)


def main() -> int:
    now_iso = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    overall_failed = []

    for out_filename, sources in SOURCES.items():
        rows, failed = build_output(out_filename, sources)
        overall_failed.extend(failed)
        out_path = os.path.join('data', out_filename)
        write_json(out_path, {
            'generated_utc': now_iso,
            'window_hours': DAYS * 24,
            'envelope': {'min_lng': -180, 'min_lat': 15, 'max_lng': -65, 'max_lat': 72},
            'sources': sources,
            'sources_failed_this_run': failed,
            'count': len(rows),
            'detections': rows,
        })
        sys.stderr.write(f'[write] {out_path}: {len(rows)} rows\n')

    # Watchdog: load existing health, increment consecutive_failures only
    # when ALL sources for a run failed. One source falling back is
    # warning-noise, both is real degradation.
    health_path = os.path.join('data', 'health.json')
    try:
        with open(health_path) as f:
            health = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        health = {'consecutive_failures': 0, 'last_success_utc': None, 'status': 'unknown'}

    all_failed = bool(overall_failed) and len(overall_failed) >= len(sum(SOURCES.values(), []))
    if all_failed:
        health['consecutive_failures'] = health.get('consecutive_failures', 0) + 1
    else:
        health['consecutive_failures'] = 0
        health['last_success_utc'] = now_iso

    # Status flips to "degraded" after 4 consecutive misses ~ 1h silent
    # at our 15-min cron. Same threshold as firestorm-imsr-data.
    if health['consecutive_failures'] >= 4:
        health['status'] = 'degraded'
    elif health['consecutive_failures'] == 0:
        health['status'] = 'ok'
    else:
        health['status'] = 'flaky'

    health['last_run_utc'] = now_iso
    health['last_run_failed_sources'] = overall_failed
    write_json(health_path, health)
    sys.stderr.write(f'[health] consecutive_failures={health["consecutive_failures"]} status={health["status"]}\n')

    return 1 if all_failed else 0


if __name__ == '__main__':
    raise SystemExit(main())
