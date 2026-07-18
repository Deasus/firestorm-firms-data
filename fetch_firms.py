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
import socket
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

from deconflict import Deconflicter   # v2_deconflict-mvp — spatial join against firestorm-industrial-sites

# v2 — force IPv4 for all urllib requests. GitHub Actions runners
# resolve firms.modaps.eosdis.nasa.gov to both A (198.118.194.34) and
# AAAA (2001:4d0:241a:40c0::34); Python 3.12 urllib prefers AAAA but
# the runner's IPv6 egress is broken/half-configured for that route,
# so every connection raises [Errno 101] Network is unreachable.
# Curl falls back to IPv4 automatically; urllib doesn't.
# Verified: same runner instance, same DNS, curl --ipv4 returns 200,
# urllib + default getaddrinfo throws errno 101.
# Fix: drop AAAA records from getaddrinfo so urllib only ever sees A.
_orig_getaddrinfo = socket.getaddrinfo
def _ipv4_only_getaddrinfo(*args, **kwargs):
    return [r for r in _orig_getaddrinfo(*args, **kwargs) if r[0] == socket.AF_INET]
socket.getaddrinfo = _ipv4_only_getaddrinfo

MAP_KEY = os.environ.get('FIRMS_MAP_KEY', '').strip()
if not MAP_KEY:
    sys.exit('FIRMS_MAP_KEY env var required; register at firms.modaps.eosdis.nasa.gov/api/map_key')

# US filter envelope (CONUS + AK + HI + PR). Applied client-side after
# fetch — the FIRMS API supports a `<minLon,minLat,maxLon,maxLat>` URL
# segment, BUT the literal commas in that path get silently re-encoded
# by something in the GitHub Actions egress chain (proxy / WAF / http
# library) to %2C, which FIRMS' router then rejects with HTTP 400
# 'Invalid area.'  Local curl + local Python urllib both work fine.
# Workaround: fetch the global feed (`world` keyword, ~155KB CSV) and
# filter to this envelope client-side.  No commas in the URL path,
# no encoding tax, sidesteps the GHA-egress mangling entirely.
# Trade-off: 1 extra MAP_KEY transaction and ~150KB more bytes per
# fetch — well under the 5000 tx / 10 min quota.
US_FILTER = (-180.0, 15.0, -65.0, 72.0)   # min_lng, min_lat, max_lng, max_lat

# FIRMS area API day_range is 1-10 (verified 2026-06-09 against live
# /api/area/csv endpoint with day=10 returning 200; older docs cap at
# 5 — both seem accepted today; staying at 1 for cadence).
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

# v2_deconflict-mvp — module-level singleton, populated on first use.
# Load-once at pipeline start: the industrial_sites layer is a slow-moving
# reference (~weekly refresh upstream). Every detection then gets enriched
# without re-fetching the ~200 KB reference file per source.
_DECONFLICTER: Deconflicter | None = None

def _get_deconflicter() -> Deconflicter:
    global _DECONFLICTER
    if _DECONFLICTER is None:
        _DECONFLICTER = Deconflicter()
        _DECONFLICTER.load()   # NEVER raises — best-effort by design
    return _DECONFLICTER


def fetch_source(source: str) -> list[dict]:
    """Pull one FIRMS source as CSV + parse into list[dict]. Returns
    [] on transient failure; raises on misconfigured request.

    v2_348l — retry ladder extended from 3 to 5 attempts with longer
    URLError backoff (2s, 4s, 8s, 16s) after operators reported
    email-alert noise 2026-07-17 driven by NASA FIRMS API transient
    timeouts on VIIRS_SNPP_NRT + VIIRS_NOAA20_NRT. Log inspection
    showed the failing runs saw the same URL timeout on all 3
    attempts within a ~2-minute window; a 5th attempt at t+30s often
    succeeds once NASA's server settles.
    """
    # `world` instead of bbox — see US_FILTER comment up top for why.
    url = f'{API_BASE}/{MAP_KEY}/{source}/world/{DAYS}'
    headers = {'User-Agent': 'firestorm-firms-data/1.0',
               'Accept': 'text/csv,*/*'}
    last_err = None
    MAX_ATTEMPTS = 5
    for attempt in range(MAX_ATTEMPTS):
        if attempt:
            # v2_348l — exponential backoff, cap at 16s. Total worst-case
            # per source across 5 attempts: 2+4+8+16 = 30s of backoff plus
            # 5×60s HTTP timeouts = ~5.5 min. Fits within the 15-min
            # workflow timeout for BOTH cycles even if all sources go
            # bad simultaneously.
            time.sleep(min(2 ** attempt, 16))
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                body = resp.read().decode('utf-8', errors='replace')
            break   # success
        except urllib.error.HTTPError as e:
            last_err = e
            # CRITICAL — read the response body. FIRMS returns the actual
            # error message in the 400 body ('Invalid MAP_KEY.', 'Invalid
            # area.', 'Invalid day range.'). Without this, every failure
            # mode looks identical and we burn rounds guessing.
            try:
                err_body = e.read().decode('utf-8', errors='replace')[:300]
            except Exception:
                err_body = '<no body>'
            sys.stderr.write(f'[fetch_source] attempt {attempt+1}/{MAX_ATTEMPTS}: HTTPError {e.code} for {source}: {e.reason} | body={err_body!r}\n')
            # Loud bail on the things that will never resolve via retry:
            #   - Bad/expired/whitespace-padded MAP_KEY (FIRMS returns 400
            #     'Invalid MAP_KEY.' — confusing because it's the same
            #     status code as a transient 400, distinguished by body)
            #   - 401/403 — explicit auth failure
            if 'Invalid MAP_KEY' in err_body:
                sys.exit(f'[fetch_source] FIRMS rejected MAP_KEY (env var len={len(MAP_KEY)}): {err_body.strip()[:120]}')
            if e.code in (401, 403):
                return []
            continue
        except urllib.error.URLError as e:
            last_err = e
            sys.stderr.write(f'[fetch_source] attempt {attempt+1}/{MAX_ATTEMPTS}: URLError for {source}: {e.reason}\n')
            continue
    else:
        sys.stderr.write(f'[fetch_source] all retries exhausted for {source}; giving up\n')
        return []

    # FIRMS API returns text "Invalid MAP_KEY." literally on 200 with
    # bad key — guard against that.
    if body.startswith('Invalid '):
        sys.exit(f'[fetch_source] {source}: {body.strip()[:80]}')

    rows = []
    min_lng, min_lat, max_lng, max_lat = US_FILTER
    for r in csv.DictReader(io.StringIO(body)):
        try:
            lat = float(r.get('latitude', ''))
            lng = float(r.get('longitude', ''))
        except (TypeError, ValueError):
            continue
        # Client-side filter to the US envelope. The `world` URL returns
        # the global FIRMS feed (~all continents); we want only CONUS +
        # AK + HI + PR for the dashboard.
        if not (min_lat <= lat <= max_lat and min_lng <= lng <= max_lng):
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
        det = {
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
        }
        # v2_deconflict-mvp — inline enrichment. Adds nearest_infra_m,
        # nearest_infra_id, nearest_infra_class, nearest_infra_name,
        # deconfliction_flag. Never raises — falls through to
        # {flag='clear'} on any error.
        try:
            det.update(_get_deconflicter().enrich(det))
        except Exception as _e:
            sys.stderr.write(f'[deconflict] enrichment failed on one row: {_e}\n')
            det.setdefault('deconfliction_flag', 'clear')
        rows.append(det)
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
    # Ensure parent dir exists (data/ doesn't get tracked by git when empty,
    # so a fresh runner clone won't have it).
    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
    tmp = path + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(payload, f, separators=(',', ':'))
    os.replace(tmp, path)


def main() -> int:
    now_iso = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    overall_failed = []

    # Pre-flight: probe the MAP_KEY status endpoint. Surfaces (a) auth
    # validity, (b) current_transactions vs transaction_limit (5000/10min),
    # (c) the fact that the secret made it to the runner intact. Catches
    # the silent "secret has trailing whitespace / is empty / never
    # propagated" failure mode that v1 of this pipeline hit on GHA.
    try:
        status_url = f'https://firms.modaps.eosdis.nasa.gov/mapserver/mapkey_status/?MAP_KEY={MAP_KEY}'
        with urllib.request.urlopen(status_url, timeout=15) as r:
            status_body = r.read().decode('utf-8', errors='replace')
        sys.stderr.write(f'[mapkey_status] (key_len={len(MAP_KEY)}) {status_body[:300]}\n')
    except Exception as e:
        sys.stderr.write(f'[mapkey_status] probe failed (key_len={len(MAP_KEY)}): {e}\n')

    for out_filename, sources in SOURCES.items():
        rows, failed = build_output(out_filename, sources)
        overall_failed.extend(failed)
        # v2_deconflict-mvp — per-run deconfliction summary. Operators/devs
        # can watch the "flagged vs clear" ratio on the GHA run summary +
        # in the output JSON itself. Frontend reads this to render the
        # header chip row (FIRE ANOMALIES / DECONFLICTED / INDUSTRIAL / ...).
        flag_counts: dict[str, int] = {}
        for det in rows:
            flag_counts[det.get('deconfliction_flag') or 'clear'] = \
                flag_counts.get(det.get('deconfliction_flag') or 'clear', 0) + 1
        clear_n = flag_counts.get('clear', 0)
        pct_clear = (100.0 * clear_n / len(rows)) if rows else 0.0
        sys.stderr.write(f'[deconflict] {out_filename}: {clear_n}/{len(rows)} clear ({pct_clear:.0f}%) | flags={flag_counts}\n')

        out_path = os.path.join('data', out_filename)
        write_json(out_path, {
            'generated_utc': now_iso,
            'window_hours': DAYS * 24,
            'envelope': {'min_lng': -180, 'min_lat': 15, 'max_lng': -65, 'max_lat': 72},
            'sources': sources,
            'sources_failed_this_run': failed,
            'count': len(rows),
            'deconfliction_counts': flag_counts,
            'deconfliction_meta': {
                'reference_layer_loaded': (_DECONFLICTER.loaded if _DECONFLICTER else False),
                'n_sources':              (_DECONFLICTER.n_sources if _DECONFLICTER else 0),
                'has_land_polygon':       (_DECONFLICTER.has_land if _DECONFLICTER else False),
            },
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

    # v2_348l — exit-code threshold on consecutive_failures, not per-run
    # all_failed. Operators reported email-alert noise 2026-07-17: NASA
    # FIRMS transient timeouts cause single-run failures ~13% of the time,
    # but GHA emails on every non-zero exit code. Under the old return
    # rule that was 6-10 emails/day for what was really just NASA hiccuping
    # for one cycle. New rule: exit 1 only when consecutive_failures >= 3
    # (roughly 45 min of continuous failure at our 15-min cron — the same
    # threshold at which health.status flips to 'flaky' via >=1, 'degraded'
    # via >=4). Within-threshold failures still write health.json so the
    # firestorm-health tool + operator dashboard can surface the flap, and
    # they still leave [health] logs in the GHA UI for anyone reading the
    # run details — they just don't page.
    ALERT_THRESHOLD = 3
    should_alert = health['consecutive_failures'] >= ALERT_THRESHOLD
    if all_failed and not should_alert:
        sys.stderr.write(
            f'[exit] all_failed=true but consecutive_failures={health["consecutive_failures"]} '
            f'< {ALERT_THRESHOLD} threshold — exiting 0 to suppress single-hiccup alert '
            f'(health.json still records the flap)\n'
        )
    return 1 if should_alert else 0


if __name__ == '__main__':
    raise SystemExit(main())
