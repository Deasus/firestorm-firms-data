"""Global VIIRS/MODIS outputs (2026-10-16).

fetch_firms.py has always downloaded the FIRMS *world* feed and then thrown away
everything outside the US envelope. This module keeps the rest, split by region,
in a compact columnar format the CINDER client loads per region tab.

Files (written to data/global/, published to the single-commit `global` branch by
the workflow -- never committed to main, so the repo history does not grow by
several MB every few minutes):

  index.json                 regions, bboxes, file names, counts, source status
  <region>-viirs.json        every VIIRS detection in the region (S-NPP, NOAA-20, NOAA-21)
  <region>-modis.json        every MODIS detection in the region (Aqua, Terra)
  overview.json              world view: per-region totals + 0.5 degree cells + top fires

Compact detection rows (same fields for VIIRS and MODIS):
  [lat, lng, frp, brightness_k, confidence, satellite, acq_date, acq_time, daynight,
   deconfliction_flag, nearest_infra_m, nearest_infra_class, nearest_infra_name]
(the three infra fields are null when the flag is 'clear' or nothing is nearby).

Regions are assigned first-match in REGIONS order, so every detection lands in
exactly one file.
"""
import json
import math
import os
import sys
from datetime import datetime, timezone

GLOBAL_DIR = os.path.join('data', 'global')

# (key, label, (min_lat, max_lat, min_lng, max_lng)) -- first match wins.
REGIONS = [
    ('europe',        'Europe',                      (34.0, 72.0, -25.0, 45.0)),
    ('middle-east',   'Middle East',                 (12.0, 42.0, 34.0, 63.0)),
    ('africa',        'Africa',                      (-36.0, 37.5, -26.0, 52.0)),
    ('oceania',       'Australia & Oceania',         (-50.0, 0.0, 112.0, 180.0)),
    ('south-america', 'South America & Amazonia',    (-56.0, 13.0, -92.0, -30.0)),
    ('north-america', 'North America',               (7.0, 84.0, -170.0, -50.0)),
    ('asia',          'Asia',                        (-11.0, 82.0, 26.0, 180.0)),
]
OTHER = ('other', 'Other (oceans, Pacific islands, polar)')
FIELDS = ['lat', 'lng', 'frp', 'brightness_k', 'confidence', 'satellite', 'acq_date', 'acq_time',
          'daynight', 'deconfliction_flag', 'nearest_infra_m', 'nearest_infra_class', 'nearest_infra_name']
SAT_TO_SRC = {'N': 'VIIRS_SNPP_NRT', 'N20': 'VIIRS_NOAA20_NRT', 'N21': 'VIIRS_NOAA21_NRT',
              'A': 'MODIS_NRT', 'T': 'MODIS_NRT', 'Aqua': 'MODIS_NRT', 'Terra': 'MODIS_NRT'}
CELL_DEG = 0.5
TOP_N = 500


def region_of(lat, lng):
    for key, _label, (a, b, c, d) in REGIONS:
        if a <= lat <= b and c <= lng <= d:
            return key
    return OTHER[0]


def _infra_name(det):
    """Site name + licence credit where the source requires it (ODbL / CC BY)."""
    name = det.get('nearest_infra_name')
    sid = str(det.get('nearest_infra_id') or '')
    if not name:
        return None
    if sid.startswith('osm:'):
        return f'{name} · © OpenStreetMap contributors'
    if sid.startswith('wri_gppd:'):
        return f'{name} · WRI GPPD (CC BY 4.0)'
    return name


def _row(det):
    flagged = (det.get('deconfliction_flag') or 'clear') != 'clear'
    return [det['lat'], det['lng'], det.get('frp'), det.get('brightness_k'), det.get('confidence'),
            det.get('satellite'), det.get('acq_date'), det.get('acq_time'), det.get('daynight'),
            det.get('deconfliction_flag') or 'clear',
            det.get('nearest_infra_m') if flagged else None,
            det.get('nearest_infra_class') if flagged else None,
            _infra_name(det) if flagged else None]


def _write(path, payload):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(payload, f, separators=(',', ':'))
    os.replace(tmp, path)


def _prev_rows(region, sensor):
    """Previous run's rows for one region file (restored from the `global` branch by
    the workflow). Used only to carry forward a source that failed this run."""
    try:
        with open(os.path.join(GLOBAL_DIR, f'{region}-{sensor}.json')) as f:
            return json.load(f).get('rows', [])
    except (OSError, ValueError):
        return []


def _in_window(row, now, hours):
    try:
        t = datetime.strptime(f'{row[6]} {int(row[7] or 0):04d}', '%Y-%m-%d %H%M').replace(tzinfo=timezone.utc)
        return (now - t).total_seconds() <= hours * 3600
    except (TypeError, ValueError, IndexError):
        return True


def write_global(all_rows, failed_sources, sources_by_sensor, now_iso, window_hours):
    """all_rows: {'viirs': [det...], 'modis': [det...]} -- global, already deconflicted.
    failed_sources: FIRMS source names that failed this run (their rows carry forward)."""
    now = datetime.strptime(now_iso[:19], '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc)
    keys = [r[0] for r in REGIONS] + [OTHER[0]]
    index = {'generated_utc': now_iso, 'window_hours': window_hours, 'fields': FIELDS,
             'regions': {}, 'sources': sources_by_sensor, 'sources_failed_this_run': failed_sources,
             'note': 'Every detection is assigned to exactly one region (first bbox match, REGIONS order).'}
    for key, label, bbox in REGIONS + [(OTHER[0], OTHER[1], None)]:
        index['regions'][key] = {'label': label, 'bbox': bbox, 'files': {}, 'counts': {}}

    cells = {}          # (lat_i, lng_i) -> [n_viirs, n_modis, frp_max, frp_sum, n_flagged]
    top = []
    for sensor, dets in all_rows.items():
        buckets = {k: [] for k in keys}
        for d in dets:
            buckets[region_of(d['lat'], d['lng'])].append(_row(d))
        failed_here = [s for s in failed_sources if s in sources_by_sensor.get(sensor, [])]
        for key in keys:
            rows = buckets[key]
            carried = 0
            if failed_here:
                for r in _prev_rows(key, sensor):
                    if SAT_TO_SRC.get(r[5]) in failed_here and _in_window(r, now, window_hours + 3):
                        rows.append(r)
                        carried += 1
            flags = {}
            for r in rows:
                flags[r[9]] = flags.get(r[9], 0) + 1
                ci = (math.floor(r[0] / CELL_DEG), math.floor(r[1] / CELL_DEG))
                c = cells.setdefault(ci, [0, 0, 0.0, 0.0, 0])
                c[0 if sensor == 'viirs' else 1] += 1
                frp = r[2] or 0.0
                c[2] = max(c[2], frp)
                c[3] += frp
                if r[9] != 'clear':
                    c[4] += 1
                elif frp > 0:
                    top.append((frp, key, r))
            fname = f'{key}-{sensor}.json'
            _write(os.path.join(GLOBAL_DIR, fname), {
                'generated_utc': now_iso, 'region': key, 'sensor': sensor.upper(),
                'window_hours': window_hours, 'fields': FIELDS, 'count': len(rows),
                'carried_forward_rows': carried, 'deconfliction_counts': flags, 'rows': rows,
            })
            index['regions'][key]['files'][sensor] = fname
            index['regions'][key]['counts'][sensor] = {'total': len(rows), 'clear': flags.get('clear', 0),
                                                       'flagged': len(rows) - flags.get('clear', 0)}
            sys.stderr.write(f'[global] {fname}: {len(rows)} rows ({flags.get("clear", 0)} clear, carried {carried})\n')

    top.sort(key=lambda t: -t[0])
    overview = {
        'generated_utc': now_iso, 'window_hours': window_hours, 'cell_deg': CELL_DEG,
        'cell_fields': ['lat', 'lng', 'n_viirs', 'n_modis', 'frp_max', 'frp_sum', 'n_flagged'],
        'cells': [[round((i + 0.5) * CELL_DEG, 3), round((j + 0.5) * CELL_DEG, 3), c[0], c[1],
                   round(c[2], 1), round(c[3], 1), c[4]] for (i, j), c in cells.items()],
        'top_fields': ['region'] + FIELDS,
        'top_clear_by_frp': [[k] + r for _f, k, r in top[:TOP_N]],
        'totals': {key: v['counts'] for key, v in index['regions'].items()},
    }
    _write(os.path.join(GLOBAL_DIR, 'overview.json'), overview)
    index['files'] = {'overview': 'overview.json'}
    _write(os.path.join(GLOBAL_DIR, 'index.json'), index)
    sys.stderr.write(f'[global] overview.json: {len(overview["cells"])} cells, top {min(TOP_N, len(top))} fires\n')
