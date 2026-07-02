"""VIIRS/MODIS false-positive deconfliction — spatial join against
FIRESTORM's industrial-sites reference layer.

Given a stream of FIRMS detections (already normalized to the pipeline's
lat/lng/frp/confidence shape), this module adds:

    nearest_infra_m       int  — distance in meters to the closest known thermal source
    nearest_infra_id      str  — provenance-safe source id, e.g. epa_frs:110000... / gvp:333010
    nearest_infra_class   str  — one of: industrial | volcano | (future: solar)
    nearest_infra_name    str  — human-readable source name for operator tooltips
    deconfliction_flag    str  — one of:
                                    clear                 (real fire candidate — nothing nearby, on land, not low-confidence)
                                    low_confidence        (VIIRS confidence == 'low')
                                    proximate_industrial  (within buffer of EPA-FRS thermal facility)
                                    proximate_volcano     (within buffer of GVP volcano summit)
                                    offshore              (detection is over water per Natural Earth 10m land)

The buffer distance is per-source (each industrial site publishes its own
buffer_m — MVP: 500m for EPA-FRS points, 5000m for GVP volcanoes).

Data-source URL is public — served from firestorm-industrial-sites via
raw.githubusercontent.com, same pattern as every other FIRESTORM feed.

Failure modes:
  - If the industrial-sites layer is unreachable at ingest, we log a warning
    and every detection gets deconfliction_flag='clear' (no change to
    behavior). We NEVER hard-fail — the fire feed is life-safety, the
    filter is a nice-to-have on top.
  - If sklearn is unavailable in the runtime, we fall back to a pure-Python
    brute-force nearest-neighbor. Slower (~ms/detection at 5k sources) but
    correct.
"""
import io
import json
import math
import os
import sys
import urllib.request
import zipfile
from typing import Optional

# Public raw URLs — same pattern as firestorm-firms-data → firestorm-lightning-data etc.
INDUSTRIAL_SITES_URL = 'https://raw.githubusercontent.com/Deasus/firestorm-industrial-sites/main/data/industrial_sites.min.geojson'
LAND_POLY_URL        = 'https://raw.githubusercontent.com/Deasus/firestorm-industrial-sites/main/data/land_10m.geojson'

# Meters conversion for haversine distances (Earth radius commonly cited)
EARTH_RADIUS_M = 6_371_000.0

HTTP_TIMEOUT = 30


def _http_json(url: str) -> Optional[dict]:
    """Best-effort fetch of a JSON URL. Returns None on any failure — the
    deconfliction layer is a nice-to-have, not a fail-hard dep."""
    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'firestorm-firms-data/1.0',
            'Accept-Encoding': 'gzip',
        })
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            body = resp.read()
            # Handle gzip manually — urllib doesn't auto-decompress
            if resp.headers.get('Content-Encoding') == 'gzip':
                import gzip
                body = gzip.decompress(body)
            return json.loads(body.decode('utf-8', errors='replace'))
    except Exception as e:
        sys.stderr.write(f'[deconflict] fetch failed for {url}: {e}\n')
        return None


class Deconflicter:
    """Load-once, enrich-many. Instantiate at pipeline start; call enrich()
    for each detection.

    Attributes:
        loaded          bool — True if reference data was fetched and indexed
        n_sources       int  — number of point sources in the index
        has_land        bool — True if the land polygon was loaded (offshore test enabled)
    """

    def __init__(self) -> None:
        self.loaded = False
        self.has_land = False
        self.n_sources = 0
        self._tree = None                  # sklearn BallTree if available, else None
        self._lats_rad: list[float] = []   # parallel arrays, brute-force path
        self._lngs_rad: list[float] = []
        self._props: list[dict] = []
        self._land_bboxes: list[tuple[float, float, float, float]] = []  # (minlat,maxlat,minlng,maxlng)
        self._land_polys: list[list[list[list[float]]]] = []             # per-poly list of rings

    def load(self) -> None:
        """Fetch both reference layers and build the spatial index. Idempotent."""
        if self.loaded:
            return
        sys.stderr.write('[deconflict] fetching industrial_sites.min.geojson...\n')
        fc = _http_json(INDUSTRIAL_SITES_URL)
        if not fc or not isinstance(fc.get('features'), list):
            sys.stderr.write('[deconflict] industrial sites unavailable; enrichment disabled\n')
            return

        feats = fc['features']
        for f in feats:
            geom = f.get('geometry') or {}
            if geom.get('type') != 'Point':
                continue
            coords = geom.get('coordinates') or []
            if len(coords) < 2:
                continue
            try:
                lng, lat = float(coords[0]), float(coords[1])
            except (TypeError, ValueError):
                continue
            self._lats_rad.append(math.radians(lat))
            self._lngs_rad.append(math.radians(lng))
            self._props.append(f.get('properties') or {})
        self.n_sources = len(self._props)
        sys.stderr.write(f'[deconflict] indexed {self.n_sources} point sources\n')

        # Prefer sklearn BallTree when available — O(log N) per query, negligible at 5k sources
        try:
            from sklearn.neighbors import BallTree
            import numpy as np
            if self.n_sources > 0:
                pts = np.column_stack([self._lats_rad, self._lngs_rad])
                self._tree = BallTree(pts, metric='haversine')
                sys.stderr.write('[deconflict] BallTree indexed\n')
        except ImportError:
            sys.stderr.write('[deconflict] sklearn not available — brute-force nearest-neighbor\n')

        # Land polygon (offshore test)
        sys.stderr.write('[deconflict] fetching land_10m.geojson...\n')
        land_fc = _http_json(LAND_POLY_URL)
        if land_fc and isinstance(land_fc.get('features'), list):
            for f in land_fc['features']:
                geom = f.get('geometry') or {}
                t = geom.get('type')
                if t == 'Polygon':
                    self._add_polygon(geom.get('coordinates') or [])
                elif t == 'MultiPolygon':
                    for poly in geom.get('coordinates') or []:
                        self._add_polygon(poly)
            self.has_land = bool(self._land_polys)
            sys.stderr.write(f'[deconflict] indexed {len(self._land_polys)} land polygons\n')

        self.loaded = True

    def _add_polygon(self, coords: list) -> None:
        """Store polygon + bbox for fast pre-filtering."""
        if not coords or not coords[0]:
            return
        outer = coords[0]
        lats = [pt[1] for pt in outer if len(pt) >= 2]
        lngs = [pt[0] for pt in outer if len(pt) >= 2]
        if not lats or not lngs:
            return
        self._land_bboxes.append((min(lats), max(lats), min(lngs), max(lngs)))
        self._land_polys.append(coords)

    def enrich(self, det: dict) -> dict:
        """Given a detection dict with lat/lng/confidence keys, return a new
        dict of enrichment fields to merge in. Never mutates the input."""
        out = {
            'nearest_infra_m':     None,
            'nearest_infra_id':    None,
            'nearest_infra_class': None,
            'nearest_infra_name':  None,
            'deconfliction_flag':  'clear',
        }

        # Confidence-low is authoritative regardless of proximity — NASA says
        # dropping confidence=low kills ~80% of false positives. We flag but
        # do not drop (life-safety default: never suppress silently).
        conf = (det.get('confidence') or '').strip().lower()
        if conf == 'low':
            out['deconfliction_flag'] = 'low_confidence'

        lat = det.get('lat')
        lng = det.get('lng')
        if lat is None or lng is None:
            return out

        # Nearest source (only if we have any indexed at all)
        if self.loaded and self.n_sources > 0:
            nearest_i, dist_m = self._nearest(lat, lng)
            if nearest_i is not None:
                props = self._props[nearest_i]
                out['nearest_infra_m']    = int(dist_m)
                out['nearest_infra_id']   = props.get('source_id')
                out['nearest_infra_class'] = props.get('class')
                out['nearest_infra_name'] = props.get('name')
                buffer_m = props.get('buffer_m') or _default_buffer(props.get('class'))
                if dist_m <= buffer_m:
                    # confidence-low outranks proximate flags — if it's low-conf
                    # AND near an industrial site, both facts are true but
                    # low_confidence is the more actionable filter (operator
                    # picks the buttion that hides low-conf globally).
                    if out['deconfliction_flag'] == 'clear':
                        cls = props.get('class')
                        if cls == 'volcano':
                            out['deconfliction_flag'] = 'proximate_volcano'
                        elif cls == 'industrial':
                            out['deconfliction_flag'] = 'proximate_industrial'
                        elif cls == 'solar':                 # forward-compat for v2
                            out['deconfliction_flag'] = 'proximate_solar'
                        else:
                            out['deconfliction_flag'] = 'proximate_' + str(cls)

        # Offshore test (only apply if the detection isn't already flagged as
        # near a known industrial/volcano source — those subsume it)
        if self.has_land and out['deconfliction_flag'] in ('clear', 'low_confidence'):
            if not self._is_on_land(lat, lng):
                # An offshore hit near a known industrial source (offshore
                # flare platform) would already be flagged above; anything
                # remaining offshore + not-flagged is sun-glint / ship /
                # unregistered platform — call it offshore.
                out['deconfliction_flag'] = 'offshore' if out['deconfliction_flag'] == 'clear' else out['deconfliction_flag']

        return out

    def _nearest(self, lat: float, lng: float) -> tuple[Optional[int], float]:
        """Return (index, distance_m) of the closest source."""
        lat_r = math.radians(lat)
        lng_r = math.radians(lng)
        if self._tree is not None:
            import numpy as np
            dist_rad, idx = self._tree.query(np.array([[lat_r, lng_r]]), k=1)
            i = int(idx[0][0])
            d = float(dist_rad[0][0]) * EARTH_RADIUS_M
            return i, d
        # Brute force fallback
        best_i = -1
        best_d = float('inf')
        for i in range(self.n_sources):
            d = _haversine_rad(lat_r, lng_r, self._lats_rad[i], self._lngs_rad[i])
            if d < best_d:
                best_d = d
                best_i = i
        if best_i < 0:
            return None, float('inf')
        return best_i, best_d * EARTH_RADIUS_M

    def _is_on_land(self, lat: float, lng: float) -> bool:
        """Return True if (lat, lng) is inside any land polygon."""
        for bbox, poly in zip(self._land_bboxes, self._land_polys):
            min_lat, max_lat, min_lng, max_lng = bbox
            if lat < min_lat or lat > max_lat or lng < min_lng or lng > max_lng:
                continue
            if _point_in_polygon(lng, lat, poly):
                return True
        return False


def _default_buffer(cls: Optional[str]) -> int:
    if cls == 'volcano':
        return 5000
    if cls == 'solar':
        return 375
    return 500


def _haversine_rad(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in radians (multiply by EARTH_RADIUS_M for meters)."""
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * math.asin(min(1.0, math.sqrt(a)))


def _point_in_polygon(x: float, y: float, poly_rings: list) -> bool:
    """Ray-casting point-in-polygon. poly_rings is [[outer], [hole1], ...]
    Point is 'inside' if inside outer ring AND not inside any hole.
    """
    if not poly_rings:
        return False
    if not _in_ring(x, y, poly_rings[0]):
        return False
    for hole in poly_rings[1:]:
        if _in_ring(x, y, hole):
            return False
    return True


def _in_ring(x: float, y: float, ring: list) -> bool:
    """Even-odd ray casting on a single ring."""
    n = len(ring)
    if n < 3:
        return False
    inside = False
    j = n - 1
    for i in range(n):
        xi, yi = ring[i][0], ring[i][1]
        xj, yj = ring[j][0], ring[j][1]
        if ((yi > y) != (yj > y)) and (x < (xj - xi) * (y - yi) / ((yj - yi) or 1e-12) + xi):
            inside = not inside
        j = i
    return inside
