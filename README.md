# firestorm-firms-data

NASA FIRMS (MODIS + VIIRS) thermal-anomaly pipeline for FIRESTORM.

## Why this exists

The frontend used to query the ESRI Living Atlas FeatureServer mirror of
MODIS_Thermal_v1 and Satellite_VIIRS_Thermal_Hotspots directly. That host
has three quirks that cost ~8 rounds of patches:

1. **MODIS layer has no `latitude`/`longitude` attributes.** Only
   `OBJECTID`, `BRIGHTNESS`, `SCAN`, `TRACK`, `SATELLITE`, `CONFIDENCE`,
   `VERSION`, `BRIGHT_T31`, `FRP`, `ACQ_DATE`, `DAYNIGHT`, `HOURS_OLD`,
   `DAY_OF_ACQ`. VIIRS layer has them. Same vendor, two layers, two
   schemas — single extract path can't handle both.
2. **Native SR is Web Mercator (`wkid:102100`).** Geometry x/y are
   meters from the prime meridian, not lat/lng. Required `outSR:4326`
   tax on every query, or client-side projection math.
3. **16,000 records-per-page hard cap.** VIIRS US-wide is ~41k records
   → 3 sequential paged queries. AI tools that read mid-paginate get
   partial answers that contradict the badge.

This pipeline normalizes both sensors into the same JSON shape and
removes every one of those quirks at the source.

## Output

| File | Contents |
|---|---|
| `data/firms.json` | MODIS Aqua + Terra, 24h US-wide |
| `data/viirs.json` | VIIRS S-NPP + NOAA-20, 24h US-wide |
| `data/health.json` | Pipeline watchdog (consecutive failures, status) |

Detection record shape (both files):

```json
{
  "lat": 36.4012,
  "lng": -119.2107,
  "brightness_k": 327.4,
  "frp": 18.3,
  "confidence": "nominal",
  "acq_date": "2026-06-09",
  "acq_time": "1842",
  "satellite": "Aqua",
  "sensor": "MODIS",
  "daynight": "D"
}
```

## Auth

Requires a free NASA FIRMS MAP_KEY. Register at
https://firms.modaps.eosdis.nasa.gov/api/map_key — instant email.

Set as `FIRMS_MAP_KEY` GitHub Actions secret on the repo.

## Cadence

`*/15 * * * *` cron + 4-cycle in-run loop with `gh workflow run`
self-redispatch. Effective cadence ~3 min. Same pattern as
firestorm-lightning-data, firestorm-ngfs-data, firestorm-goes-fire-data.

FIRMS API rate-limits at ~5000 transactions/10 min per key — well above
our cadence (~16 fetches/hour × 3 sources = 48/hour).

## Caveats

- US-wide bbox `-180,15,-65,72` covers CONUS + AK + HI + PR. Globe
  outside this is dropped at the API level (the bbox is a server-side
  filter, so cost is paid by NASA not us).
- 1-day window is rolling 24h from the run timestamp. If you want a
  longer window, change `DAYS = 1` in `fetch_firms.py`.
- Hard cap: 20k records for MODIS, 100k for VIIRS. Fire-season days
  in the US peak around 41k VIIRS — well under the cap.
