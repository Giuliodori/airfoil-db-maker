# Pipeline Details

This document describes the technical behavior of the repository. If you only want the final database, download it from [Releases](https://github.com/Giuliodori/airfoil-db-maker/releases).

## Final Artifact

The production output is:
- `output/airfoil.db`

The public final database is sanitized during merge:
- raw upstream coordinate text is removed from the final artifact
- local filesystem paths used during staging are removed from the final artifact

Intermediate staging databases:
- `_local/db/profiles.db`
- `_local/db/usage.db`
- `_local/db/polars.db`

## Build Modules

### `build_profiles_db.py`

Builds `profiles.db`.

What it does:
- downloads the official UIUC coordinate archive
- extracts `.dat` airfoil files
- parses point coordinates
- normalizes profiles to unit chord
- resamples profile points to a consistent downstream format
- closes the trailing edge for CAD-ready curves
- runs geometry validation checks
- quarantines suspicious profiles
- re-imports manually reviewed airfoils from quarantine

Primary outputs:
- `_local/db/profiles.db`
- `_local/normalized/uiuc/`
- `_local/quarantine_profiles/uiuc/`
- `_local/quarantine_reviewed/uiuc/`

### `build_usage_db.py`

Builds `usage.db`.

What it does:
- downloads the UIUC airfoil usage page
- extracts structured aircraft/airfoil rows
- normalizes airfoil labels
- tries to match usage names to canonical profiles present in `profiles.db`

Primary outputs:
- `_local/db/usage.db`
- `_local/raw/usage/`

### `build_polars_db.py`

Builds `polars.db`.

What it does:
- reads eligible profiles from `profiles.db`
- checks for `tools/xfoil.exe`
- if missing, tries to download `XFOIL6.99.zip` from the MIT XFOIL distribution page and extract `xfoil.exe`
- runs a quick XFOIL gate at `alpha = 0`
- runs a conservative alpha sweep on passing profiles
- stores polar rows in SQLite
- stores per-airfoil run summaries in `airfoil_xfoil_runs`

Primary outputs:
- `_local/db/polars.db`
- `_local/xfoil/airfoils_dat/`
- `_local/xfoil/polars/`
- `_local/xfoil/logs/`

### `build_ratings_db.py`

Builds rating tables inside `polars.db`.

What it does:
- reads geometry from `profiles.db`
- reads XFOIL polars and run quality from `polars.db`
- reads usage/application diversity from `usage.db`
- computes relative dataset scores for performance, docility, robustness, confidence, and versatility
- stores aggregate ratings and supporting details

Primary outputs:
- `airfoil_ratings`
- `airfoil_rating_details`
- `airfoil_rating_reynolds`

### `merge_airfoil_db.py`

Builds the final database:
- copies `profiles.db` as the base
- imports usage tables from `usage.db`
- imports polar and rating tables from `polars.db`
- removes excluded airfoils
- removes orphan rows
- rebuilds `airfoil_usage_summary` and `airfoil_filter_presets` for runtime filters
- clears raw geometry payloads and local staging paths before publishing

### `main.py`

Runs the full pipeline end to end:
1. build profiles
2. build usage
3. build polars
4. build ratings
5. merge final database

## Database Content

Depending on the current build state, `output/airfoil.db` contains:
- `airfoils`
- `airfoil_sources`
- `airfoil_applications`
- `airfoil_polars_xfoil`
- `airfoil_ratings`
- `airfoil_usage_summary`
- `airfoil_filter_presets`

During staging (before final slimming), the build may also include:
- `aircraft_usage_rows`
- `airfoil_xfoil_runs`
- `airfoil_rating_details`
- `airfoil_rating_reynolds`
- `source_meta`

## Naming

Canonical airfoil names are compact and normalized:
- lowercase
- no spaces
- no `.`
- no `,`
- no `_`
- no `-`

Examples:
- `NACA 2412` -> `naca2412`
- `Clark Y` -> `clarky`
- `FX 63-137` -> `fx63137`

## Geometry Validation

Validation includes checks for:
- monotonic upper/lower `x`
- local negative thickness
- anomalous duplicate points
- excessive segment length
- spikes or sharp geometry artifacts
- self-intersections
- suspicious trailing edges

Profiles that fail validation are quarantined instead of entering the main dataset.

## XFOIL Configuration

Current defaults:
- Reynolds: `150000`, `250000`, `500000`, `1250000`
- Mach: `0.0`
- Ncrit: `9.0`
- alpha sweep: `-4` to `10` with step `2`

Run outcomes include:
- `ok`
- `partial_convergence`
- `no_convergence`
- `timeout`
- `gate_no_convergence`
- `gate_timeout`

Airfoils that fail real XFOIL compatibility checks can be excluded from the final merged database.

## Commands

Build only profiles:

```powershell
python build_profiles_db.py
```

Build only usage:

```powershell
python build_usage_db.py
```

Build only XFOIL polars:

```powershell
python build_polars_db.py
```

Build everything:

```powershell
python main.py
```

## Environment

Practical requirements:
- Python 3.11+
- network access if automatic XFOIL download is allowed

The build scripts use the Python standard library only.

## Operational Notes

- `main.py` performs a full rebuild of staging databases and the final merged database.
- `quarantine_reviewed/uiuc/` is intentionally preserved across rebuilds.
- `ONLY_NAMES` and `LIMIT_AIRFOILS` in `build_polars_db.py` are useful for quick XFOIL tuning runs.
- `tools/xfoil.exe` is not committed to the repository.
