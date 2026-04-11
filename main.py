"""Orchestrate the complete airfoil database build pipeline.

This module is the single entrypoint for rebuilding the full project dataset
from source geometry and derived artifacts.

Pipeline order:
1. ``build_profiles_db.py``
   Imports UIUC geometry files, normalizes the airfoils to a fixed point
   count, force-closes the trailing edge for CAD-oriented output, validates
   the geometry, and writes ``profiles.db``.
2. ``build_usage_db.py``
   Scrapes and structures the UIUC airfoil usage tables into ``usage.db``.
3. ``build_polars_db.py``
   Runs XFOIL on the eligible profiles and stores polars plus per-run status
   in ``polars.db``.
4. ``build_ratings_db.py``
   Computes lightweight relative ratings from geometry and polar data and
   stores them in ``polars.db``.
5. ``merge_airfoil_db.py``
   Merges the staging databases into the final production database
   ``output/airfoil.db``.

The default behavior is a full rebuild: each staging database is recreated
from scratch unless the corresponding ``reset_*`` flag is disabled.

Notes:
- ``profiles.db``, ``usage.db``, and ``polars.db`` live under ``_local/db``.
- The final deliverable is ``output/airfoil.db``.
- If a prerequisite step is skipped, downstream modules may fail with explicit
  guidance about which builder must be run first.
- This module is intended as the normal command-line entrypoint:
  ``python main.py``.
"""

from __future__ import annotations

from build_polars_db import build_polars_database
from build_profiles_db import build_profiles_database
from build_ratings_db import build_ratings_database
from build_usage_db import build_usage_database
from merge_airfoil_db import merge_databases


def main(
    force_redownload_profiles: bool = False,
    reset_profiles: bool = True,
    reset_usage: bool = True,
    reset_polars: bool = True,
    reset_ratings: bool = True,
) -> None:
    """Run the full pipeline in dependency order.

    Args:
        force_redownload_profiles:
            When ``True``, forces a fresh download of the UIUC geometry archive
            before rebuilding ``profiles.db``.
        reset_profiles:
            When ``True``, recreates ``profiles.db`` from scratch.
        reset_usage:
            When ``True``, recreates ``usage.db`` from scratch.
        reset_polars:
            When ``True``, recreates ``polars.db`` from scratch.
        reset_ratings:
            When ``True``, drops and rebuilds the rating tables inside
            ``polars.db`` before the final merge.

    This function does not return data. Its effect is the creation or refresh
    of the staging databases and the final merged ``output/airfoil.db``.
    """
    print("[STEP 1/5] Building profiles.db")
    build_profiles_database(
        force_redownload=force_redownload_profiles,
        reset_db=reset_profiles,
    )

    print("\n[STEP 2/5] Building usage.db")
    build_usage_database(reset_db=reset_usage)

    print("\n[STEP 3/5] Building polars.db")
    build_polars_database(reset_db=reset_polars)

    print("\n[STEP 4/5] Building airfoil ratings")
    build_ratings_database(reset_db=reset_ratings)

    print("\n[STEP 5/5] Merging into airfoil.db")
    merge_databases()


if __name__ == "__main__":
    main()
