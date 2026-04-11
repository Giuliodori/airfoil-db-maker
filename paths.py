from __future__ import annotations

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
LOCAL_DIR = BASE_DIR / "_local"
TOOLS_DIR = BASE_DIR / "tools"

DOWNLOAD_DIR = LOCAL_DIR / "downloads"
RAW_DIR = LOCAL_DIR / "raw"
RAW_UIUC_DIR = RAW_DIR / "uiuc"
NORMALIZED_DIR = LOCAL_DIR / "normalized"
NORMALIZED_UIUC_DIR = NORMALIZED_DIR / "uiuc"
QUARANTINE_DIR = LOCAL_DIR / "quarantine_profiles"
QUARANTINE_UIUC_DIR = QUARANTINE_DIR / "uiuc"
REVIEWED_QUARANTINE_DIR = LOCAL_DIR / "quarantine_reviewed"
REVIEWED_QUARANTINE_UIUC_DIR = REVIEWED_QUARANTINE_DIR / "uiuc"
DB_DIR = LOCAL_DIR / "db"
OUTPUT_DIR = BASE_DIR / "output"
ARCHIVE_DIR = LOCAL_DIR / "archive"
XFOIL_DIR = LOCAL_DIR / "xfoil"
XFOIL_DAT_DIR = XFOIL_DIR / "airfoils_dat"
XFOIL_POLAR_DIR = XFOIL_DIR / "polars"
XFOIL_LOG_DIR = XFOIL_DIR / "logs"
XFOIL_EXE_PATH = TOOLS_DIR / "xfoil.exe"

PROFILES_DB_PATH = DB_DIR / "profiles.db"
USAGE_DB_PATH = DB_DIR / "usage.db"
POLARS_DB_PATH = DB_DIR / "polars.db"
AIRFOIL_DB_PATH = OUTPUT_DIR / "airfoil.db"

LEGACY_PROFILES_DB_PATHS = [
    DB_DIR / "airfoils.db",
    BASE_DIR / "airfoils.db",
]
LEGACY_USAGE_DB_PATHS = [
    DB_DIR / "airfoil_usage.db",
    BASE_DIR / "airfoil_usage.db",
    BASE_DIR / "airfoil_usage",
]
LEGACY_POLARS_DB_PATHS = []
LEGACY_AIRFOIL_DB_PATHS = [
    DB_DIR / "airfoils_merged.db",
    BASE_DIR / "airfoils_merged.db",
]


def ensure_local_dirs() -> None:
    for path in (
        DOWNLOAD_DIR,
        TOOLS_DIR,
        RAW_DIR,
        RAW_UIUC_DIR,
        NORMALIZED_DIR,
        NORMALIZED_UIUC_DIR,
        QUARANTINE_DIR,
        QUARANTINE_UIUC_DIR,
        REVIEWED_QUARANTINE_DIR,
        REVIEWED_QUARANTINE_UIUC_DIR,
        DB_DIR,
        OUTPUT_DIR,
        ARCHIVE_DIR,
        XFOIL_DAT_DIR,
        XFOIL_POLAR_DIR,
        XFOIL_LOG_DIR,
    ):
        path.mkdir(parents=True, exist_ok=True)


def resolve_existing_path(preferred: Path, legacy_paths: list[Path]) -> Path:
    if preferred.exists():
        return preferred

    for legacy_path in legacy_paths:
        if legacy_path.exists():
            return legacy_path

    return preferred


def resolve_profiles_db_path() -> Path:
    return resolve_existing_path(PROFILES_DB_PATH, LEGACY_PROFILES_DB_PATHS)


def resolve_usage_db_path() -> Path:
    return resolve_existing_path(USAGE_DB_PATH, LEGACY_USAGE_DB_PATHS)


def resolve_polars_db_path() -> Path:
    return resolve_existing_path(POLARS_DB_PATH, LEGACY_POLARS_DB_PATHS)


def resolve_airfoil_db_path() -> Path:
    return resolve_existing_path(AIRFOIL_DB_PATH, LEGACY_AIRFOIL_DB_PATHS)


def resolve_xfoil_exe_path() -> Path:
    env_path = os.environ.get("XFOIL_EXE")
    if env_path:
        return Path(env_path).expanduser().resolve()
    return XFOIL_EXE_PATH


# Backward-compatible aliases during migration.
GEOMETRY_DB_PATH = PROFILES_DB_PATH
MERGED_DB_PATH = AIRFOIL_DB_PATH


def resolve_geometry_db_path() -> Path:
    return resolve_profiles_db_path()


def resolve_merged_db_path() -> Path:
    return resolve_airfoil_db_path()
