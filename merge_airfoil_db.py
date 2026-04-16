from __future__ import annotations

"""Merge staging databases into the final production `airfoil.db`."""

import shutil
import sqlite3
import re

from paths import (
    AIRFOIL_DB_PATH,
    DB_DIR,
    ensure_local_dirs,
    resolve_polars_db_path,
    resolve_profiles_db_path,
    resolve_usage_db_path,
)

USAGE_TABLES = [
    "aircraft_usage_rows",
    "airfoil_applications",
    "source_meta",
]

POLARS_TABLES = [
    "airfoil_polars_xfoil",
    "airfoil_xfoil_runs",
    "airfoil_ratings",
    "airfoil_rating_details",
    "airfoil_rating_reynolds",
]

RUNTIME_TABLES = [
    "airfoils",
    "airfoil_applications",
    "airfoil_polars_xfoil",
    "airfoil_ratings",
    "airfoil_usage_summary",
    "airfoil_filter_presets",
]

PUBLIC_DB_NULL_COLUMNS = {
    "airfoils": [
        "raw_file_path",
        "normalized_file_path",
        "raw_dat",
    ],
    "airfoil_polars_xfoil": [
        "polar_file_path",
    ],
    "airfoil_xfoil_runs": [
        "log_file_path",
        "polar_file_path",
    ],
}


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name=?
        """,
        (table_name,),
    )
    return cur.fetchone() is not None


def get_create_table_sql(conn: sqlite3.Connection, table_name: str) -> str | None:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT sql
        FROM sqlite_master
        WHERE type='table' AND name=?
        """,
        (table_name,),
    )
    row = cur.fetchone()
    return row[0] if row else None


def get_indexes_sql(conn: sqlite3.Connection, table_name: str) -> list[str]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT sql
        FROM sqlite_master
        WHERE type='index'
          AND tbl_name=?
          AND sql IS NOT NULL
        """,
        (table_name,),
    )
    return [row[0] for row in cur.fetchall() if row[0]]


def copy_table_schema(
    src_conn: sqlite3.Connection,
    dst_conn: sqlite3.Connection,
    table_name: str,
) -> None:
    create_sql = get_create_table_sql(src_conn, table_name)
    if not create_sql:
        raise RuntimeError(f"Schema non trovato per la tabella '{table_name}'.")

    dst_cur = dst_conn.cursor()
    dst_cur.execute(create_sql)

    for idx_sql in get_indexes_sql(src_conn, table_name):
        dst_cur.execute(idx_sql)

    dst_conn.commit()


def get_column_names(conn: sqlite3.Connection, table_name: str) -> list[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    return [row[1] for row in cur.fetchall()]


def clear_table(conn: sqlite3.Connection, table_name: str) -> None:
    cur = conn.cursor()
    cur.execute(f"DELETE FROM {table_name}")
    conn.commit()


def copy_table_data(
    src_conn: sqlite3.Connection,
    dst_conn: sqlite3.Connection,
    table_name: str,
) -> int:
    columns = get_column_names(src_conn, table_name)
    if not columns:
        raise RuntimeError(f"Nessuna colonna trovata per la tabella '{table_name}'.")

    col_list = ", ".join(columns)
    placeholders = ", ".join(["?"] * len(columns))

    src_cur = src_conn.cursor()
    dst_cur = dst_conn.cursor()

    src_cur.execute(f"SELECT {col_list} FROM {table_name}")
    rows = src_cur.fetchall()

    if rows:
        dst_cur.executemany(
            f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})",
            rows,
        )
        dst_conn.commit()

    return len(rows)


def replace_table_from_source(
    src_conn: sqlite3.Connection,
    dst_conn: sqlite3.Connection,
    table_name: str,
) -> int:
    if not table_exists(src_conn, table_name):
        print(f"[WARN] Tabella non trovata, salto: {table_name}")
        return 0

    if not table_exists(dst_conn, table_name):
        copy_table_schema(src_conn, dst_conn, table_name)
    else:
        clear_table(dst_conn, table_name)

    copied = copy_table_data(src_conn, dst_conn, table_name)
    print(f"[OK] {table_name}: copiate {copied} righe")
    return copied


def run_integrity_check(conn: sqlite3.Connection, label: str) -> None:
    cur = conn.cursor()
    cur.execute("PRAGMA integrity_check")
    result = cur.fetchone()
    status = result[0] if result else "unknown"
    print(f"[CHECK] integrity_check {label}: {status}")


def prune_final_airfoils(conn: sqlite3.Connection) -> int:
    cur = conn.cursor()
    if table_exists(conn, "airfoil_xfoil_runs"):
        count_sql = """
            SELECT COUNT(*)
            FROM airfoils
            WHERE exclude_from_final = 1
               OR name IN (
                    SELECT airfoil_name
                    FROM airfoil_xfoil_runs
                    GROUP BY airfoil_name
                    HAVING SUM(CASE WHEN exclude_from_final = 0 THEN 1 ELSE 0 END) = 0
               )
        """
        delete_sql = """
            DELETE FROM airfoils
            WHERE exclude_from_final = 1
               OR name IN (
                    SELECT airfoil_name
                    FROM airfoil_xfoil_runs
                    GROUP BY airfoil_name
                    HAVING SUM(CASE WHEN exclude_from_final = 0 THEN 1 ELSE 0 END) = 0
               )
        """
    else:
        count_sql = "SELECT COUNT(*) FROM airfoils WHERE exclude_from_final = 1"
        delete_sql = "DELETE FROM airfoils WHERE exclude_from_final = 1"

    cur.execute(count_sql)
    removed = cur.fetchone()[0]
    cur.execute(delete_sql)
    conn.commit()
    return removed


def prune_orphan_rows(
    conn: sqlite3.Connection,
    table_name: str,
    name_column: str = "airfoil_name",
    remove_nulls: bool = False,
) -> int:
    if not table_exists(conn, table_name):
        return 0

    cur = conn.cursor()
    null_clause = f" OR {name_column} IS NULL" if remove_nulls else ""
    cur.execute(
        f"""
        DELETE FROM {table_name}
        WHERE {name_column} NOT IN (SELECT name FROM airfoils)
           {null_clause}
        """
    )
    removed = cur.rowcount if cur.rowcount is not None else 0
    conn.commit()
    return removed


def scrub_table_columns(
    conn: sqlite3.Connection,
    table_name: str,
    column_names: list[str],
) -> int:
    if not table_exists(conn, table_name):
        return 0

    available_columns = set(get_column_names(conn, table_name))
    target_columns = [name for name in column_names if name in available_columns]
    if not target_columns:
        return 0

    cur = conn.cursor()
    assignments = ", ".join(f"{name} = NULL" for name in target_columns)
    where_clause = " OR ".join(f"{name} IS NOT NULL" for name in target_columns)
    cur.execute(
        f"""
        UPDATE {table_name}
        SET {assignments}
        WHERE {where_clause}
        """
    )
    updated = cur.rowcount if cur.rowcount is not None else 0
    conn.commit()
    return updated


def scrub_public_artifact(conn: sqlite3.Connection) -> dict[str, int]:
    scrubbed_rows: dict[str, int] = {}
    for table_name, columns in PUBLIC_DB_NULL_COLUMNS.items():
        scrubbed_rows[table_name] = scrub_table_columns(conn, table_name, columns)

    conn.execute("VACUUM")
    conn.commit()
    return scrubbed_rows


def normalize_alias_term(raw: str) -> str:
    s = (raw or "").strip().strip('"').strip("'")
    s = s.replace("–", "-").replace("—", "-")
    s = re.sub(r"\s+", " ", s).strip().lower()
    s = s.replace(" ", "")
    s = s.replace(".", "")
    s = s.replace(",", "")
    s = s.replace("_", "")
    s = s.replace("-", "")
    s = s.replace("%", "")
    s = s.replace("(", "")
    s = s.replace(")", "")
    s = s.replace("/", "")
    return s


def expand_structured_alias_candidates(norm_candidate: str) -> list[str]:
    """Generate deterministic alias variants for common notation families."""
    out: list[str] = []
    c = norm_candidate or ""
    if not c:
        return out

    m = re.fullmatch(r"naca(6\d)a?(\d)(\d{2,3})([a-z]?)", c)
    if m:
        family = m.group(1)
        d1 = m.group(2)
        tail = m.group(3)
        suffix = m.group(4)
        compact = f"{family}{d1}{tail}"
        out.extend(
            [
                f"n{compact}",
                f"naca{compact}",
                f"n{compact}{suffix}" if suffix else "",
                f"naca{compact}{suffix}" if suffix else "",
            ]
        )
        if suffix == "":
            out.extend([f"n{compact}a", f"naca{compact}a"])

    m2 = re.fullmatch(r"naca(6\d)(\d)(\d{2})", c)
    if m2:
        family = m2.group(1)
        x = m2.group(2)
        yz = m2.group(3)
        for ins in ("1", "2", "3", "4"):
            out.append(f"naca{family}{ins}{x}{yz}")
        out.append(f"n{family}{x}{yz}")
        out.append(f"n{family}{x}{yz}a")

    m3 = re.fullmatch(r"naca(6\d\d\d\d[a-z]?)", c)
    if m3:
        out.append(f"n{m3.group(1)}")

    seen = set()
    ordered = []
    for item in out:
        if not item or item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


def build_alias_catalog(conn: sqlite3.Connection) -> int:
    """Create and populate a search alias table for all airfoils."""
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS airfoil_aliases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alias_norm TEXT NOT NULL,
            alias_raw TEXT NOT NULL,
            airfoil_name TEXT NOT NULL,
            alias_source TEXT NOT NULL,
            confidence REAL NOT NULL DEFAULT 1.0,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(alias_norm, airfoil_name)
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_airfoil_aliases_norm
        ON airfoil_aliases(alias_norm)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_airfoil_aliases_name
        ON airfoil_aliases(airfoil_name)
        """
    )
    cur.execute("DELETE FROM airfoil_aliases")

    cur.execute("SELECT name, COALESCE(title, '') FROM airfoils WHERE name IS NOT NULL")
    rows = cur.fetchall()

    records: list[tuple[str, str, str, str, float]] = []

    def add(alias_raw: str, airfoil_name: str, alias_source: str, confidence: float = 1.0) -> None:
        alias_norm = normalize_alias_term(alias_raw)
        if not alias_norm:
            return
        records.append((alias_norm, alias_raw.strip(), airfoil_name, alias_source, confidence))

    for airfoil_name, title in rows:
        name = str(airfoil_name).strip()
        if not name:
            continue
        title = str(title).strip()
        name_norm = normalize_alias_term(name)

        add(name, name, "name_exact", 1.0)
        add(name_norm, name, "name_norm", 1.0)

        m_naca_short = re.fullmatch(r"n(\d{4,5})", name_norm)
        if m_naca_short:
            add(f"NACA {m_naca_short.group(1)}", name, "naca_family", 0.99)
            add(f"naca{m_naca_short.group(1)}", name, "naca_family", 0.99)

        m_goe_short = re.fullmatch(r"goe(\d+)", name_norm)
        if m_goe_short:
            add(f"Goettingen {m_goe_short.group(1)}", name, "goettingen_family", 0.99)
            add(f"goettingen{m_goe_short.group(1)}", name, "goettingen_family", 0.99)

        if name_norm.startswith("fx"):
            add(f"Wortmann {name}", name, "wortmann_family", 0.99)
            add(f"wortmann{name_norm}", name, "wortmann_family", 0.99)

        if title:
            add(title, name, "title_exact", 0.98)
            title_wo_airfoil = re.sub(r"\bairfoils?\b", "", title, flags=re.IGNORECASE).strip()
            if title_wo_airfoil:
                add(title_wo_airfoil, name, "title_wo_airfoil", 0.97)

        # Deterministic notation expansions from canonical name/title forms.
        seed_aliases = {
            normalize_alias_term(name),
            normalize_alias_term(title),
        }
        for seed in [s for s in seed_aliases if s]:
            for expanded in expand_structured_alias_candidates(seed):
                add(expanded, name, "structured_expand", 0.995)

    cur.executemany(
        """
        INSERT OR IGNORE INTO airfoil_aliases (
            alias_norm, alias_raw, airfoil_name, alias_source, confidence
        ) VALUES (?, ?, ?, ?, ?)
        """,
        records,
    )
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM airfoil_aliases")
    return int(cur.fetchone()[0] or 0)


def rebuild_usage_search_view(conn: sqlite3.Connection) -> None:
    """Create a convenience view for alias-based usage lookups."""
    cur = conn.cursor()
    cur.execute("DROP VIEW IF EXISTS airfoil_usage_search")
    cur.execute(
        """
        CREATE VIEW airfoil_usage_search AS
        SELECT
            aa.alias_norm,
            aa.alias_raw,
            aa.airfoil_name,
            ap.aircraft_name,
            ap.aircraft_section,
            ap.role_code,
            ap.role_label,
            ap.airfoil_raw AS source_airfoil_raw,
            ap.source,
            ap.source_url,
            ap.match_method,
            ap.match_score,
            ap.confidence
        FROM airfoil_aliases aa
        JOIN airfoil_applications ap
          ON ap.matched_profile_name = aa.airfoil_name
        """
    )
    conn.commit()


def ensure_runtime_indexes(conn: sqlite3.Connection) -> None:
    """Create indexes optimized for the GUI/runtime read patterns."""
    cur = conn.cursor()

    index_statements = [
        """
        CREATE INDEX IF NOT EXISTS idx_airfoil_applications_matched_profile_conf
        ON airfoil_applications(matched_profile_name, confidence DESC, id DESC)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_airfoil_applications_profile_type_nocase
        ON airfoil_applications(matched_profile_name, profile_type_tag COLLATE NOCASE)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_airfoil_applications_reason_tag_nocase
        ON airfoil_applications(matched_profile_name, reason_tag COLLATE NOCASE)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_airfoil_ratings_airfoil_id_desc
        ON airfoil_ratings(airfoil_name, id DESC)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_airfoils_runtime_filter
        ON airfoils(exclude_from_final, is_valid_geometry, name)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_airfoil_polars_runtime_reynolds
        ON airfoil_polars_xfoil(airfoil_name, mach, ncrit, converged, reynolds)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_airfoil_polars_runtime_rows
        ON airfoil_polars_xfoil(airfoil_name, reynolds, mach, ncrit, converged, alpha_deg)
        """,
    ]

    for sql in index_statements:
        cur.execute(sql)
    conn.commit()


def build_usage_summary_table(conn: sqlite3.Connection, top_n: int = 3) -> int:
    """Build compact per-airfoil usage summary rows for fast GUI listing."""
    n = max(1, int(top_n))
    cur = conn.cursor()
    # Recreate table every run so schema changes (e.g. removing legacy columns)
    # are applied deterministically on existing databases.
    cur.execute("DROP TABLE IF EXISTS airfoil_usage_summary")
    cur.execute(
        """
        CREATE TABLE airfoil_usage_summary (
            airfoil_name TEXT PRIMARY KEY,
            usage_count INTEGER NOT NULL DEFAULT 0,
            top_usage TEXT,
            top_aircraft TEXT,
            top_usages TEXT,
            top_sources TEXT,
            autostable_score REAL,
            autostable_cm0_est REAL,
            autostable_slope_est REAL,
            autostable_re_triplets INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_airfoil_usage_summary_count
        ON airfoil_usage_summary(usage_count DESC, airfoil_name)
        """
    )

    cur.execute(
        """
        INSERT INTO airfoil_usage_summary (
            airfoil_name,
            usage_count,
            top_usage,
            top_aircraft,
            top_usages,
            top_sources,
            autostable_score,
            autostable_cm0_est,
            autostable_slope_est,
            autostable_re_triplets,
            updated_at
        )
        WITH base AS (
            SELECT
                a.name AS airfoil_name,
                COUNT(ap.id) AS usage_count
            FROM airfoils a
            LEFT JOIN airfoil_applications ap
              ON ap.matched_profile_name = a.name
            GROUP BY a.name
        ),
        alpha_slice AS (
            SELECT
                px.airfoil_name,
                px.alpha_deg,
                AVG(px.cm) AS cm_avg
            FROM airfoil_polars_xfoil px
            WHERE COALESCE(px.converged, 0) = 1
              AND px.cm IS NOT NULL
              AND px.alpha_deg IN (0.0, 2.0, 4.0)
            GROUP BY px.airfoil_name, px.alpha_deg
        ),
        per_re_triplets AS (
            SELECT
                px.airfoil_name,
                px.reynolds,
                SUM(CASE WHEN px.alpha_deg = 0.0 AND COALESCE(px.converged, 0) = 1 THEN 1 ELSE 0 END) AS a0,
                SUM(CASE WHEN px.alpha_deg = 2.0 AND COALESCE(px.converged, 0) = 1 THEN 1 ELSE 0 END) AS a2,
                SUM(CASE WHEN px.alpha_deg = 4.0 AND COALESCE(px.converged, 0) = 1 THEN 1 ELSE 0 END) AS a4
            FROM airfoil_polars_xfoil px
            GROUP BY px.airfoil_name, px.reynolds
        ),
        re_triplet_counts AS (
            SELECT
                t.airfoil_name,
                COUNT(*) AS re_triplet_count
            FROM per_re_triplets t
            WHERE t.a0 > 0 AND t.a2 > 0 AND t.a4 > 0
            GROUP BY t.airfoil_name
        ),
        autostable_metrics AS (
            SELECT
                a.airfoil_name,
                COUNT(*) AS samples,
                COUNT(DISTINCT a.alpha_deg) AS alpha_points,
                (
                    (
                        COUNT(*) * SUM(a.alpha_deg * a.cm_avg)
                        - SUM(a.alpha_deg) * SUM(a.cm_avg)
                    ) / NULLIF(
                        COUNT(*) * SUM(a.alpha_deg * a.alpha_deg)
                        - SUM(a.alpha_deg) * SUM(a.alpha_deg),
                        0
                    )
                ) AS dcm_dalpha,
                (
                    (
                        SUM(a.cm_avg)
                        - (
                            (
                                COUNT(*) * SUM(a.alpha_deg * a.cm_avg)
                                - SUM(a.alpha_deg) * SUM(a.cm_avg)
                            ) / NULLIF(
                                COUNT(*) * SUM(a.alpha_deg * a.alpha_deg)
                                - SUM(a.alpha_deg) * SUM(a.alpha_deg),
                                0
                            )
                        ) * SUM(a.alpha_deg)
                    ) / NULLIF(COUNT(*), 0)
                ) AS cm0_est
            FROM alpha_slice a
            GROUP BY a.airfoil_name
        )
        SELECT
            b.airfoil_name,
            b.usage_count,
            (
                SELECT ap.role_label
                FROM airfoil_applications ap
                WHERE ap.matched_profile_name = b.airfoil_name
                  AND ap.role_label IS NOT NULL
                  AND TRIM(ap.role_label) <> ''
                ORDER BY COALESCE(ap.confidence, 0) DESC, ap.id DESC
                LIMIT 1
            ) AS top_usage,
            (
                SELECT ap.aircraft_name
                FROM airfoil_applications ap
                WHERE ap.matched_profile_name = b.airfoil_name
                  AND ap.aircraft_name IS NOT NULL
                  AND TRIM(ap.aircraft_name) <> ''
                ORDER BY COALESCE(ap.confidence, 0) DESC, ap.id DESC
                LIMIT 1
            ) AS top_aircraft,
            (
                SELECT GROUP_CONCAT(item, ' | ')
                FROM (
                    SELECT
                        CASE
                            WHEN ap.aircraft_name IS NOT NULL AND TRIM(ap.aircraft_name) <> ''
                                THEN TRIM(ap.role_label) || ' @ ' || TRIM(ap.aircraft_name)
                            ELSE TRIM(ap.role_label)
                        END AS item
                    FROM airfoil_applications ap
                    WHERE ap.matched_profile_name = b.airfoil_name
                      AND ap.role_label IS NOT NULL
                      AND TRIM(ap.role_label) <> ''
                    ORDER BY COALESCE(ap.confidence, 0) DESC, ap.id DESC
                    LIMIT ?
                )
            ) AS top_usages,
            (
                SELECT GROUP_CONCAT(src, ' | ')
                FROM (
                    SELECT DISTINCT COALESCE(ap.source, '') AS src
                    FROM airfoil_applications ap
                    WHERE ap.matched_profile_name = b.airfoil_name
                      AND COALESCE(ap.source, '') <> ''
                    ORDER BY src ASC
                    LIMIT ?
                )
            ) AS top_sources,
            ROUND(
                100.0 * (
                    0.65 * COALESCE(
                        MAX(-1.0, MIN(1.0, (-am.dcm_dalpha) / 0.004)),
                        -1.0
                    )
                    + 0.25 * COALESCE(
                        MAX(-1.0, MIN(1.0, 1.0 - (ABS(am.cm0_est) / 0.030))),
                        -1.0
                    )
                    + 0.10 * COALESCE(
                        MAX(-1.0, MIN(1.0, (CAST(rt.re_triplet_count AS REAL) / 3.0) - 1.0)),
                        -1.0
                    )
                ),
                3
            ) AS autostable_score,
            am.cm0_est AS autostable_cm0_est,
            am.dcm_dalpha AS autostable_slope_est,
            COALESCE(rt.re_triplet_count, 0) AS autostable_re_triplets,
            CURRENT_TIMESTAMP AS updated_at
        FROM base b
        LEFT JOIN autostable_metrics am
          ON am.airfoil_name = b.airfoil_name
        LEFT JOIN re_triplet_counts rt
          ON rt.airfoil_name = b.airfoil_name
        ORDER BY b.airfoil_name ASC
        """,
        (n, n),
    )
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM airfoil_usage_summary")
    return int(cur.fetchone()[0] or 0)


def build_filter_presets_table(conn: sqlite3.Connection) -> int:
    """Create/update GUI filter presets in DB so app logic is data-driven."""
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS airfoil_filter_presets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            label TEXT NOT NULL UNIQUE,
            profile_type_filter TEXT,
            usage_filter TEXT,
            display_order INTEGER NOT NULL DEFAULT 0,
            enabled INTEGER NOT NULL DEFAULT 1,
            note TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_airfoil_filter_presets_order
        ON airfoil_filter_presets(enabled, display_order, label)
        """
    )

    presets = [
        ("All", "", "", 0, 1, "No profile type filter"),
        ("Symmetric", "symmetric", "", 10, 1, "Near-zero camber profiles"),
        ("Autostable", "autostable", "", 20, 1, "Derived from Cm trend and Cm0 proxy"),
        ("Rotating", "rotor_efficiency", "", 30, 1, "Rotor/blade usage contexts"),
        ("High Lift", "high_lift", "", 40, 1, "High-lift usage contexts"),
        ("General Purpose", "general_purpose", "", 50, 1, "General purpose usage contexts"),
    ]
    cur.executemany(
        """
        INSERT INTO airfoil_filter_presets (
            label, profile_type_filter, usage_filter, display_order, enabled, note
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(label) DO UPDATE SET
            profile_type_filter = excluded.profile_type_filter,
            usage_filter = excluded.usage_filter,
            display_order = excluded.display_order,
            enabled = excluded.enabled,
            note = excluded.note
        """,
        presets,
    )
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM airfoil_filter_presets WHERE COALESCE(enabled, 1) = 1")
    return int(cur.fetchone()[0] or 0)


def slim_public_database_in_place(conn: sqlite3.Connection) -> dict[str, int]:
    """Keep only runtime tables inside airfoil.db and vacuum."""
    cur = conn.cursor()
    keep_tables = set(RUNTIME_TABLES)
    dropped: dict[str, int] = {"tables": 0, "views": 0}

    cur.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='view'
        """
    )
    for (view_name,) in cur.fetchall():
        cur.execute(f"DROP VIEW IF EXISTS {view_name}")
        dropped["views"] += 1

    cur.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table'
          AND name NOT LIKE 'sqlite_%'
        """
    )
    for (table_name,) in cur.fetchall():
        if table_name in keep_tables:
            continue
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        dropped["tables"] += 1

    conn.commit()
    conn.execute("VACUUM")
    conn.commit()
    return dropped


def merge_databases() -> None:
    """Merge staging databases into the final `airfoil.db` output."""
    ensure_local_dirs()

    profiles_db = resolve_profiles_db_path()
    usage_db = resolve_usage_db_path()
    polars_db = resolve_polars_db_path()
    merged_db = AIRFOIL_DB_PATH
    DB_DIR.mkdir(parents=True, exist_ok=True)

    print("PROFILES_DB =", profiles_db)
    print("USAGE_DB    =", usage_db)
    print("POLARS_DB   =", polars_db)
    print("AIRFOIL_DB  =", merged_db)

    if not profiles_db.exists():
        raise FileNotFoundError(f"DB profili non trovato: {profiles_db}")
    if not usage_db.exists():
        raise FileNotFoundError(
            "DB usage non trovato. Attesi uno di questi path:\n"
            f" - {usage_db}\n"
            f" - {usage_db.with_suffix('')}"
        )
    if not polars_db.exists():
        raise FileNotFoundError(f"DB polars non trovato: {polars_db}")

    if merged_db.exists():
        merged_db.unlink()

    shutil.copy2(profiles_db, merged_db)
    print(f"[OK] Copiato DB base profili in: {merged_db}")

    profiles_conn = sqlite3.connect(profiles_db)
    usage_conn = sqlite3.connect(usage_db)
    polars_conn = sqlite3.connect(polars_db)
    merged_conn = sqlite3.connect(merged_db)

    try:
        run_integrity_check(profiles_conn, "profiles")
        run_integrity_check(usage_conn, "usage")
        run_integrity_check(polars_conn, "polars")
        run_integrity_check(merged_conn, "merged-initial")

        total_usage_rows = 0
        for table_name in USAGE_TABLES:
            total_usage_rows += replace_table_from_source(usage_conn, merged_conn, table_name)

        total_polar_rows = 0
        for table_name in POLARS_TABLES:
            total_polar_rows += replace_table_from_source(polars_conn, merged_conn, table_name)

        removed_airfoils = prune_final_airfoils(merged_conn)
        removed_orphans = {
            "airfoil_polars_xfoil": prune_orphan_rows(merged_conn, "airfoil_polars_xfoil"),
            "airfoil_xfoil_runs": prune_orphan_rows(merged_conn, "airfoil_xfoil_runs"),
            "airfoil_ratings": prune_orphan_rows(merged_conn, "airfoil_ratings"),
            "airfoil_rating_details": prune_orphan_rows(merged_conn, "airfoil_rating_details"),
            "airfoil_rating_reynolds": prune_orphan_rows(merged_conn, "airfoil_rating_reynolds"),
            "airfoil_applications": prune_orphan_rows(
                merged_conn,
                "airfoil_applications",
                name_column="matched_profile_name",
                remove_nulls=True,
            ),
        }
        alias_count = build_alias_catalog(merged_conn)
        rebuild_usage_search_view(merged_conn)
        usage_summary_count = build_usage_summary_table(merged_conn, top_n=3)
        filter_presets_count = build_filter_presets_table(merged_conn)
        ensure_runtime_indexes(merged_conn)
        scrubbed_rows = scrub_public_artifact(merged_conn)

        run_integrity_check(merged_conn, "merged-final")

        cur = merged_conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [row[0] for row in cur.fetchall()]

        print("\n===== MERGE COMPLETATO =====")
        print("DB finale:", merged_db)
        print("Tabelle presenti:")
        for table_name in tables:
            print(" -", table_name)

        print("\nConteggi principali:")
        for table_name in [
            "airfoils",
            "airfoil_polars_xfoil",
            "airfoil_applications",
            "aircraft_usage_rows",
            "airfoil_ratings",
            "airfoil_rating_reynolds",
            "airfoil_usage_summary",
            "airfoil_filter_presets",
        ]:
            if table_exists(merged_conn, table_name):
                cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                print(f" - {table_name}: {cur.fetchone()[0]}")

        print(f"\nRighe usage copiate: {total_usage_rows}")
        print(f"Righe polars copiate: {total_polar_rows}")
        print(f"Profili esclusi dal DB finale: {removed_airfoils}")
        print("Righe orfane rimosse:")
        for table_name, removed in removed_orphans.items():
            print(f" - {table_name}: {removed}")
        print("Righe ripulite per la pubblicazione:")
        for table_name, updated in scrubbed_rows.items():
            print(f" - {table_name}: {updated}")
        dropped = slim_public_database_in_place(merged_conn)
        run_integrity_check(merged_conn, "merged-slim")
        slim_mb = merged_db.stat().st_size / (1024 * 1024)
        print(f"Alias profilo generati: {alias_count}")
        print(f"Righe usage summary generate: {usage_summary_count}")
        print(f"Preset filtri attivi: {filter_presets_count}")
        print("\nDB slim in-place:")
        print(f" - path: {merged_db}")
        print(f" - size: {slim_mb:.2f} MB")
        print(f" - dropped tables: {dropped['tables']}")
        print(f" - dropped views: {dropped['views']}")

    finally:
        profiles_conn.close()
        usage_conn.close()
        polars_conn.close()
        merged_conn.close()


if __name__ == "__main__":
    merge_databases()
