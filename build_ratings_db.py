from __future__ import annotations

"""Build the staging airfoil ratings inside `polars.db`.

The ratings are lightweight guidance scores derived from geometry and XFOIL
polars. They are meant for relative comparison inside this dataset, not as a
substitute for detailed aerodynamic analysis.
"""

import json
import math
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from statistics import mean, pstdev

from paths import (
    ensure_local_dirs,
    resolve_polars_db_path,
    resolve_profiles_db_path,
)

RATING_VERSION = "v1"

PERFORMANCE_WEIGHTS = {
    "best_ld": 0.40,
    "best_cl": 0.25,
    "usable_alpha_span": 0.35,
}

DOCILITY_WEIGHTS = {
    "coverage_ratio": 0.30,
    "cl_smoothness": 0.20,
    "cd_smoothness": 0.15,
    "cm_stability": 0.15,
    "camber_moderation": 0.10,
    "thickness_moderation": 0.10,
}

ROBUSTNESS_WEIGHTS = {
    "thickness_moderation": 0.20,
    "camber_moderation": 0.20,
    "thickness_x_moderation": 0.15,
    "coverage_ratio": 0.25,
    "reynolds_consistency": 0.20,
}

CONFIDENCE_WEIGHTS = {
    "coverage_ratio": 0.35,
    "valid_reynolds_ratio": 0.30,
    "converged_points": 0.20,
    "usable_alpha_span": 0.15,
}

CATEGORY_WEIGHTS = {
    "performance": PERFORMANCE_WEIGHTS,
    "docility": DOCILITY_WEIGHTS,
    "robustness": ROBUSTNESS_WEIGHTS,
    "confidence": CONFIDENCE_WEIGHTS,
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ensure_profiles_table(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type='table' AND name='airfoils'
        """
    )
    if cur.fetchone():
        return
    raise RuntimeError("La tabella 'airfoils' non esiste in profiles.db")


def ensure_polars_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    required = ["airfoil_polars_xfoil", "airfoil_xfoil_runs"]
    missing = []
    for table_name in required:
        cur.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type='table' AND name=?
            """,
            (table_name,),
        )
        if not cur.fetchone():
            missing.append(table_name)
    if missing:
        raise RuntimeError(
            "Tabelle mancanti in polars.db: " + ", ".join(missing) +
            ". Esegui prima: python build_polars_db.py"
        )


def ensure_rating_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS airfoil_ratings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            airfoil_name TEXT NOT NULL UNIQUE,
            performance_score REAL NOT NULL,
            docility_score REAL NOT NULL,
            robustness_score REAL NOT NULL,
            confidence_score REAL NOT NULL,
            rating_version TEXT NOT NULL,
            rating_notes TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_airfoil_ratings_name
        ON airfoil_ratings(airfoil_name)
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS airfoil_rating_details (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            airfoil_name TEXT NOT NULL,
            category TEXT NOT NULL,
            metric_code TEXT NOT NULL,
            raw_value REAL,
            normalized_value REAL,
            weight REAL NOT NULL,
            contribution REAL NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_airfoil_rating_details_name
        ON airfoil_rating_details(airfoil_name)
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS airfoil_rating_reynolds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            airfoil_name TEXT NOT NULL,
            reynolds REAL NOT NULL,
            coverage_ratio REAL NOT NULL,
            converged_points REAL NOT NULL,
            expected_points REAL NOT NULL,
            best_ld REAL NOT NULL,
            best_cl REAL NOT NULL,
            usable_alpha_span REAL NOT NULL,
            cl_smoothness REAL NOT NULL,
            cd_smoothness REAL NOT NULL,
            cm_stability REAL NOT NULL,
            run_status TEXT,
            rating_version TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(airfoil_name, reynolds)
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_airfoil_rating_reynolds_name
        ON airfoil_rating_reynolds(airfoil_name)
        """
    )
    conn.commit()


def reset_rating_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS airfoil_rating_reynolds")
    cur.execute("DROP TABLE IF EXISTS airfoil_rating_details")
    cur.execute("DROP TABLE IF EXISTS airfoil_ratings")
    conn.commit()


def fetch_profiles(conn: sqlite3.Connection) -> dict[str, dict[str, float]]:
    ensure_profiles_table(conn)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            name,
            max_thickness,
            max_camber,
            max_thickness_x
        FROM airfoils
        WHERE is_valid_geometry = 1
          AND is_xfoil_compatible = 1
          AND exclude_from_final = 0
        ORDER BY name
        """
    )
    return {
        row[0]: {
            "max_thickness": float(row[1] or 0.0),
            "max_camber": float(row[2] or 0.0),
            "max_thickness_x": float(row[3] or 0.0),
        }
        for row in cur.fetchall()
    }


def fetch_polars(conn: sqlite3.Connection):
    ensure_polars_tables(conn)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            airfoil_name,
            reynolds,
            alpha_deg,
            cl,
            cd,
            cm,
            converged
        FROM airfoil_polars_xfoil
        ORDER BY airfoil_name, reynolds, alpha_deg
        """
    )

    polars_by_airfoil: dict[str, dict[float, list[dict[str, float]]]] = defaultdict(lambda: defaultdict(list))
    for airfoil_name, reynolds, alpha_deg, cl, cd, cm, converged in cur.fetchall():
        polars_by_airfoil[airfoil_name][float(reynolds)].append(
            {
                "alpha_deg": float(alpha_deg),
                "cl": None if cl is None else float(cl),
                "cd": None if cd is None else float(cd),
                "cm": None if cm is None else float(cm),
                "converged": int(converged),
            }
        )
    return polars_by_airfoil


def fetch_runs(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            airfoil_name,
            reynolds,
            expected_count,
            converged_count,
            run_status,
            exclude_from_final
        FROM airfoil_xfoil_runs
        ORDER BY airfoil_name, reynolds
        """
    )
    runs_by_airfoil: dict[str, dict[float, dict[str, float | str]]] = defaultdict(dict)
    for row in cur.fetchall():
        airfoil_name, reynolds, expected_count, converged_count, run_status, exclude_from_final = row
        runs_by_airfoil[airfoil_name][float(reynolds)] = {
            "expected_count": int(expected_count),
            "converged_count": int(converged_count),
            "run_status": str(run_status),
            "exclude_from_final": int(exclude_from_final),
        }
    return runs_by_airfoil


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    pos = (len(ordered) - 1) * q
    lower = math.floor(pos)
    upper = math.ceil(pos)
    if lower == upper:
        return ordered[lower]
    frac = pos - lower
    return ordered[lower] * (1.0 - frac) + ordered[upper] * frac


def normalize_to_score(raw_value: float, p5: float, p95: float) -> float:
    if p95 <= p5:
        return 50.0
    score = 100.0 * (raw_value - p5) / (p95 - p5)
    return max(0.0, min(100.0, score))


def average_abs_second_difference(values: list[float]) -> float:
    if len(values) < 3:
        return 0.0
    second_diffs = []
    for idx in range(1, len(values) - 1):
        second_diffs.append(abs(values[idx + 1] - 2 * values[idx] + values[idx - 1]))
    return mean(second_diffs) if second_diffs else 0.0


def compute_raw_metrics_for_airfoil(
    geometry: dict[str, float],
    polars_by_re: dict[float, list[dict[str, float]]],
    runs_by_re: dict[float, dict[str, float | str]],
) -> dict[str, float]:
    converged_rows = []
    coverage_ratios = []
    successful_reynolds = 0

    cl_roughness_values = []
    cd_roughness_values = []
    cm_range_values = []

    for reynolds, run in runs_by_re.items():
        expected_count = int(run["expected_count"])
        converged_count = int(run["converged_count"])
        coverage_ratio = (converged_count / expected_count) if expected_count else 0.0
        coverage_ratios.append(coverage_ratio)
        if converged_count > 0:
            successful_reynolds += 1

        re_rows = [row for row in polars_by_re.get(reynolds, []) if row["converged"] == 1]
        re_rows.sort(key=lambda item: item["alpha_deg"])
        converged_rows.extend(re_rows)

        cl_values = [row["cl"] for row in re_rows if row["cl"] is not None]
        cd_values = [row["cd"] for row in re_rows if row["cd"] is not None]
        cm_values = [row["cm"] for row in re_rows if row["cm"] is not None]

        if len(cl_values) >= 3:
            cl_roughness_values.append(average_abs_second_difference(cl_values))
        if len(cd_values) >= 3:
            cd_roughness_values.append(average_abs_second_difference(cd_values))
        if len(cm_values) >= 2:
            cm_range_values.append(max(cm_values) - min(cm_values))

    ld_candidates = []
    cl_candidates = []
    alpha_values = []
    for row in converged_rows:
        cl = row["cl"]
        cd = row["cd"]
        alpha = row["alpha_deg"]
        if cl is not None:
            cl_candidates.append(cl)
        if cd is not None and cl is not None and cd > 0 and cl > 0:
            ld_candidates.append(cl / cd)
        alpha_values.append(alpha)

    total_expected = sum(int(run["expected_count"]) for run in runs_by_re.values())
    total_converged = sum(int(run["converged_count"]) for run in runs_by_re.values())
    valid_reynolds_ratio = (successful_reynolds / len(runs_by_re)) if runs_by_re else 0.0
    coverage_ratio = (total_converged / total_expected) if total_expected else 0.0
    usable_alpha_span = (max(alpha_values) - min(alpha_values)) if len(alpha_values) >= 2 else 0.0
    reynolds_consistency = 0.0 if len(coverage_ratios) <= 1 else pstdev(coverage_ratios)

    thickness = geometry["max_thickness"]
    camber = abs(geometry["max_camber"])
    thickness_x = geometry["max_thickness_x"]

    return {
        "best_ld": max(ld_candidates) if ld_candidates else 0.0,
        "best_cl": max(cl_candidates) if cl_candidates else 0.0,
        "usable_alpha_span": usable_alpha_span,
        "coverage_ratio": coverage_ratio,
        "cl_smoothness": -mean(cl_roughness_values) if cl_roughness_values else 0.0,
        "cd_smoothness": -mean(cd_roughness_values) if cd_roughness_values else 0.0,
        "cm_stability": -mean(cm_range_values) if cm_range_values else 0.0,
        "thickness_moderation": -abs(thickness - 0.12),
        "camber_moderation": -camber,
        "reynolds_consistency": -reynolds_consistency,
        "valid_reynolds_ratio": valid_reynolds_ratio,
        "converged_points": float(total_converged),
        "thickness_x_moderation": -abs(thickness_x - 0.30),
    }


def compute_raw_metrics_for_reynolds(
    re_rows: list[dict[str, float]],
    run_data: dict[str, float | str] | None,
) -> dict[str, float | str]:
    converged_rows = [row for row in re_rows if row["converged"] == 1]
    converged_rows.sort(key=lambda item: item["alpha_deg"])

    cl_values = [row["cl"] for row in converged_rows if row["cl"] is not None]
    cd_values = [row["cd"] for row in converged_rows if row["cd"] is not None]
    cm_values = [row["cm"] for row in converged_rows if row["cm"] is not None]
    alpha_values = [row["alpha_deg"] for row in converged_rows]

    ld_candidates = []
    for row in converged_rows:
        cl = row["cl"]
        cd = row["cd"]
        if cl is not None and cd is not None and cl > 0 and cd > 0:
            ld_candidates.append(cl / cd)

    expected_points = float(run_data["expected_count"]) if run_data else 0.0
    converged_points = float(run_data["converged_count"]) if run_data else float(len(converged_rows))
    coverage_ratio = (converged_points / expected_points) if expected_points else 0.0

    return {
        "coverage_ratio": coverage_ratio,
        "converged_points": converged_points,
        "expected_points": expected_points,
        "best_ld": max(ld_candidates) if ld_candidates else 0.0,
        "best_cl": max(cl_values) if cl_values else 0.0,
        "usable_alpha_span": (max(alpha_values) - min(alpha_values)) if len(alpha_values) >= 2 else 0.0,
        "cl_smoothness": -average_abs_second_difference(cl_values) if len(cl_values) >= 3 else 0.0,
        "cd_smoothness": -average_abs_second_difference(cd_values) if len(cd_values) >= 3 else 0.0,
        "cm_stability": -(max(cm_values) - min(cm_values)) if len(cm_values) >= 2 else 0.0,
        "run_status": str(run_data["run_status"]) if run_data else "missing",
    }


def compute_category_score(
    raw_metrics: dict[str, float],
    normalizers: dict[str, tuple[float, float]],
    weights: dict[str, float],
) -> tuple[float, list[dict[str, float]]]:
    contributions = []
    total = 0.0
    for metric_code, weight in weights.items():
        raw_value = raw_metrics.get(metric_code, 0.0)
        p5, p95 = normalizers[metric_code]
        normalized = normalize_to_score(raw_value, p5, p95)
        contribution = normalized * weight
        total += contribution
        contributions.append(
            {
                "metric_code": metric_code,
                "raw_value": raw_value,
                "normalized_value": normalized,
                "weight": weight,
                "contribution": contribution,
            }
        )
    return total, contributions


def build_normalizers(raw_metrics_by_airfoil: dict[str, dict[str, float]]) -> dict[str, tuple[float, float]]:
    metric_names = set()
    for metrics in raw_metrics_by_airfoil.values():
        metric_names.update(metrics.keys())

    normalizers = {}
    for metric_name in metric_names:
        values = [metrics[metric_name] for metrics in raw_metrics_by_airfoil.values()]
        normalizers[metric_name] = (percentile(values, 0.05), percentile(values, 0.95))
    return normalizers


def build_category_normalizers(
    preliminary_scores_by_airfoil: dict[str, dict[str, float]],
) -> dict[str, tuple[float, float]]:
    normalizers = {}
    for category in CATEGORY_WEIGHTS:
        values = [
            float(category_scores.get(category, 0.0))
            for category_scores in preliminary_scores_by_airfoil.values()
        ]
        normalizers[category] = (percentile(values, 0.05), percentile(values, 0.95))
    return normalizers


def upsert_airfoil_rating(
    conn: sqlite3.Connection,
    airfoil_name: str,
    performance_score: float,
    docility_score: float,
    robustness_score: float,
    confidence_score: float,
    rating_notes: str,
) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO airfoil_ratings (
            airfoil_name,
            performance_score,
            docility_score,
            robustness_score,
            confidence_score,
            rating_version,
            rating_notes,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(airfoil_name) DO UPDATE SET
            performance_score=excluded.performance_score,
            docility_score=excluded.docility_score,
            robustness_score=excluded.robustness_score,
            confidence_score=excluded.confidence_score,
            rating_version=excluded.rating_version,
            rating_notes=excluded.rating_notes,
            created_at=excluded.created_at
        """,
        (
            airfoil_name,
            performance_score,
            docility_score,
            robustness_score,
            confidence_score,
            RATING_VERSION,
            rating_notes,
            utc_now(),
        ),
    )


def insert_rating_details(
    conn: sqlite3.Connection,
    airfoil_name: str,
    category: str,
    contributions: list[dict[str, float]],
) -> None:
    cur = conn.cursor()
    for item in contributions:
        cur.execute(
            """
            INSERT INTO airfoil_rating_details (
                airfoil_name,
                category,
                metric_code,
                raw_value,
                normalized_value,
                weight,
                contribution,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                airfoil_name,
                category,
                item["metric_code"],
                item["raw_value"],
                item["normalized_value"],
                item["weight"],
                item["contribution"],
                utc_now(),
            ),
        )


def upsert_rating_reynolds(
    conn: sqlite3.Connection,
    airfoil_name: str,
    reynolds: float,
    metrics: dict[str, float | str],
) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO airfoil_rating_reynolds (
            airfoil_name,
            reynolds,
            coverage_ratio,
            converged_points,
            expected_points,
            best_ld,
            best_cl,
            usable_alpha_span,
            cl_smoothness,
            cd_smoothness,
            cm_stability,
            run_status,
            rating_version,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(airfoil_name, reynolds) DO UPDATE SET
            coverage_ratio=excluded.coverage_ratio,
            converged_points=excluded.converged_points,
            expected_points=excluded.expected_points,
            best_ld=excluded.best_ld,
            best_cl=excluded.best_cl,
            usable_alpha_span=excluded.usable_alpha_span,
            cl_smoothness=excluded.cl_smoothness,
            cd_smoothness=excluded.cd_smoothness,
            cm_stability=excluded.cm_stability,
            run_status=excluded.run_status,
            rating_version=excluded.rating_version,
            created_at=excluded.created_at
        """,
        (
            airfoil_name,
            float(reynolds),
            float(metrics["coverage_ratio"]),
            float(metrics["converged_points"]),
            float(metrics["expected_points"]),
            float(metrics["best_ld"]),
            float(metrics["best_cl"]),
            float(metrics["usable_alpha_span"]),
            float(metrics["cl_smoothness"]),
            float(metrics["cd_smoothness"]),
            float(metrics["cm_stability"]),
            str(metrics["run_status"]),
            RATING_VERSION,
            utc_now(),
        ),
    )


def build_ratings_database(reset_db: bool = True) -> None:
    ensure_local_dirs()

    profiles_db = resolve_profiles_db_path()
    polars_db = resolve_polars_db_path()
    print("PROFILES_DB =", profiles_db)
    print("POLARS_DB   =", polars_db)

    profiles_conn = sqlite3.connect(str(profiles_db))
    polars_conn = sqlite3.connect(str(polars_db))
    try:
        ensure_profiles_table(profiles_conn)
        ensure_polars_tables(polars_conn)

        if reset_db:
            reset_rating_tables(polars_conn)
        ensure_rating_tables(polars_conn)

        profiles = fetch_profiles(profiles_conn)
        polars_by_airfoil = fetch_polars(polars_conn)
        runs_by_airfoil = fetch_runs(polars_conn)

        raw_metrics_by_airfoil = {}
        for airfoil_name, geometry in profiles.items():
            raw_metrics_by_airfoil[airfoil_name] = compute_raw_metrics_for_airfoil(
                geometry=geometry,
                polars_by_re=polars_by_airfoil.get(airfoil_name, {}),
                runs_by_re=runs_by_airfoil.get(airfoil_name, {}),
            )

        normalizers = build_normalizers(raw_metrics_by_airfoil)

        cur = polars_conn.cursor()
        cur.execute("DELETE FROM airfoil_ratings")
        cur.execute("DELETE FROM airfoil_rating_details")
        cur.execute("DELETE FROM airfoil_rating_reynolds")

        preliminary_scores_by_airfoil: dict[str, dict[str, float]] = {}
        preliminary_details_by_airfoil: dict[str, dict[str, list[dict[str, float]]]] = {}
        for airfoil_name in sorted(raw_metrics_by_airfoil):
            raw_metrics = raw_metrics_by_airfoil[airfoil_name]

            performance_score_raw, performance_details = compute_category_score(
                raw_metrics, normalizers, PERFORMANCE_WEIGHTS
            )
            docility_score_raw, docility_details = compute_category_score(
                raw_metrics, normalizers, DOCILITY_WEIGHTS
            )
            robustness_score_raw, robustness_details = compute_category_score(
                raw_metrics, normalizers, ROBUSTNESS_WEIGHTS
            )
            confidence_score_raw, confidence_details = compute_category_score(
                raw_metrics, normalizers, CONFIDENCE_WEIGHTS
            )

            preliminary_scores_by_airfoil[airfoil_name] = {
                "performance": performance_score_raw,
                "docility": docility_score_raw,
                "robustness": robustness_score_raw,
                "confidence": confidence_score_raw,
            }
            preliminary_details_by_airfoil[airfoil_name] = {
                "performance": performance_details,
                "docility": docility_details,
                "robustness": robustness_details,
                "confidence": confidence_details,
            }

        category_normalizers = build_category_normalizers(preliminary_scores_by_airfoil)

        for airfoil_name in sorted(raw_metrics_by_airfoil):
            raw_metrics = raw_metrics_by_airfoil[airfoil_name]
            preliminary_scores = preliminary_scores_by_airfoil[airfoil_name]
            performance_details = preliminary_details_by_airfoil[airfoil_name]["performance"]
            docility_details = preliminary_details_by_airfoil[airfoil_name]["docility"]
            robustness_details = preliminary_details_by_airfoil[airfoil_name]["robustness"]
            confidence_details = preliminary_details_by_airfoil[airfoil_name]["confidence"]

            performance_score = round(
                normalize_to_score(
                    preliminary_scores["performance"],
                    *category_normalizers["performance"],
                ),
                1,
            )
            docility_score = round(
                normalize_to_score(
                    preliminary_scores["docility"],
                    *category_normalizers["docility"],
                ),
                1,
            )
            robustness_score = round(
                normalize_to_score(
                    preliminary_scores["robustness"],
                    *category_normalizers["robustness"],
                ),
                1,
            )
            confidence_score = round(
                normalize_to_score(
                    preliminary_scores["confidence"],
                    *category_normalizers["confidence"],
                ),
                1,
            )

            rating_notes = json.dumps(
                {
                    "version": RATING_VERSION,
                    "score_normalization": "two_stage_percentile_p5_p95",
                    "coverage_ratio": round(raw_metrics["coverage_ratio"], 4),
                    "valid_reynolds_ratio": round(raw_metrics["valid_reynolds_ratio"], 4),
                    "best_ld": round(raw_metrics["best_ld"], 4),
                    "best_cl": round(raw_metrics["best_cl"], 4),
                    "usable_alpha_span": round(raw_metrics["usable_alpha_span"], 4),
                    "dominant_limits": [
                        label for label, active in (
                            ("low_coverage", raw_metrics["coverage_ratio"] < 0.5),
                            ("few_valid_reynolds", raw_metrics["valid_reynolds_ratio"] < 0.5),
                            ("low_best_ld", raw_metrics["best_ld"] < 20.0),
                        )
                        if active
                    ],
                    "relative_to_dataset": True,
                },
                ensure_ascii=False,
            )

            upsert_airfoil_rating(
                polars_conn,
                airfoil_name,
                performance_score,
                docility_score,
                robustness_score,
                confidence_score,
                rating_notes,
            )
            insert_rating_details(polars_conn, airfoil_name, "performance", performance_details)
            insert_rating_details(polars_conn, airfoil_name, "docility", docility_details)
            insert_rating_details(polars_conn, airfoil_name, "robustness", robustness_details)
            insert_rating_details(polars_conn, airfoil_name, "confidence", confidence_details)

            reynolds_values = sorted(
                set(polars_by_airfoil.get(airfoil_name, {}).keys()) |
                set(runs_by_airfoil.get(airfoil_name, {}).keys())
            )
            for reynolds in reynolds_values:
                per_re_metrics = compute_raw_metrics_for_reynolds(
                    re_rows=polars_by_airfoil.get(airfoil_name, {}).get(reynolds, []),
                    run_data=runs_by_airfoil.get(airfoil_name, {}).get(reynolds),
                )
                upsert_rating_reynolds(polars_conn, airfoil_name, reynolds, per_re_metrics)

        polars_conn.commit()

        cur.execute("SELECT COUNT(*) FROM airfoil_ratings")
        rating_count = cur.fetchone()[0]
        cur.execute("SELECT AVG(performance_score), AVG(docility_score), AVG(robustness_score), AVG(confidence_score) FROM airfoil_ratings")
        avg_row = cur.fetchone()

        print("\n===== RATINGS COMPLETATI =====")
        print("Airfoil rated:", rating_count)
        print(
            "Medie score:",
            f"performance={avg_row[0]:.1f}",
            f"docility={avg_row[1]:.1f}",
            f"robustness={avg_row[2]:.1f}",
            f"confidence={avg_row[3]:.1f}",
        )

        print("\nTop 5 performance:")
        cur.execute(
            """
            SELECT airfoil_name, performance_score, confidence_score
            FROM airfoil_ratings
            ORDER BY performance_score DESC, confidence_score DESC, airfoil_name ASC
            LIMIT 5
            """
        )
        for airfoil_name, score, confidence in cur.fetchall():
            print(f" - {airfoil_name}: performance={score:.1f}, confidence={confidence:.1f}")

        print("\nTop 5 docility:")
        cur.execute(
            """
            SELECT airfoil_name, docility_score, confidence_score
            FROM airfoil_ratings
            ORDER BY docility_score DESC, confidence_score DESC, airfoil_name ASC
            LIMIT 5
            """
        )
        for airfoil_name, score, confidence in cur.fetchall():
            print(f" - {airfoil_name}: docility={score:.1f}, confidence={confidence:.1f}")

        print("\nTop 5 robustness:")
        cur.execute(
            """
            SELECT airfoil_name, robustness_score, confidence_score
            FROM airfoil_ratings
            ORDER BY robustness_score DESC, confidence_score DESC, airfoil_name ASC
            LIMIT 5
            """
        )
        for airfoil_name, score, confidence in cur.fetchall():
            print(f" - {airfoil_name}: robustness={score:.1f}, confidence={confidence:.1f}")

        print("\nConfidence bassa (< 30):")
        cur.execute(
            """
            SELECT COUNT(*)
            FROM airfoil_ratings
            WHERE confidence_score < 30
            """
        )
        print(f" - {cur.fetchone()[0]} profili")

    finally:
        profiles_conn.close()
        polars_conn.close()


if __name__ == "__main__":
    build_ratings_database()
