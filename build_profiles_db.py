"""Build the staging database of airfoil geometries.

The module downloads the UIUC bulk coordinate archive, parses and normalizes
the `.dat` files, computes basic geometric metrics, and stores the result in
`profiles.db`.

Profiles are preserved broadly at this stage. Potentially problematic
geometries are marked with validation flags so later steps can decide whether
to exclude them from XFOIL processing or from the final merged database.
"""

import os
import stat
import re
import json
import sqlite3
import zipfile
import shutil
import urllib.request
from bisect import bisect_right
from datetime import datetime, timezone
from typing import List, Tuple, Optional, Dict
import math
import ssl
import urllib.error

from paths import (
    DB_DIR,
    DOWNLOAD_DIR,
    NORMALIZED_UIUC_DIR,
    PROFILES_DB_PATH,
    QUARANTINE_UIUC_DIR,
    REVIEWED_QUARANTINE_UIUC_DIR,
    RAW_UIUC_DIR,
    ensure_local_dirs,
)

# Primary geometry source (official bulk archive)
UIUC_ZIP_URL = "https://m-selig.ae.illinois.edu/ads/archives/coord_seligFmt.zip"
# Optional reference URLs saved as metadata only
UIUC_DB_PAGE_URL = "https://m-selig.ae.illinois.edu/ads/coord_database.html"
UIUC_FOLDER_URL = "https://m-selig.ae.illinois.edu/ads/coord_seligFmt/"
AIRFOILTOOLS_LIST_URL = "https://airfoiltools.com/airfoil/details"
AIRFOILTOOLS_SEARCH_URL = "https://airfoiltools.com/search/index"

RAW_DIR = str(RAW_UIUC_DIR)
NORMALIZED_DIR = str(NORMALIZED_UIUC_DIR)
QUARANTINE_DIR = str(QUARANTINE_UIUC_DIR)
REVIEWED_QUARANTINE_DIR = str(REVIEWED_QUARANTINE_UIUC_DIR)
DB_PATH = str(PROFILES_DB_PATH)
ZIP_PATH = str(DOWNLOAD_DIR / "coord_seligFmt.zip")
ERROR_LOG_PATH = str(DB_DIR / "profiles_import_errors.txt")
MANIFEST_PATH = str(DB_DIR / "profiles_sources_manifest.json")
TE_AUTOCLOSE_LOG_PATH = str(DB_DIR / "profiles_te_autoclosed.txt")

FORCE_CLOSED_TRAILING_EDGE = True
NORMALIZE_POINT_COUNT = True
TARGET_POINT_COUNT = 161

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Python airfoil-db-builder"


def ensure_dirs() -> None:
    ensure_local_dirs()


def _reset_dir(path: str) -> None:
    def _handle_remove_readonly(func, target_path, exc_info):
        try:
            os.chmod(target_path, stat.S_IWRITE)
            func(target_path)
        except OSError:
            raise exc_info[1]

    if os.path.isdir(path):
        shutil.rmtree(path, onerror=_handle_remove_readonly)
    os.makedirs(path, exist_ok=True)


def _safe_unlink(path: str) -> None:
    try:
        if os.path.exists(path):
            os.remove(path)
    except OSError:
        pass


def download_file(url: str, dest_path: str) -> None:
    """
    Robust download.
    First tries normal HTTPS verification.
    If UIUC SSL verification fails, retries once with an unverified SSL context
    only for that host.
    """
    print(f"[INFO] Download: {url}")

    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})

    try:
        with urllib.request.urlopen(req, timeout=90) as response, open(dest_path, "wb") as f:
            f.write(response.read())
        print(f"[OK] Saved: {dest_path}")
        return

    except urllib.error.URLError as e:
        reason = getattr(e, "reason", None)
        is_ssl_error = isinstance(reason, ssl.SSLCertVerificationError)

        if not (is_ssl_error and "m-selig.ae.illinois.edu" in url):
            raise

        print("[WARN] SSL certificate verification failed on UIUC host.")
        print("[WARN] Retrying with unverified SSL context for this source only...")

    insecure_ctx = ssl._create_unverified_context()
    with urllib.request.urlopen(req, timeout=90, context=insecure_ctx) as response, open(dest_path, "wb") as f:
        f.write(response.read())

    print(f"[OK] Saved: {dest_path} (downloaded with relaxed SSL check)")


def extract_zip(zip_path: str, extract_to: str) -> None:
    print(f"[INFO] Extracting zip: {zip_path}")
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_to)
    print(f"[OK] Extracted to: {extract_to}")


def list_dat_files(folder: str) -> List[str]:
    out = []
    for root, _, files in os.walk(folder):
        for name in files:
            if name.lower().endswith(".dat"):
                out.append(os.path.join(root, name))
    out.sort()
    return out


def read_text_file(path: str) -> str:
    encodings = ["utf-8", "latin-1", "cp1252"]
    for enc in encodings:
        try:
            with open(path, "r", encoding=enc) as f:
                return f.read()
        except UnicodeDecodeError:
            pass
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return f.read()


def is_float_token(token: str) -> bool:
    try:
        float(token)
        return True
    except ValueError:
        return False


def clean_line(line: str) -> str:
    line = line.strip()
    line = line.replace(",", " ")
    line = re.sub(r"\s+", " ", line)
    return line


def parse_airfoil_dat(text: str) -> Tuple[str, List[Tuple[float, float]]]:
    """
    Returns:
    - title
    - point list [(x, y), ...]

    Handles classic UIUC/Selig-style files where the first line is the title.
    Skips blank lines, # comments, and non-numeric rows.
    If a third numeric column exists, it is ignored.
    """
    lines = text.splitlines()
    if not lines:
        raise ValueError("File vuoto.")

    title = lines[0].strip() if lines[0].strip() else "unknown"
    points: List[Tuple[float, float]] = []

    for raw in lines[1:]:
        line = clean_line(raw)
        if not line:
            continue
        if line.startswith("#"):
            continue

        parts = line.split(" ")
        if len(parts) < 2:
            continue
        if not is_float_token(parts[0]) or not is_float_token(parts[1]):
            continue

        x = float(parts[0])
        y = float(parts[1])
        points.append((x, y))

    if len(points) < 5:
        raise ValueError("Troppi pochi punti validi.")

    return title, points


def remove_consecutive_duplicates(points: List[Tuple[float, float]], tol: float = 1e-12) -> List[Tuple[float, float]]:
    if not points:
        return points

    out = [points[0]]
    for p in points[1:]:
        if abs(p[0] - out[-1][0]) > tol or abs(p[1] - out[-1][1]) > tol:
            out.append(p)
    return out


def remove_closing_duplicate(points: List[Tuple[float, float]], tol: float = 1e-12) -> List[Tuple[float, float]]:
    """Drop the final point if it closes the polyline by repeating the first."""
    if len(points) < 2:
        return points

    first_x, first_y = points[0]
    last_x, last_y = points[-1]
    if abs(first_x - last_x) <= tol and abs(first_y - last_y) <= tol:
        return points[:-1]
    return points


def normalize_airfoil(points: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
    """
    Simple robust normalization:
    - x_min -> 0
    - x_max -> 1
    - y scaled by the same chord

    This keeps the original point order intact.
    """
    xs = [p[0] for p in points]
    x_min = min(xs)
    x_max = max(xs)
    chord = x_max - x_min

    if chord <= 0:
        raise ValueError("Corda non valida.")

    return [((x - x_min) / chord, y / chord) for x, y in points]


def close_trailing_edge(points_norm: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
    if not FORCE_CLOSED_TRAILING_EDGE:
        return points_norm
    if len(points_norm) < 2:
        return points_norm

    first_x, first_y = points_norm[0]
    last_x, last_y = points_norm[-1]
    if abs(first_x - last_x) <= 1e-12 and abs(first_y - last_y) <= 1e-12:
        return points_norm

    te_x = max(first_x, last_x)
    te_y = 0.5 * (first_y + last_y)

    closed = list(points_norm)
    closed[0] = (te_x, te_y)
    closed[-1] = (te_x, te_y)
    return closed


def _build_cosine_x_samples(count: int) -> List[float]:
    if count < 2:
        raise ValueError("Servono almeno 2 campioni per superficie.")
    return [
        0.5 * (1.0 - math.cos(math.pi * i / (count - 1)))
        for i in range(count)
    ]


def _dedupe_consecutive_points(
    points: List[Tuple[float, float]],
    tol: float = 1e-12,
) -> List[Tuple[float, float]]:
    if not points:
        return []
    out: List[Tuple[float, float]] = [points[0]]
    for x, y in points[1:]:
        px, py = out[-1]
        if math.hypot(x - px, y - py) <= tol:
            continue
        out.append((x, y))
    return out


def _pchip_slopes(xs: List[float], ys: List[float]) -> List[float]:
    n = len(xs)
    if n < 2:
        raise ValueError("PCHIP richiede almeno 2 punti.")

    if n == 2:
        d = (ys[1] - ys[0]) / (xs[1] - xs[0])
        return [d, d]

    h = [xs[i + 1] - xs[i] for i in range(n - 1)]
    if any(v <= 0.0 for v in h):
        raise ValueError("Ascisse non strettamente crescenti per PCHIP.")
    d = [(ys[i + 1] - ys[i]) / h[i] for i in range(n - 1)]

    m = [0.0] * n

    for k in range(1, n - 1):
        dk1 = d[k - 1]
        dk = d[k]
        if dk1 == 0.0 or dk == 0.0 or (dk1 > 0.0) != (dk > 0.0):
            m[k] = 0.0
            continue
        w1 = 2.0 * h[k] + h[k - 1]
        w2 = h[k] + 2.0 * h[k - 1]
        m[k] = (w1 + w2) / ((w1 / dk1) + (w2 / dk))

    m0 = ((2.0 * h[0] + h[1]) * d[0] - h[0] * d[1]) / (h[0] + h[1])
    if (m0 > 0.0) != (d[0] > 0.0):
        m0 = 0.0
    elif (d[0] > 0.0) != (d[1] > 0.0) and abs(m0) > abs(3.0 * d[0]):
        m0 = 3.0 * d[0]
    m[0] = m0

    mn = ((2.0 * h[-1] + h[-2]) * d[-1] - h[-1] * d[-2]) / (h[-1] + h[-2])
    if (mn > 0.0) != (d[-1] > 0.0):
        mn = 0.0
    elif (d[-1] > 0.0) != (d[-2] > 0.0) and abs(mn) > abs(3.0 * d[-1]):
        mn = 3.0 * d[-1]
    m[-1] = mn

    return m


def _pchip_eval(xs: List[float], ys: List[float], ms: List[float], xq: float) -> float:
    n = len(xs)
    if xq <= xs[0]:
        return ys[0]
    if xq >= xs[-1]:
        return ys[-1]

    idx = bisect_right(xs, xq) - 1
    if idx < 0:
        idx = 0
    if idx >= n - 1:
        idx = n - 2

    x0 = xs[idx]
    x1 = xs[idx + 1]
    y0 = ys[idx]
    y1 = ys[idx + 1]
    m0 = ms[idx]
    m1 = ms[idx + 1]

    h = x1 - x0
    if h <= 0.0:
        return y0
    t = (xq - x0) / h
    t2 = t * t
    t3 = t2 * t

    h00 = 2.0 * t3 - 3.0 * t2 + 1.0
    h10 = t3 - 2.0 * t2 + t
    h01 = -2.0 * t3 + 3.0 * t2
    h11 = t3 - t2
    return h00 * y0 + h10 * h * m0 + h01 * y1 + h11 * h * m1


def _resample_surface_arc_pchip(
    surface_fwd: List[Tuple[float, float]],
    target_count: int,
) -> Optional[List[Tuple[float, float]]]:
    pts = _dedupe_consecutive_points(surface_fwd)
    if len(pts) < 2:
        return None

    s = [0.0]
    for (x1, y1), (x2, y2) in zip(pts, pts[1:]):
        s.append(s[-1] + math.hypot(x2 - x1, y2 - y1))
    if s[-1] <= 1e-15:
        return None

    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    mx = _pchip_slopes(s, xs)
    my = _pchip_slopes(s, ys)

    sampled: List[Tuple[float, float]] = []
    for u in _build_cosine_x_samples(target_count):
        sq = u * s[-1]
        sampled.append((
            _pchip_eval(s, xs, mx, sq),
            _pchip_eval(s, ys, my, sq),
        ))
    return sampled


def resample_airfoil_points(
    points_norm: List[Tuple[float, float]],
    target_point_count: int = TARGET_POINT_COUNT,
) -> List[Tuple[float, float]]:
    if not NORMALIZE_POINT_COUNT:
        return points_norm
    if len(points_norm) < 5:
        return points_norm

    try:
        if target_point_count < 11:
            return points_norm
        if target_point_count % 2 == 0:
            target_point_count += 1

        upper_raw, lower_raw = split_upper_lower(points_norm)
        upper_fwd = list(reversed(upper_raw))
        lower_fwd = list(lower_raw)

        if len(upper_fwd) < 2 or len(lower_fwd) < 2:
            return points_norm

        surface_count = (target_point_count + 1) // 2
        upper_sampled_fwd = _resample_surface_arc_pchip(upper_fwd, surface_count)
        lower_sampled_fwd = _resample_surface_arc_pchip(lower_fwd, surface_count)
        if upper_sampled_fwd is None or lower_sampled_fwd is None:
            return points_norm

        upper_sampled_desc = list(reversed(upper_sampled_fwd))
        return upper_sampled_desc + lower_sampled_fwd[1:]
    except Exception:
        return points_norm


def trailing_edge_gap(points_norm: List[Tuple[float, float]]) -> float:
    if len(points_norm) < 2:
        return 0.0
    return abs(points_norm[0][1] - points_norm[-1][1])


def split_upper_lower(points: List[Tuple[float, float]]) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
    """
    Simple heuristic:
    the minimum-x point is treated as the leading edge.
    First part = upper (TE->LE)
    Second part = lower (LE->TE)
    """
    if len(points) < 5:
        raise ValueError("Punti insufficienti.")

    le_idx = min(range(len(points)), key=lambda i: points[i][0])

    upper = points[: le_idx + 1]
    lower = points[le_idx:]

    if len(upper) < 2 or len(lower) < 2:
        raise ValueError("Impossibile separare upper/lower.")

    return upper, lower


def interpolate_surface_y(surface: List[Tuple[float, float]], x_query: float) -> Optional[float]:
    for i in range(len(surface) - 1):
        x1, y1 = surface[i]
        x2, y2 = surface[i + 1]

        if (x1 <= x_query <= x2) or (x2 <= x_query <= x1):
            if abs(x2 - x1) < 1e-12:
                return 0.5 * (y1 + y2)
            t = (x_query - x1) / (x2 - x1)
            return y1 + t * (y2 - y1)
    return None


def compute_basic_metrics(points_norm: List[Tuple[float, float]]) -> Dict[str, float]:
    upper, lower = split_upper_lower(points_norm)

    x_samples = [i / 200.0 for i in range(201)]
    thickness_values = []
    camber_values = []

    for xq in x_samples:
        yu = interpolate_surface_y(upper, xq)
        yl = interpolate_surface_y(lower, xq)
        if yu is None or yl is None:
            continue

        thickness = abs(yu - yl)
        camber = 0.5 * (yu + yl)
        thickness_values.append((xq, thickness))
        camber_values.append((xq, camber))

    if not thickness_values:
        raise ValueError("Impossibile calcolare spessore/camber.")

    max_thickness_x, max_thickness = max(thickness_values, key=lambda t: t[1])
    max_camber_x, max_camber = max(camber_values, key=lambda t: abs(t[1]))

    te_y_first = points_norm[0][1]
    te_y_last = points_norm[-1][1]
    trailing_edge_gap = abs(te_y_first - te_y_last)
    trailing_edge_closed = 1 if trailing_edge_gap < 1e-4 else 0

    return {
        "n_points": len(points_norm),
        "max_thickness": max_thickness,
        "max_thickness_x": max_thickness_x,
        "max_camber": max_camber,
        "max_camber_x": max_camber_x,
        "trailing_edge_gap": trailing_edge_gap,
        "trailing_edge_closed": trailing_edge_closed,
    }


def _is_non_increasing_x(points: List[Tuple[float, float]], tol: float = 1e-6) -> bool:
    return all(points[i + 1][0] <= points[i][0] + tol for i in range(len(points) - 1))


def _is_non_decreasing_x(points: List[Tuple[float, float]], tol: float = 1e-6) -> bool:
    return all(points[i + 1][0] >= points[i][0] - tol for i in range(len(points) - 1))


def _max_segment_length(points: List[Tuple[float, float]]) -> float:
    max_len = 0.0
    for (x1, y1), (x2, y2) in zip(points, points[1:]):
        seg_len = ((x2 - x1) ** 2 + (y2 - y1) ** 2) ** 0.5
        if seg_len > max_len:
            max_len = seg_len
    return max_len


def _has_duplicate_points(points: List[Tuple[float, float]], tol: float = 1e-8) -> bool:
    seen = set()
    closing_duplicate = False
    if len(points) >= 2:
        first_x, first_y = points[0]
        last_x, last_y = points[-1]
        closing_duplicate = abs(first_x - last_x) <= tol and abs(first_y - last_y) <= tol

    for idx, (x, y) in enumerate(points):
        if closing_duplicate and idx == len(points) - 1:
            continue
        key = (round(x / tol), round(y / tol))
        if key in seen:
            return True
        seen.add(key)
    return False


def _has_sharp_spike(points: List[Tuple[float, float]]) -> bool:
    # Conservative threshold: reject only very sharp corners on non-tiny segments.
    min_segment = 0.01
    min_angle_deg = 12.0

    for i in range(1, len(points) - 1):
        x0, y0 = points[i - 1]
        x1, y1 = points[i]
        x2, y2 = points[i + 1]

        v1x, v1y = x0 - x1, y0 - y1
        v2x, v2y = x2 - x1, y2 - y1
        n1 = (v1x * v1x + v1y * v1y) ** 0.5
        n2 = (v2x * v2x + v2y * v2y) ** 0.5
        if n1 < min_segment or n2 < min_segment:
            continue

        cos_angle = (v1x * v2x + v1y * v2y) / (n1 * n2)
        cos_angle = max(-1.0, min(1.0, cos_angle))
        angle_deg = math.degrees(math.acos(cos_angle))
        if angle_deg < min_angle_deg:
            return True

    return False


def _orientation(a: Tuple[float, float], b: Tuple[float, float], c: Tuple[float, float], tol: float = 1e-10) -> int:
    value = (b[1] - a[1]) * (c[0] - b[0]) - (b[0] - a[0]) * (c[1] - b[1])
    if abs(value) <= tol:
        return 0
    return 1 if value > 0 else 2


def _on_segment(a: Tuple[float, float], b: Tuple[float, float], c: Tuple[float, float], tol: float = 1e-10) -> bool:
    return (
        min(a[0], c[0]) - tol <= b[0] <= max(a[0], c[0]) + tol
        and min(a[1], c[1]) - tol <= b[1] <= max(a[1], c[1]) + tol
    )


def _segments_intersect(
    p1: Tuple[float, float],
    q1: Tuple[float, float],
    p2: Tuple[float, float],
    q2: Tuple[float, float],
) -> bool:
    o1 = _orientation(p1, q1, p2)
    o2 = _orientation(p1, q1, q2)
    o3 = _orientation(p2, q2, p1)
    o4 = _orientation(p2, q2, q1)

    if o1 != o2 and o3 != o4:
        return True
    if o1 == 0 and _on_segment(p1, p2, q1):
        return True
    if o2 == 0 and _on_segment(p1, q2, q1):
        return True
    if o3 == 0 and _on_segment(p2, p1, q2):
        return True
    if o4 == 0 and _on_segment(p2, q1, q2):
        return True
    return False


def _has_self_intersection(points: List[Tuple[float, float]]) -> bool:
    if len(points) < 4:
        return False

    for i in range(len(points) - 1):
        for j in range(i + 1, len(points) - 1):
            if abs(i - j) <= 1:
                continue
            if i == 0 and j == len(points) - 2:
                continue
            if _segments_intersect(points[i], points[i + 1], points[j], points[j + 1]):
                # Ignore tiny TE-near overlaps; these are often harmless finite-TE artifacts.
                te_near = min(
                    points[i][0],
                    points[i + 1][0],
                    points[j][0],
                    points[j + 1][0],
                ) >= 0.95
                if te_near:
                    continue
                return True
    return False


def check_airfoil_geometry(points_norm: List[Tuple[float, float]]) -> Tuple[bool, List[str]]:
    """Return whether the normalized profile is acceptable for DB insertion.

    The thresholds are intentionally prudent: quarantine only clearly suspicious
    geometries and keep normal finite trailing edges.
    """
    reasons: List[str] = []

    upper, lower = split_upper_lower(points_norm)

    if not _is_non_increasing_x(upper):
        reasons.append("non_monotonic_upper")
    if not _is_non_decreasing_x(lower):
        reasons.append("non_monotonic_lower")

    for xq in [i / 200.0 for i in range(201)]:
        yu = interpolate_surface_y(upper, xq)
        yl = interpolate_surface_y(lower, xq)
        if yu is None or yl is None:
            continue
        if yl > yu + 1e-4:
            reasons.append("negative_thickness")
            break

    long_segment = _max_segment_length(points_norm) > 0.35

    if _has_duplicate_points(points_norm):
        reasons.append("duplicate_points")

    if _has_sharp_spike(points_norm):
        reasons.append("sharp_spike")

    if _has_self_intersection(points_norm):
        reasons.append("self_intersection")

    te_gap = abs(points_norm[0][1] - points_norm[-1][1])
    te_x_ok = points_norm[0][0] >= 0.8 and points_norm[-1][0] >= 0.8
    if not te_x_ok or te_gap > 0.10:
        reasons.append("bad_trailing_edge")

    # Sparse sampling alone is not enough for quarantine.
    if long_segment and reasons:
        reasons.append("long_segment")

    unique_reasons = list(dict.fromkeys(reasons))
    return len(unique_reasons) == 0, unique_reasons


def count_vertical_runs(points: List[Tuple[float, float]], tol: float = 1e-9) -> int:
    count = 0
    for (x1, _), (x2, _) in zip(points, points[1:]):
        if abs(x2 - x1) <= tol:
            count += 1
    return count


def classify_geometry(
    name: str,
    title: str,
    points_norm: List[Tuple[float, float]],
) -> Dict[str, object]:
    text = f"{name} {title}".lower()
    vertical_runs = count_vertical_runs(points_norm)

    reasons: List[str] = []
    is_valid_geometry = 1
    is_xfoil_compatible = 1
    exclude_from_final = 0

    if any(token in text for token in ("-main", "-slat", "-flap", " main ", " slat ", " flap ")):
        reasons.append("multi_element_component")
        is_xfoil_compatible = 0
        exclude_from_final = 1

    if vertical_runs >= 3:
        reasons.append("contains_vertical_segments")
        is_xfoil_compatible = 0
        exclude_from_final = 1

    try:
        split_upper_lower(points_norm)
    except ValueError:
        reasons.append("upper_lower_split_failed")
        is_valid_geometry = 0
        is_xfoil_compatible = 0
        exclude_from_final = 1

    if not reasons:
        reasons.append("ok")

    return {
        "is_valid_geometry": is_valid_geometry,
        "is_xfoil_compatible": is_xfoil_compatible,
        "exclude_from_final": exclude_from_final,
        "geometry_status": reasons[0],
        "geometry_notes": ",".join(reasons),
        "vertical_segment_count": vertical_runs,
    }


def save_normalized_dat(path: str, title: str, points: List[Tuple[float, float]]) -> None:
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(title + "\n")
        for x, y in points:
            f.write(f"{x:.6f} {y:.6f}\n")


def build_dat_text(title: str, points: List[Tuple[float, float]]) -> str:
    lines = [title]
    lines.extend(f"{x:.6f} {y:.6f}" for x, y in points)
    return "\n".join(lines) + "\n"


def save_quarantine_report(path: str, reasons: List[str]) -> None:
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        for reason in reasons:
            f.write(reason + "\n")


def detect_basic_family(name: str, title: str) -> str:
    text = f"{name} {title}".lower()
    compact = text.replace("-", "").replace("_", "").replace(" ", "")

    if re.search(r"\bnaca\s*\d+", text) or compact.startswith("naca"):
        return "naca"
    if "clark" in text:
        return "clark"
    if "eppler" in text:
        return "eppler"
    if re.search(r"\bfx\b", text):
        return "fx"
    if re.search(r"\bmh\d+", compact):
        return "mh"
    if re.search(r"\bs\d+", compact):
        return "selig"
    return "unknown"


def normalize_airfoil_name(name: str) -> str:
    """Return a compact canonical airfoil identifier.

    The normalization lowercases the name and removes spaces, dots, commas,
    underscores, and hyphens.
    """
    normalized = name.lower()
    normalized = normalized.replace(" ", "")
    normalized = normalized.replace(".", "")
    normalized = normalized.replace(",", "")
    normalized = normalized.replace("_", "")
    normalized = normalized.replace("-", "")
    return normalized


def init_db(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS airfoils (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            title TEXT,
            family TEXT,
            source TEXT,
            source_url TEXT,
            raw_file_path TEXT,
            normalized_file_path TEXT,
            raw_dat TEXT,
            x_json TEXT,
            y_json TEXT,
            n_points INTEGER,
            max_thickness REAL,
            max_thickness_x REAL,
            max_camber REAL,
            max_camber_x REAL,
            trailing_edge_gap REAL,
            trailing_edge_closed INTEGER,
            is_valid_geometry INTEGER DEFAULT 1,
            is_xfoil_compatible INTEGER DEFAULT 1,
            exclude_from_final INTEGER DEFAULT 0,
            geometry_status TEXT,
            geometry_notes TEXT,
            vertical_segment_count INTEGER DEFAULT 0,
            created_at TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS airfoil_sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE,
            label TEXT,
            url TEXT,
            kind TEXT,
            enabled INTEGER DEFAULT 1,
            note TEXT
        )
        """
    )

    cur.execute("CREATE INDEX IF NOT EXISTS idx_airfoils_name ON airfoils(name)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_airfoils_family ON airfoils(family)")
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_airfoils_final_filter "
        "ON airfoils(exclude_from_final, is_xfoil_compatible)"
    )
    conn.commit()


def seed_sources(conn: sqlite3.Connection) -> None:
    rows = [
        (
            "uiuc_zip",
            "UIUC bulk archive",
            UIUC_ZIP_URL,
            "bulk_geometry",
            1,
            "Primary source for batch import.",
        ),
        (
            "uiuc_db_page",
            "UIUC coordinate database page",
            UIUC_DB_PAGE_URL,
            "reference",
            1,
            "Official reference page for the archive and format notes.",
        ),
        (
            "uiuc_folder",
            "UIUC Selig format folder",
            UIUC_FOLDER_URL,
            "fallback_geometry",
            1,
            "Useful as future fallback if the zip fails.",
        ),
        (
            "airfoiltools_list",
            "AirfoilTools list",
            AIRFOILTOOLS_LIST_URL,
            "metadata_reference",
            0,
            "Useful later for aliases, links, previews, and enrichment.",
        ),
        (
            "airfoiltools_search",
            "AirfoilTools search",
            AIRFOILTOOLS_SEARCH_URL,
            "metadata_reference",
            0,
            "Useful later for search and metadata enrichment.",
        ),
    ]

    cur = conn.cursor()
    cur.executemany(
        """
        INSERT INTO airfoil_sources (code, label, url, kind, enabled, note)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(code) DO UPDATE SET
            label=excluded.label,
            url=excluded.url,
            kind=excluded.kind,
            enabled=excluded.enabled,
            note=excluded.note
        """,
        rows,
    )
    conn.commit()


def upsert_airfoil(
    conn: sqlite3.Connection,
    name: str,
    title: str,
    family: str,
    source: str,
    source_url: str,
    raw_file_path: str,
    normalized_file_path: str,
    raw_dat: str,
    points_norm: List[Tuple[float, float]],
    metrics: Dict[str, float],
    geometry_flags: Dict[str, object],
) -> None:
    cur = conn.cursor()

    x_json = json.dumps([p[0] for p in points_norm], ensure_ascii=False)
    y_json = json.dumps([p[1] for p in points_norm], ensure_ascii=False)

    cur.execute(
        """
        INSERT INTO airfoils (
            name, title, family, source, source_url, raw_file_path,
            normalized_file_path,
            raw_dat, x_json, y_json,
            n_points, max_thickness, max_thickness_x,
            max_camber, max_camber_x,
            trailing_edge_gap, trailing_edge_closed,
            is_valid_geometry, is_xfoil_compatible, exclude_from_final,
            geometry_status, geometry_notes, vertical_segment_count,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET
            title=excluded.title,
            family=excluded.family,
            source=excluded.source,
            source_url=excluded.source_url,
            raw_file_path=excluded.raw_file_path,
            normalized_file_path=excluded.normalized_file_path,
            raw_dat=excluded.raw_dat,
            x_json=excluded.x_json,
            y_json=excluded.y_json,
            n_points=excluded.n_points,
            max_thickness=excluded.max_thickness,
            max_thickness_x=excluded.max_thickness_x,
            max_camber=excluded.max_camber,
            max_camber_x=excluded.max_camber_x,
            trailing_edge_gap=excluded.trailing_edge_gap,
            trailing_edge_closed=excluded.trailing_edge_closed,
            is_valid_geometry=excluded.is_valid_geometry,
            is_xfoil_compatible=excluded.is_xfoil_compatible,
            exclude_from_final=excluded.exclude_from_final,
            geometry_status=excluded.geometry_status,
            geometry_notes=excluded.geometry_notes,
            vertical_segment_count=excluded.vertical_segment_count,
            created_at=excluded.created_at
        """,
        (
            name,
            title,
            family,
            source,
            source_url,
            raw_file_path,
            normalized_file_path,
            raw_dat,
            x_json,
            y_json,
            metrics["n_points"],
            metrics["max_thickness"],
            metrics["max_thickness_x"],
            metrics["max_camber"],
            metrics["max_camber_x"],
            metrics["trailing_edge_gap"],
            metrics["trailing_edge_closed"],
            int(geometry_flags["is_valid_geometry"]),
            int(geometry_flags["is_xfoil_compatible"]),
            int(geometry_flags["exclude_from_final"]),
            str(geometry_flags["geometry_status"]),
            str(geometry_flags["geometry_notes"]),
            int(geometry_flags["vertical_segment_count"]),
            datetime.now(timezone.utc).isoformat(timespec="seconds"),
        ),
    )
    conn.commit()


def write_error_log(failed_files: List[Tuple[str, str]]) -> None:
    with open(ERROR_LOG_PATH, "w", encoding="utf-8", newline="\n") as f:
        if failed_files:
            for name, err in failed_files:
                f.write(f"{name} -> {err}\n")
        else:
            f.write("No errors.\n")


def write_te_autoclose_log(rows: List[Tuple[str, float, str]]) -> None:
    with open(TE_AUTOCLOSE_LOG_PATH, "w", encoding="utf-8", newline="\n") as f:
        if rows:
            for name, gap_before, source_path in rows:
                f.write(f"{name}\tgap_before={gap_before:.8f}\t{source_path}\n")
        else:
            f.write("No trailing-edge auto-closures.\n")


def write_manifest() -> None:
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "primary_download": UIUC_ZIP_URL,
        "sources": [
            {
                "code": "uiuc_zip",
                "label": "UIUC bulk archive",
                "url": UIUC_ZIP_URL,
                "kind": "bulk_geometry",
                "enabled": True,
            },
            {
                "code": "uiuc_db_page",
                "label": "UIUC coordinate database page",
                "url": UIUC_DB_PAGE_URL,
                "kind": "reference",
                "enabled": True,
            },
            {
                "code": "uiuc_folder",
                "label": "UIUC Selig format folder",
                "url": UIUC_FOLDER_URL,
                "kind": "fallback_geometry",
                "enabled": True,
            },
            {
                "code": "airfoiltools_list",
                "label": "AirfoilTools list",
                "url": AIRFOILTOOLS_LIST_URL,
                "kind": "metadata_reference",
                "enabled": False,
            },
            {
                "code": "airfoiltools_search",
                "label": "AirfoilTools search",
                "url": AIRFOILTOOLS_SEARCH_URL,
                "kind": "metadata_reference",
                "enabled": False,
            },
        ],
    }
    with open(MANIFEST_PATH, "w", encoding="utf-8", newline="\n") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def import_reviewed_profiles(conn: sqlite3.Connection) -> int:
    reviewed_files = list_dat_files(REVIEWED_QUARANTINE_DIR)
    imported_count = 0

    for path in reviewed_files:
        try:
            filename = os.path.basename(path)
            airfoil_name = os.path.splitext(filename)[0]
            airfoil_name_normalized = normalize_airfoil_name(airfoil_name)

            raw_text = read_text_file(path)
            title, points = parse_airfoil_dat(raw_text)
            points = remove_consecutive_duplicates(points)
            points = remove_closing_duplicate(points)
            points_norm = normalize_airfoil(points)
            points_norm = close_trailing_edge(points_norm)
            points_norm = resample_airfoil_points(points_norm)
            points_norm = close_trailing_edge(points_norm)
            stored_dat = build_dat_text(title, points_norm)

            metrics = compute_basic_metrics(points_norm)
            geometry_flags = classify_geometry(airfoil_name_normalized, title, points_norm)
            family = detect_basic_family(airfoil_name_normalized, title)

            normalized_path = os.path.join(NORMALIZED_DIR, filename)
            save_normalized_dat(normalized_path, title, points_norm)

            upsert_airfoil(
                conn=conn,
                name=airfoil_name_normalized,
                title=title,
                family=family,
                source="reviewed_quarantine",
                source_url="",
                raw_file_path=path,
                normalized_file_path=normalized_path,
                raw_dat=stored_dat,
                points_norm=points_norm,
                metrics=metrics,
                geometry_flags=geometry_flags,
            )
            imported_count += 1
        except Exception as e:
            print(f"[WARN] Reviewed profile skipped: {path} -> {e}")

    return imported_count


def build_database(
    force_redownload: bool = False,
    reset_db: bool = True,
) -> None:
    """Create or rebuild the geometry staging database `profiles.db`."""
    ensure_dirs()

    if force_redownload and os.path.exists(ZIP_PATH):
        _safe_unlink(ZIP_PATH)

    if reset_db and os.path.exists(DB_PATH):
        _safe_unlink(DB_PATH)

    if not os.path.exists(ZIP_PATH):
        download_file(UIUC_ZIP_URL, ZIP_PATH)
    else:
        print(f"[INFO] Zip già presente: {ZIP_PATH}")

    _reset_dir(RAW_DIR)
    _reset_dir(NORMALIZED_DIR)
    _reset_dir(QUARANTINE_DIR)
    extract_zip(ZIP_PATH, RAW_DIR)

    dat_files = list_dat_files(RAW_DIR)
    print(f"[INFO] Trovati {len(dat_files)} file .dat")

    conn = sqlite3.connect(DB_PATH)
    try:
        init_db(conn)
        seed_sources(conn)

        ok_count = 0
        quarantine_count = 0
        reviewed_count = 0
        fail_count = 0
        failed_files: List[Tuple[str, str]] = []
        te_autoclose_rows: List[Tuple[str, float, str]] = []

        for idx, path in enumerate(dat_files, start=1):
            filename = os.path.basename(path)
            airfoil_name = os.path.splitext(filename)[0]
            airfoil_name_normalized = normalize_airfoil_name(airfoil_name)

            try:
                raw_text = read_text_file(path)
                title, points = parse_airfoil_dat(raw_text)
                points = remove_consecutive_duplicates(points)
                points = remove_closing_duplicate(points)
                points_norm = normalize_airfoil(points)
                te_gap_before = trailing_edge_gap(points_norm)
                points_norm = close_trailing_edge(points_norm)
                points_norm = resample_airfoil_points(points_norm)
                points_norm = close_trailing_edge(points_norm)
                stored_dat = build_dat_text(title, points_norm)
                if FORCE_CLOSED_TRAILING_EDGE and te_gap_before > 1e-12 and trailing_edge_gap(points_norm) <= 1e-12:
                    te_autoclose_rows.append((airfoil_name_normalized, te_gap_before, path))

                normalized_path = os.path.join(NORMALIZED_DIR, filename)
                quarantine_dat_path = os.path.join(QUARANTINE_DIR, filename)
                quarantine_report_path = os.path.join(
                    QUARANTINE_DIR, os.path.splitext(filename)[0] + ".txt"
                )

                is_geometry_ok, geometry_reasons = check_airfoil_geometry(points_norm)
                if not is_geometry_ok:
                    save_normalized_dat(quarantine_dat_path, title, points_norm)
                    save_quarantine_report(quarantine_report_path, geometry_reasons)
                    quarantine_count += 1
                    print(
                        f"[{idx}/{len(dat_files)}] QUA {airfoil_name} -> "
                        + ",".join(geometry_reasons)
                    )
                    continue

                metrics = compute_basic_metrics(points_norm)
                geometry_flags = classify_geometry(airfoil_name_normalized, title, points_norm)
                family = detect_basic_family(airfoil_name_normalized, title)

                save_normalized_dat(normalized_path, title, points_norm)

                upsert_airfoil(
                    conn=conn,
                    name=airfoil_name_normalized,
                    title=title,
                    family=family,
                    source="uiuc",
                    source_url=UIUC_ZIP_URL,
                    raw_file_path=path,
                    normalized_file_path=normalized_path,
                    raw_dat=stored_dat,
                    points_norm=points_norm,
                    metrics=metrics,
                    geometry_flags=geometry_flags,
                )

                ok_count += 1
                print(f"[{idx}/{len(dat_files)}] OK  {airfoil_name}")

            except Exception as e:
                fail_count += 1
                failed_files.append((filename, str(e)))
                print(f"[{idx}/{len(dat_files)}] ERR {filename} -> {e}")

        reviewed_count = import_reviewed_profiles(conn)

        write_error_log(failed_files)
        write_te_autoclose_log(te_autoclose_rows)
        write_manifest()

        print("\n===== COMPLETATO =====")
        print(f"Profili OK inseriti nel DB: {ok_count}")
        print(f"Profili messi in quarantena: {quarantine_count}")
        print(f"Profili recuperati da revisione manuale: {reviewed_count}")
        print(f"Errori veri di import/parsing: {fail_count}")
        print(f"Profili con trailing edge chiuso automaticamente: {len(te_autoclose_rows)}")
        print(f"Database SQLite: {DB_PATH}")
        print(f"DAT normalizzati: {NORMALIZED_DIR}")
        print(f"DAT in quarantena: {QUARANTINE_DIR}")
        print(f"DAT revisionati manualmente: {REVIEWED_QUARANTINE_DIR}")
        print(f"Log errori: {ERROR_LOG_PATH}")
        print(f"Log trailing edge auto-close: {TE_AUTOCLOSE_LOG_PATH}")
        print(
            "Normalizzazione punti:",
            f"{'attiva' if NORMALIZE_POINT_COUNT else 'disattiva'}",
            f"(target={TARGET_POINT_COUNT})",
        )
        print(f"Manifest sorgenti: {MANIFEST_PATH}")

    finally:
        conn.close()


def build_profiles_database(
    force_redownload: bool = False,
    reset_db: bool = True,
) -> None:
    """Public entry point for the geometry database build."""
    build_database(
        force_redownload=force_redownload,
        reset_db=reset_db,
    )


if __name__ == "__main__":
    build_profiles_database()
