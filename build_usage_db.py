# build_airfoil_usage_db.py
"""Build the staging database of airfoil usage references.

This module downloads the UIUC "Incomplete Guide to Airfoil Usage" page,
extracts aircraft and airfoil associations, and stores them in `usage.db`.
"""

import os
import re
import ssl
import json
import difflib
import sqlite3
import urllib.request
import urllib.error
from html.parser import HTMLParser
from datetime import datetime

from paths import (
    DB_DIR,
    RAW_DIR,
    USAGE_DB_PATH,
    ensure_local_dirs,
    resolve_profiles_db_path,
)
from usage_fallback_sources import lookup_usage_fallback

try:
    from rapidfuzz import fuzz as rf_fuzz  # type: ignore
except Exception:
    rf_fuzz = None

AIRCRAFT_URL = "https://m-selig.ae.illinois.edu/ads/aircraft.html"

USAGE_RAW_DIR = RAW_DIR / "usage"
DB_PATH = str(USAGE_DB_PATH)
RAW_HTML_PATH = str(USAGE_RAW_DIR / "aircraft.html")
RAW_TEXT_PATH = str(USAGE_RAW_DIR / "aircraft.txt")
ERRORS_PATH = str(DB_DIR / "usage_import_errors.txt")
MATCH_REVIEW_RESEARCH_PATH = str(USAGE_RAW_DIR / "match_review_research.json")
AUTO_MATCH_SCORE = 0.95
REVIEW_MATCH_SCORE = 0.92


def ensure_dirs():
    ensure_local_dirs()
    os.makedirs(USAGE_RAW_DIR, exist_ok=True)


def download_text(url: str) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Python airfoil-usage-builder"
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            return response.read().decode("utf-8", errors="replace")
    except urllib.error.URLError as e:
        reason = getattr(e, "reason", None)
        if isinstance(reason, ssl.SSLCertVerificationError) and "m-selig.ae.illinois.edu" in url:
            print("[WARN] SSL verification failed on UIUC host, retrying with relaxed SSL...")
            insecure_ctx = ssl._create_unverified_context()
            with urllib.request.urlopen(req, timeout=60, context=insecure_ctx) as response:
                return response.read().decode("utf-8", errors="replace")
        raise


class HTMLToText(HTMLParser):
    """
    Estrae testo conservando abbastanza newline da mantenere le righe tabellari.
    """
    BLOCK_TAGS = {
        "p", "div", "br", "hr", "tr", "table", "section",
        "h1", "h2", "h3", "h4", "h5", "h6", "li", "pre"
    }

    def __init__(self):
        super().__init__()
        self.parts = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() in self.BLOCK_TAGS:
            self.parts.append("\n")

    def handle_endtag(self, tag):
        if tag.lower() in self.BLOCK_TAGS:
            self.parts.append("\n")

    def handle_data(self, data):
        if data:
            self.parts.append(data)

    def get_text(self):
        text = "".join(self.parts)
        text = text.replace("\r", "\n")
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text


def html_to_text(html: str) -> str:
    parser = HTMLToText()
    parser.feed(html)
    text = parser.get_text()
    return text


def normalize_spaces(s: str) -> str:
    s = s.replace("\xa0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()


def normalize_airfoil_label(raw: str) -> str:
    s = raw.strip().strip('"').strip("'")
    s = s.replace("–", "-").replace("—", "-")
    s = re.sub(r"\s+", " ", s).strip()

    s = re.sub(r"\?$", "", s).strip()
    s = s.replace(" ?", "")
    s = s.replace("(mod B3)", "mod B3")
    s = s.replace("MOD", "mod")

    s = re.sub(r"\bNACA\s+", "NACA ", s, flags=re.IGNORECASE)

    return s


def normalize_airfoil_name(raw: str) -> str:
    s = normalize_airfoil_label(raw)
    s = s.lower()
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


def score_similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    if rf_fuzz is not None:
        ratio = float(rf_fuzz.ratio(a, b)) / 100.0
        partial = float(rf_fuzz.partial_ratio(a, b)) / 100.0
        token = float(rf_fuzz.token_set_ratio(a, b)) / 100.0
        return max(ratio, partial * 0.98, token * 0.99)
    return difflib.SequenceMatcher(None, a, b).ratio()


def family_key(norm_name: str) -> str:
    prefixes = [
        "naca", "goettingen", "goe", "raf", "fx", "wortmannfx", "ag", "ah",
        "mh", "eppler", "e", "s", "tsagi", "clark", "roncz", "nlf", "rc12",
    ]
    for p in prefixes:
        if norm_name.startswith(p):
            return p
    return "other"


def build_alias_buckets(profile_alias_index: dict[str, str]) -> dict[str, list[str]]:
    buckets: dict[str, list[str]] = {}
    for alias in profile_alias_index.keys():
        key = family_key(alias)
        buckets.setdefault(key, []).append(alias)
    return buckets


def expand_structured_alias_candidates(norm_candidate: str) -> list[str]:
    """Generate deterministic alias variants for known notation families."""
    out: list[str] = []
    c = norm_candidate or ""
    if not c:
        return out

    # NACA 6-series shorthand variants seen across datasets.
    # Examples:
    # - naca63a415 -> n63415 / naca63415
    # - naca64a212 -> n64212 / naca64212
    # - naca63015  -> n63015a (and n63015)
    m = re.fullmatch(r"naca(6\d)a?(\d)(\d{2,3})([a-z]?)", c)
    if m:
        family = m.group(1)     # 63 / 64 / 65 / 66
        d1 = m.group(2)         # e.g. 4
        tail = m.group(3)       # e.g. 15 / 212
        suffix = m.group(4)     # optional trailing letter
        compact = f"{family}{d1}{tail}"
        out.extend([
            f"n{compact}",
            f"naca{compact}",
            f"n{compact}{suffix}" if suffix else "",
            f"naca{compact}{suffix}" if suffix else "",
        ])
        # Common "A" suffix canonical forms for some 63/64 entries.
        if suffix == "":
            out.extend([f"n{compact}a", f"naca{compact}a"])

    # NACA 6-series with dash-style source forms:
    # naca63-615 / naca63615 / naca64415 -> naca63(2)-615 style ids in DB.
    m2 = re.fullmatch(r"naca(6\d)(\d)(\d{2})", c)
    if m2:
        family = m2.group(1)
        x = m2.group(2)
        yz = m2.group(3)
        # Try common insertion digit variants; keep deterministic exact lookup.
        for ins in ("1", "2", "3", "4"):
            out.append(f"naca{family}{ins}{x}{yz}")
        # Alternate compact n-forms.
        out.append(f"n{family}{x}{yz}")
        out.append(f"n{family}{x}{yz}a")

    # naca64012 / naca64015 style can exist as n64012 / n64015.
    m3 = re.fullmatch(r"naca(6\d\d\d\d[a-z]?)", c)
    if m3:
        out.append(f"n{m3.group(1)}")

    # Deduplicate preserving order.
    seen = set()
    ordered = []
    for item in out:
        if not item or item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


def ensure_profiles_airfoils_table(conn: sqlite3.Connection, profiles_db_path: str) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name='airfoils'
        """
    )
    if cur.fetchone():
        return

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row[0] for row in cur.fetchall()]
    raise RuntimeError(
        "La tabella 'airfoils' non esiste nel database selezionato.\n"
        "Esegui prima: python build_profiles_db.py\n"
        f"DB aperto: {profiles_db_path}\n"
        f"Tabelle trovate: {tables}"
    )


def load_profile_alias_index() -> dict[str, str]:
    index: dict[str, str] = {}

    def add_alias(alias: str, canonical_name: str) -> None:
        if alias:
            index.setdefault(alias, canonical_name)

    profiles_db_path = resolve_profiles_db_path()
    if not profiles_db_path.exists():
        raise FileNotFoundError(
            "profiles.db non trovato.\n"
            "Esegui prima: python build_profiles_db.py\n"
            f"DB atteso: {profiles_db_path}"
        )

    conn = sqlite3.connect(str(profiles_db_path))
    try:
        ensure_profiles_airfoils_table(conn, str(profiles_db_path))
        cur = conn.cursor()
        cur.execute("SELECT name, title FROM airfoils")
        for canonical_name, title in cur.fetchall():
            if canonical_name:
                canonical_name = str(canonical_name)
                canonical_norm = normalize_airfoil_name(canonical_name)
                add_alias(canonical_norm, canonical_name)

                # Common naming family aliases across datasets.
                m_naca_short = re.fullmatch(r"n(\d{4,5})", canonical_norm)
                if m_naca_short:
                    add_alias(f"naca{m_naca_short.group(1)}", canonical_name)

                m_goe_short = re.fullmatch(r"goe(\d+)", canonical_norm)
                if m_goe_short:
                    add_alias(f"goettingen{m_goe_short.group(1)}", canonical_name)

                if canonical_norm.startswith("fx"):
                    add_alias(f"wortmann{canonical_norm}", canonical_name)

            if title:
                title = str(title)
                add_alias(normalize_airfoil_name(title), canonical_name)

                # Title cleanup variants: remove generic noun to preserve useful token.
                title_wo_airfoil = re.sub(r"\bairfoils?\b", "", title, flags=re.IGNORECASE).strip()
                add_alias(normalize_airfoil_name(title_wo_airfoil), canonical_name)
    finally:
        conn.close()

    return index


def load_profile_metadata_index() -> dict[str, dict[str, float]]:
    index: dict[str, dict[str, float]] = {}
    profiles_db_path = resolve_profiles_db_path()
    if not profiles_db_path.exists():
        return index

    conn = sqlite3.connect(str(profiles_db_path))
    try:
        ensure_profiles_airfoils_table(conn, str(profiles_db_path))
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                name,
                COALESCE(max_thickness, 0.0),
                COALESCE(max_camber, 0.0),
                COALESCE(max_camber_x, 0.0)
            FROM airfoils
            """
        )
        for name, max_thickness, max_camber, max_camber_x in cur.fetchall():
            if not name:
                continue
            index[str(name)] = {
                "max_thickness": float(max_thickness),
                "max_camber": float(max_camber),
                "max_camber_x": float(max_camber_x),
            }
    finally:
        conn.close()

    return index


def resolve_profile_name(
    raw: str,
    profile_alias_index: dict[str, str],
    alias_buckets: dict[str, list[str]],
) -> tuple[str | None, str, float, str | None]:
    base = normalize_airfoil_name(raw)
    candidates = [base]

    raw_low = raw.lower()
    if "%" in raw_low:
        raw_wo_pct = re.sub(r"\(?\s*\d+(?:\.\d+)?\s*%\s*\)?", "", raw_low).strip()
        candidates.append(normalize_airfoil_name(raw_wo_pct))

    for key in list(candidates):
        k = key

        # Common suffixes frequently used in source tables.
        k = re.sub(r"(mod|modified|smoothed|droopedle)$", "", k)
        candidates.append(k)

        # Strip single trailing variant letter (e.g. NACA 4409R -> NACA 4409).
        k2 = re.sub(r"([a-z0-9])([a-z])$", r"\1", k)
        candidates.append(k2)

        # Strip trailing decimal artifact converted into extra digit.
        k3 = re.sub(r"^(naca\d{4})\d$", r"\1", k2)
        candidates.append(k3)

        # Family alias harmonization.
        if k3.startswith("goettingen"):
            candidates.append("goe" + k3[len("goettingen"):])
        if k3.startswith("wortmannfx"):
            candidates.append(k3[len("wortmann"):])

    # Preserve order while removing empty/duplicates.
    seen = set()
    ordered = []
    for c in candidates:
        if not c or c in seen:
            continue
        seen.add(c)
        ordered.append(c)

    for c in ordered:
        match = profile_alias_index.get(c)
        if match:
            return match, "alias_exact", 1.0, None

    # Deterministic notation expansions (before fuzzy).
    expanded = []
    for c in ordered:
        expanded.extend(expand_structured_alias_candidates(c))

    seen_exp = set()
    for c in expanded:
        if c in seen_exp:
            continue
        seen_exp.add(c)
        match = profile_alias_index.get(c)
        if match:
            return match, "alias_structured", 0.995, None

    # Advanced fuzzy matching constrained by family bucket.
    best_alias = ""
    best_name = None
    best_score = 0.0
    for c in ordered:
        if len(c) < 4:
            continue
        fam = family_key(c)
        pool = alias_buckets.get(fam) or alias_buckets.get("other", [])
        if not pool:
            continue
        local_best_alias = ""
        local_best_score = 0.0
        for alias in pool:
            s = score_similarity(c, alias)
            if s > local_best_score:
                local_best_score = s
                local_best_alias = alias
        if local_best_alias and local_best_score > best_score:
            best_alias = local_best_alias
            best_score = local_best_score
            best_name = profile_alias_index.get(local_best_alias)

    if best_name and best_score >= AUTO_MATCH_SCORE:
        return best_name, "fuzzy_auto", round(best_score, 4), None
    if best_name and best_score >= REVIEW_MATCH_SCORE:
        return None, "fuzzy_review", round(best_score, 4), best_name
    return None, "no_match", 0.0, None


def split_airfoil_variants(raw: str):
    """
    Divide stringhe tipo:
    - 'Goettingen 533/W-339'
    - 'ONERA OA209/OA207'
    """
    raw = raw.strip()
    if not raw:
        return []

    parts = [p.strip() for p in raw.split("/")]

    out = []
    for p in parts:
        if p:
            out.append(p)

    return out if out else [raw]


def guess_uncertainty(raw: str) -> float:
    raw_l = raw.lower()
    score = 1.0
    if "?" in raw:
        score -= 0.35
    if "unknown" in raw_l or raw_l.startswith("?"):
        score -= 0.45
    if "??" in raw:
        score -= 0.15
    return max(0.1, round(score, 2))


def init_db(conn: sqlite3.Connection):
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS source_meta (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_name TEXT,
        source_url TEXT,
        fetched_at TEXT,
        notes TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS aircraft_usage_rows (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        section_code TEXT NOT NULL,
        section_label TEXT NOT NULL,
        aircraft_name TEXT NOT NULL,
        col1_label TEXT NOT NULL,
        col1_value TEXT,
        col2_label TEXT NOT NULL,
        col2_value TEXT,
        source_url TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS airfoil_applications (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        airfoil_raw TEXT NOT NULL,
        airfoil_norm TEXT NOT NULL,
        matched_profile_name TEXT,
        match_method TEXT,
        match_score REAL,
        aircraft_name TEXT NOT NULL,
        aircraft_section TEXT NOT NULL,
        role_code TEXT NOT NULL,
        role_label TEXT NOT NULL,
        context_tag TEXT NOT NULL DEFAULT 'unknown',
        profile_type_tag TEXT NOT NULL DEFAULT 'unknown',
        reason_tag TEXT NOT NULL DEFAULT 'unknown',
        tag_confidence REAL NOT NULL DEFAULT 0.5,
        confidence REAL NOT NULL,
        source TEXT NOT NULL,
        source_url TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_airfoil_applications_norm
    ON airfoil_applications(airfoil_norm)
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_airfoil_applications_aircraft
    ON airfoil_applications(aircraft_name)
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS airfoil_match_review (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        airfoil_raw TEXT NOT NULL,
        airfoil_norm TEXT NOT NULL,
        suggested_profile_name TEXT NOT NULL,
        match_method TEXT NOT NULL,
        match_score REAL NOT NULL,
        aircraft_name TEXT NOT NULL,
        aircraft_section TEXT NOT NULL,
        role_code TEXT NOT NULL,
        source TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        created_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_airfoil_match_review_unique
    ON airfoil_match_review(airfoil_norm, suggested_profile_name, aircraft_section, role_code)
    """)

    conn.commit()


def ensure_airfoil_applications_columns(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(airfoil_applications)")
    existing = {str(row[1]) for row in cur.fetchall()}

    additions = [
        ("match_method", "TEXT"),
        ("match_score", "REAL"),
        ("context_tag", "TEXT NOT NULL DEFAULT 'unknown'"),
        ("profile_type_tag", "TEXT NOT NULL DEFAULT 'unknown'"),
        ("reason_tag", "TEXT NOT NULL DEFAULT 'unknown'"),
        ("tag_confidence", "REAL NOT NULL DEFAULT 0.5"),
    ]
    for col_name, col_decl in additions:
        if col_name not in existing:
            cur.execute(f"ALTER TABLE airfoil_applications ADD COLUMN {col_name} {col_decl}")
    conn.commit()


SECTION_CONFIGS = [
    {
        "header_prefix": "Conventional Aircraft:",
        "section_code": "conventional",
        "section_label": "Conventional Aircraft",
        "col1_label": "Wing Root Airfoil",
        "col2_label": "Wing Tip Airfoil",
        "role1_code": "wing_root",
        "role1_label": "Wing Root Airfoil",
        "role2_code": "wing_tip",
        "role2_label": "Wing Tip Airfoil",
    },
    {
        "header_prefix": "Canard, Tandem Wing & Three-Surface Aircraft:",
        "section_code": "canard_tandem_three_surface",
        "section_label": "Canard, Tandem Wing & Three-Surface Aircraft",
        "col1_label": "Fwd Wing Airfoil",
        "col2_label": "Aft Wing Airfoil",
        "role1_code": "forward_wing",
        "role1_label": "Forward Wing Airfoil",
        "role2_code": "aft_wing",
        "role2_label": "Aft Wing Airfoil",
    },
    {
        "header_prefix": "Helicopters,Tiltrotors & Autogyros:",
        "section_code": "rotary",
        "section_label": "Helicopters, Tilt Rotors & Autogyros",
        "col1_label": "Inbd Blade Airfoil",
        "col2_label": "Outbd Blade Airfoil",
        "role1_code": "inboard_blade",
        "role1_label": "Inboard Blade Airfoil",
        "role2_code": "outboard_blade",
        "role2_label": "Outboard Blade Airfoil",
    },
    {
        "header_prefix": "Helicopters, Tilt Rotors & Autogyros:",
        "section_code": "rotary",
        "section_label": "Helicopters, Tilt Rotors & Autogyros",
        "col1_label": "Inbd Blade Airfoil",
        "col2_label": "Outbd Blade Airfoil",
        "role1_code": "inboard_blade",
        "role1_label": "Inboard Blade Airfoil",
        "role2_code": "outboard_blade",
        "role2_label": "Outboard Blade Airfoil",
    },
]


def find_section_start(lines, header_prefix):
    for i, line in enumerate(lines):
        if normalize_spaces(line).startswith(header_prefix):
            return i
    return -1


def parse_section_rows(lines, start_idx):
    """
    Dalla riga header in poi, prende righe tabellari fino a separatore o nuova sezione.
    Le righe utili hanno tipicamente 3 colonne separate da tanti spazi.
    """
    rows = []

    for i in range(start_idx + 1, len(lines)):
        raw = lines[i].rstrip("\n")
        line = raw.rstrip()

        if not line.strip():
            continue

        slim = normalize_spaces(line)

        if slim.startswith("* * *"):
            break

        if slim.startswith("| Conventional Aircraft |"):
            break

        if slim.startswith("Conventional Aircraft:"):
            break
        if slim.startswith("Canard, Tandem Wing & Three-Surface Aircraft:"):
            break
        if slim.startswith("Helicopters,Tiltrotors & Autogyros:"):
            break
        if slim.startswith("Helicopters, Tilt Rotors & Autogyros:"):
            break

        parts = re.split(r"\s{2,}", line.strip())
        parts = [p.strip() for p in parts if p.strip()]

        if len(parts) < 3:
            continue

        aircraft_name = parts[0]
        col1 = parts[1]
        col2 = parts[2]

        rows.append((aircraft_name, col1, col2))

    return rows


def insert_row_and_applications(
    conn,
    section_cfg,
    aircraft_name,
    col1_value,
    col2_value,
    source_url,
    profile_alias_index,
    alias_buckets,
    profile_metadata_index,
):
    cur = conn.cursor()
    now = datetime.utcnow().isoformat(timespec="seconds")

    cur.execute("""
    INSERT INTO aircraft_usage_rows (
        section_code, section_label, aircraft_name,
        col1_label, col1_value, col2_label, col2_value,
        source_url, created_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        section_cfg["section_code"],
        section_cfg["section_label"],
        aircraft_name,
        section_cfg["col1_label"],
        col1_value,
        section_cfg["col2_label"],
        col2_value,
        source_url,
        now,
    ))

    def infer_context_tag(role_code: str) -> str:
        mapping = {
            "wing_root": "wing_root",
            "wing_tip": "wing_tip",
            "forward_wing": "forward_wing",
            "aft_wing": "aft_wing",
            "inboard_blade": "rotor_inboard",
            "outboard_blade": "rotor_outboard",
        }
        return mapping.get(role_code, "unknown")

    def infer_profile_type_tag(matched_name: str | None) -> str:
        if not matched_name:
            return "unknown"
        meta = profile_metadata_index.get(matched_name)
        if not meta:
            return "unknown"
        camber = abs(float(meta.get("max_camber", 0.0)))
        camber_x = float(meta.get("max_camber_x", 0.0))
        if camber <= 0.002:
            return "symmetric"
        if camber >= 0.035:
            return "high_camber"
        if camber_x >= 0.60:
            return "cambered_aft"
        if camber_x <= 0.35:
            return "cambered_forward"
        return "cambered_mid"

    def infer_reason_tag(context_tag: str, matched_name: str | None) -> tuple[str, float]:
        if context_tag.startswith("rotor_"):
            return "rotor_efficiency", 0.85
        if context_tag in ("forward_wing", "aft_wing"):
            return "stability_trim", 0.75

        meta = profile_metadata_index.get(matched_name) if matched_name else None
        if not meta:
            if context_tag == "wing_tip":
                return "low_drag", 0.60
            if context_tag == "wing_root":
                return "structural_thickness", 0.60
            return "general_purpose", 0.50

        thickness = float(meta.get("max_thickness", 0.0))
        camber = abs(float(meta.get("max_camber", 0.0)))

        if context_tag == "wing_root":
            if thickness >= 0.14:
                return "structural_thickness", 0.85
            if camber >= 0.025:
                return "high_lift", 0.75
            return "general_purpose", 0.60
        if context_tag == "wing_tip":
            if thickness <= 0.12 and camber <= 0.02:
                return "low_drag", 0.85
            return "tip_balance", 0.70
        return "general_purpose", 0.50

    def insert_review_candidate(
        raw_variant: str,
        suggested_profile_name: str,
        match_method: str,
        match_score: float,
        role_code: str,
    ) -> None:
        cur.execute(
            """
            INSERT OR IGNORE INTO airfoil_match_review (
                airfoil_raw, airfoil_norm, suggested_profile_name, match_method, match_score,
                aircraft_name, aircraft_section, role_code, source, status, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                raw_variant,
                normalize_airfoil_name(raw_variant),
                suggested_profile_name,
                match_method,
                match_score,
                aircraft_name,
                section_cfg["section_code"],
                role_code,
                "uiuc_incomplete_guide",
                "pending",
                now,
            ),
        )

    for raw_variant in split_airfoil_variants(col1_value):
        raw_variant = raw_variant.strip()
        if not raw_variant:
            continue
        matched_name, match_method, match_score, review_candidate = resolve_profile_name(
            raw_variant,
            profile_alias_index,
            alias_buckets,
        )
        if review_candidate:
            insert_review_candidate(
                raw_variant=raw_variant,
                suggested_profile_name=review_candidate,
                match_method=match_method,
                match_score=match_score,
                role_code=section_cfg["role1_code"],
            )
        context_tag = infer_context_tag(section_cfg["role1_code"])
        profile_type_tag = infer_profile_type_tag(matched_name)
        reason_tag, tag_confidence = infer_reason_tag(context_tag, matched_name)
        cur.execute("""
        INSERT INTO airfoil_applications (
            airfoil_raw, airfoil_norm, matched_profile_name, match_method, match_score, aircraft_name, aircraft_section,
            role_code, role_label, context_tag, profile_type_tag, reason_tag, tag_confidence, confidence,
            source, source_url, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            raw_variant,
            normalize_airfoil_name(raw_variant),
            matched_name,
            match_method,
            match_score,
            aircraft_name,
            section_cfg["section_code"],
            section_cfg["role1_code"],
            section_cfg["role1_label"],
            context_tag,
            profile_type_tag,
            reason_tag,
            tag_confidence,
            guess_uncertainty(raw_variant),
            "uiuc_incomplete_guide",
            source_url,
            now,
        ))

    for raw_variant in split_airfoil_variants(col2_value):
        raw_variant = raw_variant.strip()
        if not raw_variant:
            continue
        matched_name, match_method, match_score, review_candidate = resolve_profile_name(
            raw_variant,
            profile_alias_index,
            alias_buckets,
        )
        if review_candidate:
            insert_review_candidate(
                raw_variant=raw_variant,
                suggested_profile_name=review_candidate,
                match_method=match_method,
                match_score=match_score,
                role_code=section_cfg["role2_code"],
            )
        context_tag = infer_context_tag(section_cfg["role2_code"])
        profile_type_tag = infer_profile_type_tag(matched_name)
        reason_tag, tag_confidence = infer_reason_tag(context_tag, matched_name)
        cur.execute("""
        INSERT INTO airfoil_applications (
            airfoil_raw, airfoil_norm, matched_profile_name, match_method, match_score, aircraft_name, aircraft_section,
            role_code, role_label, context_tag, profile_type_tag, reason_tag, tag_confidence, confidence,
            source, source_url, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            raw_variant,
            normalize_airfoil_name(raw_variant),
            matched_name,
            match_method,
            match_score,
            aircraft_name,
            section_cfg["section_code"],
            section_cfg["role2_code"],
            section_cfg["role2_label"],
            context_tag,
            profile_type_tag,
            reason_tag,
            tag_confidence,
            guess_uncertainty(raw_variant),
            "uiuc_incomplete_guide",
            source_url,
            now,
        ))

    conn.commit()


def clear_existing_data(conn):
    cur = conn.cursor()
    cur.execute("DELETE FROM source_meta")
    cur.execute("DELETE FROM aircraft_usage_rows")
    cur.execute("DELETE FROM airfoil_applications")
    cur.execute("DELETE FROM airfoil_match_review")
    conn.commit()


def list_profiles_without_usage_candidates(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    # `airfoils` lives in profiles.db, while `airfoil_applications` lives in usage.db.
    # We compare the two sets explicitly to avoid cross-DB SQL assumptions.
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT matched_profile_name
        FROM airfoil_applications
        WHERE matched_profile_name IS NOT NULL
          AND TRIM(matched_profile_name) <> ''
        """
    )
    matched_names = {str(row[0]) for row in cur.fetchall() if row and row[0]}

    profiles_db_path = resolve_profiles_db_path()
    if not profiles_db_path.exists():
        raise FileNotFoundError(
            "profiles.db non trovato.\n"
            "Esegui prima: python build_profiles_db.py\n"
            f"DB atteso: {profiles_db_path}"
        )

    pconn = sqlite3.connect(str(profiles_db_path))
    try:
        ensure_profiles_airfoils_table(pconn, str(profiles_db_path))
        pcur = pconn.cursor()
        pcur.execute("""
        SELECT name, COALESCE(title, '')
        FROM airfoils
        WHERE name IS NOT NULL
          AND TRIM(name) <> ''
        """)
        all_profiles = [(str(row[0]), str(row[1] or "")) for row in pcur.fetchall() if row and row[0]]
    finally:
        pconn.close()

    return sorted([(name, title) for (name, title) in all_profiles if name not in matched_names], key=lambda x: x[0])


def insert_fallback_application(
    conn: sqlite3.Connection,
    matched_profile_name: str,
    usage_text: str,
    source: str,
    source_url: str,
):
    now = datetime.utcnow().isoformat(timespec="seconds")
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO airfoil_applications (
            airfoil_raw, airfoil_norm, matched_profile_name, aircraft_name, aircraft_section,
            role_code, role_label, context_tag, profile_type_tag, reason_tag, tag_confidence, confidence,
            source, source_url, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            matched_profile_name,
            normalize_airfoil_name(matched_profile_name),
            matched_profile_name,
            usage_text,
            "fallback",
            "unknown",
            "Unknown Role",
            "unknown",
            "unknown",
            "general_purpose",
            0.50,
            0.60,
            source,
            source_url,
            now,
        ),
    )
    conn.commit()


def insert_coverage_fallback_application(
    conn: sqlite3.Connection,
    matched_profile_name: str,
    profile_title: str,
):
    """Guarantee at least one usage row for every profile with low-confidence fallback."""
    usage_text = "Unknown usage (needs review)"
    if profile_title:
        usage_text = f"Unknown usage (needs review) - {profile_title}"

    now = datetime.utcnow().isoformat(timespec="seconds")
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO airfoil_applications (
            airfoil_raw, airfoil_norm, matched_profile_name, aircraft_name, aircraft_section,
            role_code, role_label, context_tag, profile_type_tag, reason_tag, tag_confidence, confidence,
            source, source_url, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            matched_profile_name,
            normalize_airfoil_name(matched_profile_name),
            matched_profile_name,
            usage_text,
            "fallback",
            "unknown",
            "Unknown Role",
            "unknown",
            "unknown",
            "general_purpose",
            0.20,
            0.20,
            "coverage_fallback",
            "internal://coverage_fallback",
            now,
        ),
    )
    conn.commit()


def load_review_research_promotions() -> dict[str, dict[str, str]]:
    """
    Load externally researched promotions from match_review_research.json.
    Returns map: profile_name -> {usage_text, source, source_url}
    """
    if not os.path.exists(MATCH_REVIEW_RESEARCH_PATH):
        return {}
    try:
        with open(MATCH_REVIEW_RESEARCH_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            return {}
        out: dict[str, dict[str, str]] = {}
        for item in data:
            if not isinstance(item, dict):
                continue
            recommendation = str(item.get("recommendation") or "").strip()
            if recommendation not in ("promote_bigfoil", "promote_airfoiltools"):
                continue
            profile_name = str(item.get("suggested_profile_name") or "").strip()
            usage_text = str(item.get("usage_found") or "").strip()
            source = str(item.get("usage_source") or "").strip()
            source_url = str(item.get("usage_url") or "").strip()
            if not profile_name or not usage_text or not source or not source_url:
                continue
            out[profile_name] = {
                "usage_text": usage_text,
                "source": source,
                "source_url": source_url,
            }
        return out
    except Exception:
        return {}


def apply_research_promotions(conn: sqlite3.Connection, promotions: dict[str, dict[str, str]]) -> int:
    """
    Apply researched promotions to usage applications and close corresponding review rows.
    Returns number of promoted profiles inserted.
    """
    if not promotions:
        return 0
    inserted = 0
    for profile_name, payload in promotions.items():
        try:
            insert_fallback_application(
                conn=conn,
                matched_profile_name=profile_name,
                usage_text=payload["usage_text"],
                source=payload["source"],
                source_url=payload["source_url"],
            )
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE airfoil_match_review
                SET status='promoted_auto'
                WHERE status='pending'
                  AND suggested_profile_name=?
                """,
                (profile_name,),
            )
            conn.commit()
            inserted += 1
        except Exception:
            continue
    return inserted


def build_usage_database(reset_db: bool = True):
    """Create or rebuild the usage staging database `usage.db`."""
    ensure_dirs()

    if reset_db and os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    print(f"[INFO] Downloading: {AIRCRAFT_URL}")
    html = download_text(AIRCRAFT_URL)

    with open(RAW_HTML_PATH, "w", encoding="utf-8", newline="\n") as f:
        f.write(html)

    text = html_to_text(html)

    with open(RAW_TEXT_PATH, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)

    lines = text.splitlines()
    profile_alias_index = load_profile_alias_index()
    alias_buckets = build_alias_buckets(profile_alias_index)
    profile_metadata_index = load_profile_metadata_index()

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)
    ensure_airfoil_applications_columns(conn)
    clear_existing_data(conn)

    cur = conn.cursor()
    cur.execute("""
    INSERT INTO source_meta (source_name, source_url, fetched_at, notes)
    VALUES (?, ?, ?, ?)
    """, (
        "UIUC The Incomplete Guide to Airfoil Usage",
        AIRCRAFT_URL,
        datetime.utcnow().isoformat(timespec="seconds"),
        "Parsed from current HTML layout into structured SQLite tables.",
    ))
    conn.commit()

    total_rows = 0
    errors = []

    seen_sections = set()

    for cfg in SECTION_CONFIGS:
        section_key = cfg["section_code"]
        if section_key in seen_sections and section_key == "rotary":
            continue

        start_idx = find_section_start(lines, cfg["header_prefix"])
        if start_idx < 0:
            errors.append(f"Section header not found: {cfg['header_prefix']}")
            continue

        rows = parse_section_rows(lines, start_idx)

        if rows:
            seen_sections.add(section_key)

        print(f"[INFO] Section '{cfg['section_label']}' -> {len(rows)} rows")

        for aircraft_name, col1_value, col2_value in rows:
            try:
                insert_row_and_applications(
                    conn=conn,
                    section_cfg=cfg,
                    aircraft_name=aircraft_name,
                    col1_value=col1_value,
                    col2_value=col2_value,
                    source_url=AIRCRAFT_URL,
                    profile_alias_index=profile_alias_index,
                    alias_buckets=alias_buckets,
                    profile_metadata_index=profile_metadata_index,
                )
                total_rows += 1
            except Exception as e:
                errors.append(f"{cfg['section_label']} | {aircraft_name} -> {e}")

    orphan_profiles = list_profiles_without_usage_candidates(conn)
    for profile_name, profile_title in orphan_profiles:
        try:
            fallback_items = lookup_usage_fallback(profile_name, profile_title)
            for item in fallback_items:
                usage_text = (item.get("usage_text") or "").strip()
                source = (item.get("source") or "").strip()
                source_url = (item.get("source_url") or "").strip()
                if not usage_text or not source or not source_url:
                    continue
                insert_fallback_application(
                    conn=conn,
                    matched_profile_name=profile_name,
                    usage_text=usage_text,
                    source=source,
                    source_url=source_url,
                )
        except Exception as e:
            errors.append(f"FALLBACK | {profile_name} -> {e}")

    # Promote externally researched matches (if research report is present).
    try:
        promotions = load_review_research_promotions()
        n_promoted = apply_research_promotions(conn, promotions)
    except Exception as e:
        n_promoted = 0
        errors.append(f"PROMOTION_RESEARCH | {e}")

    # Hard guarantee: ensure every profile has at least one usage candidate.
    final_orphans = list_profiles_without_usage_candidates(conn)
    for profile_name, profile_title in final_orphans:
        try:
            insert_coverage_fallback_application(
                conn=conn,
                matched_profile_name=profile_name,
                profile_title=profile_title,
            )
        except Exception as e:
            errors.append(f"COVERAGE_FALLBACK | {profile_name} -> {e}")

    with open(ERRORS_PATH, "w", encoding="utf-8", newline="\n") as f:
        if errors:
            for err in errors:
                f.write(err + "\n")
        else:
            f.write("No errors.\n")

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM aircraft_usage_rows")
    n_usage_rows = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM airfoil_applications")
    n_apps = cur.fetchone()[0]

    cur.execute("""
    SELECT COUNT(*)
    FROM airfoil_applications
    WHERE matched_profile_name IS NOT NULL
    """)
    n_matched = cur.fetchone()[0]

    cur.execute("""
    SELECT COUNT(DISTINCT matched_profile_name)
    FROM airfoil_applications
    WHERE matched_profile_name IS NOT NULL
      AND TRIM(matched_profile_name) <> ''
    """)
    n_matched_distinct_profiles = cur.fetchone()[0]

    profiles_db_path = resolve_profiles_db_path()
    n_profiles_distinct = 0
    if profiles_db_path.exists():
        pconn = sqlite3.connect(str(profiles_db_path))
        try:
            ensure_profiles_airfoils_table(pconn, str(profiles_db_path))
            pcur = pconn.cursor()
            pcur.execute("""
            SELECT COUNT(DISTINCT name)
            FROM airfoils
            WHERE name IS NOT NULL
              AND TRIM(name) <> ''
            """)
            n_profiles_distinct = pcur.fetchone()[0] or 0
        finally:
            pconn.close()

    # Coverage tiers on distinct matched profiles.
    cur.execute("""
    SELECT matched_profile_name, source, COALESCE(match_method, '')
    FROM airfoil_applications
    WHERE matched_profile_name IS NOT NULL
      AND TRIM(matched_profile_name) <> ''
    """)
    profile_sources: dict[str, set[str]] = {}
    profile_methods: dict[str, set[str]] = {}
    for profile_name, source, match_method in cur.fetchall():
        key = str(profile_name)
        profile_sources.setdefault(key, set()).add(str(source or ""))
        profile_methods.setdefault(key, set()).add(str(match_method or ""))

    confirmed_sources = {"uiuc_incomplete_guide", "bigfoil", "airfoiltools"}
    fallback_default_sources = {"coverage_fallback"}

    n_confirmed_profiles = 0
    n_inferred_profiles = 0
    n_fallback_default_profiles = 0
    for profile_name, sources in profile_sources.items():
        methods = profile_methods.get(profile_name, set())
        if sources & confirmed_sources:
            if "fuzzy_auto" in methods:
                n_inferred_profiles += 1
            else:
                n_confirmed_profiles += 1
        elif sources & fallback_default_sources:
            n_fallback_default_profiles += 1

    cur.execute("SELECT COUNT(*) FROM airfoil_match_review WHERE status='pending'")
    n_review_pending = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM airfoil_match_review WHERE status='promoted_auto'")
    n_review_promoted_auto = cur.fetchone()[0]

    cur.execute("""
    SELECT airfoil_norm, COUNT(*) AS n
    FROM airfoil_applications
    GROUP BY airfoil_norm
    ORDER BY n DESC, airfoil_norm ASC
    LIMIT 20
    """)
    top_airfoils = cur.fetchall()

    conn.close()

    summary = {
        "source_url": AIRCRAFT_URL,
        "db_path": DB_PATH,
        "usage_rows": n_usage_rows,
        "airfoil_applications": n_apps,
        "matched_profile_applications": n_matched,
        "matched_distinct_profiles": n_matched_distinct_profiles,
        "profiles_distinct_total": n_profiles_distinct,
        "profiles_coverage_percent": round(
            (n_matched_distinct_profiles / n_profiles_distinct * 100.0) if n_profiles_distinct else 0.0, 2
        ),
        "coverage_confirmed_profiles": n_confirmed_profiles,
        "coverage_inferred_profiles": n_inferred_profiles,
        "coverage_fallback_default_profiles": n_fallback_default_profiles,
        "coverage_confirmed_percent": round(
            (n_confirmed_profiles / n_profiles_distinct * 100.0) if n_profiles_distinct else 0.0, 2
        ),
        "coverage_inferred_percent": round(
            (n_inferred_profiles / n_profiles_distinct * 100.0) if n_profiles_distinct else 0.0, 2
        ),
        "coverage_fallback_default_percent": round(
            (n_fallback_default_profiles / n_profiles_distinct * 100.0) if n_profiles_distinct else 0.0, 2
        ),
        "match_review_pending": n_review_pending,
        "match_review_promoted_auto": n_review_promoted_auto,
        "research_promotions_applied": n_promoted,
        "auto_match_score_threshold": AUTO_MATCH_SCORE,
        "review_match_score_threshold": REVIEW_MATCH_SCORE,
        "top_airfoils": top_airfoils,
        "errors_file": ERRORS_PATH,
    }

    print("\n===== DONE =====")
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    build_usage_database()
