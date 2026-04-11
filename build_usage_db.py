# build_airfoil_usage_db.py
"""Build the staging database of airfoil usage references.

This module downloads the UIUC "Incomplete Guide to Airfoil Usage" page,
extracts aircraft and airfoil associations, and stores them in `usage.db`.
"""

import os
import re
import ssl
import json
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

AIRCRAFT_URL = "https://m-selig.ae.illinois.edu/ads/aircraft.html"

USAGE_RAW_DIR = RAW_DIR / "usage"
DB_PATH = str(USAGE_DB_PATH)
RAW_HTML_PATH = str(USAGE_RAW_DIR / "aircraft.html")
RAW_TEXT_PATH = str(USAGE_RAW_DIR / "aircraft.txt")
ERRORS_PATH = str(DB_DIR / "usage_import_errors.txt")


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
    return s


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
                index[normalize_airfoil_name(canonical_name)] = canonical_name
            if title:
                index.setdefault(normalize_airfoil_name(title), canonical_name)
    finally:
        conn.close()

    return index


def resolve_profile_name(raw: str, profile_alias_index: dict[str, str]) -> str | None:
    return profile_alias_index.get(normalize_airfoil_name(raw))


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
        aircraft_name TEXT NOT NULL,
        aircraft_section TEXT NOT NULL,
        role_code TEXT NOT NULL,
        role_label TEXT NOT NULL,
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

    for raw_variant in split_airfoil_variants(col1_value):
        raw_variant = raw_variant.strip()
        if not raw_variant:
            continue
        cur.execute("""
        INSERT INTO airfoil_applications (
            airfoil_raw, airfoil_norm, matched_profile_name, aircraft_name, aircraft_section,
            role_code, role_label, confidence,
            source, source_url, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            raw_variant,
            normalize_airfoil_name(raw_variant),
            resolve_profile_name(raw_variant, profile_alias_index),
            aircraft_name,
            section_cfg["section_code"],
            section_cfg["role1_code"],
            section_cfg["role1_label"],
            guess_uncertainty(raw_variant),
            "uiuc_incomplete_guide",
            source_url,
            now,
        ))

    for raw_variant in split_airfoil_variants(col2_value):
        raw_variant = raw_variant.strip()
        if not raw_variant:
            continue
        cur.execute("""
        INSERT INTO airfoil_applications (
            airfoil_raw, airfoil_norm, matched_profile_name, aircraft_name, aircraft_section,
            role_code, role_label, confidence,
            source, source_url, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            raw_variant,
            normalize_airfoil_name(raw_variant),
            resolve_profile_name(raw_variant, profile_alias_index),
            aircraft_name,
            section_cfg["section_code"],
            section_cfg["role2_code"],
            section_cfg["role2_label"],
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
    conn.commit()


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

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)
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
                )
                total_rows += 1
            except Exception as e:
                errors.append(f"{cfg['section_label']} | {aircraft_name} -> {e}")

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
        "top_airfoils": top_airfoils,
        "errors_file": ERRORS_PATH,
    }

    print("\n===== DONE =====")
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    build_usage_database()

