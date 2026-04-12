"""Build the staging database of XFOIL polars.

This module reads eligible profiles from `profiles.db`, runs a short XFOIL gate
check at 0 degrees, and only then executes the full alpha sweep for profiles
that pass the gate. Polar rows are stored in `polars.db`, while per-profile run
outcomes are stored in `airfoil_xfoil_runs`.

The run summary is used later to exclude profiles that time out or completely
fail to converge from the final merged database.
"""

import os
import re
import json
import sqlite3
import subprocess
import tempfile
import time
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone

from paths import (
    POLARS_DB_PATH,
    XFOIL_DAT_DIR,
    XFOIL_LOG_DIR,
    XFOIL_POLAR_DIR,
    XFOIL_DIR,
    ensure_local_dirs,
    resolve_profiles_db_path,
    resolve_xfoil_exe_path,
)

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
PROFILES_DB_PATH = str(resolve_profiles_db_path())
POLARS_DB_PATH_STR = str(POLARS_DB_PATH)
XFOIL_EXE = str(resolve_xfoil_exe_path())
WORK_DIR = str(XFOIL_DIR)
AIRFOIL_DAT_DIR = str(XFOIL_DAT_DIR)
POLAR_DIR = str(XFOIL_POLAR_DIR)
LOG_DIR = str(XFOIL_LOG_DIR)

# Setup analisi
REYNOLDS_LIST = [150000.0, 250000.0, 500000.0, 1250000.0]
MACH = 0.0
NCRIT = 9.0
ITER = 150

# Gate rapido per scartare i profili che falliscono subito.
GATE_ALPHA = 0.0
GATE_TIMEOUT_SECONDS = 30
GATE_STALL_SECONDS = 6
GATE_POLAR_STALL_SECONDS = 3

# Sweep completo solo per i profili che passano il gate.
ALPHA_START = -6.0
ALPHA_END = 12.0
ALPHA_STEP = 1.0
FULL_TIMEOUT_SECONDS = 60
FULL_STALL_SECONDS = 8
FULL_POLAR_STALL_SECONDS = 3
FATAL_LOG_WINDOW_LINES = 80
FATAL_TRCHEK2_THRESHOLD = 12
ENABLE_INIT_RETRY = True
ESTIMATED_SECONDS_PER_SIMULATION = 4.2

SHOW_XFOIL_WINDOW = False
ENABLE_XFOIL_GRAPHICS = False

# Se vuoi fare test rapidi
LIMIT_AIRFOILS = None
ONLY_NAMES = None # ["naca0012", "naca2412", "naca4412", "clarky", "s1223"] #oppure None
XFOIL_WINDOWS_ZIP_URL = "https://web.mit.edu/drela/Public/web/xfoil/XFOIL6.99.zip"


def ensure_dirs():
    ensure_local_dirs()


def ensure_xfoil_executable() -> None:
    if os.path.exists(XFOIL_EXE):
        return

    print(f"[INFO] XFOIL non trovato, download da: {XFOIL_WINDOWS_ZIP_URL}")
    zip_path = os.path.join(WORK_DIR, "XFOIL6.99.zip")

    urllib.request.urlretrieve(XFOIL_WINDOWS_ZIP_URL, zip_path)
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            members = {name.lower(): name for name in zf.namelist()}
            exe_member = members.get("xfoil.exe")
            if not exe_member:
                raise FileNotFoundError("xfoil.exe non trovato dentro l'archivio XFOIL6.99.zip")

            os.makedirs(os.path.dirname(XFOIL_EXE), exist_ok=True)
            with zf.open(exe_member) as src, open(XFOIL_EXE, "wb") as dst:
                dst.write(src.read())
    finally:
        if os.path.exists(zip_path):
            os.remove(zip_path)


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def format_duration(seconds: float) -> str:
    total_seconds = max(0, int(round(seconds)))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes:02d}m {secs:02d}s"
    return f"{minutes}m {secs:02d}s"


def ensure_tables(conn):
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS airfoil_polars_xfoil (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        airfoil_name TEXT NOT NULL,
        reynolds REAL NOT NULL,
        mach REAL NOT NULL,
        ncrit REAL NOT NULL,
        alpha_deg REAL NOT NULL,
        cl REAL,
        cd REAL,
        cdp REAL,
        cm REAL,
        top_xtr REAL,
        bot_xtr REAL,
        converged INTEGER DEFAULT 1,
        source TEXT DEFAULT 'xfoil',
        polar_file_path TEXT,
        created_at TEXT,
        UNIQUE(airfoil_name, reynolds, mach, ncrit, alpha_deg)
    )
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_airfoil_polars_xfoil_name
    ON airfoil_polars_xfoil(airfoil_name)
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS airfoil_xfoil_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        airfoil_name TEXT NOT NULL,
        reynolds REAL NOT NULL,
        mach REAL NOT NULL,
        ncrit REAL NOT NULL,
        gate_alpha REAL NOT NULL,
        gate_converged INTEGER DEFAULT 0,
        gate_timed_out INTEGER DEFAULT 0,
        gate_status TEXT NOT NULL,
        alpha_start REAL NOT NULL,
        alpha_end REAL NOT NULL,
        alpha_step REAL NOT NULL,
        expected_count INTEGER NOT NULL,
        converged_count INTEGER NOT NULL,
        missing_count INTEGER NOT NULL,
        return_code INTEGER,
        timed_out INTEGER DEFAULT 0,
        run_status TEXT NOT NULL,
        failure_reason TEXT,
        exclude_from_final INTEGER DEFAULT 0,
        log_file_path TEXT,
        polar_file_path TEXT,
        created_at TEXT NOT NULL,
        UNIQUE(airfoil_name, reynolds, mach, ncrit, alpha_start, alpha_end, alpha_step)
    )
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_airfoil_xfoil_runs_name
    ON airfoil_xfoil_runs(airfoil_name)
    """)

    conn.commit()


def reset_polars_tables(conn):
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS airfoil_polars_xfoil")
    cur.execute("DROP TABLE IF EXISTS airfoil_xfoil_runs")
    conn.commit()


def ensure_airfoils_table(conn):
    cur = conn.cursor()
    cur.execute("""
    SELECT name
    FROM sqlite_master
    WHERE type='table' AND name='airfoils'
    """)
    if cur.fetchone():
        return

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row[0] for row in cur.fetchall()]
    raise RuntimeError(
        "La tabella 'airfoils' non esiste nel database selezionato.\n"
        "Esegui prima: python build_profiles_db.py\n"
        "Poi esegui: python build_usage_db.py\n"
        f"DB aperto: {PROFILES_DB_PATH}\n"
        f"Tabelle trovate: {tables}"
    )


def get_airfoils(conn):
    ensure_airfoils_table(conn)
    cur = conn.cursor()

    sql = """
    SELECT name, x_json, y_json, raw_dat
    FROM airfoils
    WHERE is_valid_geometry = 1
      AND is_xfoil_compatible = 1
      AND exclude_from_final = 0
    """
    params = []

    if ONLY_NAMES:
        placeholders = ",".join(["?"] * len(ONLY_NAMES))
        sql += f" AND name IN ({placeholders})"
        params.extend(ONLY_NAMES)

    sql += " ORDER BY name"

    if LIMIT_AIRFOILS is not None:
        sql += f" LIMIT {int(LIMIT_AIRFOILS)}"

    cur.execute(sql, params)
    return cur.fetchall()


def parse_points_from_row(name, x_json, y_json, raw_dat):
    # 1) prova x_json/y_json
    if x_json and y_json:
        try:
            xs = json.loads(x_json)
            ys = json.loads(y_json)
            if len(xs) == len(ys) and len(xs) >= 5:
                return [(float(x), float(y)) for x, y in zip(xs, ys)]
        except Exception:
            pass

    # 2) fallback: parse raw_dat
    if raw_dat:
        pts = []
        for raw in raw_dat.splitlines()[1:]:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            parts = re.split(r"\s+", line.replace(",", " "))
            if len(parts) < 2:
                continue
            try:
                x = float(parts[0])
                y = float(parts[1])
                pts.append((x, y))
            except ValueError:
                continue
        if len(pts) >= 5:
            return pts

    raise ValueError(f"Impossibile ricostruire i punti del profilo {name}")


def write_airfoil_dat(path, name, points):
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(name + "\n")
        for x, y in points:
            f.write(f"{x:.8f} {y:.8f}\n")


def build_xfoil_input(dat_path, polar_path, reynolds, mach, ncrit, operation_lines, iter_count):
    # PACC vuole:
    # PACC
    # filename
    # dumpfilename (vuoto)
    lines = []
    if not ENABLE_XFOIL_GRAPHICS:
        lines.extend([
            "PLOP",
            "G F",
            "",
        ])

    lines.extend([
        f"LOAD {dat_path}",
        "PANE",
        "OPER",
        f"VISC {reynolds}",
        f"MACH {mach}",
        "VPAR",
        f"N {ncrit}",
        "",
        f"ITER {iter_count}",
        "PACC",
        polar_path,
        "",
        *operation_lines,
        "OPER",
        "PACC",
        "",
        "QUIT",
    ])
    return "\n".join(lines) + "\n"


def build_full_sweep_operation_lines(use_init: bool = False) -> list[str]:
    lines = []
    if use_init:
        lines.extend(["INIT", ""])
    lines.extend([f"ASEQ {ALPHA_START} {ALPHA_END} {ALPHA_STEP}", ""])
    return lines


def to_xfoil_relpath(path):
    rel = os.path.relpath(path, WORK_DIR)
    return rel.replace("\\", "/")


def file_mtime_or_none(path):
    try:
        if os.path.exists(path):
            return os.path.getmtime(path)
    except OSError:
        return None
    return None


def file_size_or_none(path):
    try:
        if os.path.exists(path):
            return os.path.getsize(path)
    except OSError:
        return None
    return None


class XfoilEarlyAbort(RuntimeError):
    pass


def read_log_tail(path, max_lines=FATAL_LOG_WINDOW_LINES):
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
    except OSError:
        return []
    return lines[-max_lines:]


def detect_fatal_log_pattern(log_path):
    tail = read_log_tail(log_path)
    if not tail:
        return None

    tail_text = "".join(tail).lower()
    trchek2_count = sum(1 for line in tail if "TRCHEK2: N2 convergence failed." in line)
    nan_count = sum(1 for line in tail if "nan" in line.lower())
    mrchdu_count = sum(1 for line in tail if "MRCHDU: Convergence failed" in line)

    if "sequence halted since previous  4 points did not converge" in tail_text:
        return "sequence_halted_nonconverged"

    if "viscal:  convergence failed" in tail_text:
        return "viscal_convergence_failed"

    if trchek2_count >= FATAL_TRCHEK2_THRESHOLD and nan_count >= FATAL_TRCHEK2_THRESHOLD:
        return "numerical_stall_trchek2_nan"

    if mrchdu_count >= 30 and "viscal:  convergence failed" in tail_text:
        return "numerical_stall_mrchdu_viscal"

    if "floating point overflow" in tail_text:
        return "floating_point_overflow"

    return None


def run_xfoil(
    dat_path,
    polar_path,
    log_path,
    reynolds,
    mach,
    ncrit,
    operation_lines,
    iter_count,
    timeout_seconds,
    stall_seconds,
    polar_stall_seconds=None,
):
    if not os.path.exists(XFOIL_EXE):
        raise FileNotFoundError(f"XFOIL non trovato: {XFOIL_EXE}")

    script_text = build_xfoil_input(
        dat_path=to_xfoil_relpath(dat_path),
        polar_path=to_xfoil_relpath(polar_path),
        reynolds=reynolds,
        mach=mach,
        ncrit=ncrit,
        operation_lines=operation_lines,
        iter_count=iter_count,
    )

    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".inp", encoding="utf-8", newline="\n") as tmp:
        tmp.write(script_text)
        inp_path = tmp.name

    started_at = time.monotonic()

    try:
        with open(inp_path, "r", encoding="utf-8") as stdin_file, open(
            log_path,
            "w",
            encoding="utf-8",
            newline="\n",
        ) as log_file:
            startupinfo = None
            creationflags = 0
            if os.name == "nt":
                if not SHOW_XFOIL_WINDOW:
                    startupinfo = subprocess.STARTUPINFO()
                    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                    startupinfo.wShowWindow = 0
                    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)

            proc = subprocess.Popen(
                [XFOIL_EXE],
                stdin=stdin_file,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                cwd=WORK_DIR,
                startupinfo=startupinfo,
                creationflags=creationflags,
            )

            last_progress_at = time.monotonic()
            last_log_mtime = file_mtime_or_none(log_path)
            last_polar_mtime = file_mtime_or_none(polar_path)
            last_polar_size = file_size_or_none(polar_path)
            last_polar_progress_at = time.monotonic()

            while True:
                return_code = proc.poll()
                now = time.monotonic()

                log_mtime = file_mtime_or_none(log_path)
                polar_mtime = file_mtime_or_none(polar_path)
                polar_size = file_size_or_none(polar_path)
                log_progress = log_mtime != last_log_mtime
                polar_progress = (polar_mtime != last_polar_mtime) or (polar_size != last_polar_size)

                if log_progress or polar_progress:
                    last_progress_at = now
                if log_progress:
                    last_log_mtime = log_mtime
                if polar_progress:
                    last_polar_progress_at = now
                    last_polar_mtime = polar_mtime
                    last_polar_size = polar_size

                if return_code is not None:
                    return return_code, now - started_at

                if now - started_at > timeout_seconds:
                    proc.kill()
                    proc.wait(timeout=5)
                    raise subprocess.TimeoutExpired([XFOIL_EXE], timeout_seconds)

                if stall_seconds and (now - last_progress_at) > stall_seconds:
                    proc.kill()
                    proc.wait(timeout=5)
                    raise subprocess.TimeoutExpired([XFOIL_EXE], stall_seconds)

                if (
                    polar_stall_seconds
                    and (now - started_at) > polar_stall_seconds
                    and (now - last_polar_progress_at) > polar_stall_seconds
                ):
                    proc.kill()
                    proc.wait(timeout=5)
                    raise XfoilEarlyAbort("polar_stall")

                fatal_reason = detect_fatal_log_pattern(log_path)
                if fatal_reason:
                    proc.kill()
                    proc.wait(timeout=5)
                    raise XfoilEarlyAbort(fatal_reason)

                time.sleep(0.25)
    finally:
        try:
            os.remove(inp_path)
        except OSError:
            pass


def parse_xfoil_polar_file(polar_path):
    """
    Polar tipica XFOIL:
    alpha    CL      CD      CDp     CM   Top_Xtr  Bot_Xtr
    con qualche header prima
    """
    if not os.path.exists(polar_path):
        return []

    rows = []
    with open(polar_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue

            # salta header
            if (
                "alpha" in s.lower()
                or "xfoil" in s.lower()
                or "re =" in s.lower()
                or "mach =" in s.lower()
                or "ncrit" in s.lower()
                or s.startswith("-")
            ):
                continue

            parts = re.split(r"\s+", s)
            if len(parts) < 7:
                continue

            try:
                alpha = float(parts[0])
                cl = float(parts[1])
                cd = float(parts[2])
                cdp = float(parts[3])
                cm = float(parts[4])
                top_xtr = float(parts[5])
                bot_xtr = float(parts[6])
            except ValueError:
                continue

            rows.append({
                "alpha_deg": alpha,
                "cl": cl,
                "cd": cd,
                "cdp": cdp,
                "cm": cm,
                "top_xtr": top_xtr,
                "bot_xtr": bot_xtr,
                "converged": 1,
            })

    return rows


def expected_alpha_list(start, end, step):
    out = []
    a = start
    if step == 0:
        raise ValueError("alpha_step non può essere zero")
    if step > 0:
        while a <= end + 1e-9:
            out.append(round(a, 6))
            a += step
    else:
        while a >= end - 1e-9:
            out.append(round(a, 6))
            a += step
    return out




def mark_missing_as_not_converged(found_rows, alpha_start, alpha_end, alpha_step):
    expected = expected_alpha_list(alpha_start, alpha_end, alpha_step)
    found_map = {round(r["alpha_deg"], 6): r for r in found_rows}
    final_rows = []

    for a in expected:
        row = found_map.get(round(a, 6))
        if row is None:
            final_rows.append({
                "alpha_deg": a,
                "cl": None,
                "cd": None,
                "cdp": None,
                "cm": None,
                "top_xtr": None,
                "bot_xtr": None,
                "converged": 0,
            })
        else:
            final_rows.append(row)

    return final_rows


def upsert_polar_rows(conn, airfoil_name, reynolds, mach, ncrit, polar_path, rows):
    cur = conn.cursor()

    for r in rows:
        cur.execute("""
        INSERT INTO airfoil_polars_xfoil (
            airfoil_name, reynolds, mach, ncrit, alpha_deg,
            cl, cd, cdp, cm, top_xtr, bot_xtr,
            converged, source, polar_file_path, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(airfoil_name, reynolds, mach, ncrit, alpha_deg) DO UPDATE SET
            cl=excluded.cl,
            cd=excluded.cd,
            cdp=excluded.cdp,
            cm=excluded.cm,
            top_xtr=excluded.top_xtr,
            bot_xtr=excluded.bot_xtr,
            converged=excluded.converged,
            source=excluded.source,
            polar_file_path=excluded.polar_file_path,
            created_at=excluded.created_at
        """, (
            airfoil_name,
            float(reynolds),
            float(mach),
            float(ncrit),
            float(r["alpha_deg"]),
            r["cl"],
            r["cd"],
            r["cdp"],
            r["cm"],
            r["top_xtr"],
            r["bot_xtr"],
            int(r["converged"]),
            "xfoil",
            polar_path,
            utc_now(),
        ))

    conn.commit()


def upsert_xfoil_run(
    conn,
    airfoil_name,
    reynolds,
    mach,
    ncrit,
    gate_converged,
    gate_timed_out,
    gate_status,
    expected_count,
    converged_count,
    return_code,
    timed_out,
    run_status,
    failure_reason,
    exclude_from_final,
    log_file_path,
    polar_file_path,
):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO airfoil_xfoil_runs (
            airfoil_name, reynolds, mach, ncrit,
            gate_alpha, gate_converged, gate_timed_out, gate_status,
            alpha_start, alpha_end, alpha_step,
            expected_count, converged_count, missing_count,
            return_code, timed_out, run_status, failure_reason,
            exclude_from_final, log_file_path, polar_file_path, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(airfoil_name, reynolds, mach, ncrit, alpha_start, alpha_end, alpha_step) DO UPDATE SET
            gate_converged=excluded.gate_converged,
            gate_timed_out=excluded.gate_timed_out,
            gate_status=excluded.gate_status,
            expected_count=excluded.expected_count,
            converged_count=excluded.converged_count,
            missing_count=excluded.missing_count,
            return_code=excluded.return_code,
            timed_out=excluded.timed_out,
            run_status=excluded.run_status,
            failure_reason=excluded.failure_reason,
            exclude_from_final=excluded.exclude_from_final,
            log_file_path=excluded.log_file_path,
            polar_file_path=excluded.polar_file_path,
            created_at=excluded.created_at
    """, (
        airfoil_name,
        float(reynolds),
        float(mach),
        float(ncrit),
        float(GATE_ALPHA),
        int(gate_converged),
        int(gate_timed_out),
        gate_status,
        float(ALPHA_START),
        float(ALPHA_END),
        float(ALPHA_STEP),
        int(expected_count),
        int(converged_count),
        int(expected_count - converged_count),
        return_code,
        int(timed_out),
        run_status,
        failure_reason,
        int(exclude_from_final),
        log_file_path,
        polar_file_path,
        utc_now(),
    ))
    conn.commit()


def run_one_airfoil(conn, name, points):
    dat_path = os.path.join(AIRFOIL_DAT_DIR, f"{name}.dat")
    write_airfoil_dat(dat_path, name, points)

    summary = []

    for reynolds in REYNOLDS_LIST:
        polar_filename = f"{name}_Re{int(reynolds)}_M{str(MACH).replace('.', 'p')}_N{str(NCRIT).replace('.', 'p')}.txt"
        polar_path = os.path.join(POLAR_DIR, polar_filename)
        gate_polar_filename = f"{name}_Re{int(reynolds)}_gate.txt"
        gate_polar_path = os.path.join(POLAR_DIR, gate_polar_filename)

        log_filename = f"{name}_Re{int(reynolds)}.log"
        log_path = os.path.join(LOG_DIR, log_filename)
        gate_log_filename = f"{name}_Re{int(reynolds)}_gate.log"
        gate_log_path = os.path.join(LOG_DIR, gate_log_filename)

        # pulizia file vecchi
        for path in (polar_path, log_path, gate_polar_path, gate_log_path):
            try:
                if os.path.exists(path):
                    os.remove(path)
            except OSError:
                pass

        gate_timed_out = False
        gate_return_code = None
        gate_failure_reason = None
        gate_elapsed_seconds = None
        retried_with_init = False

        try:
            gate_return_code, gate_elapsed_seconds = run_xfoil(
                dat_path=dat_path,
                polar_path=gate_polar_path,
                log_path=gate_log_path,
                reynolds=reynolds,
                mach=MACH,
                ncrit=NCRIT,
                operation_lines=[f"ALFA {GATE_ALPHA}", ""],
                iter_count=ITER,
                timeout_seconds=GATE_TIMEOUT_SECONDS,
                stall_seconds=GATE_STALL_SECONDS,
                polar_stall_seconds=GATE_POLAR_STALL_SECONDS,
            )
        except XfoilEarlyAbort as e:
            gate_failure_reason = str(e)
        except subprocess.TimeoutExpired:
            gate_timed_out = True
            gate_failure_reason = "gate_timeout"

        gate_rows = parse_xfoil_polar_file(gate_polar_path)
        gate_converged = 1 if gate_rows else 0
        gate_status = "ok"
        if gate_timed_out:
            gate_status = "timeout"
        elif not gate_rows:
            gate_status = "no_convergence"

        expected_count = len(expected_alpha_list(ALPHA_START, ALPHA_END, ALPHA_STEP))
        converged_count = 0
        timed_out = False
        return_code = None
        failure_reason = None
        elapsed_seconds = None

        exclude_from_final = 0
        run_status = "ok"
        if gate_timed_out:
            run_status = "gate_timeout"
            exclude_from_final = 1
            failure_reason = gate_failure_reason
        elif not gate_rows:
            run_status = "gate_no_convergence"
            exclude_from_final = 1
            failure_reason = "gate_no_convergence"
        else:
            full_operation_lines = build_full_sweep_operation_lines(use_init=False)
            try:
                return_code, elapsed_seconds = run_xfoil(
                    dat_path=dat_path,
                    polar_path=polar_path,
                    log_path=log_path,
                    reynolds=reynolds,
                    mach=MACH,
                    ncrit=NCRIT,
                    operation_lines=full_operation_lines,
                    iter_count=ITER,
                    timeout_seconds=FULL_TIMEOUT_SECONDS,
                    stall_seconds=FULL_STALL_SECONDS,
                    polar_stall_seconds=FULL_POLAR_STALL_SECONDS,
                )
            except XfoilEarlyAbort as e:
                failure_reason = str(e)
            except subprocess.TimeoutExpired:
                timed_out = True
                failure_reason = "timeout"

            if (timed_out or failure_reason) and ENABLE_INIT_RETRY:
                retried_with_init = True
                timed_out = False
                failure_reason = None

                for retry_path in (polar_path, log_path):
                    try:
                        if os.path.exists(retry_path):
                            os.remove(retry_path)
                    except OSError:
                        pass

                try:
                    return_code, elapsed_seconds = run_xfoil(
                        dat_path=dat_path,
                        polar_path=polar_path,
                        log_path=log_path,
                        reynolds=reynolds,
                        mach=MACH,
                        ncrit=NCRIT,
                        operation_lines=build_full_sweep_operation_lines(use_init=True),
                        iter_count=ITER,
                        timeout_seconds=FULL_TIMEOUT_SECONDS,
                        stall_seconds=FULL_STALL_SECONDS,
                        polar_stall_seconds=FULL_POLAR_STALL_SECONDS,
                    )
                except XfoilEarlyAbort as e:
                    failure_reason = str(e)
                except subprocess.TimeoutExpired:
                    timed_out = True
                    failure_reason = "timeout"

            parsed_rows = parse_xfoil_polar_file(polar_path)
            completed_rows = mark_missing_as_not_converged(
                found_rows=parsed_rows,
                alpha_start=ALPHA_START,
                alpha_end=ALPHA_END,
                alpha_step=ALPHA_STEP,
            )

            if completed_rows:
                upsert_polar_rows(
                    conn=conn,
                    airfoil_name=name,
                    reynolds=reynolds,
                    mach=MACH,
                    ncrit=NCRIT,
                    polar_path=polar_path,
                    rows=completed_rows,
                )

            converged_count = sum(1 for r in completed_rows if r["converged"] == 1)
            convergence_ratio = (converged_count / expected_count) if expected_count else 0.0

            if timed_out:
                run_status = "timeout"
                exclude_from_final = 1
            elif failure_reason:
                run_status = "no_convergence"
                exclude_from_final = 1
            elif converged_count == 0:
                run_status = "no_convergence"
                failure_reason = failure_reason or "no_convergence"
                exclude_from_final = 1
            elif convergence_ratio < 0.5:
                run_status = "partial_convergence"
                failure_reason = failure_reason or f"low_convergence_ratio:{convergence_ratio:.2f}"

        upsert_xfoil_run(
            conn=conn,
            airfoil_name=name,
            reynolds=reynolds,
            mach=MACH,
            ncrit=NCRIT,
            gate_converged=gate_converged,
            gate_timed_out=gate_timed_out,
            gate_status=gate_status,
            expected_count=expected_count,
            converged_count=converged_count,
            return_code=return_code if return_code is not None else gate_return_code,
            timed_out=timed_out,
            run_status=run_status,
            failure_reason=failure_reason,
            exclude_from_final=exclude_from_final,
            log_file_path=log_path if gate_status == "ok" else gate_log_path,
            polar_file_path=polar_path if gate_status == "ok" else gate_polar_path,
        )

        summary.append({
            "reynolds": reynolds,
            "return_code": return_code if return_code is not None else gate_return_code,
            "expected": expected_count,
            "converged": converged_count,
            "missing": expected_count - converged_count,
            "gate_status": gate_status,
            "gate_elapsed_seconds": gate_elapsed_seconds,
            "run_status": run_status,
            "elapsed_seconds": elapsed_seconds,
            "retried_with_init": retried_with_init,
            "exclude_from_final": exclude_from_final,
        })

    return summary


def main(reset_db: bool = True):
    ensure_dirs()
    ensure_xfoil_executable()
    total_started_at = time.monotonic()

    source_conn = sqlite3.connect(PROFILES_DB_PATH)
    output_conn = sqlite3.connect(POLARS_DB_PATH_STR)
    try:
        if reset_db:
            reset_polars_tables(output_conn)
        ensure_tables(output_conn)
        airfoils = get_airfoils(source_conn)

        print("PROFILES_DB_PATH =", PROFILES_DB_PATH)
        print("POLARS_DB_PATH =", POLARS_DB_PATH_STR)
        print("XFOIL_EXE =", XFOIL_EXE)
        print("Profili da processare =", len(airfoils))
        print("Reynolds =", REYNOLDS_LIST)
        print("Gate alpha =", GATE_ALPHA)
        print("Alpha sweep =", ALPHA_START, ALPHA_END, ALPHA_STEP)
        total_simulations = len(airfoils) * len(REYNOLDS_LIST)
        estimated_total_seconds = total_simulations * ESTIMATED_SECONDS_PER_SIMULATION
        estimated_finish = datetime.now() + timedelta(seconds=estimated_total_seconds)
        print(
            "Stima fine run =",
            estimated_finish.strftime("%Y-%m-%d %H:%M:%S"),
            f"(media stimata {ESTIMATED_SECONDS_PER_SIMULATION:.2f}s/simulazione, {total_simulations} simulazioni, durata stimata {format_duration(estimated_total_seconds)})",
        )

        total_expected_points = 0
        total_converged_points = 0
        coverage_by_re = {float(reynolds): {"expected": 0, "converged": 0} for reynolds in REYNOLDS_LIST}

        for idx, (name, x_json, y_json, raw_dat) in enumerate(airfoils, start=1):
            airfoil_started_at = time.monotonic()
            try:
                points = parse_points_from_row(name, x_json, y_json, raw_dat)
                summary = run_one_airfoil(output_conn, name, points)

                pieces = []
                for s in summary:
                    total_expected_points += s["expected"]
                    total_converged_points += s["converged"]
                    coverage_by_re[float(s["reynolds"])]["expected"] += s["expected"]
                    coverage_by_re[float(s["reynolds"])]["converged"] += s["converged"]
                    gate_secs = f"{s['gate_elapsed_seconds']:.1f}s" if s["gate_elapsed_seconds"] is not None else "-"
                    run_secs = f"{s['elapsed_seconds']:.1f}s" if s["elapsed_seconds"] is not None else "-"
                    retry_tag = "+INIT" if s["retried_with_init"] else ""
                    pieces.append(
                        f"Re={int(s['reynolds'])}: gate={s['gate_status']}({gate_secs}) conv={s['converged']}/{s['expected']} rc={s['return_code']} status={s['run_status']}{retry_tag}({run_secs})"
                    )

                airfoil_elapsed = time.monotonic() - airfoil_started_at
                print(f"[{idx}/{len(airfoils)}] OK  {name} ({airfoil_elapsed:.1f}s) | " + " | ".join(pieces))

            except Exception as e:
                airfoil_elapsed = time.monotonic() - airfoil_started_at
                print(f"[{idx}/{len(airfoils)}] ERR {name} ({airfoil_elapsed:.1f}s) -> {e}")

        total_elapsed = time.monotonic() - total_started_at
        mean_seconds_per_airfoil = (total_elapsed / len(airfoils)) if airfoils else 0.0
        mean_seconds_per_simulation = (total_elapsed / total_simulations) if total_simulations else 0.0
        total_coverage_pct = (100.0 * total_converged_points / total_expected_points) if total_expected_points else 0.0
        print(f"Copertura totale = {total_converged_points}/{total_expected_points} ({total_coverage_pct:.1f}%)")
        for reynolds in REYNOLDS_LIST:
            stats = coverage_by_re[float(reynolds)]
            re_pct = (100.0 * stats['converged'] / stats['expected']) if stats["expected"] else 0.0
            print(f" - Re={int(reynolds)}: {stats['converged']}/{stats['expected']} ({re_pct:.1f}%)")
        print(f"Tempo medio osservato = {mean_seconds_per_airfoil:.2f}s/profilo")
        print(f"Tempo medio osservato = {mean_seconds_per_simulation:.2f}s/simulazione")
        print(f"Tempo totale build_polars_db = {total_elapsed:.1f}s")

    finally:
        source_conn.close()
        output_conn.close()


def build_polars_database(reset_db: bool = True):
    """Public entry point for the XFOIL polar database build."""
    main(reset_db=reset_db)


if __name__ == "__main__":
    build_polars_database()
