import json
import os
import re
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from html import unescape
from pathlib import Path
from typing import Dict, List

from paths import RAW_DIR


USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Python airfoil-usage-fallback"
REQUEST_TIMEOUT = 30
MAX_REMOTE_PER_PROVIDER = int(os.environ.get("USAGE_FALLBACK_MAX_REMOTE_PER_PROVIDER", "1200"))
CACHE_VERSION = 2

FALLBACK_CACHE_DIR = RAW_DIR / "usage"
FALLBACK_CACHE_PATH = FALLBACK_CACHE_DIR / "usage_fallback_cache.json"
BIGFOIL_HOME_URL = "https://www.bigfoil.com/"

_CACHE: dict | None = None
_BIGFOIL_INDEX: dict[str, str] | None = None
_REMOTE_ATTEMPTS: dict[str, int] = {
    "bigfoil_v2": 0,
    "airfoiltools_v2": 0,
    "mh_aerotools_v2": 0,
}


def _download_text(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as response:
            return response.read().decode("utf-8", errors="replace")
    except urllib.error.URLError as e:
        reason = getattr(e, "reason", None)
        if isinstance(reason, ssl.SSLCertVerificationError):
            insecure_ctx = ssl._create_unverified_context()
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT, context=insecure_ctx) as response:
                return response.read().decode("utf-8", errors="replace")
        raise


def _normalize_token(raw: str) -> str:
    s = (raw or "").strip().lower()
    s = unescape(s)
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def _default_cache() -> dict:
    return {
        "_meta": {"version": CACHE_VERSION},
        "bigfoil_index": {},
        "bigfoil_v2": {},
        "airfoiltools_v2": {},
        "mh_aerotools_v2": {},
    }


def _load_cache() -> dict:
    global _CACHE
    if _CACHE is not None:
        return _CACHE

    base = _default_cache()
    if not FALLBACK_CACHE_PATH.exists():
        _CACHE = base
        return _CACHE

    try:
        with open(FALLBACK_CACHE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            _CACHE = base
            return _CACHE
        meta = data.get("_meta", {})
        if not isinstance(meta, dict) or meta.get("version") != CACHE_VERSION:
            _CACHE = base
            return _CACHE
        for key in base:
            if key not in data or not isinstance(data[key], dict):
                data[key] = {}
        _CACHE = data
        return _CACHE
    except Exception:
        _CACHE = base
        return _CACHE


def _save_cache() -> None:
    FALLBACK_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache = _load_cache()

    # OneDrive can transiently lock files during sync; retry and degrade gracefully.
    for attempt in range(3):
        tmp = Path(str(FALLBACK_CACHE_PATH) + f".tmp{attempt}")
        try:
            with open(tmp, "w", encoding="utf-8", newline="\n") as f:
                json.dump(cache, f, ensure_ascii=False, indent=2)
            os.replace(tmp, FALLBACK_CACHE_PATH)
            return
        except OSError:
            try:
                if tmp.exists():
                    tmp.unlink()
            except OSError:
                pass
            time.sleep(0.15 * (attempt + 1))

    try:
        with open(FALLBACK_CACHE_PATH, "w", encoding="utf-8", newline="\n") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except OSError:
        return


def _get_cached(provider_key: str, profile_name: str):
    cache = _load_cache()
    provider_map = cache.get(provider_key, {})
    return provider_map.get(profile_name.lower().strip())


def _set_cached(provider_key: str, profile_name: str, items: List[Dict[str, str]]) -> None:
    cache = _load_cache()
    cache.setdefault(provider_key, {})[profile_name.lower().strip()] = items
    try:
        _save_cache()
    except OSError:
        pass


def _allow_remote(provider_key: str) -> bool:
    return _REMOTE_ATTEMPTS.get(provider_key, 0) < MAX_REMOTE_PER_PROVIDER


def _mark_remote(provider_key: str) -> None:
    _REMOTE_ATTEMPTS[provider_key] = _REMOTE_ATTEMPTS.get(provider_key, 0) + 1


def _extract_title_candidates(profile_title: str) -> list[str]:
    t = (profile_title or "").strip().lower()
    if not t:
        return []

    words = re.findall(r"[a-z0-9]+", t)
    out: list[str] = []

    # Single words that already look like airfoil IDs.
    for w in words:
        if re.fullmatch(r"[a-z]{1,6}\d[a-z0-9]{0,8}", w):
            out.append(w)

    # Adjacent combos, useful for patterns like "AH21 7%" -> "ah217".
    for i in range(len(words) - 1):
        a, b = words[i], words[i + 1]
        if re.search(r"\d", a) and (re.fullmatch(r"\d{1,3}", b) or re.fullmatch(r"[a-z]{1,3}", b)):
            out.append(a + b)
        if re.fullmatch(r"[a-z]{1,6}", a) and re.fullmatch(r"\d{2,5}[a-z]?", b):
            out.append(a + b)

    # Compact full title as last resort.
    full = _normalize_token(t)
    if full:
        out.append(full)

    seen = set()
    ordered = []
    for item in out:
        if item and item not in seen:
            seen.add(item)
            ordered.append(item)
    return ordered


def _bigfoil_profile_candidates(token: str, title_candidates: list[str] | None = None) -> list[str]:
    out = [token]

    if token.startswith("naca"):
        out.append(token[len("naca"):])
        m = re.fullmatch(r"naca(\d{4,5})", token)
        if m:
            out.append("n" + m.group(1))
    if token.startswith("goe"):
        out.append("goettingen" + token[len("goe"):])
    if token.startswith("goettingen"):
        out.append("goe" + token[len("goettingen"):])

    for suffix in ("mod", "modified", "smoothed"):
        if token.endswith(suffix):
            out.append(token[: -len(suffix)])

    for item in (title_candidates or []):
        out.append(item)

    seen = set()
    ordered = []
    for item in out:
        if item and item not in seen:
            seen.add(item)
            ordered.append(item)
    return ordered


def _bigfoil_build_index() -> dict[str, str]:
    html = _download_text(BIGFOIL_HOME_URL)
    index: dict[str, str] = {}
    for m in re.finditer(r'<A\s+HREF="([^"]+_info\.php)"[^>]*>(.*?)</A>', html, flags=re.IGNORECASE | re.DOTALL):
        href = m.group(1).strip()
        name_raw = re.sub(r"<[^>]+>", " ", m.group(2))
        name_raw = re.sub(r"\s+", " ", unescape(name_raw)).strip()
        key = _normalize_token(name_raw)
        if not key:
            continue
        info_url = urllib.parse.urljoin(BIGFOIL_HOME_URL, href)
        index.setdefault(key, info_url)
    return index


def _get_bigfoil_index() -> dict[str, str]:
    global _BIGFOIL_INDEX
    if _BIGFOIL_INDEX is not None:
        return _BIGFOIL_INDEX

    cache = _load_cache()
    cached_idx = cache.get("bigfoil_index", {})
    if isinstance(cached_idx, dict) and cached_idx:
        _BIGFOIL_INDEX = {str(k): str(v) for k, v in cached_idx.items() if k and v}
        return _BIGFOIL_INDEX

    if not _allow_remote("bigfoil_v2"):
        _BIGFOIL_INDEX = {}
        return _BIGFOIL_INDEX

    try:
        _mark_remote("bigfoil_v2")
        idx = _bigfoil_build_index()
        cache["bigfoil_index"] = idx
        _BIGFOIL_INDEX = idx
        _save_cache()
        return _BIGFOIL_INDEX
    except Exception:
        _BIGFOIL_INDEX = {}
        return _BIGFOIL_INDEX


def lookup_bigfoil(profile_name: str, profile_title: str = "") -> List[Dict[str, str]]:
    provider_key = "bigfoil_v2"
    cached = _get_cached(provider_key, profile_name)
    if cached is not None:
        return cached

    token = _normalize_token(profile_name)
    if not token or len(token) < 3:
        _set_cached(provider_key, profile_name, [])
        return []
    title_candidates = _extract_title_candidates(profile_title)

    idx = _get_bigfoil_index()
    if not idx:
        _set_cached(provider_key, profile_name, [])
        return []

    info_url = ""
    for candidate in _bigfoil_profile_candidates(token, title_candidates):
        if candidate in idx:
            info_url = idx[candidate]
            break

    if not info_url:
        _set_cached(provider_key, profile_name, [])
        return []

    if not _allow_remote(provider_key):
        return []

    try:
        _mark_remote(provider_key)
        info_html = _download_text(info_url)
        apps_m = re.search(
            r'HREF=["\'](?P<href>/apps/[0-9a-f\-]+_apps\.php)["\'][^>]*>\s*Applications\s*\((?P<n>\d+)\)',
            info_html,
            flags=re.IGNORECASE,
        )
        if not apps_m:
            _set_cached(provider_key, profile_name, [])
            return []

        app_count = int(apps_m.group("n"))
        if app_count <= 0:
            _set_cached(provider_key, profile_name, [])
            return []

        apps_rel = apps_m.group("href")
        apps_url = urllib.parse.urljoin(info_url, apps_rel)
        summary = f"BigFoil reports {app_count} applications."

        try:
            apps_html = _download_text(apps_url)
            names = re.findall(
                r'<A HREF=["\']/[0-9a-f\-]+_info\.php["\']>([^<]{2,120})</A>\s*\(',
                apps_html,
                flags=re.IGNORECASE,
            )
            cleaned = []
            seen = set()
            for name in names:
                txt = re.sub(r"\s+", " ", unescape(name)).strip()
                key = txt.lower()
                if not txt or key in seen:
                    continue
                seen.add(key)
                cleaned.append(txt)
                if len(cleaned) >= 5:
                    break
            if cleaned:
                summary += " Examples: " + ", ".join(cleaned) + "."
        except Exception:
            pass

        found = [{
            "usage_text": summary,
            "source": "bigfoil",
            "source_url": apps_url,
        }]
        _set_cached(provider_key, profile_name, found)
        return found
    except Exception:
        _set_cached(provider_key, profile_name, [])
        return []


def _clean_airfoiltools_text(text: str) -> str:
    s = re.sub(r"\s+", " ", text).strip()
    s = s.replace("Line 163 - Invalid characters :", " ")
    s = re.sub(r"Preview Details", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"Airfoil Tools Search \d+ airfoils", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"-->\s*Airfoil\s+\S+\s+Details.*$", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip(" ;,.")
    return s


def lookup_airfoiltools(profile_name: str, profile_title: str = "") -> List[Dict[str, str]]:
    provider_key = "airfoiltools_v2"
    cached = _get_cached(provider_key, profile_name)
    if cached is not None:
        return cached

    token = _normalize_token(profile_name)
    if not token or len(token) < 3:
        _set_cached(provider_key, profile_name, [])
        return []

    if not _allow_remote(provider_key):
        return []

    candidate_slugs = [token]
    for t in _extract_title_candidates(profile_title):
        if 3 <= len(t) <= 24:
            candidate_slugs.append(t)
    m_naca = re.fullmatch(r"naca(\d{4,5})", token)
    if m_naca:
        candidate_slugs.append("n" + m_naca.group(1))
    if token.startswith("goettingen"):
        candidate_slugs.append("goe" + token[len("goettingen"):])
    if token.startswith("wortmannfx"):
        candidate_slugs.append(token[len("wortmann"):])

    seen = set()
    ordered_slugs = []
    for slug in candidate_slugs:
        if slug and slug not in seen:
            seen.add(slug)
            ordered_slugs.append(slug)

    try:
        _mark_remote(provider_key)
        for slug in ordered_slugs:
            url = f"http://airfoiltools.com/airfoil/details?airfoil={slug}-il"
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as response:
                final_url = response.geturl()
                html = response.read().decode("utf-8", errors="replace")

            if "/airfoil/details?airfoil=" not in final_url or "/search/" in final_url:
                continue

            text = re.sub(r"<[^>]+>", " ", html)
            text = re.sub(r"\s+", " ", text).strip()

            # Conservative extraction: keep only clear usage phrases.
            phrases = re.findall(
                r"(used on[^.;]{10,180}|used by[^.;]{10,180}|designed for[^.;]{10,180})",
                text,
                flags=re.IGNORECASE,
            )
            cleaned = []
            for phrase in phrases:
                s = _clean_airfoiltools_text(phrase)
                low = s.lower()
                if not s:
                    continue
                if "airfoil database search" in low or "tweet" in low:
                    continue
                if low in {x.lower() for x in cleaned}:
                    continue
                cleaned.append(s)
                if len(cleaned) >= 2:
                    break

            if cleaned:
                found = [{
                    "usage_text": " ; ".join(cleaned),
                    "source": "airfoiltools",
                    "source_url": final_url,
                }]
                _set_cached(provider_key, profile_name, found)
                return found

        _set_cached(provider_key, profile_name, [])
        return []
    except Exception:
        _set_cached(provider_key, profile_name, [])
        return []


def lookup_mh_aerotools(profile_name: str, profile_title: str = "") -> List[Dict[str, str]]:
    provider_key = "mh_aerotools_v2"
    cached = _get_cached(provider_key, profile_name)
    if cached is not None:
        return cached
    _set_cached(provider_key, profile_name, [])
    return []


def lookup_usage_fallback(profile_name: str, profile_title: str = "") -> List[Dict[str, str]]:
    for fn in (lookup_bigfoil, lookup_airfoiltools, lookup_mh_aerotools):
        found = fn(profile_name, profile_title)
        if found:
            return found
    return []
