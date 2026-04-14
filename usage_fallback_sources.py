import json
import os
import re
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict, List

from paths import RAW_DIR


USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Python airfoil-usage-fallback"
REQUEST_TIMEOUT = 30
MAX_REMOTE_PER_PROVIDER = int(os.environ.get("USAGE_FALLBACK_MAX_REMOTE_PER_PROVIDER", "120"))
MAX_DDG_RESULTS = 5

FALLBACK_CACHE_DIR = RAW_DIR / "usage"
FALLBACK_CACHE_PATH = FALLBACK_CACHE_DIR / "usage_fallback_cache.json"

_CACHE: dict | None = None
_REMOTE_ATTEMPTS: dict[str, int] = {
    "bigfoil": 0,
    "airfoiltools": 0,
    "mh_aerotools": 0,
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


def _load_cache() -> dict:
    global _CACHE
    if _CACHE is not None:
        return _CACHE

    base = {
        "bigfoil": {},
        "airfoiltools": {},
        "mh_aerotools": {},
    }
    if not FALLBACK_CACHE_PATH.exists():
        _CACHE = base
        return _CACHE

    try:
        with open(FALLBACK_CACHE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            _CACHE = base
            return _CACHE
        for provider in base:
            if provider not in data or not isinstance(data[provider], dict):
                data[provider] = {}
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

    # Last fallback: try direct overwrite; if still blocked, skip caching this cycle.
    try:
        with open(FALLBACK_CACHE_PATH, "w", encoding="utf-8", newline="\n") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except OSError:
        return


def _get_cached(provider: str, profile_name: str):
    cache = _load_cache()
    provider_map = cache.get(provider, {})
    return provider_map.get(profile_name.lower().strip())


def _set_cached(provider: str, profile_name: str, items: List[Dict[str, str]]) -> None:
    cache = _load_cache()
    cache.setdefault(provider, {})[profile_name.lower().strip()] = items
    try:
        _save_cache()
    except OSError:
        pass


def _normalize_token(raw: str) -> str:
    s = (raw or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def _allow_remote(provider: str) -> bool:
    return _REMOTE_ATTEMPTS.get(provider, 0) < MAX_REMOTE_PER_PROVIDER


def _mark_remote(provider: str) -> None:
    _REMOTE_ATTEMPTS[provider] = _REMOTE_ATTEMPTS.get(provider, 0) + 1


def _decode_ddg_url(url: str) -> str:
    m = re.search(r"uddg=([^&]+)", url)
    if not m:
        return url
    return urllib.parse.unquote(m.group(1))


def _ddg_search_urls(query: str, domain: str) -> list[str]:
    search_url = "https://duckduckgo.com/html/?q=" + urllib.parse.quote_plus(query)
    html = _download_text(search_url)
    hrefs = re.findall(r'<a[^>]+class="result__a"[^>]+href="([^"]+)"', html, flags=re.IGNORECASE)

    decoded = []
    for href in hrefs:
        real = _decode_ddg_url(href)
        if domain.lower() not in real.lower():
            continue
        decoded.append(real)

    # Deduplicate preserving order.
    seen = set()
    out = []
    for url in decoded:
        if url in seen:
            continue
        seen.add(url)
        out.append(url)
        if len(out) >= MAX_DDG_RESULTS:
            break
    return out


def lookup_bigfoil(profile_name: str) -> List[Dict[str, str]]:
    cached = _get_cached("bigfoil", profile_name)
    if cached is not None:
        return cached

    token = _normalize_token(profile_name)
    if not token or len(token) < 3:
        _set_cached("bigfoil", profile_name, [])
        return []

    if not _allow_remote("bigfoil"):
        return []

    try:
        _mark_remote("bigfoil")
        candidates = _ddg_search_urls(f'site:bigfoil.com "{profile_name}" "_info"', "bigfoil.com")
        info_url = ""
        for url in candidates:
            if "_info" in url:
                info_url = url
                break
        if not info_url:
            _set_cached("bigfoil", profile_name, [])
            return []

        info_html = _download_text(info_url)
        apps_m = re.search(
            r'HREF=["\'](?P<href>/apps/[0-9a-f\-]+_apps\.php)["\'][^>]*>\s*Applications\s*\((?P<n>\d+)\)',
            info_html,
            flags=re.IGNORECASE,
        )
        if not apps_m:
            _set_cached("bigfoil", profile_name, [])
            return []

        app_count = int(apps_m.group("n"))
        if app_count <= 0:
            _set_cached("bigfoil", profile_name, [])
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
                txt = re.sub(r"\s+", " ", name).strip()
                if not txt or txt.lower() in seen:
                    continue
                seen.add(txt.lower())
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
        _set_cached("bigfoil", profile_name, found)
        return found
    except Exception:
        _set_cached("bigfoil", profile_name, [])
        return []


def lookup_airfoiltools(profile_name: str) -> List[Dict[str, str]]:
    cached = _get_cached("airfoiltools", profile_name)
    if cached is not None:
        return cached

    token = _normalize_token(profile_name)
    if not token or len(token) < 3:
        _set_cached("airfoiltools", profile_name, [])
        return []

    if not _allow_remote("airfoiltools"):
        return []

    candidate_slugs = [token]
    m_naca = re.fullmatch(r"naca(\d{4,5})", token)
    if m_naca:
        candidate_slugs.append("n" + m_naca.group(1))
    if token.startswith("goettingen"):
        candidate_slugs.append("goe" + token[len("goettingen"):])
    if token.startswith("wortmannfx"):
        candidate_slugs.append(token[len("wortmann"):])

    # Deduplicate preserving order.
    seen_slugs = set()
    ordered_slugs = []
    for slug in candidate_slugs:
        if slug not in seen_slugs:
            seen_slugs.add(slug)
            ordered_slugs.append(slug)

    try:
        _mark_remote("airfoiltools")
        for slug in ordered_slugs:
            url = f"http://airfoiltools.com/airfoil/details?airfoil={slug}-il"
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as response:
                final_url = response.geturl()
                html = response.read().decode("utf-8", errors="replace")

            if "/airfoil/details?airfoil=" not in final_url:
                continue
            if "/search/" in final_url:
                continue

            text = re.sub(r"<[^>]+>", " ", html)
            text = re.sub(r"\s+", " ", text).strip()
            matches = re.findall(
                r"((?:used on|used by|designed for|applications?)\s+[^.]{15,220})",
                text,
                flags=re.IGNORECASE,
            )
            cleaned = []
            for item in matches:
                s = re.sub(r"\s+", " ", item).strip()
                if "airfoil database search" in s.lower():
                    continue
                if s.lower() in {x.lower() for x in cleaned}:
                    continue
                cleaned.append(s)
                if len(cleaned) >= 3:
                    break

            if cleaned:
                found = [{
                    "usage_text": " ; ".join(cleaned),
                    "source": "airfoiltools",
                    "source_url": final_url,
                }]
                _set_cached("airfoiltools", profile_name, found)
                return found

        _set_cached("airfoiltools", profile_name, [])
        return []
    except Exception:
        _set_cached("airfoiltools", profile_name, [])
        return []


def lookup_mh_aerotools(profile_name: str) -> List[Dict[str, str]]:
    cached = _get_cached("mh_aerotools", profile_name)
    if cached is not None:
        return cached
    _set_cached("mh_aerotools", profile_name, [])
    return []


def lookup_usage_fallback(profile_name: str) -> List[Dict[str, str]]:
    for fn in (lookup_bigfoil, lookup_airfoiltools, lookup_mh_aerotools):
        found = fn(profile_name)
        if found:
            return found
    return []
