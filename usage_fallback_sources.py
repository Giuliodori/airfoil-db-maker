import ssl
import urllib.error
import urllib.request
from typing import Dict, List


USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Python airfoil-usage-fallback"


def _download_text(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return response.read().decode("utf-8", errors="replace")
    except urllib.error.URLError as e:
        reason = getattr(e, "reason", None)
        if isinstance(reason, ssl.SSLCertVerificationError):
            insecure_ctx = ssl._create_unverified_context()
            with urllib.request.urlopen(req, timeout=30, context=insecure_ctx) as response:
                return response.read().decode("utf-8", errors="replace")
        raise


def lookup_bigfoil(profile_name: str) -> List[Dict[str, str]]:
    # Placeholder conservativo:
    # la funzione verrà estesa con parser dedicato per Applications(N>0).
    _ = profile_name
    return []


def lookup_airfoiltools(profile_name: str) -> List[Dict[str, str]]:
    # Placeholder conservativo:
    # la funzione verrà estesa con parser dedicato per frasi "used on"/"designed for".
    _ = profile_name
    return []


def lookup_mh_aerotools(profile_name: str) -> List[Dict[str, str]]:
    # Placeholder conservativo:
    # la funzione verrà estesa con parser dedicato famiglia MH.
    _ = profile_name
    return []


def lookup_usage_fallback(profile_name: str) -> List[Dict[str, str]]:
    for fn in (lookup_bigfoil, lookup_airfoiltools, lookup_mh_aerotools):
        found = fn(profile_name)
        if found:
            return found
    return []
