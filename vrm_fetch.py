#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import datetime as dt
import json
import math
import os
import sys
import time
import unicodedata
from typing import Any, Dict, List, Optional, Tuple

import requests
from zoneinfo import ZoneInfo

BASE = "https://vrmapi.victronenergy.com/v2"

# --- Tunables to prevent hangs ---
CONNECT_TIMEOUT = float(os.environ.get("VRM_CONNECT_TIMEOUT", "4"))  # seconds
READ_TIMEOUT = float(os.environ.get("VRM_READ_TIMEOUT", "6"))  # seconds
RETRIES = int(os.environ.get("VRM_RETRIES", "2"))  # attempts (total GETs = RETRIES)
BACKOFF_BASE = float(os.environ.get("VRM_BACKOFF_BASE", "0.4"))  # seconds
TOTAL_BUDGET_SEC = float(
    os.environ.get("VRM_TOTAL_TIMEOUT", "25")
)  # overall script budget

SESSION = requests.Session()


def auth_headers(token: str) -> Dict[str, str]:
    return {
        "X-Authorization": f"Token {token}",
        "Accept": "application/json",
        "User-Agent": "vrm-fetch-v9/alarms/1.0 (+python)",
    }


def api_get(
    path: str, token: str, params: Optional[Dict[str, Any]] = None, *, t0: float
) -> Dict[str, Any]:
    """GET with bounded retries and overall-budget awareness."""
    url = path if path.startswith("http") else f"{BASE}{path}"
    attempt = 0
    while attempt < RETRIES:
        remaining = TOTAL_BUDGET_SEC - (time.monotonic() - t0)
        if remaining <= 0:
            raise TimeoutError(
                f"Global timeout exceeded ({TOTAL_BUDGET_SEC}s) before calling {url}"
            )
        # Per-request time budget (split between connect & read, bounded by remaining)
        connect_t = min(CONNECT_TIMEOUT, max(0.5, remaining / 2))
        read_t = min(READ_TIMEOUT, max(0.5, remaining / 2))
        try:
            r = SESSION.get(
                url,
                headers=auth_headers(token),
                params=params,
                timeout=(connect_t, read_t),
                allow_redirects=False,
                stream=False,
            )
            if r.status_code == 401:
                raise SystemExit("401 Unauthorized (token inválido o sin permisos).")
            r.raise_for_status()
            return r.json()
        except (requests.Timeout, requests.ConnectionError) as e:
            attempt += 1
            if attempt >= RETRIES:
                raise
            # brief, bounded backoff but do not violate global budget
            sleep_s = min(BACKOFF_BASE * (2 ** (attempt - 1)), max(0.1, remaining / 4))
            time.sleep(sleep_s)
        except requests.RequestException:
            # Non-timeout HTTP errors: do not spin forever; fail fast
            raise
    # Should not reach here
    raise TimeoutError(f"Failed to GET {url} within retry budget")


def strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn"
    )


def norm(s: str) -> str:
    return strip_accents(s or "").casefold()


def users_me(token: str, t0: float) -> Optional[int]:
    j = api_get("/users/me", token, t0=t0)
    user = j.get("user") if isinstance(j, dict) else None
    if isinstance(user, dict):
        return user.get("id")
    return None


def list_installations_by_user(
    token: str, user_id: int, t0: float
) -> List[Dict[str, Any]]:
    j = api_get(f"/users/{user_id}/installations", token, t0=t0)
    return j.get("records", []) if isinstance(j, dict) else []


def build_site_tz_map(installs: List[Dict[str, Any]]) -> Dict[int, str]:
    m: Dict[int, str] = {}
    for it in installs:
        sid = it.get("idSite")
        tz = it.get("timezone") or it.get("timeZone") or it.get("tz")
        if isinstance(sid, int) and isinstance(tz, str) and tz:
            m[sid] = tz
    return m


def pick_site_id(installs: List[Dict[str, Any]], want_contains: str) -> Optional[int]:
    target = norm(want_contains)
    for it in installs:
        name = it.get("name") or ""
        if target in norm(name):
            return it.get("idSite")
    return None


def venus_stats(token: str, site_id: int, t0: float) -> Dict[str, Any]:
    return api_get(
        f"/installations/{site_id}/stats", token, params={"type": "venus"}, t0=t0
    )


def get_active_alarms(token: str, site_id: int, t0: float) -> List[Dict[str, Any]]:
    """
    Fetch active alarms for a site. We try to be flexible with response shape.
    """
    try:
        j = api_get(
            f"/installations/{site_id}/alarms", token, params={"active": "true"}, t0=t0
        )
    except Exception:
        return []
    data = j.get("data") if isinstance(j, dict) else None
    # possible shapes:
    #  - {"success": true, "records": [...]}
    #  - {"success": true, "data": {"records": [...]}}  (less likely here)
    recs = []
    if isinstance(j, dict) and "records" in j and isinstance(j["records"], list):
        recs = j["records"]
    elif isinstance(data, dict) and isinstance(data.get("records"), list):
        recs = data["records"]
    elif isinstance(j, dict) and isinstance(j.get("alarms"), list):
        recs = j["alarms"]
    elif isinstance(data, dict) and isinstance(data.get("alarms"), list):
        recs = data["alarms"]

    out: List[Dict[str, Any]] = []
    for a in recs or []:
        active_flag = a.get("active")
        state = a.get("state")
        if active_flag is True or active_flag == 1 or state in ("active", 1, "1"):
            out.append(
                {
                    "time": a.get("startTime") or a.get("timestamp") or a.get("time"),
                    "name": a.get("name") or a.get("title") or a.get("code"),
                    "severity": a.get("severity"),
                    "message": a.get("message") or a.get("text"),
                }
            )
    return out


def last_point_value(
    entries: List[List[Any]], prefer_avg: bool = False
) -> Tuple[Optional[int], Optional[float]]:
    """
    entries: list of points. Each point is:
      [ts, value] or [ts, avg, min, max].
    If prefer_avg is True, take index 1 (avg) when available.
    Returns (timestamp_ms, value)
    """
    if not isinstance(entries, list) or not entries:
        return (None, None)
    # Scan from the end to get the latest non-null; bounded
    for idx in range(len(entries) - 1, -1, -1):
        pt = entries[idx]
        if not isinstance(pt, (list, tuple)) or len(pt) < 2:
            continue
        ts = pt[0] if isinstance(pt[0], (int, float)) else None
        if ts is None:
            continue
        if prefer_avg and len(pt) >= 2 and isinstance(pt[1], (int, float)):
            val = float(pt[1])
        else:
            val = pt[1] if isinstance(pt[1], (int, float)) else None
        if val is not None and not math.isnan(val):
            return (int(ts), float(val))
    return (None, None)


def iso_utc(dt_obj: dt.datetime) -> str:
    return dt_obj.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")


def utc_now_iso() -> str:
    return iso_utc(dt.datetime.now(dt.UTC))


def site_local_ms_to_utc_iso(ms: int, site_tz: Optional[str]) -> str:
    try:
        if site_tz:
            local = dt.datetime.fromtimestamp(ms / 1000, ZoneInfo(site_tz))
            return iso_utc(local)
    except Exception:
        pass
    return (
        dt.datetime.fromtimestamp(ms / 1000, dt.UTC).isoformat().replace("+00:00", "Z")
    )


def main() -> int:
    t0 = time.monotonic()
    token = (
        os.environ.get("VRM_TOKEN")
        or "e5352471d358f93967e2e1b0bd660a33b63e6342935230f45098744174dd0687"
    )

    user_id = users_me(token, t0)
    if user_id is None:
        print("No pude obtener user id con /users/me", file=sys.stderr)
        return 2

    installs = list_installations_by_user(token, user_id, t0)
    if not installs:
        print("No hay instalaciones en /users/{id}/installations", file=sys.stderr)
        return 3

    site_tz_map = build_site_tz_map(installs)

    gen_id = pick_site_id(installs, "Generacion") or pick_site_id(
        installs, "generación"
    )
    con_id = pick_site_id(installs, "Consumo") or pick_site_id(installs, "consumo")

    out = {
        "timestamp_utc": utc_now_iso(),
        "timestamp_data": None,
        "generación": {
            "solar": {"potencia_w": None},
            "red": {"potencia_w": None},
            "bateria": {"bateria_soc_pct": None},
            "alarmas": [],  # <-- nuevo
        },
        "consumo": {
            "potencia_w": None,
            "alarmas": [],  # <-- nuevo
        },
        "notes": [],
    }

    timestamp_candidates: List[Tuple[int, Optional[str]]] = []

    # GENERACIÓN
    if gen_id is not None:
        vgen = venus_stats(token, gen_id, t0)
        rec_g = vgen.get("records", {}) if isinstance(vgen, dict) else {}
        gen_tz = site_tz_map.get(gen_id)

        ts, val = last_point_value(rec_g.get("solar_yield", []), prefer_avg=False)
        if val is not None:
            out["generación"]["solar"]["potencia_w"] = val
        if ts is not None:
            timestamp_candidates.append((ts, gen_tz))

        ts, val = last_point_value(rec_g.get("from_to_grid", []), prefer_avg=False)
        if val is not None:
            out["generación"]["red"]["potencia_w"] = val
        if ts is not None:
            timestamp_candidates.append((ts, gen_tz))

        ts, val = last_point_value(rec_g.get("bs", []), prefer_avg=True)
        if val is not None:
            out["generación"]["bateria"]["bateria_soc_pct"] = val
        if ts is not None:
            timestamp_candidates.append((ts, gen_tz))

        # alarmas activas
        try:
            out["generación"]["alarmas"] = get_active_alarms(token, gen_id, t0)
        except Exception as e:
            out["notes"].append(f"Error leyendo alarmas de generación: {e}")
    else:
        out["notes"].append(
            "No se encontró la instalación de generación (nombre contiene 'Generacion' o 'generación')."
        )

    # CONSUMO
    if con_id is not None:
        vcon = venus_stats(token, con_id, t0)
        rec_c = vcon.get("records", {}) if isinstance(vcon, dict) else {}
        con_tz = site_tz_map.get(con_id)

        ts, val = last_point_value(rec_c.get("ac_loads", []), prefer_avg=False)
        if val is not None:
            out["consumo"]["potencia_w"] = val
        if ts is not None:
            timestamp_candidates.append((ts, con_tz))

        # alarmas activas
        try:
            out["consumo"]["alarmas"] = get_active_alarms(token, con_id, t0)
        except Exception as e:
            out["notes"].append(f"Error leyendo alarmas de consumo: {e}")
    else:
        out["notes"].append(
            "No se encontró la instalación de consumo (nombre contiene 'Consumo')."
        )

    if timestamp_candidates:
        latest_ts, latest_tz = max(timestamp_candidates, key=lambda x: x[0])
        out["timestamp_data"] = site_local_ms_to_utc_iso(latest_ts, latest_tz)

    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
