"""Client HTTP direct Unibet LVS API — sans Playwright (analogue à swarm_client.py).

Usage depuis Python:
    from unibet_client import probe_lvs_session, run_lvs_fetch
    from pathlib import Path

    ok, msg = probe_lvs_session(Path("data/unibet_session.json"))
    run_lvs_fetch(
        session_file=Path("data/unibet_session.json"),
        out_path=Path("data/unibet_odds_cache.json"),
        mode="balanced",
    )
"""
from __future__ import annotations

import gzip
import json
import os
import time
from pathlib import Path
from typing import Callable

import http.client
import random
import ssl
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

BASE = Path(__file__).resolve().parent
DEFAULT_BASE_URL = os.environ.get("UNIBET_BASE_URL", "https://www.unibet.fr")
DEFAULT_SESSION_FILE = BASE / "data" / "unibet_session.json"

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

_PRICE_KEYS = frozenset(
    k.lower() for k in (
        "price", "odds", "odd", "decimal", "coefficient", "coef",
        "currentprice", "displayodds", "oddsdecimal",
    )
)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _is_odd(x) -> bool:
    if isinstance(x, bool) or x is None:
        return False
    if isinstance(x, str):
        x = x.strip().replace(",", ".")
    try:
        v = float(x)
    except (TypeError, ValueError):
        return False
    return 1.01 <= v <= 1000.0


def _has_betting_odds(obj) -> bool:
    """Renvoie True si obj contient au moins une cote valide (standalone, sans import unibet)."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k.lower() in _PRICE_KEYS and _is_odd(v):
                return True
            if _has_betting_odds(v):
                return True
    elif isinstance(obj, list):
        for item in obj:
            if _has_betting_odds(item):
                return True
    return False


def _capture_profile(mode: str) -> tuple[int, int, int]:
    """(max_parent_scan, max_pages_per_parent, max_ff_events)"""
    m = (mode or "balanced").strip().lower()
    if m == "fast":
        return 220, 4, 500
    if m == "full":
        return 2500, 20, 0   # 0 = illimité
    return 800, 8, 2000


# ── Token refresh ─────────────────────────────────────────────────────────────

def _refresh_token(cookie: str, *, base_url: str = DEFAULT_BASE_URL) -> str:
    """Récupère un hsToken frais via /lvs-api/acc/token (sans Playwright)."""
    data = _fetch_json("/lvs-api/acc/token", cookie, "", base_url=base_url, timeout=15)
    if isinstance(data, dict):
        tok = (data.get("hsToken") or data.get("token") or "").strip()
        if tok:
            return tok
    return ""


# ── Session ───────────────────────────────────────────────────────────────────

def _raw_session_json_from_env() -> str | None:
    """Lit la session depuis les variables d'environnement (priorité Railway)."""
    s = (os.environ.get("UNIBET_SESSION_JSON") or "").strip()
    if s:
        return s
    b64 = (os.environ.get("UNIBET_SESSION_JSON_B64") or "").strip()
    if b64:
        import base64
        try:
            return base64.b64decode(b64).decode("utf-8")
        except (ValueError, UnicodeDecodeError) as e:
            raise ValueError(f"UNIBET_SESSION_JSON_B64 invalide : {e}") from e
    c = (os.environ.get("UNIBET_COOKIE") or "").strip()
    if c:
        tok = (os.environ.get("UNIBET_TOKEN") or "").strip()
        return json.dumps({"cookie": c, "x_lvs_hstoken": tok, "updated_at": ""}, ensure_ascii=False)
    return None


def load_session(session_path: Path) -> tuple[str, str]:
    """Charge (cookie, x_lvs_hstoken) depuis env ou fichier.

    Priorité :
    1. UNIBET_SESSION_JSON   (JSON brut)
    2. UNIBET_SESSION_JSON_B64 (JSON base64)
    3. UNIBET_COOKIE + UNIBET_TOKEN (strings séparés)
    4. Fichier session_path
    """
    env_blob = _raw_session_json_from_env()
    if env_blob:
        try:
            data = json.loads(env_blob)
        except json.JSONDecodeError as e:
            raise ValueError(f"Session JSON invalide (env) : {e}") from e
        ck = (data.get("cookie") or "").strip()
        if not ck:
            raise ValueError("Session env : clé « cookie » vide ou absente")
        session_path.parent.mkdir(parents=True, exist_ok=True)
        session_path.write_text(env_blob, encoding="utf-8")
        tok = (data.get("x_lvs_hstoken") or data.get("token") or "").strip()
        return ck, tok

    if not session_path.exists():
        raise FileNotFoundError(
            f"Session absente : {session_path}.\n"
            "Lancez d'abord : python unibet.py capture --headed\n"
            "Ou définissez UNIBET_SESSION_JSON, UNIBET_SESSION_JSON_B64, ou UNIBET_COOKIE."
        )
    data = json.loads(session_path.read_text(encoding="utf-8"))
    ck = (data.get("cookie") or "").strip()
    if not ck:
        raise ValueError(f"Clé « cookie » vide dans {session_path}")
    tok = (data.get("x_lvs_hstoken") or data.get("token") or "").strip()
    return ck, tok


# ── HTTP fetch avec connection pooling thread-local ───────────────────────────

_tls = threading.local()
_ssl_ctx = ssl.create_default_context()


def _get_conn(host: str, timeout: int) -> http.client.HTTPSConnection:
    """Retourne la connexion HTTPS persistante du thread courant pour ce host."""
    if not hasattr(_tls, "conns"):
        _tls.conns = {}
    conn = _tls.conns.get(host)
    if conn is None:
        conn = http.client.HTTPSConnection(host, timeout=timeout, context=_ssl_ctx)
        _tls.conns[host] = conn
    else:
        conn.timeout = timeout
    return conn


def _fetch_json(
    path: str,
    cookie: str,
    token: str,
    *,
    base_url: str = DEFAULT_BASE_URL,
    timeout: int = 45,
) -> dict | list | None:
    """GET {base_url}{path} avec les headers Unibet (keep-alive). Retourne le JSON ou None."""
    full_url = base_url.rstrip("/") + path
    parsed = urlparse(full_url)
    host = parsed.netloc
    req_path = parsed.path + (f"?{parsed.query}" if parsed.query else "")

    headers = {
        "Cookie": cookie,
        "User-Agent": UA,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "fr-FR,fr;q=0.9",
        "Accept-Encoding": "gzip",
        "Referer": f"{base_url}/sport",
        "Origin": base_url,
        "Connection": "keep-alive",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
    }
    if token:
        headers["X-LVS-HSToken"] = token

    for attempt in range(2):
        try:
            conn = _get_conn(host, timeout)
            conn.request("GET", req_path, headers=headers)
            resp = conn.getresponse()
            raw = resp.read()
            status = resp.status

            if status == 429:
                time.sleep(2.0 + random.random())
                # fermer et recréer la connexion après 429
                conn.close()
                _tls.conns.pop(host, None)
                if attempt == 0:
                    continue
                return None

            if status >= 400:
                return None

            enc = (resp.getheader("Content-Encoding") or "").lower()
            if "gzip" in enc:
                raw = gzip.decompress(raw)
            return json.loads(raw.decode("utf-8", errors="replace"))

        except Exception:
            # Connexion cassée → fermer et retenter une fois
            try:
                _tls.conns.pop(host, None)
            except Exception:
                pass
            if attempt == 0:
                continue
            return None

    return None


# ── Session probe ─────────────────────────────────────────────────────────────

def probe_lvs_session(
    session_file: Path,
    *,
    base_url: str = DEFAULT_BASE_URL,
    timeout_s: float = 15.0,
) -> tuple[bool, str]:
    """Teste rapidement la validité de la session sans lancer Playwright."""
    try:
        cookie, token = load_session(session_file)
    except FileNotFoundError as e:
        return False, str(e)
    except (ValueError, json.JSONDecodeError) as e:
        return False, str(e)

    if not cookie:
        return False, "Cookie vide"

    # Test 1 : quick-access (léger, sans auth obligatoire)
    data = _fetch_json(
        "/service-sport-enligne-bff/v1/quick-access",
        cookie, token,
        base_url=base_url,
        timeout=int(timeout_s),
    )
    if data is not None:
        return True, "session OK (quick-access)"

    # Test 2 : EPT minimal
    data = _fetch_json(
        "/lvs-api/ept?originId=3&lineId=1&up=1&hidden=0"
        "&liveCount=e&preCount=e&status=OPEN&includeAllMarkets=0",
        cookie, token,
        base_url=base_url,
        timeout=int(timeout_s),
    )
    if data is not None:
        return True, "session OK (EPT)"

    return False, "toutes les requêtes ont retourné None — session expirée ou bloquée"


# ── Parent discovery ──────────────────────────────────────────────────────────

def _collect_parent_ids_from_node(node, out: set[str]) -> None:
    """Parcourt récursivement un nœud EPT et collecte les IDs de parents (pXXX)."""
    if isinstance(node, dict):
        nid = node.get("id")
        if isinstance(nid, int):
            out.add(f"p{nid}")
        for v in node.values():
            _collect_parent_ids_from_node(v, out)
    elif isinstance(node, list):
        for ch in node:
            _collect_parent_ids_from_node(ch, out)


# ── Main fetch ────────────────────────────────────────────────────────────────

def run_lvs_fetch(
    *,
    session_file: Path,
    out_path: Path,
    mode: str = "balanced",
    max_parents: int | None = None,
    base_url: str = DEFAULT_BASE_URL,
    delay_s: float = 0.0,
    progress: Callable[[str], None] | None = None,
    full_markets: bool = True,
    workers: int | None = None,
) -> dict:
    """Fetch complet LVS API sans Playwright — deux passes parallélisées.

    Passe 1 : découverte de tous les événements via /lvs-api/next/50/{pid}
    Passe 2 : détail complet (totaux, handicaps, buteurs…) via /lvs-api/ff/{eid}
              → 88-154 marchés par événement au lieu de 1
    """
    log = progress or (lambda m: print(m, flush=True))
    default_parent_scan, default_max_pages, default_max_ff = _capture_profile(mode)
    max_parent_scan = max_parents or int(
        os.environ.get("UNIBET_MAX_PARENT_SCAN", str(default_parent_scan))
    )
    max_pages = int(os.environ.get("UNIBET_MAX_PAGES_PER_PARENT", str(default_max_pages)))
    max_ff_events = int(os.environ.get("UNIBET_MAX_FF_EVENTS", str(default_max_ff)))

    # Nombre de workers parallèles selon le mode
    _default_workers = {"fast": 40, "balanced": 20, "full": 12}
    n_workers = workers or int(
        os.environ.get("UNIBET_WORKERS", str(_default_workers.get(mode, 15)))
    )

    cookie, token = load_session(session_file)
    log(f"[unibet-client] session chargée — token={'présent' if token else 'absent'}")

    # Rafraîchissement du token sans Playwright
    fresh = _refresh_token(cookie, base_url=base_url)
    if fresh:
        token = fresh
        log("[unibet-client] token rafraîchi via /lvs-api/acc/token")
    elif not token:
        log("[unibet-client] avertissement: token absent, certaines requêtes peuvent échouer")

    t0 = time.time()

    # ── 1. Seeds via quick-access ─────────────────────────────────────────────
    parent_ids: set[str] = set()
    quick = _fetch_json(
        "/service-sport-enligne-bff/v1/quick-access",
        cookie, token, base_url=base_url, timeout=30,
    )
    if isinstance(quick, list):
        for block in quick:
            if not isinstance(block, dict):
                continue
            for it in (block.get("items") or []):
                if not isinstance(it, dict):
                    continue
                for key in ("sportId", "competitionId"):
                    v = it.get(key)
                    if isinstance(v, int):
                        parent_ids.add(f"p{v}")
        log(f"[unibet-client] quick-access: {len(parent_ids)} parents")
    else:
        log("[unibet-client] quick-access: aucune réponse")

    # ── 2. Seeds via EPT (arbre complet) ─────────────────────────────────────
    ept_data = _fetch_json(
        "/lvs-api/ept?originId=3&lineId=1&up=1&hidden=0"
        "&liveCount=e&preCount=e&status=OPEN,SUSPENDED"
        "&clockStatus=NOT_STARTED,STARTED,PAUSED,END_OF_PERIOD,ADJUST,INTERMISSION"
        "&includeAllMarkets=0",
        cookie, token, base_url=base_url, timeout=60,
    )
    if isinstance(ept_data, dict):
        _collect_parent_ids_from_node(ept_data.get("ept"), parent_ids)
        _collect_parent_ids_from_node(ept_data.get("hors"), parent_ids)
        log(f"[unibet-client] EPT: {len(parent_ids)} parents total")
    else:
        log("[unibet-client] EPT: aucune réponse")

    # Fallback minimal si aucun parent trouvé
    if not parent_ids:
        parent_ids.update({"p240", "p239", "p227", "p2100", "p22877", "p1100"})
        log("[unibet-client] fallback parents par défaut")

    # ── 3. Passe 1 parallèle : découverte événements via /lvs-api/next ────────
    # Chaque parent est traité dans un thread (pages séquentielles par parent).
    # Les nouveaux parents découverts sont soumis au pool dynamiquement.

    caps: list[dict] = []
    event_ids: list[str] = []
    seen_events: set[str] = set()
    done: set[str] = set()

    log(
        f"[unibet-client] passe 1 — {len(parent_ids)} parents initiaux, "
        f"limite={max_parent_scan}, pages/parent={max_pages}, workers={n_workers}"
    )

    def _fetch_parent(pid: str) -> tuple[list[dict], list[str], list[str]]:
        """Fetch toutes les pages d'un parent. Retourne (captures, event_ids, child_parent_ids)."""
        p_caps: list[dict] = []
        p_eids: list[str] = []
        p_children: list[str] = []
        next_seen: set = set()

        for page_index in range(max_pages):
            path = (
                f"/lvs-api/next/50/{pid}"
                f"?lineId=1&originId=3&breakdownEventsIntoDays=true"
                f"&showPromotions=true&pageIndex={page_index}"
            )
            data = _fetch_json(path, cookie, token, base_url=base_url, timeout=45)
            if not isinstance(data, dict):
                break

            p_caps.append({
                "url": base_url.rstrip("/") + path,
                "status": 200,
                "kind": "http",
                "pass": 1,
                "has_odds": _has_betting_odds(data),
                "data": data,
            })

            items = data.get("items")
            if isinstance(items, dict):
                for k in items:
                    if not isinstance(k, str):
                        continue
                    if k.startswith("e") or k.startswith("l"):
                        p_eids.append(k)
                    elif k.startswith("p"):
                        p_children.append(k)

            nxt = data.get("nextEventId")
            if nxt is not None:
                if nxt in next_seen:
                    break
                next_seen.add(nxt)
            elif page_index == 0 and nxt is None:
                break

        return p_caps, p_eids, p_children

    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        futures: dict = {}

        for pid in sorted(parent_ids):
            if len(done) >= max_parent_scan:
                break
            done.add(pid)
            futures[executor.submit(_fetch_parent, pid)] = pid

        completed = 0
        while futures:
            done_future = next(as_completed(futures))
            pid = futures.pop(done_future)
            completed += 1

            try:
                p_caps, p_eids, p_children = done_future.result()
            except Exception:
                continue

            caps.extend(p_caps)
            for eid in p_eids:
                if eid not in seen_events:
                    seen_events.add(eid)
                    event_ids.append(eid)

            # Soumettre les nouveaux parents découverts
            for cpid in p_children:
                if cpid not in done and len(done) < max_parent_scan:
                    done.add(cpid)
                    futures[executor.submit(_fetch_parent, cpid)] = cpid

            if completed % 25 == 0:
                elapsed_now = round(time.time() - t0, 1)
                log(
                    f"[unibet-client] passe 1 — {completed} parents traités "
                    f"(en attente={len(futures)}, événements={len(event_ids)}, "
                    f"captures={len(caps)}, {elapsed_now}s)"
                )

    p1_caps = len(caps)
    log(
        f"[unibet-client] passe 1 terminée — "
        f"{completed} parents, {len(event_ids)} événements, {p1_caps} captures"
    )

    # ── 4. Passe 2 parallèle : détail complet via /lvs-api/ff/{eid} ──────────
    if full_markets and event_ids:
        ff_limit = max_ff_events if max_ff_events > 0 else len(event_ids)
        ff_list = event_ids[:ff_limit]
        log(f"[unibet-client] passe 2 — {len(ff_list)} événements → /lvs-api/ff/ (workers={n_workers})")

        def _fetch_ff(eid: str) -> dict | None:
            ff_path = (
                f"/lvs-api/ff/{eid}"
                f"?lineId=1&originId=3&ext=1&showPromotions=true&showMarketTypeGroups=true"
            )
            ff_data = _fetch_json(ff_path, cookie, token, base_url=base_url, timeout=30)
            if not isinstance(ff_data, dict):
                return None
            return {
                "url": base_url.rstrip("/") + ff_path,
                "status": 200,
                "kind": "http",
                "pass": 2,
                "event_id": eid,
                "has_odds": _has_betting_odds(ff_data),
                "data": ff_data,
            }

        ff_completed = 0
        with ThreadPoolExecutor(max_workers=n_workers) as executor:
            ff_futures = {executor.submit(_fetch_ff, eid): eid for eid in ff_list}
            for future in as_completed(ff_futures):
                ff_completed += 1
                result = future.result()
                if result:
                    caps.append(result)

                if ff_completed % 100 == 0:
                    elapsed_now = round(time.time() - t0, 1)
                    log(
                        f"[unibet-client] passe 2 — {ff_completed}/{len(ff_list)} événements "
                        f"({elapsed_now}s)"
                    )

        p2_caps = len(caps) - p1_caps
        log(f"[unibet-client] passe 2 terminée — {p2_caps} captures ff/")

    elapsed = round(time.time() - t0, 2)
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    payload = {
        "generated_at": now,
        "source": "unibet",
        "mode": "lvs_direct_fetch",
        "base_url": base_url,
        "fetch": {
            "mode": mode,
            "max_parent_scan": max_parent_scan,
            "max_pages": max_pages,
            "max_ff_events": max_ff_events,
            "full_markets": full_markets,
            "elapsed_seconds": elapsed,
        },
        "stats": {
            "elapsed_seconds": elapsed,
            "json_captures": len(caps),
            "ws_captures": 0,
            "pages_visited": len(done),
            "events_discovered": len(event_ids),
        },
        "captures": caps,
        "ws_captures": [],
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"[unibet-client] {len(caps)} captures → {out_path} ({elapsed}s)")
    return payload
