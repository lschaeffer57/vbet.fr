#!/usr/bin/env python3
"""vbet.fr — scraper Swarm. CLI : capture | serve | export-storage"""
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import secrets
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

# ── Config ──────────────────────────────────────────────────────────────────
BASE    = Path(__file__).resolve().parent
DATA    = BASE / "data"
CACHE   = DATA / "vbet_odds_cache.json"
SESSION = DATA / "vbet_session.json"
PROFILE = DATA / "vbet_playwright_profile"
LANDING = os.environ.get("VBET_LANDING", "https://www.vbet.fr/fr/sports")

# Swarm — même backend que daznbet.fr, site_id différent (auto-détecté à la capture)
WS_URL  = os.environ.get("VBET_SWARM_WS", "wss://swarm-2.vbet.fr/")
ORIGIN  = os.environ.get("VBET_ORIGIN",   "https://www.vbet.fr")

# Injecter les valeurs dans swarm_client avant import
os.environ.setdefault("DAZNBET_ORIGIN",   ORIGIN)
os.environ.setdefault("DAZNBET_SWARM_WS", WS_URL)

# ── Utilities ────────────────────────────────────────────────────────────────
_MERGE_GAME_FIELDS = (
    "team1_name", "team2_name", "markets_count",
    "sportcast_id", "game_number", "is_started", "is_blocked",
)

def _read_cache(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))

def _write_json(path, data):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def session_path():
    raw = (os.environ.get("VBET_SESSION_FILE") or "").strip()
    return Path(raw).expanduser().resolve() if raw else SESSION

def save_session(cookie: str, site_id: str):
    session_path().parent.mkdir(parents=True, exist_ok=True)
    session_path().write_text(
        json.dumps({
            "cookie":     cookie.strip(),
            "site_id":    site_id,
            "updated_at": datetime.now(timezone.utc)
                          .replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        }, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

def load_session() -> tuple[str, str]:
    """Retourne (cookie, site_id) depuis l'env (Railway) ou le fichier local."""
    # Priorité env Railway
    cookie_env = (os.environ.get("VBET_COOKIE") or "").strip()
    site_id_env = (os.environ.get("VBET_SITE_ID") or "").strip()
    if cookie_env:
        return cookie_env, site_id_env

    if not session_path().exists():
        raise FileNotFoundError(
            f"Session absente : {session_path()}. "
            "Lancez : python vbet.py capture --headed --manual"
        )
    data = json.loads(session_path().read_text(encoding="utf-8"))
    cookie  = (data.get("cookie")  or "").strip()
    site_id = (data.get("site_id") or "").strip() or site_id_env
    if not cookie:
        raise ValueError("Clé 'cookie' vide dans le fichier session")
    return cookie, site_id

def _cookie_header_from_playwright_context(ctx) -> str:
    parts = []
    for c in ctx.cookies():
        if c.get("name") and c.get("value") is not None:
            parts.append(f'{c["name"]}={c["value"]}')
    return "; ".join(parts)

def _restore_storage_state(ctx):
    b64 = (os.environ.get("VBET_STORAGE_STATE_B64") or "").strip()
    if not b64:
        return
    try:
        state = json.loads(base64.b64decode(b64).decode("utf-8"))
        cookies = state.get("cookies") or []
        if cookies:
            ctx.add_cookies(cookies)
            print(f"[vbet] storage_state: {len(cookies)} cookies injectés", flush=True)
    except Exception as e:
        print(f"[vbet] storage_state: erreur injection — {e}", flush=True)

# ── Odds parsing (identique daznbet.fr) ─────────────────────────────────────
def _norm_sport(s: str) -> str:
    return s.lower().replace("-", " ").replace("_", " ").strip()

def _ev_price(ev):
    for k in ("price", "Price", "decimal", "Decimal"):
        if k in ev:
            try:
                v = float(ev[k])
                if 1.001 <= v <= 10000:
                    return v
            except (TypeError, ValueError):
                pass
    return None

def _float_or_none(x):
    try: return float(x)
    except (TypeError, ValueError): return None

def _line_cut(ev, m):
    for obj in (ev, m):
        if isinstance(obj, dict):
            v = _float_or_none(obj.get("base"))
            if v is not None:
                return v
    return None

def _extra_info_blob(ev, m) -> str:
    for obj in (ev, m):
        if not isinstance(obj, dict):
            continue
        ei = obj.get("extra_info")
        if ei is None or ei == "":
            continue
        if isinstance(ei, (dict, list)):
            return json.dumps(ei, ensure_ascii=False)
        return str(ei).strip()
    return ""

def _pretty_market_type(mt: str) -> str:
    if not mt: return ""
    s = re.sub(r"([a-z])([A-Z0-9])", r"\1 \2", mt)
    s = re.sub(r"(\d)([A-Za-z])", r"\1 \2", s)
    return re.sub(r"\s+", " ", s).strip()

def _selection_display(r: dict) -> str:
    sel = (r.get("selection") or "").strip()
    return sel or (r.get("event_type") or "").strip()

def _slug_period(mt):
    t = (mt or "").lower()
    if any(x in t for x in ("handicap", "asian", "spread", "margin")):
        slug = "spread"
    elif any(x in t for x in ("overunder", "over/", "total", "both", "btts", "score", "goal")):
        slug = "total"
    else:
        slug = "moneyline"
    m = mt or ""
    if re.search(r"(?i)first.?quarter|q1\b", m):    per = "Q1"
    elif re.search(r"(?i)second.?quarter|q2\b", m): per = "Q2"
    elif re.search(r"(?i)third.?quarter|q3\b", m):  per = "Q3"
    elif re.search(r"(?i)fourth.?quarter|q4\b", m): per = "Q4"
    elif re.search(r"(?i)first.?set|set.?1", m):    per = "S1"
    elif re.search(r"(?i)second.?set|set.?2", m):   per = "S2"
    elif re.search(r"(?i)half.?time|firsthalf|1sthalf", m) and "second" not in m.lower(): per = "HT"
    elif re.search(r"(?i)second.?half|2ndhalf", m) or m.startswith("SecondHalf"): per = "FT"
    elif m.startswith(("HalfTime", "FirstHalf")): per = "HT"
    else: per = "FT"
    return slug, per

def _market_line_label(r: dict) -> str:
    gn = (r.get("group_name") or "").strip()
    mn = (r.get("market_name") or "").strip()
    ei = (r.get("extra_info") or "").strip()
    mt = (r.get("market_type") or "").strip()
    parts = [p for p in (gn, mn) if p]
    if ei: parts.append(ei)
    if parts: return " — ".join(parts)
    return _pretty_market_type(mt)

def _period_for_row(r: dict) -> str:
    blob = " ".join(filter(None, (
        r.get("market_type"), r.get("market_name"),
        r.get("group_name"), r.get("extra_info"),
    )))
    _, per = _slug_period(blob)
    return per

def _walk_sports(obj):
    if isinstance(obj, dict):
        sp = obj.get("sport")
        if isinstance(sp, dict) and sp:
            s0 = next(iter(sp.values()), None)
            if isinstance(s0, dict) and "region" in s0:
                for sval in sp.values():
                    if isinstance(sval, dict):
                        yield sval
        for v in obj.values():
            yield from _walk_sports(v)
    elif isinstance(obj, list):
        for x in obj:
            yield from _walk_sports(x)

def _iter_games(sport_node):
    if not isinstance(sport_node, dict): return
    sid   = sport_node.get("id")
    sname = str(sport_node.get("name") or sport_node.get("alias") or "").strip()
    sn    = _norm_sport(sname)
    for reg in (sport_node.get("region") or {}).values():
        if not isinstance(reg, dict): continue
        rn = (reg.get("name") or "").strip()
        for comp in (reg.get("competition") or {}).values():
            if not isinstance(comp, dict): continue
            cn = (comp.get("name") or "").strip()
            for game in (comp.get("game") or {}).values():
                if isinstance(game, dict) and "team1_name" in game:
                    yield {"sport_id": sid, "sport_name": sname, "sport_norm": sn,
                           "region_name": rn, "competition_name": cn, "game": game}

def _merge_games_from_payload(payload: dict):
    merged = {}
    for cap in (payload.get("captures") or []) + (payload.get("ws_captures") or []):
        if not isinstance(cap, dict) or cap.get("data") is None: continue
        root = cap["data"]
        for block in _walk_sports(root):
            for ctx in _iter_games(block):
                g   = ctx["game"]
                gid = g.get("id")
                if gid is None: continue
                if gid not in merged:
                    merged[gid] = {"ctx": ctx, "game": dict(g)}
                else:
                    mg = merged[gid]["game"]
                    m0 = mg.get("market") or {}
                    m1 = g.get("market")  or {}
                    if isinstance(m0, dict) and isinstance(m1, dict):
                        mg["market"] = {**m0, **m1}
                    elif isinstance(m1, dict) and m1:
                        mg["market"] = m1
                    for k in _MERGE_GAME_FIELDS:
                        if (mg.get(k) in (None, "", 0)) and g.get(k) not in (None, ""):
                            mg[k] = g[k]
    return merged

def _swarm_rows(payload: dict):
    rows = []
    for gid, item in _merge_games_from_payload(payload).items():
        ctx = item["ctx"]
        g   = item["game"]
        t1  = (g.get("team1_name") or "").strip()
        t2  = (g.get("team2_name") or "").strip()
        ts  = g.get("start_ts")
        for m in (g.get("market") or {}).values():
            if not isinstance(m, dict): continue
            mt    = (m.get("type")  or "").strip()
            mname = (m.get("name")  or "").strip()
            for ev in (m.get("event") or {}).values():
                if not isinstance(ev, dict): continue
                pr = _ev_price(ev)
                if pr is None: continue
                rows.append({
                    "sport_id":        ctx["sport_id"],
                    "sport":           ctx["sport_name"],
                    "region":          ctx["region_name"],
                    "competition":     ctx["competition_name"],
                    "game_id":         gid,
                    "team1":           t1,
                    "team2":           t2,
                    "team1_id":        g.get("team1_id"),
                    "team2_id":        g.get("team2_id"),
                    "start_ts":        ts,
                    "markets_count":   g.get("markets_count"),
                    "is_blocked":      g.get("is_blocked"),
                    "market_id":       m.get("id"),
                    "market_name":     mname,
                    "group_name":      (m.get("group_name") or "").strip(),
                    "market_type":     mt,
                    "line":            _line_cut(ev, m),
                    "extra_info":      _extra_info_blob(ev, m),
                    "event_type":      (ev.get("type_1") or "").strip(),
                    "selection_id":    ev.get("id"),
                    "selection":       (ev.get("name") or "").strip(),
                    "decimal":         pr,
                })
    uniq = {}
    for r in rows:
        k = (r.get("game_id"), r.get("market_id"), r.get("selection_id"))
        uniq[k] = r
    return list(uniq.values())

def build_downloads(rows):
    sports = {}
    for r in rows:
        sp = (r.get("sport") or "Unknown").strip()
        sports.setdefault(sp, {"total_rows": 0, "competitions": {}})
        region = (r.get("region") or "").strip()
        comp   = (r.get("competition") or "").strip()
        ck     = f"{region} - {comp}" if region and comp else (comp or region or "Autres")
        sports[sp]["competitions"].setdefault(ck, {})
        gid = r.get("game_id")
        gk  = str(gid) if gid is not None else f'{r.get("team1")}|{r.get("team2")}|{r.get("start_ts")}'
        cmap = sports[sp]["competitions"][ck]
        if gk not in cmap:
            ts = r.get("start_ts")
            d  = (datetime.fromtimestamp(int(ts), tz=timezone.utc)
                  .replace(microsecond=0).isoformat()
                  if isinstance(ts, (int, float)) and ts > 0 else "")
            t1s = (r.get("team1") or "").strip()
            t2s = (r.get("team2") or "").strip()
            cmap[gk] = {"match": f"{t1s} - {t2s}" if t1s or t2s else "", "date": d, "markets": []}
        entry = {
            "market":    _market_line_label(r),
            "period":    _period_for_row(r),
            "selection": _selection_display(r),
            "odds":      float(r["decimal"]),
        }
        ln = r.get("line")
        if ln is not None: entry["line"] = ln
        et = (r.get("event_type") or "").strip()
        if et: entry["side"] = et
        cmap[gk]["markets"].append(entry)
        sports[sp]["total_rows"] += 1
    out = {}
    for sp, d in sports.items():
        comps = {k: list(v.values()) for k, v in d["competitions"].items()}
        out[sp] = {
            "total_rows":    d["total_rows"],
            "total_matches": sum(len(x) for x in d["competitions"].values()),
            "competitions":  comps,
        }
    return {"generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), "sports": out}

# ── Capture Playwright ───────────────────────────────────────────────────────
def run_capture(headless=True, manual=False):
    from playwright.sync_api import sync_playwright
    from swarm_client import parse_afec

    ua = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    detected = {"site_id": ""}  # auto-détection depuis request_session WS

    def on_ws_frame(ws_url, payload):
        try:
            text = bytes(payload).decode("utf-8", errors="ignore") if isinstance(payload, (bytes, bytearray)) else str(payload)
            if not text or text[0] not in "[{": return
            data = json.loads(text)
        except Exception: return
        # Extraire site_id depuis la commande request_session envoyée par le navigateur
        if isinstance(data, dict) and data.get("command") == "request_session":
            sid = str((data.get("params") or {}).get("site_id", ""))
            if sid and not detected["site_id"]:
                detected["site_id"] = sid
                print(f"[vbet] site_id détecté : {sid}", flush=True)

    def on_ws(ws):
        if not any(x in ws.url for x in ("swarm", "vbet")):
            return
        def h(p): on_ws_frame(ws.url, p)
        try:
            ws.on("framereceived", h)
            ws.on("framesent",     h)
        except Exception: pass

    with sync_playwright() as p:
        PROFILE.mkdir(parents=True, exist_ok=True)
        ctx = None
        for channel in ("chrome", None):
            try:
                kw = dict(
                    user_data_dir=str(PROFILE),
                    headless=headless,
                    viewport={"width": 1365, "height": 900},
                    locale="fr-FR",
                    timezone_id="Europe/Paris",
                    user_agent=ua,
                    extra_http_headers={"Accept-Language": "fr-FR,fr;q=0.9"},
                    args=["--disable-blink-features=AutomationControlled", "--disable-dev-shm-usage"],
                )
                if channel: kw["channel"] = channel
                ctx = p.chromium.launch_persistent_context(**kw)
                break
            except Exception:
                continue
        if ctx is None:
            raise RuntimeError("Chromium indisponible")

        ctx.add_init_script("Object.defineProperty(navigator, 'webdriver', { get: () => undefined });")
        _restore_storage_state(ctx)
        ctx.on("websocket", on_ws)

        page = ctx.new_page()
        print(f"[vbet] capture: → {LANDING}", flush=True)
        page.goto(LANDING, wait_until="domcontentloaded", timeout=120_000)

        for _ in range(8):
            page.mouse.wheel(0, 1800)
            page.wait_for_timeout(1000)
        page.wait_for_timeout(int(os.environ.get("VBET_CAPTURE_WAIT_MS", "15000")))

        if manual and not headless:
            print("[vbet] >>> Connectez-vous sur vbet.fr, puis appuyez sur Entrée…", flush=True)
            try: input()
            except EOFError: pass
            page.wait_for_timeout(8000)

        # Attendre le cookie Swarm (_immortal|user-hashX)
        deadline = time.time() + float(os.environ.get("VBET_CAPTURE_SWARM_WAIT_S", "60"))
        while time.time() < deadline:
            ch = _cookie_header_from_playwright_context(ctx)
            if parse_afec(ch):
                break
            page.wait_for_timeout(2000)

        ch      = _cookie_header_from_playwright_context(ctx)
        site_id = detected["site_id"] or os.environ.get("VBET_SITE_ID", "")

        if not parse_afec(ch):
            print("[vbet] AVERTISSEMENT : cookie Swarm absent. Connectez-vous manuellement avec --headed --manual", flush=True)
        if not site_id:
            print("[vbet] AVERTISSEMENT : site_id non détecté. Définissez VBET_SITE_ID manuellement.", flush=True)

        save_session(ch, site_id)
        print(f"[vbet] Session sauvegardée → {session_path()} (site_id={site_id!r})", flush=True)

        # Export storage_state (pour Railway)
        try:
            state = ctx.storage_state()
            b64   = base64.b64encode(json.dumps(state, ensure_ascii=False).encode()).decode()
            print(f"\n[vbet] Copiez dans Railway → VBET_STORAGE_STATE_B64:\n{b64}\n", flush=True)
            (BASE / "data" / "vbet_storage_state_b64.txt").write_text(b64, encoding="utf-8")
        except Exception as e:
            print(f"[vbet] export storage_state: {e}", flush=True)

        ctx.close()

# ── Swarm fetch ──────────────────────────────────────────────────────────────
def run_fetch(cookie: str, site_id: str, out_path: Path):
    from swarm_client import run_swarm_fetch
    run_swarm_fetch(
        session_file=session_path(),
        out_path=out_path,
        ws_url=WS_URL,
        site_id=site_id,
        mode="full",
        sports_filter=None,
        max_competitions=int(os.environ.get("VBET_MAX_COMPETITIONS", "0")),
        delay_s=float(os.environ.get("VBET_FETCH_DELAY_S", "0")),
    )

# ── Serve ────────────────────────────────────────────────────────────────────
def cmd_serve():
    import uvicorn
    from fastapi import FastAPI, HTTPException

    cf      = Path(os.environ.get("VBET_ODDS_CACHE_FILE", str(CACHE)))
    out_flat = Path(os.environ.get("VBET_OUT_FLAT", str(BASE / "output.json")))
    app     = FastAPI(title="Vbet")

    tok = (os.environ.get("VBET_SERVE_TOKEN") or "").strip()

    def load():
        f = out_flat if out_flat.exists() else None
        if f is None:
            raise HTTPException(503, "Cache absent — fetch en cours")
        try:
            return _read_cache(f)
        except json.JSONDecodeError as e:
            raise HTTPException(500, str(e)) from e

    @app.get("/odds")
    def odds():
        return load()

    @app.get("/health")
    def health():
        if not out_flat.exists():
            return {"status": "waiting", "cache_exists": False}
        age = time.time() - out_flat.stat().st_mtime
        return {"status": "healthy" if age < 600 else "stale", "cache_exists": True,
                "cache_age_seconds": round(age, 1)}

    @app.get("/session/status")
    def session_status():
        if not session_path().exists():
            return {"has_file": False, "has_swarm_cookie": False}
        try:
            data    = json.loads(session_path().read_text(encoding="utf-8"))
            cookie  = (data.get("cookie") or "").strip()
            site_id = (data.get("site_id") or "").strip()
            from swarm_client import parse_afec
            return {"has_file": True, "has_swarm_cookie": bool(parse_afec(cookie)),
                    "site_id": site_id}
        except Exception as e:
            return {"has_file": True, "has_swarm_cookie": False, "error": str(e)}

    # Boucle fetch continue
    def _auto_fetch_loop():
        while not session_path().exists():
            time.sleep(2)
        time.sleep(2)
        cooldown = float(os.environ.get("VBET_FETCH_COOLDOWN_S", "1"))
        while True:
            try:
                cookie, site_id = load_session()
                if not site_id:
                    print("[vbet serve] site_id manquant — fetch ignoré", flush=True)
                    time.sleep(30)
                    continue
                run_fetch(cookie, site_id, cf)
                raw       = _read_cache(cf)
                processed = build_downloads(_swarm_rows(raw))
                _write_json(out_flat, processed)
                print(f"[vbet serve] output.json mis à jour ({len(processed.get('sports', {}))} sports)", flush=True)
            except FileNotFoundError as e:
                print(f"[vbet serve] session absente — {e}", flush=True)
                time.sleep(30)
                continue
            except Exception as e:
                print(f"[vbet serve] fetch erreur — {e}", flush=True)
                time.sleep(10)
            time.sleep(cooldown)

    threading.Thread(target=_auto_fetch_loop, daemon=True).start()

    port = int(os.environ.get("PORT", "8003"))
    print(f"[vbet] API → http://0.0.0.0:{port}", flush=True)
    uvicorn.run(app, host="0.0.0.0", port=port)

# ── CLI ──────────────────────────────────────────────────────────────────────
def main():
    p = argparse.ArgumentParser(description="vbet.fr scraper")
    sub = p.add_subparsers(dest="cmd")

    c_cap = sub.add_parser("capture", help="Capture Playwright (session + site_id)")
    c_cap.add_argument("--headed",  action="store_true")
    c_cap.add_argument("--manual",  action="store_true", help="Pause pour connexion manuelle")

    sub.add_parser("serve", help="API /odds en continu (port 8003)")

    a = p.parse_args()
    if a.cmd == "capture":
        run_capture(headless=not a.headed, manual=a.manual)
    elif a.cmd == "serve":
        cmd_serve()
    else:
        p.print_help()

if __name__ == "__main__":
    main()
