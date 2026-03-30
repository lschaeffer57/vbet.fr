#!/usr/bin/env python3
"""unibet.fr — capture Playwright + extraction des cotes (CLI: capture | flat | run | fast | probe | cycle | serve)."""
from __future__ import annotations

import argparse
import json
import os
import re
import secrets
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

BASE = Path(__file__).resolve().parent
DATA = BASE / "data"
CACHE = DATA / "unibet_odds_cache.json"
SESSION = DATA / "unibet_session.json"
LANDING = os.environ.get("UNIBET_LANDING", "https://www.unibet.fr/sport")
PROFILE = DATA / "unibet_playwright_profile"

_PRICE_KEYS = frozenset(
    k.lower() for k in (
        "price", "odds", "odd", "decimal", "coefficient", "coef",
        "currentprice", "displayodds", "oddsdecimal",
        "currentpriceup", "currentpricedown", "priceup", "pricedown",
        "americanodds", "fractionalodds", "trueodds",
    )
)
_TEAM_KEYS = ("home", "away", "team1", "team2", "participant", "competitor")


def _write_json(path: str | Path, data) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _read_json(path: str | Path) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


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


def _to_odd(x) -> float | None:
    if isinstance(x, bool) or x is None:
        return None
    if isinstance(x, str):
        x = x.strip().replace(",", ".")
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if 1.01 <= v <= 1000.0 else None


def has_betting_odds_in_json(obj) -> bool:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k.lower() in _PRICE_KEYS and _is_odd(v):
                return True
            if has_betting_odds_in_json(v):
                return True
    elif isinstance(obj, list):
        for item in obj:
            if has_betting_odds_in_json(item):
                return True
    return False


def _extract_name(d: dict) -> str:
    for k in ("name", "label", "title", "caption", "description"):
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _extract_match_name(d: dict) -> str:
    for k in ("eventName", "matchName", "name", "title"):
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    h = ""
    a = ""
    for k in _TEAM_KEYS:
        v = d.get(k)
        if isinstance(v, dict):
            n = _extract_name(v)
            if "home" in k or "team1" in k:
                h = h or n
            elif "away" in k or "team2" in k:
                a = a or n
        elif isinstance(v, str):
            if "home" in k or "team1" in k:
                h = h or v.strip()
            elif "away" in k or "team2" in k:
                a = a or v.strip()
    if h or a:
        return f"{h} - {a}".strip(" -")
    return ""


def _walk(obj, parents: list[dict] | None = None):
    if parents is None:
        parents = []
    if isinstance(obj, dict):
        cur = parents + [obj]
        yield obj, cur
        for v in obj.values():
            yield from _walk(v, cur)
    elif isinstance(obj, list):
        for x in obj:
            yield from _walk(x, parents)


def _extract_flat_rows(payload: dict) -> list[dict]:
    rows = []
    for cap_i, cap in enumerate((payload.get("captures") or []) + (payload.get("ws_captures") or [])):
        data = cap.get("data")
        if not isinstance(data, (dict, list)):
            continue
        if isinstance(data, dict) and isinstance(data.get("items"), dict):
            rows.extend(_extract_rows_from_lvs_items(data, cap, cap_i))
            continue
        for node, parents in _walk(data):
            if not isinstance(node, dict):
                continue
            odd_key = None
            odd_val = None
            for k, v in node.items():
                if k.lower() in _PRICE_KEYS:
                    fv = _to_odd(v)
                    if fv is not None:
                        odd_key, odd_val = k, fv
                    break
            if odd_val is None:
                continue

            selection = _extract_name(node)
            market = ""
            match = ""
            sport = ""
            competition = ""
            event_id = node.get("id") or node.get("eventId")
            market_id = node.get("marketId") or node.get("id")
            selection_id = node.get("selectionId") or node.get("id")
            start_ts = None

            for p in reversed(parents):
                if not isinstance(p, dict):
                    continue
                if not match:
                    match = _extract_match_name(p)
                if not sport:
                    sport = str(p.get("sportName") or p.get("sport") or "").strip()
                if not competition:
                    competition = str(
                        p.get("competitionName") or p.get("leagueName") or p.get("tournamentName") or ""
                    ).strip()
                if not market:
                    market = str(p.get("marketName") or p.get("betOfferName") or p.get("criterion") or "").strip()
                if start_ts is None:
                    ts = p.get("startTime") or p.get("startDate") or p.get("kickOffTime")
                    if isinstance(ts, (int, float)):
                        start_ts = int(ts / 1000) if ts > 10_000_000_000 else int(ts)
                if isinstance(event_id, (str, int)) and event_id not in ("", 0):
                    pass
                else:
                    event_id = p.get("eventId") or p.get("id")

            if not selection:
                selection = (node.get("outcome") or node.get("type") or "").strip() if isinstance(node.get("outcome"), str) else ""
            if not selection:
                selection = "Selection"

            rows.append({
                "decimal": odd_val,
                "price_key": odd_key,
                "label": selection,
                "sport": sport or None,
                "competition": competition or None,
                "match": match or None,
                "market": market or None,
                "selection": selection,
                "event_id": event_id,
                "market_id": market_id,
                "selection_id": selection_id,
                "start_ts": start_ts,
                "source_url": cap.get("url"),
                "capture_kind": cap.get("kind"),
                "capture_index": cap_i,
            })

    uniq = {}
    for r in rows:
        k = (
            r.get("event_id"),
            r.get("market_id"),
            r.get("selection_id"),
            r.get("decimal"),
        )
        uniq[k] = r
    return list(uniq.values())


def _extract_rows_from_lvs_items(data: dict, cap: dict, cap_i: int) -> list[dict]:
    rows = []
    items = data.get("items") or {}
    if not isinstance(items, dict):
        return rows

    def get_item(item_id):
        if not item_id:
            return None
        v = items.get(str(item_id))
        return v if isinstance(v, dict) else None

    for oid, o in items.items():
        if not (isinstance(oid, str) and oid.startswith("o") and isinstance(o, dict)):
            continue
        odd = _to_odd(o.get("price"))
        if odd is None:
            continue

        m_id = o.get("parent")
        m = get_item(m_id) or {}
        e_id = m.get("parent")
        e = get_item(e_id) or {}
        p = get_item(e.get("parent")) or {}

        sport = ""
        competition = ""
        region = ""
        pth = e.get("path")
        if isinstance(pth, dict):
            sport = str(pth.get("Sport") or "").strip()
            competition = str(pth.get("League") or "").strip()
            region = str(pth.get("Category") or "").strip()

        a = str(e.get("a") or "").strip()
        b = str(e.get("b") or "").strip()
        match = str(e.get("desc") or "").strip() or (f"{a} - {b}" if a or b else "")

        start_ts = None
        start_raw = e.get("start")
        if isinstance(start_raw, str) and re.fullmatch(r"\d{10}", start_raw):
            yy = 2000 + int(start_raw[0:2])
            mm = int(start_raw[2:4])
            dd = int(start_raw[4:6])
            hh = int(start_raw[6:8])
            mi = int(start_raw[8:10])
            try:
                dt = datetime(yy, mm, dd, hh, mi, tzinfo=timezone.utc)
                start_ts = int(dt.timestamp())
            except Exception:
                start_ts = None
        elif isinstance(start_raw, (int, float)):
            start_ts = int(start_raw / 1000) if start_raw > 10_000_000_000 else int(start_raw)

        # Extraction de la ligne (handicap/total) — Kambi stocke en millièmes (2500 → 2.5)
        raw_line = m.get("line") if m else None
        line_val = None
        if raw_line is not None:
            try:
                v = float(raw_line)
                line_val = round(v / 1000, 3) if v > 100 else round(v, 3)
            except (TypeError, ValueError):
                pass

        rows.append({
            "decimal": odd,
            "price_key": "price",
            "label": str(o.get("desc") or "").strip() or "Selection",
            "sport": sport or None,
            "region": region or None,
            "competition": competition or str(p.get("desc") or "").strip() or None,
            "match": match or None,
            "market": str(m.get("desc") or "").strip() or None,
            "selection": str(o.get("desc") or "").strip() or "Selection",
            "line": line_val,
            "event_id": e_id,
            "market_id": m_id,
            "selection_id": oid,
            "start_ts": start_ts,
            "source_url": cap.get("url"),
            "capture_kind": cap.get("kind"),
            "capture_index": cap_i,
        })
    return rows


def _save_session_from_ctx(ctx, token: str) -> None:
    """Sauvegarde cookie + X-LVS-HSToken dans data/unibet_session.json."""
    try:
        parts = [
            f'{c["name"]}={c["value"]}'
            for c in ctx.cookies()
            if c.get("name") and c.get("value") is not None
        ]
        cookie = "; ".join(parts)
        if not cookie:
            return
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        SESSION.parent.mkdir(parents=True, exist_ok=True)
        SESSION.write_text(
            json.dumps(
                {"cookie": cookie, "x_lvs_hstoken": token or "", "updated_at": now},
                ensure_ascii=False, indent=2,
            ),
            encoding="utf-8",
        )
        print(f"[unibet] session sauvegardée -> {SESSION}", flush=True)
    except Exception as e:
        print(f"[unibet] avertissement : impossible de sauvegarder la session : {e}", flush=True)


def _sports_root_url(landing: str) -> str:
    u = (landing or "").strip().rstrip("/")
    i = u.find("/sport")
    if i != -1:
        return u[: i + len("/sport")]
    return u


def _capture_extra_urls(landing: str) -> list[str]:
    raw = os.environ.get("UNIBET_CAPTURE_EXPAND_PATHS", "").strip()
    if raw:
        paths = [p.strip("/") for p in raw.split("|") if p.strip()]
    else:
        paths = ["football", "tennis", "basketball", "hockey-sur-glace", "rugby", "handball"]
    root = _sports_root_url(landing)
    return [f"{root}/{p}" for p in paths]


def _url_maybe_odds(u: str) -> bool:
    s = (u or "").lower()
    if "unibet.fr" not in s:
        return False
    return any(
        x in s for x in (
            "/services-api/sportsbookdata/",
            "/service-sport-enligne-bff/",
            "/lvs-api/",
            "betoffer",
            "odds",
            "event",
            "kambi",
        )
    )


def _capture_profile(mode: str) -> tuple[int, int]:
    m = (mode or "balanced").strip().lower()
    if m == "fast":
        return 220, 4
    if m == "full":
        return 2500, 20
    return 800, 8


def run_capture(
    cache_file: str | Path,
    landing: str = LANDING,
    headless: bool = True,
    mode: str = "balanced",
) -> dict:
    from playwright.sync_api import sync_playwright

    os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", str(BASE / "ms-playwright"))
    wait_ms = int(os.environ.get("UNIBET_CAPTURE_WAIT_MS", "6000"))
    default_parent_scan, default_max_pages = _capture_profile(mode)
    max_pages = int(os.environ.get("UNIBET_MAX_PAGES_PER_PARENT", str(default_max_pages)))
    caps, seen_u = [], set()
    wss, seen_w = [], set()

    def on_resp(resp):
        try:
            ct = (resp.headers.get("content-type") or "").lower()
            if "json" not in ct:
                return
            url = resp.url
            if not _url_maybe_odds(url) or url in seen_u:
                return
            txt = resp.text()
            if len(txt) < 30:
                return
            data = json.loads(txt)
            seen_u.add(url)
            caps.append({
                "url": url,
                "status": resp.status,
                "kind": "http",
                "has_odds": has_betting_odds_in_json(data),
                "data": data,
            })
        except Exception:
            pass

    def on_ws_frame(ws_url, payload):
        try:
            txt = bytes(payload).decode("utf-8", errors="ignore") if isinstance(payload, (bytes, bytearray)) else str(payload)
            if len(txt) < 2 or txt[0] not in "[{":
                return
            data = json.loads(txt)
        except Exception:
            return
        if not has_betting_odds_in_json(data):
            return
        fp = hash((ws_url, txt[:4000]))
        if fp in seen_w:
            return
        seen_w.add(fp)
        wss.append({"url": ws_url, "kind": "websocket", "data": data})

    def on_ws(ws):
        if "unibet" not in ws.url and "kambi" not in ws.url:
            return
        def h(p):
            on_ws_frame(ws.url, p)
        try:
            ws.on("framereceived", h)
            ws.on("framesent", h)
        except Exception:
            pass

    ua = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    stealth = "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"

    started = time.time()
    with sync_playwright() as p:
        PROFILE.mkdir(parents=True, exist_ok=True)
        ctx = p.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE),
            headless=headless,
            viewport={"width": 1365, "height": 900},
            locale="fr-FR",
            timezone_id="Europe/Paris",
            user_agent=ua,
            extra_http_headers={"Accept-Language": "fr-FR,fr;q=0.9"},
            args=["--disable-blink-features=AutomationControlled", "--disable-dev-shm-usage"],
        )
        ctx.add_init_script(stealth)
        token_box = {"x_lvs_hstoken": ""}

        def on_req(req):
            try:
                h = req.headers.get("x-lvs-hstoken", "")
                if h and not token_box["x_lvs_hstoken"]:
                    token_box["x_lvs_hstoken"] = h
            except Exception:
                pass

        ctx.on("request", on_req)
        ctx.on("response", on_resp)
        ctx.on("websocket", on_ws)
        page = ctx.new_page()

        visited = [landing]
        print(f"[unibet] capture: -> {landing}", flush=True)
        page.goto(landing, wait_until="domcontentloaded", timeout=120000)
        page.wait_for_timeout(wait_ms)

        # Sauvegarde anticipée de la session dès que la page est chargée
        # (permet au fetch loop de démarrer sans attendre la fin du scan API)
        _save_session_from_ctx(ctx, token_box["x_lvs_hstoken"])

        # API automatique: récupère parents sport/compétition puis pagine /lvs-api/next
        def _collect_parent_ids_from_ept_node(node, out_set: set[str]) -> None:
            if isinstance(node, dict):
                nid = node.get("id")
                if isinstance(nid, int):
                    out_set.add(f"p{nid}")
                path = node.get("path")
                if isinstance(path, list):
                    for ch in path:
                        _collect_parent_ids_from_ept_node(ch, out_set)
            elif isinstance(node, list):
                for ch in node:
                    _collect_parent_ids_from_ept_node(ch, out_set)

        quick = page.evaluate(
            """async () => {
                const r = await fetch('/service-sport-enligne-bff/v1/quick-access', { credentials: 'include' });
                if (!r.ok) return [];
                return await r.json();
            }"""
        )
        parent_ids = set()
        sport_ids = set()
        if isinstance(quick, list):
            for block in quick:
                if not isinstance(block, dict):
                    continue
                for it in (block.get("items") or []):
                    if not isinstance(it, dict):
                        continue
                    sid = it.get("sportId")
                    cid = it.get("competitionId")
                    if isinstance(sid, int):
                        sport_ids.add(sid)
                        parent_ids.add(f"p{sid}")
                    if isinstance(cid, int):
                        parent_ids.add(f"p{cid}")

        # Découverte globale via EPT (arbre complet sportsbook)
        ept = page.evaluate(
            """async () => {
                const q = '/lvs-api/ept?originId=3&lineId=1&up=1&hidden=0'
                        + '&liveCount=e&preCount=e&status=OPEN,SUSPENDED'
                        + '&clockStatus=NOT_STARTED,STARTED,PAUSED,END_OF_PERIOD,ADJUST,INTERMISSION'
                        + '&includeAllMarkets=1';
                const r = await fetch(q, { credentials: 'include' });
                if (!r.ok) return null;
                try { return await r.json(); } catch (_) { return null; }
            }"""
        )
        if isinstance(ept, dict):
            _collect_parent_ids_from_ept_node(ept.get("ept"), parent_ids)
            _collect_parent_ids_from_ept_node(ept.get("hors"), parent_ids)
        # fallback sur routes sport
        for u in _capture_extra_urls(landing):
            if u not in visited:
                visited.append(u)
                try:
                    page.goto(u, wait_until="domcontentloaded", timeout=120000)
                    page.wait_for_timeout(2500)
                except Exception:
                    pass

        # parents complémentaires depuis sports connus
        for sid in sport_ids:
            parent_ids.add(f"p{sid}")
        if not parent_ids:
            parent_ids.update({"p240", "p239", "p227", "p2100", "p22877", "p1100"})

        queue = sorted(parent_ids)
        done_parents = set()
        discovered_limit = int(os.environ.get("UNIBET_MAX_PARENT_SCAN", str(default_parent_scan)))
        print(f"[unibet] scan API parents(init): {len(queue)}", flush=True)
        idx = 0
        while queue and len(done_parents) < discovered_limit:
            pid = queue.pop(0)
            if pid in done_parents:
                continue
            done_parents.add(pid)
            idx += 1
            next_seen = set()
            for page_index in range(max_pages):
                rel = (
                    f"/lvs-api/next/50/{pid}"
                    f"?lineId=1&originId=3&breakdownEventsIntoDays=true&showPromotions=true"
                    f"&includeAllMarkets=1&pageIndex={page_index}"
                )
                js = """async ({rel, token}) => {
                    const hdr = token ? { 'X-LVS-HSToken': token } : {};
                    for (let i = 0; i < 3; i++) {
                        try {
                            const r = await fetch(rel, { credentials: 'include', headers: hdr });
                            const txt = await r.text();
                            let data = null;
                            try { data = JSON.parse(txt); } catch (_) {}
                            if (r.ok) return { ok: true, status: r.status, url: r.url, data };
                            if (r.status !== 429 && r.status < 500) {
                                return { ok: false, status: r.status, url: r.url, data };
                            }
                        } catch (_) {}
                        await new Promise(res => setTimeout(res, 250 * (i + 1)));
                    }
                    return { ok: false, status: 599, url: rel, data: null };
                }"""
                out = page.evaluate(js, {"rel": rel, "token": token_box["x_lvs_hstoken"]})
                # Certains parents renvoient 401 en pageIndex=0; on tente pages suivantes.
                if not isinstance(out, dict) or not isinstance(out.get("data"), dict):
                    continue
                if not out.get("ok"):
                    break
                d = out["data"]
                u = out.get("url") or rel
                fp = f"{u}#pi={page_index}"
                if fp in seen_u:
                    continue
                seen_u.add(fp)
                caps.append({
                    "url": u,
                    "status": out.get("status"),
                    "kind": "http",
                    "has_odds": has_betting_odds_in_json(d),
                    "data": d,
                })
                # Découverte récursive: nouveaux parents (catégories/ligues) trouvés dans items.
                items = d.get("items") if isinstance(d, dict) else None
                if isinstance(items, dict):
                    for k in items.keys():
                        if isinstance(k, str) and k.startswith("p") and k not in done_parents:
                            queue.append(k)
                nxt = d.get("nextEventId")
                if nxt is not None:
                    if nxt in next_seen:
                        break
                    next_seen.add(nxt)
            if idx % 25 == 0:
                print(f"[unibet] scan API: {idx} parents (queue={len(queue)})", flush=True)
        # Sauvegarde de la session pour les appels directs (unibet_client / fast)
        _save_session_from_ctx(ctx, token_box["x_lvs_hstoken"])
        ctx.close()

    elapsed = round(time.time() - started, 2)
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    payload = {
        "generated_at": now,
        "source": "unibet",
        "mode": "playwright_network_capture",
        "landing": landing,
        "visited_urls": visited,
        "stats": {
            "elapsed_seconds": elapsed,
            "json_captures": len(caps),
            "ws_captures": len(wss),
            "pages_visited": len(visited),
        },
        "captures": caps,
        "ws_captures": wss,
    }
    _write_json(cache_file, payload)
    print(f"[unibet] {len(caps)} HTTP + {len(wss)} WS -> {cache_file} ({elapsed}s)", flush=True)
    return payload


def cmd_capture(a):
    run_capture(a.cache, landing=a.landing or LANDING, headless=(not a.headed), mode=a.mode)


def cmd_probe(a):
    """Vérifie la validité de la session sans lancer Playwright."""
    from unibet_client import probe_lvs_session
    sess = Path(a.session) if getattr(a, "session", None) else SESSION
    ok, msg = probe_lvs_session(sess)
    status = "OK" if ok else "ECHEC"
    print(f"[unibet] probe session: {status} — {msg}", flush=True)
    raise SystemExit(0 if ok else 1)


def cmd_fast(a):
    """Fetch direct LVS API sans Playwright (nécessite data/unibet_session.json)."""
    from unibet_client import run_lvs_fetch
    sess = Path(a.session) if getattr(a, "session", None) else SESSION
    cache = Path(a.cache)
    payload = run_lvs_fetch(
        session_file=sess,
        out_path=cache,
        mode=a.mode,
        workers=getattr(a, "workers", None) or None,
    )
    rows = _extract_flat_rows(payload)
    grouped = _group_by_sport(rows)
    out = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "sports": grouped,
    }
    _write_json(a.out, out)
    total = sum(d["total_rows"] for d in grouped.values())
    print(f"[unibet] fast: {total} cotes -> {a.out}", flush=True)


def cmd_flat(a):
    raw = _read_json(a.cache)
    rows = _extract_flat_rows(raw)
    out = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "source": "unibet",
        "cache_file": str(Path(a.cache).resolve()),
        "total_odds": len(rows),
        "odds": rows,
    }
    _write_json(a.out, out)
    print(f"[unibet] {len(rows)} lignes de cotes -> {a.out}", flush=True)


def cmd_run(a):
    run_capture(a.cache, landing=a.landing or LANDING, headless=(not a.headed), mode=a.mode)
    cmd_flat(argparse.Namespace(cache=a.cache, out=a.out))


# ── Cycle ─────────────────────────────────────────────────────────────────────

def cmd_cycle(a):
    """Boucle permanente : probe → capture Playwright si session KO → fast fetch → sleep → repeat."""
    from unibet_client import probe_lvs_session

    sess       = Path(a.session)
    out_file   = Path(a.out)
    cache_file = Path(a.cache)
    sleep_s    = float(os.environ.get("UNIBET_CYCLE_SLEEP_S", str(getattr(a, "cycle_sleep_s", 5.0))))
    no_capture = getattr(a, "no_capture", False)
    skip_probe = getattr(a, "skip_probe", False)
    max_rounds = int(getattr(a, "max_rounds", 0))   # 0 = infini

    def do_capture():
        print("[unibet] cycle: rafraîchissement session (Playwright)…", flush=True)
        run_capture(
            cache_file,
            landing=getattr(a, "landing", None) or LANDING,
            headless=not getattr(a, "capture_headed", False),
            mode="fast",   # capture légère pour récupérer les cookies
        )

    round_i = 0
    while True:
        round_i += 1
        if max_rounds and round_i > max_rounds:
            print(f"[unibet] cycle: {max_rounds} tours effectués.", flush=True)
            return

        print(f"[unibet] cycle: tour {round_i}" + (f"/{max_rounds}" if max_rounds else ""), flush=True)

        # ── 1. Probe session ────────────────────────────────────────────────
        if not skip_probe:
            ok, msg = probe_lvs_session(sess)
            if ok:
                print(f"[unibet] probe: {msg}", flush=True)
            else:
                print(f"[unibet] probe: échec — {msg}", flush=True)
                if no_capture:
                    print("[unibet] cycle: --no-capture actif, abandon.", flush=True)
                    sys.exit(1)
                try:
                    do_capture()
                except Exception as e:
                    print(f"[unibet] cycle: capture impossible : {e}", flush=True)
                    time.sleep(sleep_s)
                    continue
                time.sleep(min(sleep_s, 3.0))

        # ── 2. Fast fetch ───────────────────────────────────────────────────
        try:
            cmd_fast(argparse.Namespace(
                session=str(sess),
                cache=str(cache_file),
                out=str(out_file),
                mode=getattr(a, "mode", "fast"),
            ))
        except Exception as e:
            print(f"[unibet] cycle: erreur fetch — {e}", flush=True)
            if not no_capture:
                try:
                    do_capture()
                except Exception as ex:
                    print(f"[unibet] cycle: capture échouée : {ex}", flush=True)
            time.sleep(sleep_s)
            continue

        time.sleep(sleep_s)


# ── Serve ─────────────────────────────────────────────────────────────────────

def _read_flat(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


_HT_KEYWORDS = ("mi-temps", "1re mi", "2e mi", "halftime", "half time", "1st half", "2nd half")
_DRAW_LABELS  = frozenset(("nul", "match nul", "n", "égalité", "draw", "x"))
_OVER_LABELS  = frozenset(("plus de", "plus", "over", "+"))
_UNDER_LABELS = frozenset(("moins de", "moins", "under", "-"))


def _derive_period(market: str) -> str:
    m = (market or "").lower()
    return "HT" if any(kw in m for kw in _HT_KEYWORDS) else "FT"


def _derive_side(selection: str, match: str) -> str:
    sel = (selection or "").strip()
    sl  = sel.lower()
    if sl in _DRAW_LABELS:
        return "X"
    if sl in _OVER_LABELS:
        return "Over"
    if sl in _UNDER_LABELS:
        return "Under"
    if sl in ("w1", "1"):
        return "W1"
    if sl in ("w2", "2"):
        return "W2"
    # Séparer les équipes — supporte " - " et " vs " et " v "
    sep = None
    for s in (" - ", " vs ", " v "):
        if s in (match or ""):
            sep = s
            break
    if sep:
        home, _, away = match.partition(sep)
        home, away = home.strip(), away.strip()
        if sel == home or home.startswith(sel) or sel.startswith(home):
            return "W1"
        if sel == away or away.startswith(sel) or sel.startswith(away):
            return "W2"
    return sel


def _group_by_sport(rows: list[dict]) -> dict:
    """Regroupe les cotes plates en {sport: {total_rows, total_matches, competitions: {comp: [match_obj]}}}."""
    from collections import defaultdict
    tree: dict[str, dict[str, dict]] = defaultdict(lambda: defaultdict(dict))
    for r in rows:
        sport = (r.get("sport") or "Autres").strip()
        comp  = (r.get("competition") or "Inconnue").strip()
        match = (r.get("match") or "").strip()
        mkey  = f"{match}|{r.get('start_ts') or ''}"
        if mkey not in tree[sport][comp]:
            tree[sport][comp][mkey] = {
                "match":   match,
                "date":    datetime.fromtimestamp(r["start_ts"], tz=timezone.utc).isoformat()
                           if r.get("start_ts") else None,
                "markets": [],
            }
        sel    = (r.get("selection") or r.get("label") or "").strip()
        period = _derive_period(r.get("market") or "")
        side   = _derive_side(sel, match)

        entry: dict = {
            "market":    r.get("market") or "",
            "period":    period,
            "selection": sel,
            "odds":      r.get("decimal"),
            "side":      side,
        }

        # Ligne (handicap / total) — depuis raw data ou parsing du nom de marché
        line = r.get("line")
        if line is None and side in ("Over", "Under"):
            m_name = r.get("market") or ""
            lm = re.search(r'(\d+(?:[.,]\d+)?)', m_name)
            if lm:
                try:
                    line = float(lm.group(1).replace(",", "."))
                except ValueError:
                    pass
        if line is not None:
            entry["line"] = line

        tree[sport][comp][mkey]["markets"].append(entry)

    out = {}
    for sport, comps in sorted(tree.items()):
        comp_out: dict = {}
        total_rows = 0
        matches_set: set = set()
        for comp, matches in sorted(comps.items()):
            comp_out[comp] = list(matches.values())
            for m in matches.values():
                total_rows += len(m["markets"])
                matches_set.add(m["match"])
        out[sport] = {
            "total_rows":    total_rows,
            "total_matches": len(matches_set),
            "competitions":  comp_out,
        }
    return out


def cmd_serve(_a=None):
    import threading
    import uvicorn
    from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
    from fastapi.middleware.gzip import GZipMiddleware
    from pydantic import BaseModel, ConfigDict, Field

    cache_file = Path(os.getenv("UNIBET_ODDS_CACHE_FILE", str(CACHE)))
    out_flat   = Path(os.getenv("UNIBET_OUT_FLAT", str(BASE / "unibet_output.json")))
    sess_file  = Path(os.getenv("UNIBET_SESSION_FILE", str(SESSION)))

    app = FastAPI(title="Unibet")
    app.add_middleware(GZipMiddleware, minimum_size=1000)

    _refresh_lock    = threading.Lock()
    _refresh_running = False
    _fetch_lock      = threading.Lock()
    _fetch_running   = False

    class SessionBody(BaseModel):
        model_config = ConfigDict(extra="ignore")
        cookie:         str | None = Field(None)
        x_lvs_hstoken:  str | None = Field(None)
        session:        dict | None = Field(None)

    def _check_token(request: Request) -> None:
        tok = (os.getenv("UNIBET_INGEST_TOKEN") or "").strip()
        if not tok:
            raise HTTPException(503, "Définissez UNIBET_INGEST_TOKEN sur le serveur.")
        auth = (request.headers.get("Authorization") or "").strip()
        if not auth.startswith("Bearer "):
            raise HTTPException(401, "Header Authorization: Bearer <token> requis")
        if not secrets.compare_digest(auth[7:].strip(), tok):
            raise HTTPException(403, "Token invalide")

    def _load_odds():
        if not out_flat.exists():
            raise HTTPException(503, "Cache absent — premier fetch en cours")
        try:
            return _read_flat(out_flat)
        except json.JSONDecodeError as e:
            raise HTTPException(500, str(e)) from e

    # ── Endpoints publics ──────────────────────────────────────────────────────

    @app.get("/odds")
    def odds(sport: str | None = None):
        data = _load_odds()
        grouped = data.get("sports") or {}
        if sport:
            matched = {k: v for k, v in grouped.items() if sport.lower() in k.lower()}
            if not matched:
                raise HTTPException(404, f"Sport '{sport}' introuvable. Disponibles : {list(grouped)}")
            return {"generated_at": data.get("generated_at"), "sports": matched}
        return {"generated_at": data.get("generated_at"), "sports": grouped}

    @app.get("/odds/sports")
    def odds_sports():
        data = _load_odds()
        grp  = data.get("sports") or {}
        return {
            "sports": [
                {"name": k, "total_rows": v["total_rows"], "total_matches": v["total_matches"]}
                for k, v in grp.items()
            ]
        }

    @app.get("/health")
    def health():
        if not out_flat.exists():
            return {"status": "waiting", "cache_exists": False}
        age = time.time() - out_flat.stat().st_mtime
        return {
            "status":            "healthy" if age < 600 else "stale",
            "cache_exists":      True,
            "cache_age_seconds": round(age, 1),
        }

    @app.get("/session/status")
    def session_status():
        if not sess_file.exists():
            return {"has_file": False, "has_cookie": False, "refresh_running": _refresh_running}
        try:
            data  = json.loads(sess_file.read_text(encoding="utf-8"))
            ck    = (data.get("cookie") or "").strip()
            tok   = (data.get("x_lvs_hstoken") or "").strip()
            return {
                "has_file":       True,
                "has_cookie":     bool(ck),
                "has_token":      bool(tok),
                "updated_at":     data.get("updated_at"),
                "refresh_running": _refresh_running,
            }
        except Exception:
            return {"has_file": True, "has_cookie": False, "refresh_running": _refresh_running}

    # ── Endpoints protégés ─────────────────────────────────────────────────────

    @app.post("/session")
    def ingest_session(request: Request, body: SessionBody):
        """Injecte une session manuellement (depuis votre navigateur)."""
        _check_token(request)
        ck  = ""
        tok = ""
        if body.cookie:
            ck  = body.cookie.strip()
            tok = (body.x_lvs_hstoken or "").strip()
        elif body.session:
            ck  = (body.session.get("cookie") or "").strip()
            tok = (body.session.get("x_lvs_hstoken") or "").strip()
        if not ck:
            raise HTTPException(400, "Fournissez « cookie » (string) ou « session.cookie »")
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        sess_file.parent.mkdir(parents=True, exist_ok=True)
        sess_file.write_text(
            json.dumps({"cookie": ck, "x_lvs_hstoken": tok, "updated_at": now}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return {"ok": True, "has_token": bool(tok), "path": str(sess_file.resolve())}

    @app.post("/session/refresh")
    def refresh_session(request: Request, background_tasks: BackgroundTasks):
        """Déclenche une re-capture Playwright (session expirée)."""
        nonlocal _refresh_running
        _check_token(request)
        with _refresh_lock:
            if _refresh_running:
                raise HTTPException(409, "Une capture est déjà en cours")
            _refresh_running = True

        def job():
            nonlocal _refresh_running
            try:
                run_capture(cache_file, landing=LANDING, headless=True, mode="fast")
            except Exception as e:
                print(f"[unibet serve] refresh: {e}", file=sys.stderr)
            finally:
                with _refresh_lock:
                    _refresh_running = False

        background_tasks.add_task(job)
        return {"ok": True, "status": "started"}

    @app.post("/fetch")
    def trigger_fetch(request: Request, background_tasks: BackgroundTasks):
        """Déclenche manuellement un fetch LVS direct."""
        nonlocal _fetch_running
        _check_token(request)
        with _fetch_lock:
            if _fetch_running:
                raise HTTPException(409, "Un fetch est déjà en cours")
            _fetch_running = True

        def _do_fetch():
            nonlocal _fetch_running
            try:
                from unibet_client import run_lvs_fetch
                payload = run_lvs_fetch(
                    session_file=sess_file,
                    out_path=cache_file,
                    mode=os.environ.get("UNIBET_FETCH_MODE", "fast"),
                )
                rows = _extract_flat_rows(payload)
                _write_json(out_flat, {
                    "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    "source": "unibet",
                    "total_odds": len(rows),
                    "odds": rows,
                })
            except Exception as e:
                print(f"[unibet serve] fetch: {e}", file=sys.stderr)
            finally:
                with _fetch_lock:
                    _fetch_running = False

        background_tasks.add_task(_do_fetch)
        return {"ok": True, "status": "started"}

    @app.get("/")
    def root():
        return {
            "endpoints": [
                "GET  /odds?sport=Football",
                "GET  /odds/sports",
                "GET  /health",
                "GET  /session/status",
                "POST /session          (Bearer UNIBET_INGEST_TOKEN)",
                "POST /session/refresh  (Bearer UNIBET_INGEST_TOKEN)",
                "POST /fetch            (Bearer UNIBET_INGEST_TOKEN)",
            ]
        }

    # ── Auto-session manager (probe + re-capture périodique) ──────────────────

    def _probe_ok() -> bool:
        from unibet_client import probe_lvs_session
        ok, msg = probe_lvs_session(sess_file)
        if not ok:
            print(f"[unibet serve] probe: {msg}", file=sys.stderr)
        return ok

    def _do_capture():
        try:
            run_capture(cache_file, landing=LANDING, headless=True, mode="fast")
            return True
        except Exception as e:
            print(f"[unibet serve] capture: {e}", file=sys.stderr)
            return False

    # Session manager toujours actif — capture une session visiteur si absente
    probe_interval = float(os.environ.get("UNIBET_SESSION_PROBE_INTERVAL_S", "3600"))

    def _auto_session_manager():
        # Si pas de session (ni env vars ni fichier), en obtenir une via Playwright
        if not (
            (os.environ.get("UNIBET_SESSION_JSON_B64") or "").strip()
            or (os.environ.get("UNIBET_SESSION_JSON") or "").strip()
            or (os.environ.get("UNIBET_COOKIE") or "").strip()
            or sess_file.exists()
        ):
            print("[unibet serve] aucune session — capture initiale…", flush=True)
            _do_capture()
        else:
            time.sleep(5)
            if not _probe_ok():
                print("[unibet serve] session invalide → re-capture…", flush=True)
                _do_capture()
        while True:
            time.sleep(probe_interval)
            if not _probe_ok():
                print("[unibet serve] session expirée → re-capture…", flush=True)
                _do_capture()

    threading.Thread(target=_auto_session_manager, daemon=True).start()

    # ── Boucle de fetch continu ────────────────────────────────────────────────

    cooldown_s = float(os.environ.get("UNIBET_FETCH_COOLDOWN_S", "5"))
    fetch_mode = os.environ.get("UNIBET_FETCH_MODE", "fast")
    print(
        f"[unibet serve] auto-fetch continu (mode={fetch_mode}, cooldown={cooldown_s}s)",
        flush=True,
    )

    def _auto_fetch_loop():
        nonlocal _fetch_running
        # Attendre que la session existe
        while not sess_file.exists():
            print("[unibet serve] attente session…", flush=True)
            time.sleep(5)
        time.sleep(2)
        while True:
            with _fetch_lock:
                _fetch_running = True
            try:
                from unibet_client import run_lvs_fetch
                payload = run_lvs_fetch(
                    session_file=sess_file,
                    out_path=cache_file,
                    mode=fetch_mode,
                )
                rows = _extract_flat_rows(payload)
                grouped = _group_by_sport(rows)
                _write_json(out_flat, {
                    "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    "sports": grouped,
                })
                total = sum(d["total_rows"] for d in grouped.values())
                print(f"[unibet serve] auto-fetch: {total} cotes → {out_flat}", flush=True)
            except Exception as e:
                print(f"[unibet serve] auto-fetch: {e}", file=sys.stderr)
                # Si la session est KO, tenter une re-capture
                if "session" in str(e).lower() or "cookie" in str(e).lower():
                    print("[unibet serve] auto-fetch: tentative re-capture…", flush=True)
                    _do_capture()
                time.sleep(10)
            finally:
                with _fetch_lock:
                    _fetch_running = False
            time.sleep(cooldown_s)

    threading.Thread(target=_auto_fetch_loop, daemon=True).start()

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.getenv("UNIBET_PORT", os.getenv("PORT", "8004"))),
    )


def main():
    p = argparse.ArgumentParser(prog="unibet.py")
    sub = p.add_subparsers(dest="cmd", required=True)

    c1 = sub.add_parser("capture", help="Playwright -> cache JSON")
    c1.add_argument("--cache", default=str(CACHE))
    c1.add_argument("--landing", default=None)
    c1.add_argument("--headed", action="store_true")
    c1.add_argument("--mode", choices=("fast", "balanced", "full"), default="balanced")
    c1.set_defaults(_fn=cmd_capture)

    c2 = sub.add_parser("flat", help="cache -> output lignes de cotes")
    c2.add_argument("--cache", default=str(CACHE))
    c2.add_argument("--out", default=str(BASE / "unibet_output.json"))
    c2.set_defaults(_fn=cmd_flat)

    c3 = sub.add_parser("run", help="capture + flat")
    c3.add_argument("--cache", default=str(CACHE))
    c3.add_argument("--landing", default=None)
    c3.add_argument("--headed", action="store_true")
    c3.add_argument("--out", default=str(BASE / "unibet_output.json"))
    c3.add_argument("--mode", choices=("fast", "balanced", "full"), default="balanced")
    c3.set_defaults(_fn=cmd_run)

    c4 = sub.add_parser("probe", help="vérifie la session (sans Playwright)")
    c4.add_argument("--session", default=str(SESSION))
    c4.set_defaults(_fn=cmd_probe)

    c5 = sub.add_parser("fast", help="fetch direct LVS API sans Playwright")
    c5.add_argument("--session", default=str(SESSION))
    c5.add_argument("--cache", default=str(CACHE))
    c5.add_argument("--out", default=str(BASE / "unibet_output.json"))
    c5.add_argument("--mode", choices=("fast", "balanced", "full"), default="balanced")
    c5.add_argument("--workers", type=int, default=0, help="threads parallèles (0=auto)")
    c5.set_defaults(_fn=cmd_fast)

    c6 = sub.add_parser("cycle", help="boucle permanente : probe → capture si KO → fast fetch → repeat")
    c6.add_argument("--session",         default=str(SESSION))
    c6.add_argument("--cache",           default=str(CACHE))
    c6.add_argument("--out",             default=str(BASE / "unibet_output.json"))
    c6.add_argument("--mode",            choices=("fast", "balanced", "full"), default="fast",
                    help="mode du fast fetch (défaut: fast = 220 parents)")
    c6.add_argument("--cycle-sleep-s",   type=float, default=5.0, dest="cycle_sleep_s",
                    help="pause entre chaque tour (défaut: 5s)")
    c6.add_argument("--max-rounds",      type=int, default=0, dest="max_rounds",
                    help="nombre de tours max (0 = infini, défaut: 0)")
    c6.add_argument("--no-capture",      action="store_true", dest="no_capture",
                    help="ne pas relancer Playwright si session KO")
    c6.add_argument("--skip-probe",      action="store_true", dest="skip_probe",
                    help="sauter la vérification de session avant chaque fetch")
    c6.add_argument("--capture-headed",  action="store_true", dest="capture_headed",
                    help="afficher le navigateur lors des captures de session")
    c6.add_argument("--landing",         default=None)
    c6.set_defaults(_fn=cmd_cycle)

    c7 = sub.add_parser("serve", help="API /odds en continu (port 8004)")
    c7.set_defaults(_fn=lambda _: cmd_serve())

    args = p.parse_args()
    args._fn(args)


if __name__ == "__main__":
    main()
