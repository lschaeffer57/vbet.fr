#!/usr/bin/env python3
"""vbet.fr — capture Playwright (Swarm). CLI : run | fast | cycle | capture | fetch | api | flat | lines | spec | downloads | serve"""
import argparse
import json
import os
import re
import secrets
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

BASE    = Path(__file__).resolve().parent
DATA    = BASE / "data"
CACHE   = DATA / "vbet_odds_cache.json"
SESSION = DATA / "vbet_session.json"
PROFILE = DATA / "vbet_playwright_profile"
LANDING = os.environ.get("VBET_LANDING", "https://www.vbet.fr/fr/sports")

_PRICE_KEYS = frozenset(
    k.lower() for k in (
        "price", "decimal", "coefficient", "odds", "odd", "coef",
        "decimalprice", "decimalPrice", "Price", "Decimal",
    )
)
_PLAYER_PROP_RE = re.compile(
    r"(?i)(total\s*(points|goals|assists|rebounds|shots)|points\s*\+\s*assists|double\s*double|triple\s*double|3\s*points|three\s*point|"
    r"PlayerProp|player\s*prop|\bpasses\b|\brebounds?\b)",
)
_FB_DENY_TYPE   = re.compile(
    r"(?i)(AnytimeGoalscorer|PlayerToScore|CorrectScore|HalfTimeCorrectScore|2ndHalfCorrectScore|"
    r"Corner|Card|Booking|Offside|Penalty|Shot|ShotOn|Foul|Throw)"
)
_FB_DENY_PREFIX = re.compile(r"^1-\d+Minutes")
_TN_DENY = re.compile(r"(?i)CorrectScore|exact.*score")
_BK_DENY = re.compile(r"(?i)AnytimeGoalscorer|correct.*score")
_HK_DENY = re.compile(r"(?i)CorrectScore|fight|penalty.*shootout")
_MERGE_GAME_FIELDS = (
    "team1_name", "team2_name", "markets_count",
    "sportcast_id", "game_number", "is_started", "is_blocked",
)


# ── Utils ────────────────────────────────────────────────────────────────────

def _read_cache(path: str | Path) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _write_json(path: str | Path, data) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def session_path() -> Path:
    raw = (os.environ.get("VBET_SESSION_FILE") or "").strip()
    return Path(raw).expanduser().resolve() if raw else SESSION


def _load_site_id_from_session() -> str:
    sp = session_path()
    if sp.exists():
        try:
            data = json.loads(sp.read_text(encoding="utf-8"))
            sid  = (data.get("site_id") or "").strip()
            if sid:
                return sid
        except Exception:
            pass
    return ""


def _resolve_site_id(arg_site_id: str = "") -> str:
    """Priority : CLI arg → VBET_SITE_ID env → session file."""
    if arg_site_id:
        return arg_site_id
    env = (os.environ.get("VBET_SITE_ID") or "").strip()
    if env:
        return env
    return _load_site_id_from_session()


def save_session(cookie: str, site_id: str = "") -> None:
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


def _cookie_header_from_playwright_context(ctx) -> str:
    parts = []
    for c in ctx.cookies():
        if c.get("name") and c.get("value") is not None:
            parts.append(f'{c["name"]}={c["value"]}')
    return "; ".join(parts)


# ── Playwright helpers ───────────────────────────────────────────────────────

def _auto_login(page) -> bool:
    """Connexion automatique via VBET_USERNAME + VBET_PASSWORD."""
    username = (os.environ.get("VBET_USERNAME") or "").strip()
    password = (os.environ.get("VBET_PASSWORD") or "").strip()
    if not username or not password:
        return False

    print("[vbet] auto-login: tentative de connexion…", flush=True)

    for sel in [
        "text=Connexion", "text=Se connecter", "text=Login",
        "[data-test='login-button']", "[class*='login'][class*='btn']",
        "[class*='btn'][class*='login']", ".header-login-btn", "a.login", "button.login",
    ]:
        try:
            page.click(sel, timeout=3000)
            page.wait_for_timeout(1500)
            print(f"[vbet] auto-login: bouton login cliqué ({sel})", flush=True)
            break
        except Exception:
            continue

    filled_email = False
    for sel in [
        "input[name='email']", "input[name='username']", "input[name='login']",
        "input[type='email']", "input[placeholder*='mail' i]",
        "input[placeholder*='identifiant' i]", "input[placeholder*='login' i]",
        "input[autocomplete='username']", "input[autocomplete='email']",
    ]:
        try:
            page.fill(sel, username, timeout=4000)
            filled_email = True
            print(f"[vbet] auto-login: email rempli ({sel})", flush=True)
            break
        except Exception:
            continue

    if not filled_email:
        print("[vbet] auto-login: champ email introuvable — abandon", flush=True)
        return False

    filled_pw = False
    for sel in [
        "input[name='password']", "input[type='password']", "input[name='pass']",
        "input[placeholder*='assword' i]", "input[placeholder*='ot de passe' i]",
        "input[autocomplete='current-password']",
    ]:
        try:
            page.fill(sel, password, timeout=4000)
            filled_pw = True
            print(f"[vbet] auto-login: mot de passe rempli ({sel})", flush=True)
            break
        except Exception:
            continue

    if not filled_pw:
        print("[vbet] auto-login: champ password introuvable — abandon", flush=True)
        return False

    submitted = False
    for sel in [
        "button[type='submit']", "input[type='submit']",
        "button:has-text('Connexion')", "button:has-text('Se connecter')",
        "button:has-text('Login')", "button:has-text('Valider')",
        "[data-test='submit']", ".submit-btn", ".login-submit",
    ]:
        try:
            page.click(sel, timeout=4000)
            submitted = True
            print(f"[vbet] auto-login: soumis ({sel})", flush=True)
            break
        except Exception:
            continue

    if not submitted:
        for sel in ["input[type='password']", "input[name='password']"]:
            try:
                page.press(sel, "Enter", timeout=3000)
                submitted = True
                break
            except Exception:
                continue

    if not submitted:
        print("[vbet] auto-login: bouton submit introuvable — abandon", flush=True)
        return False

    page.wait_for_timeout(6000)
    print("[vbet] auto-login: formulaire soumis, attente session…", flush=True)
    return True


def _update_railway_storage_state(ctx, site_id: str = "") -> None:
    """Après capture : met à jour VBET_STORAGE_STATE_B64 sur Railway via API GraphQL."""
    import base64
    import urllib.request

    token      = (os.environ.get("RAILWAY_TOKEN") or "").strip()
    project_id = (os.environ.get("RAILWAY_PROJECT_ID") or "").strip()
    env_id     = (os.environ.get("RAILWAY_ENVIRONMENT_ID") or "").strip()
    service_id = (os.environ.get("RAILWAY_SERVICE_ID") or "").strip()

    if not all([token, project_id, env_id, service_id]):
        return

    try:
        state = ctx.storage_state()
        b64   = base64.b64encode(
            json.dumps(state, ensure_ascii=False).encode()
        ).decode()
        payload = json.dumps({
            "query": "mutation variableUpsert($input: VariableUpsertInput!) { variableUpsert(input: $input) }",
            "variables": {
                "input": {
                    "projectId":     project_id,
                    "environmentId": env_id,
                    "serviceId":     service_id,
                    "name":          "VBET_STORAGE_STATE_B64",
                    "value":         b64,
                }
            },
        }).encode()
        req = urllib.request.Request(
            "https://backboard.railway.app/graphql/v2",
            data=payload,
            headers={
                "Content-Type":  "application/json",
                "Authorization": f"Bearer {token}",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode())
        if result.get("errors"):
            print(f"[vbet] Railway API: {result['errors']}", file=sys.stderr)
        else:
            n = len(state.get("cookies") or [])
            print(f"[vbet] VBET_STORAGE_STATE_B64 mis à jour sur Railway ({n} cookies)", flush=True)
    except Exception as e:
        print(f"[vbet] Railway API update storage state: {e}", file=sys.stderr)


def _restore_storage_state(ctx) -> None:
    """Injecte les cookies depuis VBET_STORAGE_STATE_B64."""
    import base64
    b64 = (os.environ.get("VBET_STORAGE_STATE_B64") or "").strip()
    if not b64:
        return
    try:
        state   = json.loads(base64.b64decode(b64).decode("utf-8"))
        cookies = state.get("cookies") or []
        if cookies:
            ctx.add_cookies(cookies)
            print(f"[vbet] {len(cookies)} cookies restaurés (VBET_STORAGE_STATE_B64)", flush=True)
    except Exception as e:
        print(f"[vbet] VBET_STORAGE_STATE_B64 invalide : {e}", file=sys.stderr)


def _wait_for_swarm_cookie(ctx, page, timeout_s=None):
    """Poll jusqu'à ce que _immortal|user-hashX soit présent."""
    from swarm_client import parse_afec

    t = float(
        timeout_s if timeout_s is not None
        else os.environ.get("VBET_CAPTURE_SWARM_WAIT_S", "180")
    )
    deadline = time.time() + t
    while time.time() < deadline:
        ch = _cookie_header_from_playwright_context(ctx)
        if ch and parse_afec(ch):
            print("[vbet] Cookie Swarm (_immortal|user-hashX) détecté.", flush=True)
            return True
        try:
            page.mouse.wheel(0, 400)
        except Exception:
            pass
        page.wait_for_timeout(3000)
    print("[vbet] Fin d'attente Swarm sans user-hashX — poursuite avec les cookies actuels.", flush=True)
    return False


# ── Odds helpers (identiques daznbet) ───────────────────────────────────────

def _is_odd(x):
    if isinstance(x, bool) or x is None:
        return False
    try:
        v = float(x)
    except (TypeError, ValueError):
        return False
    return 1.01 <= v <= 500.0


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


def _norm_sport(n):
    return (n or "").strip().lower()


def _pdf_football(mt: str, mname: str) -> bool:
    t = (mt or "").strip()
    if _FB_DENY_TYPE.search(t) or _FB_DENY_PREFIX.search(t):
        return False
    if "First10Minutes" in t or "1st Goal Time" in t:
        return False
    return True


def _pdf_tennis(mt: str, mname: str) -> bool:
    t = (mt or "").strip()
    if _TN_DENY.search(t):
        return False
    return bool(re.search(
        r"(?i)(P1XP2|Handicap|Asian|OverUnder|Over\/Under|Total|Set|Game|Winner|Match|1st|First|tennis)",
        t + " " + (mname or ""),
    ))


def _pdf_basketball(mt: str, mname: str) -> bool:
    blob = "{} {}".format(mt or "", mname or "")
    if _BK_DENY.search(blob):
        return False
    return bool(re.search(
        r"(?i)(Quarter|Half|Q1|Q2|Q3|Q4|period|HalfTime|FirstHalf|SecondHalf|"
        r"OverUnder|Over\/Under|Handicap|Asian|P1XP2|spread|total|point|points|"
        r"PlayerProp|player|assist|rebound|pass|PRA|stat|prop|double|triple|match|winner)",
        blob,
    ))


def _pdf_hockey(mt: str, mname: str) -> bool:
    t = (mt or "").strip()
    if _HK_DENY.search(t):
        return False
    return bool(re.search(
        r"(?i)(period|P1|P2|P3|first.?period|second|third|regular|extra|overtime|RT|ET|"
        r"OverUnder|Over\/Under|Handicap|Asian|P1XP2|total|goal)",
        t + " " + (mname or ""),
    ))


def _spec_pdf_allow(sn, mt, mname, game: dict) -> bool:
    if game.get("is_started") == 1:
        return False
    if game.get("is_blocked") == 1:
        return False
    blob = "{} {}".format(mt or "", mname or "")
    if _PLAYER_PROP_RE.search(blob):
        return ("nba" in sn or "basket" in sn) or ("hockey" in sn or "glace" in sn or "nhl" in sn)
    if "football" in sn or sn in ("soccer", "foot"):
        return _pdf_football(mt, mname)
    if "tennis" in sn:
        return _pdf_tennis(mt, mname)
    if "basket" in sn or "nba" in sn:
        return _pdf_basketball(mt, mname)
    if "hockey" in sn or "glace" in sn or "nhl" in sn:
        return _pdf_hockey(mt, mname)
    return False


def _merge_games_from_payload(payload: dict):
    merged = {}
    for cap in (payload.get("captures") or []) + (payload.get("ws_captures") or []):
        if not isinstance(cap, dict) or cap.get("data") is None:
            continue
        root = cap["data"]
        for block in _walk_sports(root):
            for ctx in _iter_games(block):
                g   = ctx["game"]
                gid = g.get("id")
                if gid is None:
                    continue
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
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _line_cut(ev: dict, m: dict):
    if isinstance(ev, dict):
        v = _float_or_none(ev.get("base"))
        if v is not None:
            return v
    if isinstance(m, dict):
        v = _float_or_none(m.get("base"))
        if v is not None:
            return v
    return None


def _extra_info_blob(ev: dict, m: dict) -> str:
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
    if not mt:
        return ""
    s = re.sub(r"([a-z])([A-Z0-9])", r"\1 \2", mt)
    s = re.sub(r"(\d)([A-Za-z])", r"\1 \2", s)
    return re.sub(r"\s+", " ", s).strip()


def _selection_display(r: dict) -> str:
    sel = (r.get("selection") or "").strip()
    et  = (r.get("event_type") or "").strip()
    return sel if sel else et


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
    if not isinstance(sport_node, dict):
        return
    sid   = sport_node.get("id")
    sname = str(sport_node.get("name") or sport_node.get("alias") or "").strip()
    sn    = _norm_sport(sname)
    for reg in (sport_node.get("region") or {}).values():
        if not isinstance(reg, dict):
            continue
        rn = (reg.get("name") or "").strip()
        for comp in (reg.get("competition") or {}).values():
            if not isinstance(comp, dict):
                continue
            cn = (comp.get("name") or "").strip()
            for game in (comp.get("game") or {}).values():
                if isinstance(game, dict) and "team1_name" in game:
                    yield {
                        "sport_id": sid, "sport_name": sname, "sport_norm": sn,
                        "region_name": rn, "competition_name": cn, "game": game,
                    }


def _swarm_rows(payload: dict, spec_only: bool):
    rows = []
    for gid, item in _merge_games_from_payload(payload).items():
        ctx = item["ctx"]
        g   = item["game"]
        t1  = (g.get("team1_name") or "").strip()
        t2  = (g.get("team2_name") or "").strip()
        ts  = g.get("start_ts")
        for m in (g.get("market") or {}).values():
            if not isinstance(m, dict):
                continue
            mt    = (m.get("type")  or "").strip()
            mname = (m.get("name")  or "").strip()
            if spec_only and not _spec_pdf_allow(ctx["sport_norm"], mt, mname, g):
                continue
            for ev in (m.get("event") or {}).values():
                if not isinstance(ev, dict):
                    continue
                pr = _ev_price(ev)
                if pr is None:
                    continue
                line = _line_cut(ev, m)
                ei   = _extra_info_blob(ev, m)
                et   = (ev.get("type_1") or "").strip()
                rows.append({
                    "sport_id":      ctx["sport_id"],
                    "sport":         ctx["sport_name"],
                    "region":        ctx["region_name"],
                    "competition":   ctx["competition_name"],
                    "game_id":       gid,
                    "team1":         t1,
                    "team2":         t2,
                    "team1_id":      g.get("team1_id"),
                    "team2_id":      g.get("team2_id"),
                    "start_ts":      ts,
                    "sportcast_id":  g.get("sportcast_id"),
                    "game_number":   g.get("game_number"),
                    "markets_count": g.get("markets_count"),
                    "is_blocked":    g.get("is_blocked"),
                    "market_id":     m.get("id"),
                    "market_name":   mname,
                    "group_name":    (m.get("group_name") or "").strip(),
                    "market_type":   mt,
                    "line":          line,
                    "extra_info":    ei,
                    "event_type":    et,
                    "selection_id":  ev.get("id"),
                    "selection":     (ev.get("name") or "").strip(),
                    "decimal":       pr,
                })
    uniq = {}
    for r in rows:
        k = (r.get("game_id"), r.get("market_id"), r.get("selection_id"))
        uniq[k] = r
    return list(uniq.values())


def extract_flat(payload: dict, spec_only: bool = False):
    rows = _swarm_rows(payload, spec_only=spec_only)
    out  = []
    for i, r in enumerate(rows):
        t1 = (r.get("team1") or "").strip()
        t2 = (r.get("team2") or "").strip()
        row = {
            "decimal":       r["decimal"],
            "price_key":     "price",
            "label":         _selection_display(r),
            "sport":         r.get("sport"),
            "region":        r.get("region"),
            "competition":   r.get("competition"),
            "match":         f"{t1} - {t2}" if t1 or t2 else None,
            "team1":         t1,
            "team2":         t2,
            "team1_id":      r.get("team1_id"),
            "team2_id":      r.get("team2_id"),
            "game_id":       r.get("game_id"),
            "start_ts":      r.get("start_ts"),
            "sportcast_id":  r.get("sportcast_id"),
            "game_number":   r.get("game_number"),
            "markets_count": r.get("markets_count"),
            "is_blocked":    r.get("is_blocked"),
            "market":        _market_line_label(r),
            "period":        _period_for_row(r),
            "market_name":   r.get("market_name"),
            "group_name":    r.get("group_name"),
            "extra_info":    r.get("extra_info") or "",
            "market_type":   r.get("market_type"),
            "market_id":     r.get("market_id"),
            "selection":     r.get("selection"),
            "selection_id":  r.get("selection_id"),
            "event_id":      r.get("selection_id"),
            "event_type":    r.get("event_type") or "",
            "source_url":    "swarm",
            "capture_kind":  "websocket",
            "capture_index": i,
        }
        ln = r.get("line")
        if ln is not None:
            row["line"] = ln
        out.append(row)
    return out


def _slug_period(mt):
    t = (mt or "").lower()
    if any(x in t for x in ("handicap", "asian", "spread", "margin", "winwith")):
        slug = "spread"
    elif any(x in t for x in ("overunder", "over/", "total", "both", "btts", "score", "goal", "multi goal", "exact")):
        slug = "total"
    else:
        slug = "moneyline"
    m = mt or ""
    if   re.search(r"(?i)first.?quarter|q1\b",    m): per = "Q1"
    elif re.search(r"(?i)second.?quarter|q2\b",   m): per = "Q2"
    elif re.search(r"(?i)third.?quarter|q3\b",    m): per = "Q3"
    elif re.search(r"(?i)fourth.?quarter|q4\b",   m): per = "Q4"
    elif re.search(r"(?i)first.?set|^1st|set.?1", m): per = "S1"
    elif re.search(r"(?i)second.?set|set.?2",     m): per = "S2"
    elif re.search(r"(?i)half.?time|firsthalf|1sthalf|halftime(?!full)", m) and "second" not in m.lower(): per = "HT"
    elif re.search(r"(?i)second.?half|2ndhalf",   m) or m.startswith("SecondHalf"): per = "FT"
    elif m == "HalfTimeResult" or m.startswith("HalfTime") or m.startswith("FirstHalf"): per = "HT"
    else: per = "FT"
    return slug, per


def _market_line_label(r: dict) -> str:
    gn = (r.get("group_name")  or "").strip()
    mn = (r.get("market_name") or "").strip()
    ei = (r.get("extra_info")  or "").strip()
    mt = (r.get("market_type") or "").strip()
    parts = [p for p in (gn, mn) if p]
    if ei:
        parts.append(ei)
    if parts:
        return " — ".join(parts)
    if mt:
        return _pretty_market_type(mt)
    return ""


def _period_for_row(r: dict) -> str:
    blob = " ".join(filter(None, (
        r.get("market_type"), r.get("market_name"),
        r.get("group_name"), r.get("extra_info"),
    )))
    _, per = _slug_period(blob)
    return per


def build_downloads(rows):
    sports = {}
    for r in rows:
        sp = (r.get("sport") or "Unknown").strip()
        sports.setdefault(sp, {"total_rows": 0, "competitions": {}})
        region = (r.get("region") or "").strip()
        comp   = (r.get("competition") or "").strip()
        ck     = f"{region} - {comp}" if region and comp else (comp or region or "Autres")
        sports[sp]["competitions"].setdefault(ck, {})
        gid  = r.get("game_id")
        gk   = str(gid) if gid is not None else f'{r.get("team1")}|{r.get("team2")}|{r.get("start_ts")}'
        cmap = sports[sp]["competitions"][ck]
        if gk not in cmap:
            ts  = r.get("start_ts")
            if isinstance(ts, (int, float)) and ts > 0:
                d = datetime.fromtimestamp(int(ts), tz=timezone.utc).replace(microsecond=0).isoformat()
            else:
                d = ""
            t1s = (r.get("team1") or "").strip()
            t2s = (r.get("team2") or "").strip()
            cmap[gk] = {
                "match":   f"{t1s} - {t2s}" if t1s or t2s else "",
                "date":    d,
                "markets": [],
                "props":   [],
            }
        entry = {
            "market":    _market_line_label(r),
            "period":    _period_for_row(r),
            "selection": _selection_display(r),
            "odds":      float(r["decimal"]),
        }
        ln = r.get("line")
        if ln is not None:
            entry["line"] = ln
        et = (r.get("event_type") or "").strip()
        if et:
            entry["side"] = et
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
    gen = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return {"generated_at": gen, "sports": out}


# ── URL helpers ──────────────────────────────────────────────────────────────

def _odds_url_marker(u: str) -> bool:
    u = u.lower()
    return any(x in u for x in ("vbet", "swarm"))


def url_maybe_odds(u: str) -> bool:
    return _odds_url_marker(u)


def ws_relevant(u: str) -> bool:
    u = u.lower()
    return _odds_url_marker(u) or "sport" in u or "bet" in u


def _sports_root_url(landing: str) -> str:
    u = (landing or "").strip().rstrip("/")
    if u.endswith("/sports"):
        return u
    i = u.find("/sports")
    if i != -1:
        return u[: i + len("/sports")].rstrip("/")
    return u


def _capture_expand_enabled() -> bool:
    v = os.environ.get("VBET_CAPTURE_EXPAND", "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def _capture_extra_urls(landing: str) -> list[str]:
    if not _capture_expand_enabled():
        return []
    raw = os.environ.get("VBET_CAPTURE_EXPAND_PATHS", "").strip()
    if raw == "-":
        return []
    if raw:
        paths = [p.strip() for p in raw.split("|") if p.strip()]
    else:
        paths = ["football", "tennis", "basketball", "hockey"]
    root = _sports_root_url(landing)
    return [f"{root}/{p}" for p in paths]


# ── Playwright capture ───────────────────────────────────────────────────────

def run_capture(cache_file, landing=LANDING, headless=True, manual=False, wait_ms=None):
    from playwright.sync_api import sync_playwright

    os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", str(BASE / "ms-playwright"))

    wait_ms    = wait_ms or int(os.environ.get("VBET_CAPTURE_WAIT_MS", "20000"))
    subpage_ms = int(os.environ.get("VBET_CAPTURE_SUBPAGE_MS", "12000"))
    caps, seen_u, wss, seen_w = [], set(), [], set()
    detected = {"site_id": ""}

    def on_resp(resp):
        try:
            ct = (resp.headers.get("content-type") or "").lower()
            if "json" not in ct:
                return
            url = resp.url
            if not url_maybe_odds(url) or url in seen_u:
                return
            text = resp.text()
            if len(text) < 30:
                return
            try:
                data = json.loads(text)
            except Exception:
                return
            if not has_betting_odds_in_json(data):
                return
            seen_u.add(url)
            caps.append({"url": url, "status": resp.status, "kind": "http", "data": data})
        except Exception:
            pass

    def on_ws_frame(ws_url, payload):
        try:
            text = bytes(payload).decode("utf-8", errors="ignore") if isinstance(payload, (bytes, bytearray)) else str(payload)
            if len(text) < 2 or text[0] not in "[{":
                return
            data = json.loads(text)
        except Exception:
            return
        # Auto-detect site_id from request_session sent by browser
        if isinstance(data, dict) and data.get("command") == "request_session":
            sid = str((data.get("params") or {}).get("site_id", ""))
            if sid and not detected["site_id"]:
                detected["site_id"] = sid
                print(f"[vbet] site_id détecté : {sid}", flush=True)
        keep = has_betting_odds_in_json(data)
        if not keep and isinstance(data, dict):
            if data.get("rid") or data.get("command") or data.get("code") is not None:
                keep = True
        if not keep:
            return
        fp = hash((ws_url, text[:4000]))
        if fp in seen_w:
            return
        seen_w.add(fp)
        wss.append({"url": ws_url, "kind": "websocket", "data": data})

    def on_ws(ws):
        if not ws_relevant(ws.url):
            return
        def h(p): on_ws_frame(ws.url, p)
        try:
            ws.on("framereceived", h)
            ws.on("framesent",     h)
        except Exception:
            pass

    stealth = "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
    ua = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    started = time.time()
    with sync_playwright() as p:
        PROFILE.mkdir(parents=True, exist_ok=True)
        common = {
            "user_data_dir": str(PROFILE),
            "headless":      headless,
            "viewport":      {"width": 1365, "height": 900},
            "locale":        "fr-FR",
            "timezone_id":   "Europe/Paris",
            "user_agent":    ua,
            "extra_http_headers": {"Accept-Language": "fr-FR,fr;q=0.9"},
            "args": ["--disable-blink-features=AutomationControlled", "--disable-dev-shm-usage"],
        }
        ctx = None
        for channel in ("chrome", None):
            try:
                kw = dict(common)
                if channel:
                    kw["channel"] = channel
                ctx = p.chromium.launch_persistent_context(**kw)
                ctx.add_init_script(stealth)
                _restore_storage_state(ctx)
                break
            except Exception:
                continue
        if ctx is None:
            raise RuntimeError("Chromium indisponible")
        ctx.on("websocket", on_ws)
        ctx.on("response",  on_resp)
        page    = ctx.new_page()
        extra   = _capture_extra_urls(landing)
        visited = [landing]
        for u in extra:
            if u not in visited and u.rstrip("/") != landing.rstrip("/"):
                visited.append(u)
        for idx, url in enumerate(visited):
            try:
                print(f"[vbet] capture: → {url}", flush=True)
                page.goto(url, wait_until="domcontentloaded", timeout=120000)
            except Exception as e:
                if idx == 0:
                    raise RuntimeError(f"Landing injoignable : {url}") from e
                print(f"[vbet] capture: ignoré {url!r} ({e})", flush=True)
                continue
            for _ in range(8):
                page.mouse.wheel(0, 1800)
                page.wait_for_timeout(1000)
            w = wait_ms if idx == 0 else subpage_ms
            page.wait_for_timeout(w)
            if idx == 0:
                _auto_login(page)
        if manual and not headless:
            print(">>> Entrée pour continuer…", flush=True)
            try:
                input()
            except EOFError:
                pass
            page.wait_for_timeout(8000)
        _wait_for_swarm_cookie(ctx, page, None)
        ch      = _cookie_header_from_playwright_context(ctx)
        site_id = detected["site_id"] or _resolve_site_id()
        if ch:
            from swarm_client import parse_afec
            if not parse_afec(ch):
                print(
                    "[vbet] Cookies enregistrés mais sans _immortal|user-hashX (Swarm). "
                    "Capture headless insuffisante si 0 WS — utilisez --headed --manual en local.",
                    flush=True,
                )
            save_session(ch, site_id)
        _update_railway_storage_state(ctx, site_id)
        ctx.close()
    elapsed = time.time() - started
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    payload = {
        "generated_at": now,
        "source":       "vbet",
        "mode":         "playwright_network_capture",
        "landing":      landing,
        "visited_urls": visited,
        "stats": {
            "elapsed_seconds": round(elapsed, 2),
            "json_captures":   len(caps),
            "ws_captures":     len(wss),
            "pages_visited":   len(visited),
        },
        "captures":    caps,
        "ws_captures": wss,
    }
    _write_json(cache_file, payload)
    print(f"[vbet] {len(caps)} HTTP + {len(wss)} WS → {cache_file} ({elapsed:.1f}s)", flush=True)
    if len(wss) == 0:
        print(
            "[vbet] 0 WS : souvent headless/bot bloqué. Essayez : "
            "./run_all.sh --headed --manual  |  ou  python vbet.py fast",
            flush=True,
        )
    return payload


def _headless():
    v = (os.environ.get("VBET_PLAYWRIGHT_HEADLESS") or os.environ.get("PLAYWRIGHT_HEADLESS") or "").strip().lower()
    return v not in ("0", "false", "no", "off")


# ── API hints ────────────────────────────────────────────────────────────────

def _rid_prefix(rid: str) -> str:
    rid = rid or ""
    m   = re.match(r"^(\D+)", rid)
    return (m.group(1).rstrip("_") if m else rid)[:64]


def swarm_api_hints(payload: dict) -> dict:
    ws_urls, http_urls = [], []
    rid_seen, rid_samples = set(), []
    for w in payload.get("ws_captures") or []:
        u = w.get("url")
        if u and u not in ws_urls:
            ws_urls.append(u)
        data = w.get("data")
        if isinstance(data, dict):
            rid = data.get("rid")
            if rid and rid not in rid_seen:
                rid_seen.add(rid)
                rid_samples.append(rid)
    for c in payload.get("captures") or []:
        u = c.get("url")
        if u and u not in http_urls:
            http_urls.append(u)
    kinds = {}
    for r in rid_samples:
        p = _rid_prefix(r)
        kinds[p] = kinds.get(p, 0) + 1
    sess           = session_path()
    cookie_preview = ""
    if sess.exists():
        try:
            j  = json.loads(sess.read_text(encoding="utf-8"))
            ck = j.get("cookie") or ""
            cookie_preview = ck[:160] + ("…" if len(ck) > 160 else "")
        except (json.JSONDecodeError, OSError):
            pass
    landing = payload.get("landing") or LANDING
    return {
        "generated_at":         datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "source":               "vbet",
        "cache_generated_at":   payload.get("generated_at"),
        "landing":              landing,
        "swarm_websocket_urls": ws_urls,
        "http_json_urls":       http_urls,
        "rid_message_samples":  rid_samples[:80],
        "rid_prefix_counts":    dict(sorted(kinds.items(), key=lambda x: -x[1])),
        "session_file":         str(sess.resolve()),
        "session_cookie_preview": cookie_preview,
        "how_to_connect": {
            "protocol":      "WebSocket JSON (champ rid côté Swarm / Vbet)",
            "origin":        "https://www.vbet.fr",
            "referer":       landing,
            "cookie_header": "Copier depuis session_file (clé cookie) après capture connectée",
        },
    }


# ── Commands ─────────────────────────────────────────────────────────────────

def cmd_fetch(a):
    from swarm_client import run_swarm_fetch

    sess    = Path(getattr(a, "session", "") or session_path())
    out     = Path(a.out)
    sports  = list(getattr(a, "sport", None) or [])
    site_id = _resolve_site_id(getattr(a, "site_id", "") or "")
    run_swarm_fetch(
        session_file=sess,
        out_path=out,
        ws_url=getattr(a, "ws_url", "") or os.environ.get("VBET_SWARM_WS", "wss://swarm-2.vbet.fr/"),
        site_id=site_id,
        mode=getattr(a, "fetch_mode", "prematch"),
        sports_filter=sports or None,
        max_competitions=int(getattr(a, "max_competitions", 0)),
        delay_s=float(getattr(a, "delay", 0.0)),
    )


def cmd_api(a):
    out  = swarm_api_hints(_read_cache(a.cache))
    _write_json(a.out, out)
    nws  = len(out["swarm_websocket_urls"])
    nhttp= len(out["http_json_urls"])
    nr   = len(out["rid_message_samples"])
    print(f"[vbet] accès API (indices) : {nws} WS, {nhttp} HTTP, {nr} rid → {a.out}", flush=True)
    for u in out["swarm_websocket_urls"][:5]:
        print(f"  WS  {u}", flush=True)


def cmd_capture(a):
    landing = a.landing or LANDING
    if a.headed:
        os.environ["VBET_PLAYWRIGHT_HEADLESS"] = "0"
    try:
        run_capture(a.cache, landing=landing, headless=_headless(), manual=a.manual)
    except Exception as e:
        print(f"Erreur: {e}", file=sys.stderr)
        if _headless() and os.environ.get("VBET_PLAYWRIGHT_AUTO_HEADED", "").strip().lower() in ("1", "true", "yes") and not a.manual:
            os.environ["VBET_PLAYWRIGHT_HEADLESS"] = "0"
            run_capture(a.cache, landing=landing, headless=False, manual=True)
        else:
            sys.exit(1)


def cmd_export_storage(a):
    """Exporte les cookies de session Playwright → VBET_STORAGE_STATE_B64 pour Railway."""
    import base64
    from playwright.sync_api import sync_playwright

    os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", str(BASE / "ms-playwright"))
    out = Path(getattr(a, "out", str(DATA / "vbet_storage_state.json")))
    ua  = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    with sync_playwright() as p:
        ctx   = p.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE),
            headless=True,
            user_agent=ua,
            args=["--disable-blink-features=AutomationControlled"],
        )
        state = ctx.storage_state()
        ctx.close()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    b64 = base64.b64encode(out.read_bytes()).decode()
    n   = len(state.get("cookies") or [])
    print(f"[vbet] {n} cookies exportés → {out}", flush=True)
    print("\n=== Copiez cette variable dans Railway → Settings → Variables ===")
    print(f"VBET_STORAGE_STATE_B64={b64}")


def cmd_flat(a):
    raw = _read_cache(a.cache)
    pdf = not getattr(a, "all_markets", False)
    out = build_downloads(_swarm_rows(raw, spec_only=pdf))
    _write_json(a.out, out)
    n   = sum(s["total_rows"] for s in out["sports"].values())
    tag = "PDF §3.1" if pdf else "tous marchés"
    print(f"[vbet] {n} cotes ({tag}) → {a.out}", flush=True)


def cmd_lines(a):
    raw  = _read_cache(a.cache)
    pdf  = not getattr(a, "all_markets", False)
    odds = extract_flat(raw, spec_only=pdf)
    out  = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "source":       "vbet",
        "cache_file":   str(Path(a.cache).resolve()),
        "total_odds":   len(odds),
        "odds":         odds,
    }
    _write_json(a.out, out)
    print(f"[vbet] {len(odds)} lignes détaillées → {a.out}", flush=True)


def cmd_spec(a):
    raw   = _read_cache(a.cache)
    rows  = _swarm_rows(raw, spec_only=True)
    by_sp = {}
    for r in rows:
        by_sp[r.get("sport") or "?"] = by_sp.get(r.get("sport") or "?", 0) + 1
    out = {
        "generated_at":      datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source":            "vbet",
        "cache_file":        str(Path(a.cache).resolve()),
        "total_selections":  len(rows),
        "selections_by_sport": by_sp,
        "selections":        rows,
    }
    _write_json(a.out, out)
    print(f"[vbet] spec {len(rows)} sélections → {a.out}", flush=True)


def cmd_downloads(a):
    raw  = _read_cache(a.cache)
    pdf  = not getattr(a, "all_markets", False)
    rows = _swarm_rows(raw, spec_only=pdf)
    out  = build_downloads(rows)
    _write_json(a.out, out)
    n   = sum(s["total_rows"] for s in out["sports"].values())
    tag = "PDF §3.1" if pdf else "tous marchés"
    print(f"[vbet] downloads ({tag}) {n} lignes → {a.out}", flush=True)


def _emit_derived_outputs(a, *, api_unless_disabled: bool) -> None:
    c  = str(a.cache)
    am = getattr(a, "all_markets", False)
    cmd_flat(argparse.Namespace(cache=c, out=a.out_flat, all_markets=am))
    if not getattr(a, "no_spec", False):
        cmd_spec(argparse.Namespace(cache=c, out=a.out_spec))
    if not getattr(a, "no_downloads", False):
        cmd_downloads(argparse.Namespace(cache=c, out=a.out_downloads, all_markets=am))
    if api_unless_disabled:
        do_api = not getattr(a, "no_api_hints", False)
    else:
        do_api = getattr(a, "api_hints", False)
    if do_api:
        cmd_api(argparse.Namespace(cache=c, out=a.out_api_hints))


def cmd_run(a):
    cmd_capture(a)
    _emit_derived_outputs(a, api_unless_disabled=False)


def cmd_fast(a):
    from swarm_client import MissingSwarmCookieError, run_swarm_fetch

    t0      = time.time()
    sess    = Path(getattr(a, "session", "") or session_path())
    cache   = Path(a.cache)
    am      = getattr(a, "all_markets", False)
    sports  = list(getattr(a, "sport", None) or [])
    delay   = float(getattr(a, "fetch_delay", 0.0))
    fmode   = getattr(a, "fetch_mode", "full")
    site_id = _resolve_site_id(getattr(a, "site_id", "") or "")
    if not site_id:
        print(
            "[vbet] fast: VBET_SITE_ID non défini. "
            "Lancez d'abord : python vbet.py capture --headed --manual (auto-détection) "
            "ou définissez VBET_SITE_ID.",
            file=sys.stderr,
        )
        sys.exit(2)
    if fmode == "full":
        print("[vbet] Mode full : requêtes Swarm « GameList » par compétition.", flush=True)
    elif fmode == "prematch":
        print("[vbet] Mode prematch : arbres seulement — utilisez --mode full pour les cotes.", flush=True)
    try:
        run_swarm_fetch(
            session_file=sess,
            out_path=cache,
            ws_url=getattr(a, "ws_url", "") or os.environ.get("VBET_SWARM_WS", "wss://swarm-2.vbet.fr/"),
            site_id=site_id,
            mode=fmode,
            sports_filter=sports or None,
            max_competitions=int(getattr(a, "max_competitions", 0)),
            delay_s=delay,
        )
    except FileNotFoundError as e:
        print(f"[vbet] fast: session ou cache impossible — {e}", file=sys.stderr)
        sys.exit(2)
    except MissingSwarmCookieError as e:
        print(f"[vbet] fast: {e}", file=sys.stderr)
        sys.exit(2)
    _emit_derived_outputs(a, api_unless_disabled=True)
    print(f"[vbet] fast OK en {time.time() - t0:.1f}s", flush=True)


def cmd_cycle(a):
    """Probe session → si KO ou Swarm refuse : capture Playwright → fast (répété)."""
    from swarm_client import SwarmSessionRejected, probe_swarm_session

    sess          = Path(getattr(a, "session", "") or session_path())
    ws_url        = getattr(a, "ws_url", "") or os.environ.get("VBET_SWARM_WS", "wss://swarm-2.vbet.fr/")
    site_id       = _resolve_site_id(getattr(a, "site_id", "") or "")
    max_rounds    = int(getattr(a, "max_rounds", 5))
    skip_probe    = getattr(a, "skip_probe", False)
    no_capture    = getattr(a, "no_capture", False)
    sleep_s       = float(os.environ.get("VBET_CYCLE_SLEEP_S", str(getattr(a, "cycle_sleep_s", 8.0))))
    probe_timeout = getattr(a, "probe_timeout", None)
    if probe_timeout is None:
        probe_timeout = float(os.environ.get("VBET_SWARM_PROBE_TIMEOUT", "45"))

    def do_capture():
        print("[vbet] cycle: rafraîchissement cookies (Playwright)…", flush=True)
        landing = getattr(a, "landing", None) or LANDING
        headed  = getattr(a, "capture_headed", False)
        manual  = getattr(a, "capture_manual", False)
        if headed:
            os.environ["VBET_PLAYWRIGHT_HEADLESS"] = "0"
        run_capture(
            Path(a.cache),
            landing=landing,
            headless=(not headed) and _headless(),
            manual=manual,
        )

    for round_i in range(1, max_rounds + 1):
        print(f"[vbet] cycle: tour {round_i}/{max_rounds}", flush=True)
        if not skip_probe:
            ok, msg = probe_swarm_session(sess, ws_url=ws_url, site_id=site_id, timeout_s=probe_timeout)
            if ok:
                print(f"[vbet] probe: {msg}", flush=True)
            else:
                print(f"[vbet] probe: échec — {msg}", flush=True)
                if no_capture:
                    print("[vbet] cycle: --no-capture actif, abandon.", flush=True)
                    sys.exit(1)
                try:
                    do_capture()
                    # Reload site_id after capture (may have been auto-detected)
                    site_id = _resolve_site_id(getattr(a, "site_id", "") or "")
                except Exception as e:
                    print(f"[vbet] cycle: capture impossible: {e}", flush=True)
                else:
                    time.sleep(min(sleep_s, 3.0))

        try:
            cmd_fast(a)
            print("[vbet] cycle: terminé avec succès.", flush=True)
            return
        except SwarmSessionRejected as e:
            print(f"[vbet] cycle: session Swarm refusée — {e}", flush=True)
            if no_capture:
                sys.exit(1)
            try:
                do_capture()
                site_id = _resolve_site_id(getattr(a, "site_id", "") or "")
            except Exception as ex:
                print(f"[vbet] cycle: capture échouée: {ex}", flush=True)
            time.sleep(sleep_s)
        except (RuntimeError, Exception) as e:
            print(f"[vbet] cycle: erreur — {e}", flush=True)
            time.sleep(sleep_s)

    print("[vbet] cycle: échec après tous les tours.", flush=True)
    sys.exit(1)


# ── Serve ────────────────────────────────────────────────────────────────────

def cmd_serve(_a=None):
    import subprocess
    import threading
    import uvicorn
    from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
    from pydantic import BaseModel, ConfigDict, Field

    cf       = Path(os.getenv("VBET_ODDS_CACHE_FILE", str(CACHE)))
    out_flat = Path(os.getenv("VBET_OUT_FLAT", str(BASE / "output.json")))
    app      = FastAPI(title="Vbet")
    _session_refresh_lock    = threading.Lock()
    _session_refresh_running = False
    _fetch_lock    = threading.Lock()
    _fetch_running = False

    class SessionIngestBody(BaseModel):
        model_config = ConfigDict(extra="ignore")
        cookie:  str | None = Field(None, description="Chaîne Cookie HTTP complète")
        session: dict | None = Field(None, description="Objet {cookie, site_id?, updated_at?}")

    def _check_ingest_token(request: Request) -> None:
        tok = (os.getenv("VBET_INGEST_TOKEN") or "").strip()
        if not tok:
            raise HTTPException(503, "Définissez la variable secrète VBET_INGEST_TOKEN sur le serveur.")
        auth = (request.headers.get("Authorization") or request.headers.get("authorization") or "").strip()
        if not auth.startswith("Bearer "):
            raise HTTPException(401, "Header Authorization: Bearer <token> requis")
        got = auth[7:].strip()
        if not secrets.compare_digest(got, tok):
            raise HTTPException(403, "Token invalide")

    def load():
        f = out_flat if out_flat.exists() else None
        if f is None:
            raise HTTPException(503, "Cache absent")
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
        return {"status": "healthy" if age < 600 else "stale", "cache_exists": True, "cache_age_seconds": round(age, 1)}

    @app.get("/session/status")
    def session_status():
        sp = session_path()
        if not sp.exists():
            return {"has_file": False, "has_swarm_cookie": False, "session_refresh_running": _session_refresh_running}
        try:
            data    = json.loads(sp.read_text(encoding="utf-8"))
            cookie  = (data.get("cookie")  or "").strip()
            site_id = (data.get("site_id") or "").strip()
            from swarm_client import parse_afec
            return {
                "has_file":               True,
                "has_swarm_cookie":       bool(parse_afec(cookie)),
                "site_id":                site_id,
                "session_refresh_running": _session_refresh_running,
            }
        except Exception:
            return {"has_file": True, "has_swarm_cookie": False, "session_refresh_running": _session_refresh_running}

    @app.post("/session/refresh")
    def refresh_session(request: Request, background_tasks: BackgroundTasks, xvfb: bool = False):
        nonlocal _session_refresh_running
        _check_ingest_token(request)
        with _session_refresh_lock:
            if _session_refresh_running:
                raise HTTPException(409, "Une capture est déjà en cours sur le serveur")
            _session_refresh_running = True

        def job():
            nonlocal _session_refresh_running
            try:
                env = os.environ.copy()
                env.setdefault("PLAYWRIGHT_BROWSERS_PATH", str(BASE / "ms-playwright"))
                if xvfb:
                    env["VBET_PLAYWRIGHT_HEADLESS"] = "0"
                    cmd = [
                        "xvfb-run", "-a", "-s", "-screen 0 1920x1080x24",
                        sys.executable, str(BASE / "vbet.py"), "capture", "--headed",
                    ]
                else:
                    cmd = [sys.executable, str(BASE / "vbet.py"), "capture"]
                subprocess.run(
                    cmd, cwd=str(BASE), env=env,
                    timeout=int(os.environ.get("VBET_CAPTURE_REFRESH_TIMEOUT_S", "900")),
                )
            except subprocess.TimeoutExpired:
                print("[vbet serve] capture refresh: timeout", file=sys.stderr)
            except Exception as e:
                print(f"[vbet serve] capture refresh: {e}", file=sys.stderr)
            finally:
                with _session_refresh_lock:
                    _session_refresh_running = False

        background_tasks.add_task(job)
        return {"ok": True, "status": "started", "xvfb": xvfb}

    @app.post("/session")
    def ingest_session(request: Request, body: SessionIngestBody):
        """Enregistre la session Swarm (depuis votre navigateur) — requiert VBET_INGEST_TOKEN."""
        _check_ingest_token(request)
        raw     = ""
        site_id = ""
        if body.cookie and body.cookie.strip():
            raw = body.cookie.strip()
        elif body.session and isinstance(body.session, dict):
            raw     = (body.session.get("cookie") or "").strip()
            site_id = (body.session.get("site_id") or "").strip()
        if not raw:
            raise HTTPException(400, "JSON : fournissez « cookie » (string) ou « session » { cookie: ... }")
        save_session(raw, site_id or _resolve_site_id())
        from swarm_client import parse_afec
        ok = bool(parse_afec(raw))
        return {"ok": True, "has_swarm_cookie": ok, "path": str(session_path().resolve())}

    @app.post("/fetch")
    def trigger_fetch(request: Request, background_tasks: BackgroundTasks):
        nonlocal _fetch_running
        _check_ingest_token(request)
        with _fetch_lock:
            if _fetch_running:
                raise HTTPException(409, "Un fetch est déjà en cours")
            _fetch_running = True

        def _do_fetch():
            nonlocal _fetch_running
            try:
                from swarm_client import run_swarm_fetch
                run_swarm_fetch(
                    session_file=session_path(),
                    out_path=cf,
                    ws_url=os.environ.get("VBET_SWARM_WS", "wss://swarm-2.vbet.fr/"),
                    site_id=_resolve_site_id(),
                    mode=os.environ.get("VBET_FETCH_MODE", "full"),
                    sports_filter=None,
                    max_competitions=int(os.environ.get("VBET_MAX_COMPETITIONS", "0")),
                    delay_s=float(os.environ.get("VBET_FETCH_DELAY_S", "0")),
                )
            except Exception as e:
                print(f"[vbet serve] fetch: {e}", file=sys.stderr)
            finally:
                with _fetch_lock:
                    _fetch_running = False

        background_tasks.add_task(_do_fetch)
        return {"ok": True, "status": "started"}

    @app.get("/")
    def root():
        return {
            "endpoints": [
                "/odds", "/health", "/session/status",
                "POST /session (Bearer)",
                "POST /session/refresh ?xvfb=true (Bearer)",
                "POST /fetch (Bearer)",
            ],
        }

    has_creds = bool(
        (os.environ.get("VBET_STORAGE_STATE_B64") or "").strip()
        or (os.environ.get("VBET_SESSION_JSON_B64") or "").strip()
        or (os.environ.get("VBET_SESSION_JSON")    or "").strip()
        or (os.environ.get("VBET_COOKIE")          or "").strip()
        or (
            (os.environ.get("VBET_USERNAME") or "").strip()
            and (os.environ.get("VBET_PASSWORD") or "").strip()
        )
    )

    def _capture_with_xvfb() -> bool:
        env = os.environ.copy()
        env["VBET_PLAYWRIGHT_HEADLESS"] = "0"
        env.setdefault("VBET_CAPTURE_EXPAND",   "0")
        env.setdefault("VBET_CAPTURE_WAIT_MS",   "5000")
        env.setdefault("VBET_CAPTURE_SWARM_WAIT_S", "60")
        cmd = [
            "xvfb-run", "-a", "-s", "-screen 0 1920x1080x24",
            sys.executable, str(BASE / "vbet.py"), "capture", "--headed",
        ]
        try:
            r = subprocess.run(
                cmd, cwd=str(BASE), env=env,
                timeout=int(os.environ.get("VBET_CAPTURE_REFRESH_TIMEOUT_S", "900")),
            )
            return r.returncode == 0
        except Exception as e:
            print(f"[vbet serve] capture xvfb: {e}", file=sys.stderr)
            return False

    def _probe_ok() -> bool:
        from swarm_client import probe_swarm_session
        ok, msg = probe_swarm_session(session_path(), site_id=_resolve_site_id())
        if not ok:
            print(f"[vbet serve] probe: {msg}", file=sys.stderr)
        return ok

    if has_creds:
        print("[vbet serve] auto-session activé (VBET_STORAGE_STATE_B64 ou credentials détectés)", flush=True)

        def _auto_session_manager():
            time.sleep(4)
            if not _probe_ok():
                print("[vbet serve] démarrage : session invalide → capture (xvfb)…", flush=True)
                _capture_with_xvfb()
            probe_interval = float(os.environ.get("VBET_SESSION_PROBE_INTERVAL_S", "3600"))
            while True:
                time.sleep(probe_interval)
                if not _probe_ok():
                    print("[vbet serve] session expirée → re-capture (xvfb)…", flush=True)
                    _capture_with_xvfb()

        threading.Thread(target=_auto_session_manager, daemon=True).start()

    cooldown_s = float(os.environ.get("VBET_FETCH_COOLDOWN_S", "1"))
    print("[vbet serve] auto-fetch continu (redémarre dès que le cycle précédent est terminé)", flush=True)

    def _auto_fetch_loop():
        nonlocal _fetch_running
        while not session_path().exists():
            time.sleep(2)
        time.sleep(2)
        while True:
            with _fetch_lock:
                _fetch_running = True
            try:
                from swarm_client import MissingSwarmCookieError, SwarmSessionRejected, run_swarm_fetch
                site_id = _resolve_site_id()
                if not site_id:
                    print("[vbet serve] auto-fetch: VBET_SITE_ID manquant — attente 30s", file=sys.stderr)
                    time.sleep(30)
                    continue
                run_swarm_fetch(
                    session_file=session_path(),
                    out_path=cf,
                    ws_url=os.environ.get("VBET_SWARM_WS", "wss://swarm-2.vbet.fr/"),
                    site_id=site_id,
                    mode=os.environ.get("VBET_FETCH_MODE", "full"),
                    sports_filter=None,
                    max_competitions=int(os.environ.get("VBET_MAX_COMPETITIONS", "0")),
                    delay_s=float(os.environ.get("VBET_FETCH_DELAY_S", "0")),
                )
                raw       = _read_cache(cf)
                processed = build_downloads(_swarm_rows(raw, spec_only=False))
                _write_json(out_flat, processed)
            except (MissingSwarmCookieError, SwarmSessionRejected) as e:
                print(f"[vbet serve] auto-fetch: session KO — {e}", file=sys.stderr)
                if has_creds:
                    print("[vbet serve] auto-fetch: re-capture (xvfb)…", flush=True)
                    _capture_with_xvfb()
                time.sleep(10)
            except Exception as e:
                print(f"[vbet serve] auto-fetch: {e}", file=sys.stderr)
                time.sleep(5)
            finally:
                with _fetch_lock:
                    _fetch_running = False
            time.sleep(cooldown_s)

    threading.Thread(target=_auto_fetch_loop, daemon=True).start()

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("VBET_PORT", os.getenv("PORT", "8003"))))


# ── Argparse helpers ─────────────────────────────────────────────────────────

def _register_fast_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--session", default=str(session_path()))
    parser.add_argument("--cache",   default=str(CACHE))
    parser.add_argument("--ws-url",  default=os.environ.get("VBET_SWARM_WS", "wss://swarm-2.vbet.fr/"))
    parser.add_argument("--site-id", default=os.environ.get("VBET_SITE_ID", ""))
    parser.add_argument(
        "--mode", dest="fetch_mode", choices=("menu", "prematch", "full"), default="full",
        help="menu | prematch (peu de cotes) | full (GameList par compétition = cotes, défaut)",
    )
    parser.add_argument("--sport",    action="append", default=[], help="Limiter à certains alias (répétable)")
    parser.add_argument("--max-competitions", type=int, default=0, dest="max_competitions",
                        help="Mode full : plafond de compétitions (0=illimité)")
    parser.add_argument("--fetch-delay", type=float, default=0.0, dest="fetch_delay",
                        help="Pause entre requêtes Swarm (s)")
    parser.add_argument("--out-flat",      default=str(BASE / "output.json"),                    dest="out_flat")
    parser.add_argument("--out-spec",      default=str(DATA / "spec_markets.json"),              dest="out_spec")
    parser.add_argument("--out-downloads", default=str(DATA / "output_downloads_format.json"),   dest="out_downloads")
    parser.add_argument("--out-api-hints", default=str(DATA / "swarm_api_hints.json"),           dest="out_api_hints")
    parser.add_argument("--no-spec",       action="store_true", dest="no_spec")
    parser.add_argument("--no-downloads",  action="store_true", dest="no_downloads")
    parser.add_argument("--no-api-hints",  action="store_true", dest="no_api_hints")
    parser.add_argument("--all-markets",   action="store_true", help="sans filtre PDF §3.1")


def main():
    p   = argparse.ArgumentParser(prog="vbet.py")
    sub = p.add_subparsers(dest="cmd", required=True)

    # run
    c0 = sub.add_parser("run", help="capture + output.json + spec + downloads")
    c0.add_argument("--cache",         default=str(CACHE))
    c0.add_argument("--out-flat",      default=str(BASE / "output.json"),                  dest="out_flat")
    c0.add_argument("--out-spec",      default=str(DATA / "spec_markets.json"),            dest="out_spec")
    c0.add_argument("--out-downloads", default=str(DATA / "output_downloads_format.json"), dest="out_downloads")
    c0.add_argument("--no-spec",       action="store_true", dest="no_spec")
    c0.add_argument("--no-downloads",  action="store_true", dest="no_downloads")
    c0.add_argument("--headed",        action="store_true")
    c0.add_argument("--manual",        action="store_true")
    c0.add_argument("--landing",       default=None)
    c0.add_argument("--all-markets",   action="store_true", help="sans filtre PDF §3.1")
    c0.add_argument("--api-hints",     action="store_true", dest="api_hints")
    c0.add_argument("--out-api-hints", default=str(DATA / "swarm_api_hints.json"), dest="out_api_hints")
    c0.set_defaults(_fn=cmd_run)

    # fast
    cf = sub.add_parser("fast", help="session + Swarm WS (rapide, sans Playwright) → cache + outputs")
    _register_fast_args(cf)
    cf.set_defaults(_fn=cmd_fast)

    # cycle
    c_cycle = sub.add_parser("cycle", help="probe session → si refus capture cookies → fast")
    _register_fast_args(c_cycle)
    c_cycle.add_argument("--max-rounds",    type=int, default=int(os.environ.get("VBET_CYCLE_MAX_ROUNDS", "5")))
    c_cycle.add_argument("--skip-probe",    action="store_true")
    c_cycle.add_argument("--no-capture",    action="store_true")
    c_cycle.add_argument("--cycle-sleep",   type=float, default=float(os.environ.get("VBET_CYCLE_SLEEP_S", "8")), dest="cycle_sleep_s")
    c_cycle.add_argument("--probe-timeout", type=float, default=None)
    c_cycle.add_argument("--capture-headed", action="store_true")
    c_cycle.add_argument("--capture-manual", action="store_true")
    c_cycle.add_argument("--landing",        default=None)
    c_cycle.set_defaults(_fn=cmd_cycle)

    # capture
    c1 = sub.add_parser("capture", help="Playwright → cache JSON")
    c1.add_argument("--cache",   default=str(CACHE))
    c1.add_argument("--headed",  action="store_true")
    c1.add_argument("--manual",  action="store_true")
    c1.add_argument("--landing", default=None)
    c1.set_defaults(_fn=cmd_capture)

    # api
    c1b = sub.add_parser("api", help="cache → indices d'accès Swarm")
    c1b.add_argument("--cache", default=str(CACHE))
    c1b.add_argument("--out",   default=str(DATA / "swarm_api_hints.json"))
    c1b.set_defaults(_fn=cmd_api)

    # fetch
    c1c = sub.add_parser("fetch", help="session → WebSocket Swarm → cache JSON")
    c1c.add_argument("--session",  default=str(session_path()))
    c1c.add_argument("--out",      default=str(CACHE))
    c1c.add_argument("--ws-url",   default=os.environ.get("VBET_SWARM_WS", "wss://swarm-2.vbet.fr/"))
    c1c.add_argument("--site-id",  default=os.environ.get("VBET_SITE_ID", ""))
    c1c.add_argument("--mode",     dest="fetch_mode", choices=("menu", "prematch", "full"), default="prematch")
    c1c.add_argument("--sport",    action="append", default=[])
    c1c.add_argument("--max-competitions", type=int, default=0)
    c1c.add_argument("--delay",    type=float, default=0.0)
    c1c.set_defaults(_fn=cmd_fetch)

    # flat
    c2 = sub.add_parser("flat", help="cache → output.json")
    c2.add_argument("--cache",       default=str(CACHE))
    c2.add_argument("--out",         default=str(BASE / "output.json"))
    c2.add_argument("--all-markets", action="store_true")
    c2.set_defaults(_fn=cmd_flat)

    # lines
    c2b = sub.add_parser("lines", help="cache → liste plate détaillée (odds[])")
    c2b.add_argument("--cache",       default=str(CACHE))
    c2b.add_argument("--out",         default=str(DATA / "odds_lines.json"))
    c2b.add_argument("--all-markets", action="store_true")
    c2b.set_defaults(_fn=cmd_lines)

    # spec
    c3 = sub.add_parser("spec", help="cache → spec PDF")
    c3.add_argument("--cache", default=str(CACHE))
    c3.add_argument("--out",   default=str(DATA / "spec_markets.json"))
    c3.set_defaults(_fn=cmd_spec)

    # downloads
    c4 = sub.add_parser("downloads", help="cache → format sports/compétitions")
    c4.add_argument("--cache",       default=str(CACHE))
    c4.add_argument("--out",         default=str(DATA / "output_downloads_format.json"))
    c4.add_argument("--all-markets", action="store_true")
    c4.set_defaults(_fn=cmd_downloads)

    # serve
    c5 = sub.add_parser("serve", help="API /odds en continu (port 8003)")
    c5.set_defaults(_fn=lambda _: cmd_serve())

    # export-storage
    c_exp = sub.add_parser("export-storage", help="Exporte cookies → VBET_STORAGE_STATE_B64 pour Railway")
    c_exp.add_argument("--out", default=str(DATA / "vbet_storage_state.json"))
    c_exp.set_defaults(_fn=cmd_export_storage)

    if len(sys.argv) == 1:
        sys.argv.append("--help")
    args = p.parse_args()
    args._fn(args)


if __name__ == "__main__":
    main()
