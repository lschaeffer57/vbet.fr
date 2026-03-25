"""Client WebSocket Swarm (Vbet.fr) — JSON { command, params, rid } sur wss://."""
from __future__ import annotations

import json
import os
import random
import re
import time
from pathlib import Path
from typing import Any, Callable, Iterator

try:
    from websocket import WebSocketConnectionClosedException, create_connection
except ImportError as e:
    create_connection = None  # type: ignore
    WebSocketConnectionClosedException = Exception  # type: ignore
    _IMPORT_ERR = e
else:
    _IMPORT_ERR = None

DEFAULT_WS      = os.environ.get("VBET_SWARM_WS",  "wss://swarm-2.vbet.fr/")
DEFAULT_SITE_ID = os.environ.get("VBET_SITE_ID",   "")
ORIGIN          = os.environ.get("VBET_ORIGIN",    "https://www.vbet.fr")


class SwarmSessionRejected(RuntimeError):
    """Session Swarm refusée (cookies expirés ou afec invalide)."""


class MissingSwarmCookieError(ValueError):
    """Cookie sans _immortal|user-hashX — nécessaire pour le WebSocket Swarm."""


def probe_swarm_session(
    session_file: Path,
    *,
    ws_url: str | None = None,
    site_id: str | None = None,
    timeout_s: float | None = None,
) -> tuple[bool, str]:
    ws_url  = ws_url  or DEFAULT_WS
    site_id = site_id or DEFAULT_SITE_ID
    t = float(
        timeout_s if timeout_s is not None
        else os.environ.get("VBET_SWARM_PROBE_TIMEOUT", "45")
    )
    try:
        cookie = load_cookie_file(session_file)
    except FileNotFoundError as e:
        return False, str(e)
    except (ValueError, json.JSONDecodeError) as e:
        return False, str(e)
    afec = parse_afec(cookie)
    if not afec:
        return False, "afec manquant (_immortal|user-hashX=… dans le cookie)"
    if _IMPORT_ERR:
        return False, "websocket-client non installé (pip install websocket-client)"
    sw = None
    try:
        sw = SwarmWS(ws_url, cookie, timeout=int(max(t, 30.0)) + 60)
        sw.connect()
        rs = build_request_session(afec, site_id)
        sw.send(rs)
        r0 = sw.recv_until_rid(rs["rid"], deadline=time.time() + t)
        if r0.get("code") != 0:
            return False, f"request_session code={r0.get('code')!r} data={r0!r}"
        return True, "request_session OK"
    except TimeoutError as e:
        return False, f"timeout probe: {e}"
    except Exception as e:
        return False, str(e)
    finally:
        if sw is not None:
            try:
                sw.close()
            except Exception:
                pass


UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _rand_digits(n: int = 15) -> str:
    return "".join(str(random.randint(0, 9)) for _ in range(n))


def new_rid(prefix: str) -> str:
    return f"{prefix}{_rand_digits()}"


def parse_afec(cookie_header: str) -> str:
    if not cookie_header:
        return ""
    m = re.search(r"_immortal\|user-hashX=([^;]+)", cookie_header, re.I)
    if m:
        return m.group(1).strip()
    for part in cookie_header.split(";"):
        part = part.strip()
        if "user-hashX=" in part and "=" in part:
            return part.split("=", 1)[1].strip()
    return ""


def _raw_session_json_from_env() -> str | None:
    """
    Lit une session depuis l'environnement :
    - VBET_SESSION_JSON      : JSON brut
    - VBET_SESSION_JSON_B64  : même JSON en base64
    - VBET_COOKIE            : uniquement la chaîne Cookie
    """
    s = (os.environ.get("VBET_SESSION_JSON") or "").strip()
    if s:
        return s
    b64 = (os.environ.get("VBET_SESSION_JSON_B64") or "").strip()
    if b64:
        import base64
        try:
            return base64.b64decode(b64).decode("utf-8")
        except (ValueError, UnicodeDecodeError) as e:
            raise ValueError(f"VBET_SESSION_JSON_B64 invalide : {e}") from e
    c = (os.environ.get("VBET_COOKIE") or "").strip()
    if c:
        return json.dumps({"cookie": c, "updated_at": ""}, ensure_ascii=False)
    return None


def load_cookie_file(session_path: Path) -> str:
    """Charge la session depuis l'env (Railway) ou depuis le fichier."""
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
        return ck

    if not session_path.exists():
        raise FileNotFoundError(
            f"Session absente : {session_path}. "
            "Définissez VBET_SESSION_JSON, VBET_SESSION_JSON_B64, "
            "VBET_COOKIE, ou créez data/vbet_session.json via : "
            "python vbet.py capture --headed --manual"
        )
    data = json.loads(session_path.read_text(encoding="utf-8"))
    ck = (data.get("cookie") or "").strip()
    if not ck:
        raise ValueError("Clé « cookie » vide dans le fichier session")
    return ck


def build_request_session(afec: str, site_id: str, rid: str | None = None) -> dict[str, Any]:
    rid = rid or new_rid("request_session")
    return {
        "command": "request_session",
        "params": {
            "language": "fra",
            "site_id":  str(site_id),
            "source":   42,
            "is_wrap_app": False,
            "afec": afec,
        },
        "rid": rid,
    }


def build_sports_menu(rid: str | None = None) -> dict[str, Any]:
    rid = rid or new_rid("SportsbookSportsMenuprematchSubscribeCmd")
    return {
        "command": "get",
        "params": {
            "source": "betting",
            "what": {
                "sport": ["name", "alias", "id", "type", "order"],
                "game":  "@count",
            },
            "where": {
                "game":  {"type": {"@in": [0, 2]}},
                "sport": {"type": {"@in": [2]}},
            },
            "subscribe": True,
        },
        "rid": rid,
    }


def build_prematch_tree(sport_alias: str, rid: str | None = None) -> dict[str, Any]:
    rid = rid or new_rid(f"Prematch_{sport_alias}SubscribeCmd")
    return {
        "command": "get",
        "params": {
            "source": "betting",
            "what": {
                "region":      ["name", "alias", "order", "id", "competition"],
                "competition": ["name", "order", "id"],
            },
            "where": {
                "sport": {"alias": sport_alias, "type": {"@in": [2]}},
                "game": {
                    "@or": [
                        {"type": {"@in": [0, 2]}},
                        {"visible_in_prematch": 1},
                    ]
                },
            },
            "subscribe": True,
        },
        "rid": rid,
    }


def build_gamelist(
    sport_alias: str,
    region_alias: str,
    competition_id: int,
    rid: str | None = None,
) -> dict[str, Any]:
    rid = rid or new_rid("GameListSubscribeCmd")
    return {
        "command": "get",
        "params": {
            "source": "betting",
            "what": {
                "sport":  ["id", "name", "alias"],
                "region": ["id", "name", "alias", "order"],
                "competition": ["id", "name", "order"],
                "game": [
                    "id", "type", "team1_name", "team2_name",
                    "team1_id", "team2_id", "info", "order", "start_ts",
                    "markets_count", "exclude_ids", "team1_reg_name", "team2_reg_name",
                    "video_id", "video_id2", "stats", "score1", "score2",
                    "show_type", "text_info", "is_stat_available", "is_started",
                    "add_info_name", "tv_info", "sportcast_id", "match_length",
                    "live_events", "is_blocked", "game_number", "sport_alias",
                    "#sport:type",
                ],
                "market": [
                    "type", "name", "order", "main_order", "id", "base",
                    "express_id", "col_count", "group_id", "group_name",
                    "cashout", "point_sequence", "sequence", "is_new",
                    "market_type", "extra_info", "group_order",
                    "prematch_express_id", "has_early_payout",
                ],
                "event": [
                    "name", "id", "price", "base", "order",
                    "type_1", "extra_info", "display_column", "ew_allowed",
                ],
            },
            "where": {
                "game": {
                    "@or": [
                        {"type": {"@in": [0, 2]}},
                        {"visible_in_prematch": 1},
                    ]
                },
                "sport":       {"alias": sport_alias, "type": {"@in": [0, 2, 5]}},
                "region":      {"alias": region_alias},
                "competition": {"id": competition_id},
            },
            "subscribe": True,
        },
        "rid": rid,
    }


def iter_region_competitions(prematch_response: dict[str, Any]) -> Iterator[tuple[str, int, str]]:
    payload = prematch_response
    if payload.get("code") != 0:
        return
    inner   = payload.get("data") or {}
    blob    = inner.get("data") or {}
    regions = blob.get("region") or {}
    if not isinstance(regions, dict):
        return
    for _rid, r in regions.items():
        if not isinstance(r, dict):
            continue
        ralias = (r.get("alias") or "").strip() or str(r.get("id") or "")
        comps  = r.get("competition") or {}
        if not isinstance(comps, dict):
            continue
        for _cid, c in comps.items():
            if not isinstance(c, dict):
                continue
            try:
                cid = int(c.get("id", _cid))
            except (TypeError, ValueError):
                continue
            name = (c.get("name") or "").strip()
            yield ralias, cid, name


def parse_sports_from_menu(menu_response: dict[str, Any]) -> list[dict[str, Any]]:
    if menu_response.get("code") != 0:
        return []
    inner  = (menu_response.get("data") or {}).get("data") or {}
    sports = inner.get("sport") or {}
    out    = []
    if not isinstance(sports, dict):
        return out
    for _k, s in sports.items():
        if isinstance(s, dict) and s.get("alias"):
            out.append({
                "alias": (s.get("alias") or "").strip(),
                "name":  (s.get("name")  or "").strip(),
                "id":    s.get("id"),
                "game":  s.get("game"),
                "order": s.get("order"),
            })
    out.sort(key=lambda x: (x.get("order") is None, x.get("order", 0)))
    return out


class SwarmWS:
    def __init__(self, ws_url: str, cookie: str, *, origin: str = ORIGIN, timeout: int = 420):
        if _IMPORT_ERR or create_connection is None:
            raise RuntimeError(
                "Installez websocket-client : pip install websocket-client"
            ) from _IMPORT_ERR
        self.ws_url  = ws_url
        self.cookie  = cookie
        self.origin  = origin
        self.timeout = timeout
        self._ws     = None
        self._buf: dict[str, dict] = {}

    def connect(self) -> None:
        hdr = [
            f"Cookie: {self.cookie}",
            f"Origin: {self.origin}",
            f"User-Agent: {UA}",
            "Accept-Language: fr-FR,fr;q=0.9",
        ]
        self._ws = create_connection(self.ws_url, header=hdr, timeout=self.timeout)

    def close(self) -> None:
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
            self._ws = None

    def send(self, obj: dict[str, Any]) -> str:
        if not self._ws:
            raise RuntimeError("WebSocket non connecté")
        raw = json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
        self._ws.send(raw)
        return obj.get("rid") or ""

    def recv_until_rid(self, expected_rid: str, deadline: float | None = None) -> dict[str, Any]:
        if not self._ws:
            raise RuntimeError("WebSocket non connecté")
        if expected_rid in self._buf:
            return self._buf.pop(expected_rid)
        end = (deadline or (time.time() + self.timeout)) if deadline is None else deadline
        while time.time() < end:
            try:
                raw = self._ws.recv()
            except WebSocketConnectionClosedException:
                break
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8", errors="ignore")
            if not raw or not isinstance(raw, str):
                continue
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if not isinstance(obj, dict):
                continue
            rid = obj.get("rid")
            if rid == expected_rid:
                return obj
            if rid:
                self._buf[rid] = obj
        raise TimeoutError(f"Réponse absente pour rid={expected_rid!r}")


def run_swarm_fetch(
    *,
    session_file: Path,
    out_path: Path,
    ws_url: str,
    site_id: str,
    mode: str,
    sports_filter: list[str] | None,
    max_competitions: int,
    delay_s: float,
    progress: Callable[[str], None] | None = None,
    prematch_timeout_s: float | None = None,
    gamelist_timeout_s: float | None = None,
) -> dict[str, Any]:
    log = progress or (lambda m: print(m, flush=True))
    t_pre = float(
        prematch_timeout_s if prematch_timeout_s is not None
        else os.environ.get("VBET_SWARM_PREMATCH_TIMEOUT", "120")
    )
    t_game = float(
        gamelist_timeout_s if gamelist_timeout_s is not None
        else os.environ.get("VBET_SWARM_GAMELIST_TIMEOUT", "300")
    )
    cookie = load_cookie_file(session_file)
    afec   = parse_afec(cookie)
    if not afec:
        raise MissingSwarmCookieError(
            "Cookie sans _immortal|user-hashX (Swarm). "
            "En local : python vbet.py capture --headed --manual. "
            "Sur Railway : VBET_SESSION_JSON_B64 ou VBET_COOKIE depuis un navigateur connecté."
        )

    ws_captures: list[dict[str, Any]] = []
    wurl = ws_url

    def record_sent(obj: dict[str, Any]) -> None:
        ws_captures.append({"url": wurl, "kind": "websocket", "data": obj})

    def record_recv(obj: dict[str, Any]) -> None:
        ws_captures.append({"url": wurl, "kind": "websocket", "data": obj})

    sock_to = int(max(t_pre, t_game) + 120)
    sw = SwarmWS(ws_url, cookie, timeout=sock_to)
    sw.connect()
    t0 = time.time()
    try:
        rs = build_request_session(afec, site_id)
        record_sent(rs)
        sw.send(rs)
        r0 = sw.recv_until_rid(rs["rid"])
        record_recv(r0)
        if r0.get("code") != 0:
            raise SwarmSessionRejected(f"request_session refusé : {r0!r}")

        mq = build_sports_menu()
        record_sent(mq)
        sw.send(mq)
        menu = sw.recv_until_rid(mq["rid"])
        record_recv(menu)
        if menu.get("code") != 0:
            raise SwarmSessionRejected(f"menu sports refusé : {menu!r}")

        sports = parse_sports_from_menu(menu)
        if sports_filter:
            fl     = {x.strip() for x in sports_filter if x.strip()}
            sports = [s for s in sports if s["alias"] in fl]

        batch_size = int(os.environ.get("VBET_SWARM_BATCH_SIZE", "20"))

        if mode == "menu":
            pass
        elif mode in ("prematch", "full"):
            pq_rids: dict[str, str] = {}
            for sp in sports:
                alias = sp["alias"]
                pq    = build_prematch_tree(alias)
                record_sent(pq)
                sw.send(pq)
                pq_rids[pq["rid"]] = alias

            all_comps: list[tuple[str, str, int]] = []
            for rid, alias in pq_rids.items():
                try:
                    pr = sw.recv_until_rid(rid, deadline=time.time() + t_pre)
                    record_recv(pr)
                    for ralias, cid, _cname in iter_region_competitions(pr):
                        all_comps.append((alias, ralias, cid))
                except TimeoutError:
                    log(f"  [skip] prematch timeout sport={alias}")

            if mode == "full":
                if max_competitions > 0:
                    all_comps = all_comps[:max_competitions]

                n_done_total = 0
                for i in range(0, len(all_comps), batch_size):
                    batch      = all_comps[i:i + batch_size]
                    batch_rids = []
                    for alias, ralias, cid in batch:
                        gq = build_gamelist(alias, ralias, cid)
                        record_sent(gq)
                        sw.send(gq)
                        batch_rids.append(gq["rid"])
                    for rid in batch_rids:
                        try:
                            gr = sw.recv_until_rid(rid, deadline=time.time() + t_game)
                            record_recv(gr)
                        except TimeoutError:
                            log(f"  [skip] gamelist timeout rid={rid[:24]}")
                    n_done_total += len(batch)
                    if n_done_total % 25 == 0:
                        log(f"  … {n_done_total} compétitions")
                    if delay_s:
                        time.sleep(delay_s)
        else:
            raise ValueError(f"mode inconnu : {mode}")
    finally:
        sw.close()

    elapsed = round(time.time() - t0, 2)
    payload = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source":       "vbet",
        "mode":         "swarm_api_fetch",
        "swarm_fetch": {
            "ws_url":          ws_url,
            "site_id":         site_id,
            "fetch_mode":      mode,
            "sports_filter":   sports_filter or [],
            "max_competitions": max_competitions,
            "elapsed_seconds": elapsed,
        },
        "stats": {
            "ws_captures":     len(ws_captures),
            "elapsed_seconds": elapsed,
        },
        "captures":    [],
        "ws_captures": ws_captures,
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"[swarm] {len(ws_captures)} messages WS → {out_path} ({elapsed}s)")
    return payload
