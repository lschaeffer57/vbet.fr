"""
winamax_client.py — Cotes Winamax en temps réel
================================================

Architecture découverte :
  - Server Socket.IO EIO=3 long-polling :
    https://sports-eu-west-3.winamax.fr/uof-sports-server/socket.io
  - Subscription : POST 42["m", {"route": "sport:{id}", "data": true, "menu": true}]
  - Réponse     : 42["m", {matches, bets, outcomes, odds, sports, ...}]
  - IMPORTANT   : le serveur exige HTTP/2 → utilise Playwright (Chromium)

  - Fallback HTML (sans navigateur) :
    GET https://www.winamax.fr/paris-sportifs/sports/{sportId}
    → PRELOADED_STATE embarqué dans le HTML (même structure JSON)

  - Modes :
    html     : HTML scraping (rapide, sans navigateur, marché principal)
    sio      : Socket.IO via Playwright (temps réel, marché principal)
    sio-full : Socket.IO + route match:{id} (toutes cotes, lent)
    live     : boucle infinie Socket.IO

Format de sortie (JSON) :
  {
    "generated_at": "...",
    "source": "winamax",
    "stats": { "matches": int, "bets": int, "sports": int },
    "sports":      { sportId:  { sportName, ... } },
    "categories":  { catId:    { categoryName, ... } },
    "tournaments": { tourId:   { tournamentName, ... } },
    "matches": {
      matchId: {
        "matchId": int, "title": str,
        "competitor1Name": str, "competitor2Name": str,
        "matchStart": int,  // epoch secondes
        "sportId": int, "categoryId": int, "tournamentId": int,
        "bets": [
          {
            "betId": int, "betTitle": str,
            "outcomes": [
              { "outcomeId": int, "label": str, "odds": float }
            ]
          }
        ]
      }
    }
  }
"""
from __future__ import annotations

import json
import os
import random
import re
import string
import time
import urllib.request
from pathlib import Path
from typing import Any, Callable, Iterator

# ── Configuration ─────────────────────────────────────────────────────────────

SIO_URL  = "https://sports-eu-west-3.winamax.fr/uof-sports-server/socket.io"
WEB_BASE = "https://www.winamax.fr/paris-sportifs/sports"

_BASE    = Path(__file__).resolve().parent

# Sports disponibles sur Winamax.fr (depuis PRELOADED_STATE)
ALL_SPORT_IDS: list[int] = [
    1,       # Football
    2,       # Basketball
    3,       # Tennis
    4,       # Hockey sur glace
    5,       # Rugby
    6,       # Baseball
    9,       # Handball
    10,      # Volley-ball
    11,      # Américain
    12,      # Golf
    13,      # Cyclisme
    16,      # Natation / Athlétisme
    17,      # Snooker
    20,      # MMA / Boxe
    23,      # Darts
    29,      # Motosport
    40,      # eSports
    43,      # Futsal
    100000,  # Paris spéciaux
]

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
_HDRS_HTML = {
    "User-Agent":      _UA,
    "Accept":          "text/html,application/xhtml+xml",
    "Accept-Language": "fr-FR,fr;q=0.9",
}
_PLAYWRIGHT_PROFILE = os.environ.get(
    "WINAMAX_PLAYWRIGHT_PROFILE",
    str(_BASE / "data" / "winamax_playwright_profile"),
)
_STEALTH = "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"

# Chemin du profil persistant (contient la session login après run_capture)
PROFILE = Path(_PLAYWRIGHT_PROFILE)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _rand_t() -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=7))


def _parse_preloaded_state(html: str) -> dict:
    m = re.search(r"var PRELOADED_STATE\s*=\s*(.*?);\s*\n", html, re.DOTALL)
    if not m:
        return {}
    try:
        data, _ = json.JSONDecoder().raw_decode(m.group(1))
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, ValueError):
        return {}


def _parse_sio_m_event(body: str) -> dict:
    """Extrait le payload JSON de la trame Socket.IO 42["m", {...}]."""
    m = re.match(r"\d+:42\[\"m\",(.*)\]$", body, re.DOTALL)
    if not m:
        return {}
    try:
        data = json.loads(m.group(1))
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, ValueError):
        return {}


def _merge_state(target: dict, source: dict) -> None:
    """Fusionne les clés dict du state Winamax (sports, matches, bets, odds…)."""
    for key in ("sports", "categories", "tournaments", "matches", "bets", "outcomes", "odds"):
        if key not in source:
            continue
        v = source[key]
        if isinstance(v, dict):
            target.setdefault(key, {}).update(v)
    if "sportIds" in source and "sportIds" not in target:
        target["sportIds"] = source["sportIds"]


def _build_flat_matches(state: dict) -> dict[str, dict]:
    """Construit la liste des matchs avec cotes imbriquées."""
    out: dict[str, dict] = {}
    odds_map     = state.get("odds", {})
    outcomes_map = state.get("outcomes", {})
    bets_map     = state.get("bets", {})

    # Index bets par matchId pour accès O(1)
    bets_by_match: dict[int, list[dict]] = {}
    for bet in bets_map.values():
        if not isinstance(bet, dict):
            continue
        mid = bet.get("matchId")
        if mid is not None:
            bets_by_match.setdefault(mid, []).append(bet)

    for mid_str, match in state.get("matches", {}).items():
        if not isinstance(match, dict):
            continue
        match_id = match.get("matchId")
        if not match_id:
            continue

        flat_bets: list[dict] = []
        for bet in bets_by_match.get(match_id, []):
            outcomes_flat: list[dict] = []
            for oid in bet.get("outcomes") or []:
                oid_s = str(oid)
                raw_odd = odds_map.get(oid_s)
                # Filtre : cote valide (>1, pas 1 = indispo ou 100 = aberrant)
                if not isinstance(raw_odd, (int, float)) or float(raw_odd) <= 1.0:
                    continue
                label = (outcomes_map.get(oid_s) or {}).get("label", "")
                outcomes_flat.append({
                    "outcomeId": oid,
                    "label":     label,
                    "odds":      float(raw_odd),
                })
            if outcomes_flat:
                flat_bets.append({
                    "betId":    bet.get("betId"),
                    "betTitle": bet.get("betTitle", ""),
                    "betType":  bet.get("betType"),
                    "outcomes": outcomes_flat,
                })

        out[str(match_id)] = {
            "matchId":         match_id,
            "title":           match.get("title", ""),
            "competitor1Name": match.get("competitor1Name", ""),
            "competitor2Name": match.get("competitor2Name", ""),
            "matchStart":      match.get("matchStart"),
            "sportId":         match.get("sportId"),
            "categoryId":      match.get("categoryId"),
            "tournamentId":    match.get("tournamentId"),
            "status":          match.get("status"),
            "bets":            flat_bets,
        }
    return out


# ── Capture Playwright (session persistante, login manuel) ────────────────────

def run_capture(*, headed: bool = True) -> None:
    """
    Ouvre Chromium en mode headed, navigue sur Winamax.
    L'utilisateur se connecte manuellement dans le navigateur.
    Le profil persistant (data/winamax_playwright_profile/) sauvegarde la session.
    Les runs suivants (sio-full) seront automatiquement authentifiés.

    Usage :
        python winamax_client.py --mode capture
        python winamax_client.py --mode capture --headed   # (headed par défaut)
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        raise RuntimeError("playwright non installé — pip install playwright")

    print(f"[winamax] Démarrage capture (profil: {_PLAYWRIGHT_PROFILE})…", flush=True)

    with sync_playwright() as pw:
        PROFILE.mkdir(parents=True, exist_ok=True)
        ctx = pw.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE),
            headless=False,          # toujours headed pour login manuel
            viewport={"width": 1366, "height": 768},
            locale="fr-FR",
            timezone_id="Europe/Paris",
            user_agent=_UA,
            extra_http_headers={"Accept-Language": "fr-FR,fr;q=0.9"},
            args=["--disable-blink-features=AutomationControlled", "--disable-dev-shm-usage"],
        )
        ctx.add_init_script(_STEALTH)
        page = ctx.new_page()

        try:
            print("[winamax] Navigation → www.winamax.fr/paris-sportifs/sports/1…", flush=True)
            page.goto(
                "https://www.winamax.fr/paris-sportifs/sports/1",
                wait_until="domcontentloaded",
                timeout=60_000,
            )
            print(
                "\n>>> Connectez-vous manuellement dans le navigateur Winamax.\n"
                ">>> Appuyez sur Entrée ici une fois connecté…",
                flush=True,
            )
            try:
                input()
            except EOFError:
                pass
            # Attendre que la session se stabilise
            page.wait_for_timeout(5000)
        finally:
            ctx.close()

    print(
        f"[winamax] Session sauvegardée dans le profil → {PROFILE}\n"
        "[winamax] Vous pouvez maintenant lancer : python winamax_client.py --mode sio-full",
        flush=True,
    )


# ── Méthode 1 : HTML scraping (rapide, sans navigateur) ──────────────────────

def fetch_sport_html(sport_id: int, timeout: int = 30) -> dict:
    """
    Récupère le PRELOADED_STATE depuis la page sport Winamax.
    Renvoie le state brut (matches, bets, odds, outcomes, sports…).
    """
    url = f"{WEB_BASE}/{sport_id}"
    req = urllib.request.Request(url, headers=_HDRS_HTML)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        html = r.read().decode("utf-8", errors="ignore")
    return _parse_preloaded_state(html)


def run_html_fetch(
    *,
    out_path: Path,
    sport_ids: list[int] | None = None,
    delay_s: float = 0.5,
    progress: Callable[[str], None] | None = None,
    timeout: int = 30,
) -> dict:
    """
    Fetch de toutes les cotes via HTML scraping.
    Rapide (~30s pour tous les sports), sans navigateur.
    Fournit le marché principal par match (1X2, Vainqueur…).
    """
    log = progress or (lambda m: print(m, flush=True))
    ids = sport_ids if sport_ids is not None else ALL_SPORT_IDS
    state: dict = {}
    t0 = time.time()

    for sport_id in ids:
        try:
            log(f"[winamax] sport {sport_id}…")
            s = fetch_sport_html(sport_id, timeout=timeout)
            if not s:
                log(f"  [skip] sport {sport_id} : aucune donnée")
                continue
            n_m = len(s.get("matches", {}))
            n_b = len(s.get("bets", {}))
            log(f"  → {n_m} matchs, {n_b} marchés")
            _merge_state(state, s)
        except Exception as e:
            log(f"  [err] sport {sport_id} : {e}")
        if delay_s > 0:
            time.sleep(delay_s)

    elapsed = round(time.time() - t0, 2)
    matches = _build_flat_matches(state)
    return _save_payload(out_path, state, matches, elapsed, "html_scraping", log)


# ── Méthode 2 : Socket.IO via Playwright (HTTP/2 requis) ─────────────────────

def _pw_common_kwargs(headless: bool = True) -> dict:
    return dict(
        user_data_dir=_PLAYWRIGHT_PROFILE,
        headless=headless,
        viewport={"width": 1366, "height": 768},
        locale="fr-FR",
        timezone_id="Europe/Paris",
        user_agent=_UA,
        extra_http_headers={"Accept-Language": "fr-FR,fr;q=0.9"},
        args=["--disable-blink-features=AutomationControlled", "--disable-dev-shm-usage"],
    )


def _sio_on_response(bucket: list[dict]):
    """Retourne un handler qui collecte les payloads Socket.IO 42["m",…]."""
    def handler(resp) -> None:
        if "socket.io" not in resp.url or "transport=polling" not in resp.url:
            return
        try:
            body = resp.body().decode("utf-8", errors="ignore")
            if '42["m"' in body and len(body) > 1000:
                data = _parse_sio_m_event(body)
                if data:
                    bucket.append(data)
        except Exception:
            pass
    return handler


def _capture_sio_sport(sport_id: int, wait_ms: int = 5000) -> dict:
    """
    Ouvre Chromium, navigue vers sports/{sport_id}, intercepte la réponse
    Socket.IO 42["m", {...}] et retourne le state brut.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        raise RuntimeError("playwright non installé — pip install playwright")

    bucket: list[dict] = []

    with sync_playwright() as pw:
        ctx = pw.chromium.launch_persistent_context(**_pw_common_kwargs())
        ctx.add_init_script(_STEALTH)
        ctx.on("response", _sio_on_response(bucket))
        page = ctx.new_page()
        try:
            page.goto(
                f"https://www.winamax.fr/paris-sportifs/sports/{sport_id}",
                wait_until="domcontentloaded",
                timeout=45_000,
            )
            page.wait_for_timeout(wait_ms)
        finally:
            ctx.close()

    merged: dict = {}
    for d in bucket:
        _merge_state(merged, d)
    return merged


def _capture_sio_match(
    match_id: int,
    sport_id: int,
    category_id: int,
    tournament_id: int,
    wait_ms: int = 8000,
) -> dict:
    """
    Navigue vers la page match (depuis la page sport pour déclencher la SPA),
    puis intercepte la réponse Socket.IO avec toutes les cotes.

    Si le profil persistant est connecté (après run_capture), la React app
    envoie automatiquement authorize({jwt}) au Socket.IO → route="match:N"
    retourne tous les marchés.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        raise RuntimeError("playwright non installé")

    bucket: list[dict] = []

    match_url = (
        f"https://www.winamax.fr/paris-sportifs/sports/{sport_id}"
        f"/categories/{category_id}/tournaments/{tournament_id}"
        f"/match-{match_id}"
    )

    with sync_playwright() as pw:
        ctx = pw.chromium.launch_persistent_context(**_pw_common_kwargs())
        ctx.add_init_script(_STEALTH)
        ctx.on("response", _sio_on_response(bucket))
        page = ctx.new_page()
        try:
            # Charger sport page d'abord (établit la session Socket.IO)
            page.goto(
                f"https://www.winamax.fr/paris-sportifs/sports/{sport_id}",
                wait_until="domcontentloaded",
                timeout=45_000,
            )
            page.wait_for_timeout(3000)
            # Naviguer SPA vers la page match (déclenche route="match:N")
            page.goto(match_url, wait_until="domcontentloaded", timeout=30_000)
            page.wait_for_timeout(wait_ms)
        finally:
            ctx.close()

    merged: dict = {}
    for d in bucket:
        _merge_state(merged, d)
    return merged


# ── Méthode 2 : fetch via Socket.IO Playwright ───────────────────────────────

def run_sio_fetch(
    *,
    out_path: Path,
    sport_ids: list[int] | None = None,
    full_match: bool = False,
    max_matches: int = 0,
    wait_ms: int = 5000,
    progress: Callable[[str], None] | None = None,
) -> dict:
    """
    Fetch de toutes les cotes via Socket.IO / Playwright (temps réel).

    Args:
        full_match : Si True, visite chaque page match pour TOUTES les cotes.
                     Nécessite d'être connecté (run_capture d'abord).
                     Plus lent (~5–10s/match) mais complet (tous les marchés).
        max_matches: Limite de matchs en mode full_match (0 = illimité).
        wait_ms    : Durée d'attente Socket.IO par page (ms).

    Note:
        Le profil persistant (data/winamax_playwright_profile/) contient la session
        login. Si connecté, la React app envoie authorize({jwt}) automatiquement
        et route="match:N" retourne tous les marchés.
    """
    log = progress or (lambda m: print(m, flush=True))

    if full_match and not PROFILE.exists():
        log(
            "[winamax] Profil non trouvé — lancez d'abord :\n"
            "  python winamax_client.py --mode capture"
        )

    ids = sport_ids if sport_ids is not None else ALL_SPORT_IDS
    state: dict = {}
    t0 = time.time()

    # Phase 1 : tous les sports → matchs + marché principal
    for sport_id in ids:
        try:
            log(f"[winamax] sport:{sport_id} (Playwright)…")
            sport_state = _capture_sio_sport(sport_id, wait_ms=wait_ms)
            n_m = len(sport_state.get("matches", {}))
            n_b = len(sport_state.get("bets", {}))
            log(f"  → {n_m} matchs, {n_b} marchés")
            _merge_state(state, sport_state)
        except Exception as e:
            log(f"  [err] sport {sport_id} : {e}")

    # Phase 2 (optionnel) : toutes les cotes par match
    if full_match:
        candidates = [
            v for v in state.get("matches", {}).values()
            if isinstance(v, dict) and v.get("matchId")
        ]
        if max_matches > 0:
            candidates = candidates[:max_matches]

        log(f"[winamax] Phase 2 : {len(candidates)} matchs (toutes cotes)…")
        for i, match in enumerate(candidates):
            try:
                mdata = _capture_sio_match(
                    match_id=match["matchId"],
                    sport_id=match.get("sportId", 1),
                    category_id=match.get("categoryId", 0),
                    tournament_id=match.get("tournamentId", 0),
                    wait_ms=wait_ms,
                )
                _merge_state(state, mdata)
                if (i + 1) % 10 == 0:
                    log(f"  … {i + 1}/{len(candidates)} matchs")
            except Exception as e:
                log(f"  [err] match {match.get('matchId')} : {e}")

    elapsed = round(time.time() - t0, 2)
    matches = _build_flat_matches(state)
    return _save_payload(out_path, state, matches, elapsed, "socketio_playwright", log)


# ── Méthode 3 : temps réel en continu (boucle) ───────────────────────────────

def run_realtime_loop(
    *,
    out_path: Path,
    sport_ids: list[int] | None = None,
    interval_s: float = 30.0,
    use_playwright: bool = False,
    progress: Callable[[str], None] | None = None,
    on_update: Callable[[dict], None] | None = None,
) -> None:
    """
    Boucle temps réel : re-fetch toutes les cotes toutes les `interval_s` secondes.

    Args:
        use_playwright : True = Socket.IO Playwright (plus fiable), False = HTML
        on_update      : callback appelé après chaque mise à jour

    Exemple :
        run_realtime_loop(out_path=Path("data/winamax_live.json"), interval_s=30)
    """
    log = progress or (lambda m: print(m, flush=True))
    log("[winamax] Démarrage boucle temps réel…")

    while True:
        try:
            if use_playwright:
                payload = run_sio_fetch(out_path=out_path, sport_ids=sport_ids, progress=log)
            else:
                payload = run_html_fetch(out_path=out_path, sport_ids=sport_ids, progress=log)
            if on_update:
                on_update(payload)
            n = payload["stats"]["matches"]
            log(f"[winamax] {n} matchs → prochain dans {interval_s}s")
            time.sleep(interval_s)
        except KeyboardInterrupt:
            log("[winamax] Arrêt.")
            break
        except Exception as e:
            log(f"[winamax] Erreur : {e} — retry dans 10s")
            time.sleep(10)


# ── Sauvegarde ────────────────────────────────────────────────────────────────

def _save_payload(
    out_path: Path,
    state: dict,
    matches: dict,
    elapsed: float,
    mode: str,
    log: Callable[[str], None],
) -> dict:
    payload = {
        "generated_at":   time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source":         "winamax",
        "mode":           mode,
        "elapsed_seconds": elapsed,
        "stats": {
            "sports":      len(state.get("sports", {})),
            "categories":  len(state.get("categories", {})),
            "tournaments": len(state.get("tournaments", {})),
            "matches":     len(matches),
            "bets":        sum(len(m.get("bets", [])) for m in matches.values()),
            "elapsed_seconds": elapsed,
        },
        "sports":      state.get("sports", {}),
        "categories":  state.get("categories", {}),
        "tournaments": state.get("tournaments", {}),
        "matches":     matches,
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    n = len(matches)
    log(f"[winamax] {n} matchs → {out_path} ({elapsed}s)")
    return payload


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        description="Winamax cotes scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--mode",
        choices=["capture", "html", "sio", "sio-full", "live"],
        default="html",
        help=(
            "capture  = Login Winamax + sauvegarde JWT (1x en local)\n"
            "html     = HTML scraping rapide (marché principal)\n"
            "sio      = Socket.IO temps réel (marché principal)\n"
            "sio-full = Socket.IO + toutes cotes/match (login requis)\n"
            "live     = boucle temps réel infinie"
        ),
    )
    parser.add_argument(
        "--out",
        default="data/winamax_odds.json",
        help="Fichier JSON de sortie",
    )
    parser.add_argument(
        "--sports",
        default="",
        help="IDs sports séparés par virgule (ex: 1,2,3). Vide = tous.",
    )
    parser.add_argument(
        "--max-matches",
        type=int,
        default=0,
        help="Limite matchs en mode sio-full (0 = illimité)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=30.0,
        help="Intervalle en secondes pour le mode live",
    )
    args = parser.parse_args()

    out_path = Path(args.out)
    sport_ids: list[int] | None = None
    if args.sports:
        sport_ids = [int(s.strip()) for s in args.sports.split(",") if s.strip()]

    if args.mode == "capture":
        run_capture()

    elif args.mode == "html":
        run_html_fetch(out_path=out_path, sport_ids=sport_ids)

    elif args.mode == "sio":
        run_sio_fetch(out_path=out_path, sport_ids=sport_ids)

    elif args.mode == "sio-full":
        run_sio_fetch(
            out_path=out_path,
            sport_ids=sport_ids,
            full_match=True,
            max_matches=args.max_matches,
        )

    elif args.mode == "live":
        run_realtime_loop(
            out_path=out_path,
            sport_ids=sport_ids,
            interval_s=args.interval,
        )
