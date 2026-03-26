#!/usr/bin/env python3
"""Cotes prématch Vbet — même logique opérationnelle qu’Unibet : session → cache → sortie.

1. **Cache** : si ``data/vbet_odds_cache.json`` est assez récent et valide, on le réutilise
   (pas de WebSocket) sauf ``--force-fetch``.
2. **Session Swarm** : sonde ``probe_swarm_session`` ; si KO → **capture Playwright**
   (comme ``vbet.py capture``) pour régénérer ``vbet_session.json`` + cookies.
3. **Fetch** : ``run_swarm_fetch`` (Swarm) en ``prematch`` ou ``full`` → réécrit le cache.

Dépendances : ``swarm_client``, ``vbet`` (même répertoire). Playwright pour la capture seulement.

Exemples ::
    python vbet_prematch_odds.py -o output.json
    VBET_SITE_ID=277 python vbet_prematch_odds.py --max-cache-age 0 --force-fetch
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

BASE = Path(__file__).resolve().parent

LOG = logging.getLogger("vbet.prematch")


class _UtcFormatter(logging.Formatter):
    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%S") + f".{int(record.msecs):03d}Z"


def setup_logging() -> None:
    level_name = (os.environ.get("LOG_LEVEL") or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    h = logging.StreamHandler(sys.stdout)
    h.setLevel(level)
    h.setFormatter(
        _UtcFormatter(
            fmt="%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
    )
    root.addHandler(h)


def _log_kv(cycle: int | None, msg: str, **fields: Any) -> None:
    parts = [msg]
    if cycle is not None:
        parts.insert(0, f"cycle={cycle}")
    for k, v in fields.items():
        parts.append(f"{k}={v}")
    LOG.info(" | ".join(parts))


# Imports projet (après BASE pour éviter imports circulaires au lint)
sys.path.insert(0, str(BASE))

from swarm_client import (  # noqa: E402
    MissingSwarmCookieError,
    SwarmSessionRejected,
    probe_swarm_session,
    run_swarm_fetch,
)
from vbet import (  # noqa: E402
    CACHE,
    LANDING,
    _read_cache,
    _swarm_rows,
    build_downloads,
    run_capture,
    session_path,
    _resolve_site_id,
)


def _cache_usable(path: Path, max_age_s: float) -> bool:
    if max_age_s <= 0:
        return False
    if not path.is_file():
        return False
    age = time.time() - path.stat().st_mtime
    if age > max_age_s:
        return False
    try:
        raw = _read_cache(path)
    except Exception:
        return False
    ws = raw.get("ws_captures") or []
    cap = raw.get("captures") or []
    if not ws and not cap:
        return False
    return True


def _rows_from_cache(path: Path, spec_only: bool) -> list[dict[str, Any]]:
    raw = _read_cache(path)
    return _swarm_rows(raw, spec_only=spec_only)


async def run_async(
    *,
    out_path: Path,
    cache_path: Path,
    max_cache_age_s: float,
    force_fetch: bool,
    skip_capture: bool,
    fetch_mode: str,
    sports: list[str] | None,
    max_competitions: int,
    fetch_delay: float,
    spec_only: bool,
    ws_url: str,
    site_id_arg: str,
    cycle: int | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    t0 = time.perf_counter()
    sess = session_path()
    site_id = _resolve_site_id(site_id_arg)

    used_cache = False
    if not force_fetch and _cache_usable(cache_path, max_cache_age_s):
        _log_kv(
            cycle,
            "phase=cache_recover",
            path=str(cache_path),
            max_age_s=max_cache_age_s,
        )
        used_cache = True
    else:
        if not site_id:
            _log_kv(
                cycle,
                "phase=site_id",
                status="missing",
                hint="VBET_SITE_ID ou capture pour auto-détection",
            )

        async def do_probe() -> tuple[bool, str]:
            return await asyncio.to_thread(
                probe_swarm_session,
                sess,
                ws_url=ws_url,
                site_id=site_id or None,
            )

        t_probe0 = time.perf_counter()
        ok, msg = await do_probe()
        ms_probe = int((time.perf_counter() - t_probe0) * 1000)
        _log_kv(cycle, "phase=probe", duration_ms=ms_probe, ok=ok, msg=msg[:200])

        if not ok and not skip_capture:
            _log_kv(cycle, "phase=capture", reason="probe_failed_or_session_missing")
            await asyncio.to_thread(
                run_capture,
                cache_path,
                landing=LANDING,
                headless=os.environ.get("VBET_PLAYWRIGHT_HEADLESS", "1").strip().lower()
                not in ("0", "false", "no", "off"),
                manual=False,
                wait_ms=None,
            )
            site_id = _resolve_site_id(site_id_arg)
        elif not ok:
            _log_kv(cycle, "phase=capture", skipped=True, reason="skip_capture")

        if not site_id:
            raise RuntimeError(
                "VBET_SITE_ID manquant après probe/capture. "
                "Définissez VBET_SITE_ID=277 ou lancez une capture avec session."
            )

        t_fetch0 = time.perf_counter()

        def _fetch() -> dict[str, Any]:
            return run_swarm_fetch(
                session_file=sess,
                out_path=cache_path,
                ws_url=ws_url,
                site_id=site_id,
                mode=fetch_mode,
                sports_filter=sports or None,
                max_competitions=max_competitions,
                delay_s=fetch_delay,
            )

        try:
            await asyncio.to_thread(_fetch)
        except MissingSwarmCookieError:
            if skip_capture:
                raise
            _log_kv(cycle, "phase=capture", reason="missing_swarm_cookie")
            await asyncio.to_thread(
                run_capture,
                cache_path,
                landing=LANDING,
                headless=os.environ.get("VBET_PLAYWRIGHT_HEADLESS", "1").strip().lower()
                not in ("0", "false", "no", "off"),
                manual=False,
                wait_ms=None,
            )
            site_id = _resolve_site_id(site_id_arg)
            await asyncio.to_thread(_fetch)
        except SwarmSessionRejected:
            if skip_capture:
                raise
            _log_kv(cycle, "phase=capture", reason="session_rejected")
            await asyncio.to_thread(
                run_capture,
                cache_path,
                landing=LANDING,
                headless=os.environ.get("VBET_PLAYWRIGHT_HEADLESS", "1").strip().lower()
                not in ("0", "false", "no", "off"),
                manual=False,
                wait_ms=None,
            )
            site_id = _resolve_site_id(site_id_arg)
            await asyncio.to_thread(_fetch)

        ms_fetch = int((time.perf_counter() - t_fetch0) * 1000)
        _log_kv(cycle, "phase=swarm_fetch", duration_ms=ms_fetch, mode=fetch_mode)

    rows = _rows_from_cache(cache_path, spec_only=spec_only)
    doc = build_downloads(rows)
    total_rows = sum(s.get("total_rows", 0) for s in doc.get("sports", {}).values())

    total_ms = int((time.perf_counter() - t0) * 1000)
    meta = {
        "schema": "vbet_odds_v1",
        "source": "vbet_prematch_odds",
        "feed": "swarm_websocket",
        "cache_file": str(cache_path.resolve()),
        "used_cache": used_cache,
        "fetch_mode": fetch_mode if not used_cache else None,
        "site_id": site_id or _resolve_site_id(site_id_arg),
        "total_selection_rows": total_rows,
        "total_ms": total_ms,
    }
    doc["meta"] = {
        **meta,
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(doc, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    _log_kv(
        cycle,
        "phase=summary",
        status="ok",
        total_ms=total_ms,
        used_cache=used_cache,
        selection_rows=total_rows,
        file=str(out_path),
    )
    return doc, meta


def main() -> None:
    p = argparse.ArgumentParser(description="Cotes prématch Vbet — cache + Swarm + capture si besoin")
    p.add_argument("-o", "--output", type=Path, default=BASE / "output_prematch.json")
    p.add_argument("--cache", type=Path, default=CACHE, help="Fichier cache JSON (défaut data/vbet_odds_cache.json)")
    p.add_argument(
        "--max-cache-age",
        type=float,
        default=float(os.environ.get("VBET_MAX_CACHE_AGE_S", "600")),
        help="Réutiliser le cache sans fetch si plus récent que N secondes (0=désactivé)",
    )
    p.add_argument("--force-fetch", action="store_true", help="Ignorer la récupération du cache")
    p.add_argument("--no-capture", action="store_true", dest="skip_capture", help="Ne pas lancer Playwright si session KO")
    p.add_argument(
        "--mode",
        choices=("menu", "prematch", "full"),
        default=os.environ.get("VBET_FETCH_MODE", "full"),
        help="Swarm : full = cotes par compétition ; prematch = arbres seulement",
    )
    p.add_argument("--sport", action="append", default=[], help="Alias sport (répétable)")
    p.add_argument("--max-competitions", type=int, default=0)
    p.add_argument("--fetch-delay", type=float, default=0.0)
    p.add_argument(
        "--all-markets",
        action="store_true",
        help="Sans filtre PDF §3.1 (tous marchés)",
    )
    p.add_argument("--ws-url", default=os.environ.get("VBET_SWARM_WS", "wss://swarm-2.vbet.fr/"))
    p.add_argument("--site-id", default=os.environ.get("VBET_SITE_ID", ""))
    p.add_argument(
        "--loop-seconds",
        type=int,
        default=int(os.environ.get("SCRAPER_LOOP_SECONDS") or "0"),
        help="Répéter toutes les N secondes (0 = une fois)",
    )
    args = p.parse_args()

    setup_logging()
    spec_only = not args.all_markets
    loop_s = max(0, args.loop_seconds)

    cycle = 0
    while True:
        cycle += 1
        t_cycle0 = time.perf_counter()
        _log_kv(
            cycle,
            "event=cycle_start",
            loop_seconds=loop_s,
            output=str(args.output),
        )
        try:
            doc, meta = asyncio.run(
                run_async(
                    out_path=args.output,
                    cache_path=args.cache,
                    max_cache_age_s=args.max_cache_age,
                    force_fetch=args.force_fetch,
                    skip_capture=args.skip_capture,
                    fetch_mode=args.mode,
                    sports=args.sport or None,
                    max_competitions=args.max_competitions,
                    fetch_delay=args.fetch_delay,
                    spec_only=spec_only,
                    ws_url=args.ws_url,
                    site_id_arg=args.site_id,
                    cycle=cycle,
                )
            )
        except Exception:
            LOG.exception("cycle failed")
            if loop_s <= 0:
                raise
            time.sleep(min(loop_s, 120))
            continue

        wall_ms = int((time.perf_counter() - t_cycle0) * 1000)
        _log_kv(
            cycle,
            "event=cycle_end",
            status="ok",
            wall_ms=wall_ms,
            rows=meta.get("total_selection_rows"),
            used_cache=meta.get("used_cache"),
        )
        print(
            f"Écrit : {args.output} — {meta.get('total_selection_rows', '?')} lignes "
            f"(cache={'oui' if meta.get('used_cache') else 'non'})",
            flush=True,
        )
        if loop_s <= 0:
            break
        time.sleep(loop_s)


if __name__ == "__main__":
    main()
