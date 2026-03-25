#!/usr/bin/env bash
# Swarm WS (sans Playwright). Prérequis : data/vbet_session.json avec cookie Swarm.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
PY="${ROOT}/.venv/bin/python"
if [[ ! -x "$PY" ]]; then
  command -v python3 >/dev/null 2>&1 || { echo "Installez Python 3 (python.org ou brew install python)" >&2; exit 1; }
  python3 -m venv .venv
fi
"$PY" -m pip install -q -U pip
"$PY" -m pip install -q -r requirements.txt
exec "$PY" vbet.py fast "$@"
