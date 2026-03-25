#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
if [[ ! -x "${ROOT}/.venv/bin/python" ]]; then
  command -v python3 >/dev/null 2>&1 || { echo "Installez Python 3." >&2; exit 1; }
  python3 -m venv .venv
fi
.venv/bin/python -m pip install -q -U pip
.venv/bin/python -m pip install -q -r requirements.txt
if ! .venv/bin/python -c "import playwright" 2>/dev/null; then
  echo "[run_all] Installation explicite de playwright…" >&2
  .venv/bin/python -m pip install -q "playwright>=1.40"
fi
.venv/bin/python -m playwright install chromium
exec .venv/bin/python vbet.py run "$@"
