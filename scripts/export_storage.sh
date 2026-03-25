#!/usr/bin/env bash
# Exporte les cookies de session Playwright → valeur à coller dans Railway
# Usage : ./scripts/export_storage.sh
# Prérequis : s'être connecté une fois via ./run_all.sh --headed --manual
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
exec "${ROOT}/.venv/bin/python" "${ROOT}/vbet.py" export-storage "$@"
