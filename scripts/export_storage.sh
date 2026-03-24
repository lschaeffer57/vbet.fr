#!/usr/bin/env bash
# Exporte les cookies vbet.fr → valeur à coller dans Railway (VBET_STORAGE_STATE_B64)
# Prérequis : s'être connecté une fois via : python vbet.py capture --headed --manual
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
B64="${ROOT}/data/vbet_storage_state_b64.txt"
if [ -f "$B64" ]; then
  echo "=== VBET_STORAGE_STATE_B64 ==="
  cat "$B64"
  echo ""
else
  echo "Fichier absent. Lancez d'abord : python vbet.py capture --headed --manual"
fi
