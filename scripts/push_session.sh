#!/usr/bin/env bash
# Pousse data/vbet_session.json vers Railway via POST /session
# Usage : VBET_INGEST_TOKEN=xxx ./scripts/push_session.sh https://your-app.up.railway.app
set -euo pipefail

ROOT="${ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
SESSION_FILE="${ROOT}/data/vbet_session.json"

RAILWAY_URL="${1:-${RAILWAY_URL:-}}"
TOKEN="${VBET_INGEST_TOKEN:-}"

if [[ -z "$RAILWAY_URL" ]]; then
  echo "Usage : VBET_INGEST_TOKEN=xxx $0 <railway_url>" >&2
  echo "  ex  : $0 https://vbet-production.up.railway.app" >&2
  exit 1
fi
if [[ -z "$TOKEN" ]]; then
  echo "Définissez VBET_INGEST_TOKEN (même valeur que la var Railway)" >&2
  exit 1
fi
if [[ ! -f "$SESSION_FILE" ]]; then
  echo "Fichier introuvable : $SESSION_FILE" >&2
  echo "Lancez d'abord : python vbet.py capture --headed --manual" >&2
  exit 1
fi

BODY="$(python3 -c "import json, pathlib; print(json.dumps({'session': json.loads(pathlib.Path('${SESSION_FILE}').read_text())}))")"

echo "[push_session] → ${RAILWAY_URL}/session"
HTTP_CODE="$(curl -s -o /tmp/_push_resp.json -w "%{http_code}" \
  -X POST "${RAILWAY_URL}/session" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d "$BODY")"

cat /tmp/_push_resp.json && echo

if [[ "$HTTP_CODE" == "200" ]]; then
  echo "[push_session] OK (HTTP 200)"
else
  echo "[push_session] Erreur HTTP $HTTP_CODE" >&2
  exit 1
fi
