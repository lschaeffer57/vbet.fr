# Vbet

Scraper de cotes vbet.fr — Swarm WebSocket (`wss://swarm-2.vbet.fr/`), `site_id=277`.

**Production Railway :** `https://vbet-production.up.railway.app`

---

## API

| Endpoint | Description |
|----------|-------------|
| `GET /odds` | Toutes les cotes (JSON) |
| `GET /health` | État du cache |
| `GET /session/status` | Cookie Swarm + site_id |
| `POST /session` | Injecter une session (Bearer) |
| `POST /session/refresh` | Relancer capture xvfb (Bearer) |
| `POST /fetch` | Fetch manuel (Bearer) |

---

## Lancer en local

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium

# Capturer session (ouvre Chrome, laisse charger vbet.fr)
python vbet.py capture --headed

# Fetch cotes via Swarm WebSocket
VBET_SITE_ID=277 python vbet.py fast --mode full

# Serveur API local
VBET_SITE_ID=277 python vbet.py serve
```

---

## Déploiement Railway

```bash
git push origin main   # Railway redéploie automatiquement
```

### Variables Railway

| Variable | Valeur | Description |
|----------|--------|-------------|
| `VBET_SITE_ID` | `277` | Site ID Swarm vbet.fr |
| `VBET_SESSION_JSON_B64` | *(base64 de data/vbet_session.json)* | Session Swarm |
| `VBET_INGEST_TOKEN` | *(secret)* | Auth Bearer pour POST /session |
| `VBET_FETCH_COOLDOWN_S` | `1` | Délai entre cycles fetch |
| `VBET_FETCH_MODE` | `full` | `menu` / `prematch` / `full` |

### Pousser une nouvelle session

```bash
VBET_INGEST_TOKEN=xxx ./scripts/push_session.sh https://vbet-production.up.railway.app
```
