# Vbet

Scraper de cotes vbet.fr — même architecture que daznbet.fr (Swarm WebSocket).
Le `site_id` vbet.fr est **auto-détecté** lors de la première capture Playwright.

---

## Lancer en local

### 1. Environnement (une seule fois)

```bash
cd /Users/Dominique/Desktop/Vbet
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium
```

### 2. Capturer la session (une seule fois)

Lance le navigateur, connecte-toi sur vbet.fr, puis ferme :

```bash
python vbet.py capture --headed --manual
```

Le `site_id` est **automatiquement détecté** depuis le trafic WebSocket du navigateur et sauvegardé dans `data/vbet_session.json`.

### 3. API locale

```bash
python vbet.py serve
```

Endpoints sur `http://127.0.0.1:8003` :

| Endpoint | Description |
|----------|-------------|
| `GET /odds` | Cotes (sports / compétitions / matchs) |
| `GET /health` | État du cache |
| `GET /session/status` | Cookie Swarm + site_id détecté |

---

## Déploiement Railway

**1. Capturer en local puis exporter :**

```bash
python vbet.py capture --headed --manual
./scripts/export_storage.sh
```

**2. Variables Railway :**

| Variable | Description |
|----------|-------------|
| `VBET_STORAGE_STATE_B64` | Valeur issue de `export_storage.sh` |
| `VBET_SITE_ID` | Auto-détecté à la capture (affiché dans les logs) |
| `VBET_SERVE_TOKEN` | Token Bearer optionnel |

**3. Déployer :**

```bash
git push origin main   # Railway redéploie automatiquement
```

---

## Variables d'environnement

| Variable | Défaut | Description |
|----------|--------|-------------|
| `VBET_STORAGE_STATE_B64` | — | Cookies Playwright (Railway) |
| `VBET_SITE_ID` | auto | Site ID Swarm (détecté à la capture) |
| `VBET_COOKIE` | — | Chaîne cookie brute (alternative) |
| `VBET_FETCH_COOLDOWN_S` | `1` | Délai entre cycles |
| `VBET_CAPTURE_WAIT_MS` | `15000` | Attente après chargement page |
| `VBET_CAPTURE_SWARM_WAIT_S` | `60` | Attente cookie Swarm |
| `PORT` | `8003` | Port serveur |
