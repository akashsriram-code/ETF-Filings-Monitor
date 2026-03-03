# ETF Filing Detection Engine

This repo now supports two operating modes:

- Backend mode (FastAPI + stream listener): `app/` for persistent TCP PDS ingestion.
- GitHub Pages mode (static + scheduled poller): `scripts/poll_filings.py` + `data/*.json` updated by GitHub Actions every 10 minutes.

## GitHub Pages Mode

GitHub Pages cannot run a persistent listener.  
This mode uses a scheduled GitHub Action (`*/10 * * * *`) to poll SEC current filings feed, apply ETF logic, send emails, and publish results to static JSON.

### Logic in Pages Mode

- Target forms: `485APOS`, `485BPOS`, `S-1`
- `S-1` filter scans first 10,000 chars for:
  - `Bitcoin`
  - `Ethereum`
  - `Digital Asset`
  - `Spot`
  - `Coinbase Custody`
- Synopsis generation via Thomson Reuters OpenArena workflow API
- Email delivery via Gmail SMTP (App Password)
- Dashboard reads:
  - `data/status.json`
  - `data/alerts.json`

### Important Limitation

- Schedule timing on GitHub Actions is best-effort. Runs are not guaranteed at exact minute boundaries.

## Quick Start

1. Install dependencies:
```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
playwright install chromium
```

2. Create `.env` (example):
```env
AUTO_START_STREAM=false
PDS_HOST=127.0.0.1
PDS_PORT=9000
REPORTER_EMAIL=reporter@example.com

SEC_USER_AGENT=ETF-Filings-Monitor/1.0 (reporter@example.com)

OPENARENA_BASE_URL=https://aiopenarena.gcs.int.thomsonreuters.com
OPENARENA_BEARER_TOKEN=
OPENARENA_WORKFLOW_ID=
OPENARENA_TIMEOUT_SECONDS=60

SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USERNAME=your.gmail@gmail.com
SMTP_PASSWORD=your_16_char_app_password
SMTP_USE_TLS=true
FROM_EMAIL=your.gmail@gmail.com
```

3. Run:
```bash
uvicorn app.main:app --reload
```

4. Open:
`http://127.0.0.1:8000`

## Core Parser Function

`app/parser.py` exposes:
- `process_pds_stream(buffer) -> (filings, remainder)`

It supports partial TCP chunks by returning unconsumed `remainder` for next read.

## API Endpoints

- `GET /api/health`
- `GET /api/status`
- `GET /api/alerts`
- `POST /api/start`
- `POST /api/stop`
- `POST /api/ingest` with JSON `{ "payload": "<raw pds text>" }`

## OpenArena Workflow Discovery

If you only have a bearer token and need a valid `workflow_id`:

```bash
$env:OPENARENA_BEARER_TOKEN="<your_token>"
python scripts/list_openarena_workflows.py --only-accessible --page-size 50
```

Filter by title keyword:

```bash
python scripts/list_openarena_workflows.py --title-contains etf --title-contains filing
```

The script prints a suggested `OPENARENA_WORKFLOW_ID` from top keyword match.

## GitHub Actions Setup

Workflow file:
- `.github/workflows/poll-filings.yml`
- `.github/workflows/backfill-filings.yml`

Add these repository secrets:
- `SEC_USER_AGENT`
- `REPORTER_EMAIL`
- `SMTP_USERNAME` (your Gmail address)
- `SMTP_PASSWORD` (Gmail App Password)
- `FROM_EMAIL` (same Gmail address)
- `OPENARENA_BEARER_TOKEN`
- `OPENARENA_WORKFLOW_ID`

Then:
1. Push to GitHub.
2. Enable GitHub Pages to serve from your main branch root.
3. Run `Poll SEC Filings` workflow once manually via `workflow_dispatch`.
4. Confirm `data/status.json` and `data/alerts.json` are updating.

### One-Week Backfill

To backfill the past week:
1. Go to `Actions -> Backfill SEC Filings -> Run workflow`.
2. Set `backfill_days` to `7` (or another number).
3. Run the workflow.

The run will combine:
- Current feed entries
- SEC daily master index entries for the last 7 days

Status output in `data/status.json` includes:
- `feed_entries`
- `backfill_entries`
- `backfill_days`

### Render Deployment

This repo includes:
- `Dockerfile`
- `render.yaml`

Steps:
1. Push this repo to GitHub.
2. In Render, create a Blueprint deployment from the repo.
3. Set environment secrets in Render dashboard:
   - `PDS_HOST`, `PDS_PORT`
   - `SEC_USER_AGENT`
   - `REPORTER_EMAIL`
   - `SMTP_HOST=smtp.gmail.com`, `SMTP_PORT=587`, `SMTP_USE_TLS=true`
   - `SMTP_USERNAME`, `SMTP_PASSWORD`, `FROM_EMAIL`
   - `OPENARENA_BEARER_TOKEN`, `OPENARENA_WORKFLOW_ID`
4. Ensure `AUTO_START_STREAM=true` in production.

### Docker Run (Any Host)

```bash
docker build -t etf-filings-monitor .
docker run --env-file .env -p 8000:8000 etf-filings-monitor
```

## Testing

```bash
pytest
```
