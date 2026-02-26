# ETF Filing Detection Engine

This repo now supports two operating modes:

- Backend mode (FastAPI + stream listener): `app/` for persistent TCP PDS ingestion.
- GitHub Pages mode (static + scheduled poller): `scripts/poll_filings.py` + `data/*.json` updated by GitHub Actions every 10 minutes.

## GitHub Pages Mode (What You Asked For)

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
- Synopsis generation via Gemini API
- Email delivery via Resend
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

GEMINI_API_KEY=
GEMINI_MODEL=gemini-1.5-pro

SMTP_HOST=smtp.yourmail.com
SMTP_PORT=587
SMTP_USERNAME=
SMTP_PASSWORD=
SMTP_USE_TLS=true
FROM_EMAIL=alerts@yourdomain.com

RESEND_API_KEY=
RESEND_FROM_EMAIL=alerts@yourdomain.com

# Optional SMTP fallback:
# SMTP_HOST=smtp.yourmail.com
# SMTP_PORT=587
# SMTP_USERNAME=
# SMTP_PASSWORD=
# SMTP_USE_TLS=true
# FROM_EMAIL=alerts@yourdomain.com
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

## GitHub Actions Setup (Required for Pages Mode)

Workflow file:
- `.github/workflows/poll-filings.yml`
- `.github/workflows/backfill-filings.yml`

Add these repository secrets:
- `SEC_USER_AGENT`
- `REPORTER_EMAIL`
- `RESEND_API_KEY`
- `RESEND_FROM_EMAIL`
- `GEMINI_API_KEY`

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

## Hosting Notes

- GitHub Pages mode works for scheduled polling only.
- If you need true real-time TCP PDS listening, use an always-on backend host (Render/Railway/Fly/VM).

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
   - `GEMINI_API_KEY`
   - `RESEND_API_KEY`, `RESEND_FROM_EMAIL`
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
