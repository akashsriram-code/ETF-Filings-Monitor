from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.config import Settings, get_settings
from app.engine import FilingEngine
from app.models import IngestPayload

ROOT_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = ROOT_DIR / "static"
TEMPLATE_DIR = ROOT_DIR / "templates"


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings: Settings = get_settings()
    settings.pdf_output_dir.mkdir(parents=True, exist_ok=True)

    engine = FilingEngine(settings)
    app.state.settings = settings
    app.state.engine = engine

    if settings.auto_start_stream:
        await engine.start()

    yield

    await engine.stop()


app = FastAPI(title="ETF Filing Detection Engine", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
templates = Jinja2Templates(directory=str(TEMPLATE_DIR))


@app.get("/", response_class=HTMLResponse)
async def home(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "title": "ETF Filing Detection Engine",
        },
    )


@app.get("/api/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/status")
async def status(request: Request):
    return await request.app.state.engine.get_status()


@app.get("/api/alerts")
async def alerts(request: Request):
    return await request.app.state.engine.get_alerts()


@app.post("/api/start")
async def start_stream(request: Request):
    await request.app.state.engine.start()
    return JSONResponse({"started": True})


@app.post("/api/stop")
async def stop_stream(request: Request):
    await request.app.state.engine.stop()
    return JSONResponse({"stopped": True})


@app.post("/api/ingest")
async def ingest_payload(request: Request, payload: IngestPayload):
    if not payload.payload.strip():
        raise HTTPException(status_code=400, detail="payload is required")
    filings_found = await request.app.state.engine.ingest_payload(payload.payload)
    return JSONResponse({"accepted": True, "filings_found": filings_found})
