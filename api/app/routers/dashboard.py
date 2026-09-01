"""`GET /dashboard` — a small, self-contained status page (CLAUDE.md Phase 6
offers "a simple Grafana dashboard or a lightweight status page"; Grafana
is explicitly marked a stretch goal in the tech stack, so this project
takes the lightweight-status-page path rather than standing up Prometheus
+ Grafana as two more containers for something optional).

Just serves a static HTML file that polls GET /metrics client-side and
draws a live queue-depth chart with plain <canvas> — no build step, no
external JS/CSS dependencies.
"""
from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter(tags=["dashboard"])

_DASHBOARD_HTML_PATH = Path(__file__).parent.parent / "static" / "dashboard.html"


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard() -> str:
    return _DASHBOARD_HTML_PATH.read_text(encoding="utf-8")
