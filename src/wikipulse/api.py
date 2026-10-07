"""Read-only serving API for the latest committed Iceberg-derived snapshot."""

from __future__ import annotations

import json
import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI(title="WikiPulse", version="0.1.0")
ROOT = Path(__file__).parents[2]
# The React app (web/) detects /api/health and switches to its "Local pipeline" mode.
WEB_DIST = Path(os.getenv("WEB_DIST", ROOT / "web/dist"))
LEGACY_HTML = Path(os.getenv("LEGACY_DASHBOARD", ROOT / "dashboard/index.html"))


def snapshot():
    path = Path(os.getenv("DATA_DIR", "data")) / "dashboard.json"
    if not path.exists():
        return {
            "generated_at": None,
            "batch_id": None,
            "batch_rows": 0,
            "queue": [],
            "alerts": [],
            "throughput": [],
        }
    data = json.loads(path.read_text())
    metrics_path = path.with_name("stream_metrics.json")
    if metrics_path.exists():
        data["metrics"] = json.loads(metrics_path.read_text())
    return data


@app.get("/api/health")
def health():
    data = snapshot()
    return {
        "status": "live" if data["generated_at"] else "waiting",
        "generated_at": data["generated_at"],
    }


@app.get("/api/dashboard")
def dashboard():
    return snapshot()


@app.get("/api/queue")
def queue():
    return snapshot()["queue"]


@app.get("/api/alerts")
def alerts():
    return snapshot()["alerts"]


@app.get("/api/metrics")
def metrics():
    return snapshot().get("metrics", {})


# Mounted last so the /api routes above take precedence over static files.
if WEB_DIST.is_dir():
    app.mount("/", StaticFiles(directory=WEB_DIST, html=True), name="web")
else:

    @app.get("/")
    def index():
        return FileResponse(LEGACY_HTML)
