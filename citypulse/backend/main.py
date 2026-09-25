"""
CityPulse — FastAPI Application Entry Point

Endpoints:
  GET  /api/v1/health-pulse           Overall city score + ward metrics + summary
  GET  /api/v1/wards                  GeoJSON-style ward data for map rendering
  GET  /api/v1/events/recent          Normalized recent events (filterable)
  POST /api/v1/simulate/incident      Inject demo scenario cluster
  WS   /ws/pulse                      Live WebSocket stream

Run:
  uvicorn main:app --reload --port 8000
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Set

from fastapi import FastAPI, File, Form, Query, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from correlation import CorrelationEngine, EventStore
from database import (
    async_count_complaints,
    async_get_complaint,
    async_insert_complaint,
    async_insert_event,
    async_insert_ward_snapshot,
    async_list_complaints,
    async_list_events,
    async_ward_trend,
    create_tables,
)
from models import (
    CivicEvent,
    ComplaintListResponse,
    ComplaintResponse,
    HealthPulseResponse,
    RecentEventsResponse,
    SimulateIncidentRequest,
    WardStatus,
    WebSocketMessage,
)
from simulator import WARDS, inject_scenario, start_all_workers
from summarizer import Summarizer

# ---------------------------------------------------------------------------
# Upload directory — images stored here, served as /uploads/<filename>
# ---------------------------------------------------------------------------
UPLOAD_DIR = Path(__file__).parent / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

ALLOWED_IMAGE_TYPES = {"image/jpeg", "image/png", "image/gif", "image/webp"}
MAX_IMAGE_BYTES = 10 * 1024 * 1024  # 10 MB

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("citypulse.main")

# ---------------------------------------------------------------------------
# Shared application state
# ---------------------------------------------------------------------------

store = EventStore()
engine = CorrelationEngine(store)
summarizer = Summarizer()

# WebSocket connection pool
_ws_clients: Set[WebSocket] = set()
_worker_tasks: list[asyncio.Task] = []

# Summarizer regenerates every N seconds
SUMMARY_INTERVAL_SECONDS = 20


# ---------------------------------------------------------------------------
# WebSocket broadcast helper
# ---------------------------------------------------------------------------

async def _broadcast(msg: WebSocketMessage) -> None:
    """Send a message to every connected WebSocket client. Remove dead connections."""
    if not _ws_clients:
        return
    payload = msg.model_dump_json()
    dead: list[WebSocket] = []
    for ws in list(_ws_clients):
        try:
            await ws.send_text(payload)
        except Exception:  # noqa: BLE001
            dead.append(ws)
    for ws in dead:
        _ws_clients.discard(ws)


# ---------------------------------------------------------------------------
# Event ingestion callback — fires for every new event from workers
# ---------------------------------------------------------------------------

async def _on_event(event: CivicEvent) -> None:
    """Store event, persist to DB, and push live update to WebSocket clients."""
    store.add(event)

    # Persist to database (non-blocking)
    await async_insert_event({
        "event_id":    event.event_id,
        "ward_id":     event.ward_id,
        "category":    event.category,
        "severity":    event.severity,
        "title":       event.title,
        "description": event.description,
        "lat":         event.coordinates.lat if event.coordinates else None,
        "lng":         event.coordinates.lng if event.coordinates else None,
        "timestamp":   event.timestamp,
    })

    msg = WebSocketMessage(
        type="new_event",
        payload={
            "event": json.loads(event.model_dump_json()),
        },
    )
    await _broadcast(msg)

    # Every 5 events, push a score update
    if store.count_last_hour() % 5 == 0:
        pulse = engine.full_pulse()
        score_msg = WebSocketMessage(
            type="score_update",
            payload={
                "city_health_score": pulse["city_health_score"],
                "active_alert_count": pulse["active_alert_count"],
                "ward_scores": {
                    wid: json.loads(ws.model_dump_json())
                    for wid, ws in pulse["ward_scores"].items()
                },
            },
        )
        await _broadcast(score_msg)


# ---------------------------------------------------------------------------
# Background summary loop
# ---------------------------------------------------------------------------

async def _summary_loop() -> None:
    """Regenerate AI summary every SUMMARY_INTERVAL_SECONDS seconds."""
    await asyncio.sleep(5)  # initial delay to let first events arrive
    while True:
        try:
            pulse = engine.full_pulse()
            recent = store.get_recent(50)
            summary_text, confidence = await summarizer.generate(
                top_correlations=pulse["correlations"],
                recent_events=recent,
                city_score=pulse["city_health_score"],
                ward_scores=pulse["ward_scores"],
            )
            msg = WebSocketMessage(
                type="new_summary",
                payload={
                    "summary": summary_text,
                    "confidence": confidence,
                    "city_health_score": pulse["city_health_score"],
                    "correlations": [
                        json.loads(c.model_dump_json()) for c in pulse["correlations"][:3]
                    ],
                },
            )
            await _broadcast(msg)
            logger.debug("Summary pushed to %d clients.", len(_ws_clients))
        except asyncio.CancelledError:
            return
        except Exception as exc:  # noqa: BLE001
            logger.error("Summary loop error: %s", exc)
        await asyncio.sleep(SUMMARY_INTERVAL_SECONDS)


# ---------------------------------------------------------------------------
# Background ward snapshot loop — saves health scores every 60 s
# ---------------------------------------------------------------------------

SNAPSHOT_INTERVAL_SECONDS = 60

async def _ward_snapshot_loop() -> None:
    """Persist ward health scores to DB every SNAPSHOT_INTERVAL_SECONDS seconds."""
    await asyncio.sleep(15)
    while True:
        try:
            ward_scores = engine.compute_ward_scores()
            for ws in ward_scores.values():
                await async_insert_ward_snapshot({
                    "ward_id":      ws.ward_id,
                    "ward_name":    ws.name,
                    "health_score": ws.health_score,
                    "status":       ws.status,
                    "active_events":ws.active_event_count,
                })
        except asyncio.CancelledError:
            return
        except Exception as exc:  # noqa: BLE001
            logger.warning("Ward snapshot error: %s", exc)
        await asyncio.sleep(SNAPSHOT_INTERVAL_SECONDS)


# ---------------------------------------------------------------------------
# Lifespan: start/stop workers
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _worker_tasks
    # Initialise database tables (idempotent)
    create_tables()
    logger.info("Starting CityPulse feed workers…")
    _worker_tasks = await start_all_workers(_on_event)
    summary_task = asyncio.create_task(_summary_loop(), name="summary_loop")
    _worker_tasks.append(summary_task)
    # Periodic ward snapshot task
    snapshot_task = asyncio.create_task(_ward_snapshot_loop(), name="ward_snapshot_loop")
    _worker_tasks.append(snapshot_task)
    logger.info("All workers started. CityPulse is live.")
    yield
    logger.info("Shutting down workers…")
    for task in _worker_tasks:
        task.cancel()
    await asyncio.gather(*_worker_tasks, return_exceptions=True)
    logger.info("CityPulse shutdown complete.")


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="CityPulse API",
    description="Live Civic Health Dashboard — AmiHacks 2026",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve the frontend static files from ../  (index.html at root)
_frontend_dir = os.path.join(os.path.dirname(__file__), "..")
app.mount("/static", StaticFiles(directory=_frontend_dir, html=True), name="static")

# Serve uploaded complaint images at /uploads/<filename>
app.mount("/uploads", StaticFiles(directory=str(UPLOAD_DIR)), name="uploads")


# ---------------------------------------------------------------------------
# REST Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/v1/health-pulse", response_model=HealthPulseResponse, tags=["Dashboard"])
async def get_health_pulse():
    """
    Returns current overall city health score, active alerts,
    ward metrics, top correlations, and the latest AI summary.
    """
    pulse = engine.full_pulse()
    recent = store.get_recent(50)

    # Ensure a summary is available
    if not summarizer.last_summary or summarizer.last_summary.startswith("Initializing"):
        await summarizer.generate(
            top_correlations=pulse["correlations"],
            recent_events=recent,
            city_score=pulse["city_health_score"],
            ward_scores=pulse["ward_scores"],
        )

    city_score = pulse["city_health_score"]
    status = "critical" if city_score < 40 else "moderate" if city_score < 70 else "normal"

    return HealthPulseResponse(
        city_health_score=city_score,
        status=status,
        active_alert_count=pulse["active_alert_count"],
        total_events_last_hour=pulse["total_events_last_hour"],
        wards=list(pulse["ward_scores"].values()),
        top_correlations=pulse["correlations"][:5],
        ai_summary=summarizer.last_summary,
        summary_confidence=summarizer.last_confidence,
    )


@app.get("/api/v1/wards", tags=["Dashboard"])
async def get_wards():
    """
    Returns GeoJSON-compatible ward data with current health status
    for map polygon/marker rendering.
    """
    ward_scores = engine.compute_ward_scores()
    features = []
    for ward_id, ws in ward_scores.items():
        info = WARDS.get(ward_id, {})
        features.append({
            "type": "Feature",
            "properties": {
                "ward_id": ward_id,
                "name": ws.name,
                "health_score": ws.health_score,
                "status": ws.status,
                "active_event_count": ws.active_event_count,
                "dominant_category": ws.dominant_category,
                "last_updated": ws.last_updated.isoformat(),
            },
            "geometry": {
                "type": "Point",
                "coordinates": [info.get("lng", 0), info.get("lat", 0)],
            },
        })

    return {
        "type": "FeatureCollection",
        "features": features,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/v1/events/recent", response_model=RecentEventsResponse, tags=["Events"])
async def get_recent_events(
    category: Optional[str] = Query(None, description="Filter by category: weather|transit|incident_311|air_quality"),
    min_severity: float = Query(0.0, ge=0.0, le=1.0, description="Minimum severity filter"),
    limit: int = Query(50, ge=1, le=200, description="Max events to return"),
):
    """
    Returns normalized recent feed events.
    Supports filtering by category and minimum severity.
    """
    cat = category if category in {"weather", "transit", "incident_311", "air_quality"} else None
    events = store.get_recent(n=limit, category=cat, min_severity=min_severity)
    return RecentEventsResponse(
        events=events,
        total=len(events),
        filtered_by_category=cat,
        filtered_by_min_severity=min_severity,
    )


@app.post("/api/v1/simulate/incident", tags=["Demo"])
async def simulate_incident(request: SimulateIncidentRequest):
    """
    DEMO ENDPOINT: Immediately inject a scenario cluster to trigger live dashboard updates.
    
    Scenarios: flash_flood | power_outage | transit_failure | air_quality_spike | custom
    """
    if request.scenario == "custom" and request.custom_events:
        injected: list[CivicEvent] = []
        for ev_data in request.custom_events:
            from models import Coordinates
            ward = ev_data.get("ward_id", "W01")
            w_info = WARDS.get(ward, WARDS["W01"])
            ev = CivicEvent(
                ward_id=ward,
                category=ev_data.get("category", "incident_311"),
                severity=float(ev_data.get("severity", 0.7)),
                coordinates=Coordinates(lat=w_info["lat"], lng=w_info["lng"]),
                title=ev_data.get("title", "Custom Incident"),
                description=ev_data.get("description", "Custom simulated incident."),
                raw_payload={"source": "Custom Demo Injection"},
            )
            await _on_event(ev)
            injected.append(ev)
    else:
        injected = await inject_scenario(
            scenario=request.scenario,
            ward_id=request.ward_id,
            callback=_on_event,
        )

    # Trigger correlation detection and push updated summary
    pulse = engine.full_pulse()
    recent = store.get_recent(50)
    summary_text, confidence = await summarizer.generate(
        top_correlations=pulse["correlations"],
        recent_events=recent,
        city_score=pulse["city_health_score"],
        ward_scores=pulse["ward_scores"],
    )

    await _broadcast(WebSocketMessage(
        type="new_summary",
        payload={
            "summary": summary_text,
            "confidence": confidence,
            "city_health_score": pulse["city_health_score"],
            "triggered_by": f"simulate/{request.scenario}",
            "correlations": [
                json.loads(c.model_dump_json()) for c in pulse["correlations"][:3]
            ],
        },
    ))

    return {
        "status": "ok",
        "scenario": request.scenario,
        "events_injected": len(injected),
        "event_ids": [e.event_id for e in injected],
        "new_city_score": pulse["city_health_score"],
        "summary": summary_text,
        "confidence": confidence,
    }


@app.get("/api/v1/correlations", tags=["Analytics"])
async def get_correlations():
    """Returns current detected correlation clusters with confidence scores."""
    correlations = engine.detect_correlations()
    return {
        "correlations": [json.loads(c.model_dump_json()) for c in correlations],
        "count": len(correlations),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/v1/anomalies", tags=["Analytics"])
async def get_anomalies():
    """Returns current spike/anomaly detections."""
    anomalies = engine.detect_anomalies()
    return {
        "anomalies": anomalies,
        "count": len(anomalies),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Complaint (311 Portal) Endpoints
# ---------------------------------------------------------------------------

@app.post("/api/v1/complaints", response_model=ComplaintResponse, tags=["Complaints"])
async def submit_complaint(
    city:           str        = Form("Jaipur"),
    submitter_name: Optional[str] = Form(None),
    contact:        Optional[str] = Form(None),
    ward:           str        = Form(...),
    ward_id:        Optional[str] = Form(None),
    issue_type:     str        = Form(...),
    description:    str        = Form(...),
    location:       Optional[str] = Form(None),
    lat:            Optional[float] = Form(None),
    lng:            Optional[float] = Form(None),
    image:          Optional[UploadFile] = File(None),
):
    """
    Submit a new civic complaint via the 311 portal.
    Accepts multipart/form-data so an optional image can be attached.
    """
    if len(description) < 10:
        return JSONResponse(status_code=422, content={"detail": "Description too short (min 10 chars)."})

    # Handle optional image upload
    image_path: Optional[str] = None
    image_filename: Optional[str] = None

    if image and image.filename:
        # Validate content type
        if image.content_type not in ALLOWED_IMAGE_TYPES:
            return JSONResponse(
                status_code=415,
                content={"detail": f"Unsupported image type '{image.content_type}'. Use JPEG, PNG, GIF, or WebP."},
            )
        # Read and size-check
        contents = await image.read()
        if len(contents) > MAX_IMAGE_BYTES:
            return JSONResponse(status_code=413, content={"detail": "Image too large (max 10 MB)."})

        # Generate a unique safe filename
        ext = Path(image.filename).suffix.lower() or ".jpg"
        safe_name = f"{uuid.uuid4().hex}{ext}"
        dest = UPLOAD_DIR / safe_name
        dest.write_bytes(contents)

        image_path = f"uploads/{safe_name}"      # relative URL path served by /uploads/
        image_filename = image.filename
        logger.info("Saved complaint image: %s (%d bytes)", safe_name, len(contents))

    # Generate reference ID
    ref_id = "CP-" + uuid.uuid4().hex[:8].upper()

    saved = await async_insert_complaint({
        "ref_id":         ref_id,
        "city":           city,
        "submitter_name": submitter_name,
        "contact":        contact,
        "ward":           ward,
        "ward_id":        ward_id,
        "issue_type":     issue_type,
        "description":    description,
        "location":       location,
        "lat":            lat,
        "lng":            lng,
        "image_path":     image_path,
        "image_filename": image_filename,
        "status":         "open",
    })

    logger.info("Complaint submitted: ref=%s city=%s ward=%s type=%s", ref_id, city, ward, issue_type)
    return ComplaintResponse(**saved)


@app.get("/api/v1/complaints", response_model=ComplaintListResponse, tags=["Complaints"])
async def list_complaints(
    city:   Optional[str] = Query(None, description="Filter by city name"),
    ward:   Optional[str] = Query(None, description="Filter by ward name"),
    status: Optional[str] = Query(None, description="Filter by status: open|in_progress|resolved"),
    limit:  int = Query(50, ge=1, le=200),
    page:   int = Query(1, ge=1),
):
    """
    List submitted civic complaints. Supports filtering by city, ward, and status.
    """
    offset = (page - 1) * limit
    rows  = await async_list_complaints(city=city, ward=ward, status=status, limit=limit, offset=offset)
    total = await async_count_complaints(city=city)
    return ComplaintListResponse(
        complaints=[ComplaintResponse(**r) for r in rows],
        total=total,
        page=page,
        limit=limit,
    )


@app.get("/api/v1/complaints/{ref_id}", response_model=ComplaintResponse, tags=["Complaints"])
async def get_complaint(ref_id: str):
    """Fetch a single complaint by its reference ID (e.g. CP-A1B2C3D4)."""
    row = await async_get_complaint(ref_id.upper())
    if not row:
        return JSONResponse(status_code=404, content={"detail": f"Complaint {ref_id} not found."})
    return ComplaintResponse(**row)


@app.get("/api/v1/db/events", tags=["Database"])
async def list_db_events(
    category:     Optional[str] = Query(None),
    ward_id:      Optional[str] = Query(None),
    min_severity: float = Query(0.0, ge=0.0, le=1.0),
    limit:        int   = Query(100, ge=1, le=500),
):
    """Query persisted civic events from the database."""
    rows = await async_list_events(category=category, ward_id=ward_id,
                                   min_severity=min_severity, limit=limit)
    return {"events": rows, "total": len(rows)}


@app.get("/api/v1/db/ward-trend/{ward_id}", tags=["Database"])
async def get_ward_trend(ward_id: str, city: str = Query("Jaipur"), limit: int = Query(50, ge=1, le=200)):
    """Returns recent ward health-score snapshots for trend visualisation."""
    rows = await async_ward_trend(ward_id=ward_id.upper(), city=city, limit=limit)
    return {"ward_id": ward_id, "city": city, "snapshots": rows, "total": len(rows)}


@app.get("/", tags=["System"])
async def root():
    return {
        "service": "CityPulse API",
        "version": "1.0.0",
        "status": "live",
        "docs": "/docs",
        "websocket": "ws://localhost:8000/ws/pulse",
    }


@app.get("/api/v1/ping", tags=["System"])
async def ping():
    return {"pong": True, "ts": datetime.now(timezone.utc).isoformat()}


# ---------------------------------------------------------------------------
# WebSocket endpoint
# ---------------------------------------------------------------------------

@app.websocket("/ws/pulse")
async def websocket_pulse(ws: WebSocket):
    """
    Live WebSocket stream. Pushes:
      - new_event:            every incoming civic event
      - score_update:         every 5 events (ward + city scores)
      - new_summary:          every 20 seconds (AI narrative)
      - correlation_detected: when new clusters are found
      - ping:                 every 30 seconds to keep-alive
    """
    await ws.accept()
    _ws_clients.add(ws)
    client = ws.client.host if ws.client else "unknown"
    logger.info("WebSocket client connected: %s  (total=%d)", client, len(_ws_clients))

    try:
        # Send immediate state snapshot on connect
        pulse = engine.full_pulse()
        await ws.send_text(WebSocketMessage(
            type="score_update",
            payload={
                "city_health_score": pulse["city_health_score"],
                "active_alert_count": pulse["active_alert_count"],
                "ward_scores": {
                    wid: json.loads(ws_obj.model_dump_json())
                    for wid, ws_obj in pulse["ward_scores"].items()
                },
                "summary": summarizer.last_summary,
                "confidence": summarizer.last_confidence,
                "correlations": [
                    json.loads(c.model_dump_json()) for c in pulse["correlations"][:3]
                ],
            },
        ).model_dump_json())

        # Keep-alive ping loop
        while True:
            await asyncio.sleep(30)
            await ws.send_text(WebSocketMessage(
                type="ping",
                payload={"ts": datetime.now(timezone.utc).isoformat()},
            ).model_dump_json())

    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected: %s", client)
    except Exception as exc:  # noqa: BLE001
        logger.warning("WebSocket error (%s): %s", client, exc)
    finally:
        _ws_clients.discard(ws)
        logger.info("WebSocket clients remaining: %d", len(_ws_clients))
