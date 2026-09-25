"""
CityPulse — SQLite Database Layer
Uses SQLAlchemy (sync) for simplicity — all DB calls are wrapped in
run_in_executor so they don't block the async event loop.

Tables
------
complaints        Civic complaints submitted via the 311 portal (with optional image)
civic_events      Persisted feed events (weather / transit / 311 / air_quality)
ward_snapshots    Periodic ward health-score snapshots for trend analysis
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from functools import partial
from typing import Any, Optional

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    Integer,
    String,
    Text,
    create_engine,
    event,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

logger = logging.getLogger("citypulse.database")

# ---------------------------------------------------------------------------
# Database path — overridable via DB_PATH env var
# ---------------------------------------------------------------------------

_DB_PATH = os.getenv("DB_PATH", os.path.join(os.path.dirname(__file__), "citypulse.db"))
_ENGINE_URL = f"sqlite:///{_DB_PATH}"

engine = create_engine(
    _ENGINE_URL,
    connect_args={"check_same_thread": False},   # required for SQLite + FastAPI
    echo=False,
)

# Enable WAL mode for better concurrent read performance
@event.listens_for(engine, "connect")
def _set_wal_mode(dbapi_conn, _):
    dbapi_conn.execute("PRAGMA journal_mode=WAL")
    dbapi_conn.execute("PRAGMA foreign_keys=ON")


SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


# ---------------------------------------------------------------------------
# ORM base
# ---------------------------------------------------------------------------

class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# ORM models
# ---------------------------------------------------------------------------

class DBComplaint(Base):
    """A civic complaint submitted through the 311 portal."""
    __tablename__ = "complaints"

    id            = Column(Integer, primary_key=True, index=True)
    ref_id        = Column(String(32), unique=True, index=True, nullable=False)
    city          = Column(String(64), nullable=False, default="Jaipur")
    submitter_name= Column(String(120), nullable=True)
    contact       = Column(String(120), nullable=True)
    ward          = Column(String(120), nullable=False)
    issue_type    = Column(String(120), nullable=False)
    description   = Column(Text, nullable=False)
    location      = Column(String(200), nullable=True)
    image_path    = Column(String(512), nullable=True)   # relative path under uploads/
    image_filename= Column(String(256), nullable=True)   # original filename
    status        = Column(String(32), nullable=False, default="open")   # open|in_progress|resolved
    ward_id       = Column(String(8),  nullable=True)    # W01–W08 etc.
    lat           = Column(Float, nullable=True)
    lng           = Column(Float, nullable=True)
    created_at    = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at    = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                           onupdate=lambda: datetime.now(timezone.utc), nullable=False)


class DBCivicEvent(Base):
    """Persisted copy of every CivicEvent ingested from feed workers."""
    __tablename__ = "civic_events"

    id          = Column(Integer, primary_key=True, index=True)
    event_id    = Column(String(64), unique=True, index=True, nullable=False)
    city        = Column(String(64), nullable=False, default="Jaipur")
    ward_id     = Column(String(8),  nullable=False, index=True)
    category    = Column(String(32), nullable=False, index=True)
    severity    = Column(Float,      nullable=False)
    title       = Column(String(200), nullable=False)
    description = Column(Text,       nullable=True)
    lat         = Column(Float, nullable=True)
    lng         = Column(Float, nullable=True)
    timestamp   = Column(DateTime, index=True, nullable=False)
    created_at  = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)


class DBWardSnapshot(Base):
    """Periodic snapshot of a ward's health score — used for trend charts."""
    __tablename__ = "ward_snapshots"

    id           = Column(Integer, primary_key=True, index=True)
    city         = Column(String(64), nullable=False, default="Jaipur")
    ward_id      = Column(String(8),  nullable=False, index=True)
    ward_name    = Column(String(120), nullable=False)
    health_score = Column(Float, nullable=False)
    status       = Column(String(32), nullable=False)
    active_events= Column(Integer, nullable=False, default=0)
    snapped_at   = Column(DateTime, index=True, default=lambda: datetime.now(timezone.utc), nullable=False)


# ---------------------------------------------------------------------------
# Schema creation
# ---------------------------------------------------------------------------

def create_tables() -> None:
    """Create all tables if they don't exist. Safe to call multiple times."""
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables ready at: %s", _DB_PATH)


# ---------------------------------------------------------------------------
# Synchronous CRUD helpers (called via run_in_executor from async code)
# ---------------------------------------------------------------------------

def _get_db() -> Session:
    return SessionLocal()


# ── Complaints ────────────────────────────────────────────────────────────────

def db_insert_complaint(data: dict[str, Any]) -> dict[str, Any]:
    """Insert a new complaint row and return the saved record as a dict."""
    db = _get_db()
    try:
        obj = DBComplaint(**data)
        db.add(obj)
        db.commit()
        db.refresh(obj)
        return _complaint_to_dict(obj)
    finally:
        db.close()


def db_list_complaints(
    city: Optional[str] = None,
    ward: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    db = _get_db()
    try:
        q = db.query(DBComplaint)
        if city:
            q = q.filter(DBComplaint.city == city)
        if ward:
            q = q.filter(DBComplaint.ward == ward)
        if status:
            q = q.filter(DBComplaint.status == status)
        rows = q.order_by(DBComplaint.created_at.desc()).offset(offset).limit(limit).all()
        return [_complaint_to_dict(r) for r in rows]
    finally:
        db.close()


def db_get_complaint(ref_id: str) -> Optional[dict[str, Any]]:
    db = _get_db()
    try:
        obj = db.query(DBComplaint).filter(DBComplaint.ref_id == ref_id).first()
        return _complaint_to_dict(obj) if obj else None
    finally:
        db.close()


def db_count_complaints(city: Optional[str] = None) -> int:
    db = _get_db()
    try:
        q = db.query(DBComplaint)
        if city:
            q = q.filter(DBComplaint.city == city)
        return q.count()
    finally:
        db.close()


def _complaint_to_dict(obj: DBComplaint) -> dict[str, Any]:
    return {
        "id":             obj.id,
        "ref_id":         obj.ref_id,
        "city":           obj.city,
        "submitter_name": obj.submitter_name,
        "contact":        obj.contact,
        "ward":           obj.ward,
        "issue_type":     obj.issue_type,
        "description":    obj.description,
        "location":       obj.location,
        "image_path":     obj.image_path,
        "image_filename": obj.image_filename,
        "status":         obj.status,
        "ward_id":        obj.ward_id,
        "lat":            obj.lat,
        "lng":            obj.lng,
        "created_at":     obj.created_at.isoformat() if obj.created_at else None,
        "updated_at":     obj.updated_at.isoformat() if obj.updated_at else None,
    }


# ── Civic Events ──────────────────────────────────────────────────────────────

def db_insert_event(data: dict[str, Any]) -> None:
    """Persist a civic event. Silently ignores duplicate event_ids."""
    db = _get_db()
    try:
        exists = db.query(DBCivicEvent).filter(DBCivicEvent.event_id == data["event_id"]).first()
        if exists:
            return
        obj = DBCivicEvent(**data)
        db.add(obj)
        db.commit()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to persist event %s: %s", data.get("event_id"), exc)
        db.rollback()
    finally:
        db.close()


def db_list_events(
    category: Optional[str] = None,
    ward_id: Optional[str] = None,
    min_severity: float = 0.0,
    limit: int = 100,
) -> list[dict[str, Any]]:
    db = _get_db()
    try:
        q = db.query(DBCivicEvent)
        if category:
            q = q.filter(DBCivicEvent.category == category)
        if ward_id:
            q = q.filter(DBCivicEvent.ward_id == ward_id)
        if min_severity > 0:
            q = q.filter(DBCivicEvent.severity >= min_severity)
        rows = q.order_by(DBCivicEvent.timestamp.desc()).limit(limit).all()
        return [
            {
                "event_id":    r.event_id,
                "city":        r.city,
                "ward_id":     r.ward_id,
                "category":    r.category,
                "severity":    r.severity,
                "title":       r.title,
                "description": r.description,
                "lat":         r.lat,
                "lng":         r.lng,
                "timestamp":   r.timestamp.isoformat() if r.timestamp else None,
            }
            for r in rows
        ]
    finally:
        db.close()


# ── Ward Snapshots ────────────────────────────────────────────────────────────

def db_insert_ward_snapshot(data: dict[str, Any]) -> None:
    db = _get_db()
    try:
        db.add(DBWardSnapshot(**data))
        db.commit()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to persist ward snapshot: %s", exc)
        db.rollback()
    finally:
        db.close()


def db_ward_trend(ward_id: str, city: str = "Jaipur", limit: int = 50) -> list[dict[str, Any]]:
    db = _get_db()
    try:
        rows = (
            db.query(DBWardSnapshot)
            .filter(DBWardSnapshot.ward_id == ward_id, DBWardSnapshot.city == city)
            .order_by(DBWardSnapshot.snapped_at.desc())
            .limit(limit)
            .all()
        )
        return [
            {
                "ward_id":      r.ward_id,
                "ward_name":    r.ward_name,
                "health_score": r.health_score,
                "status":       r.status,
                "active_events":r.active_events,
                "snapped_at":   r.snapped_at.isoformat() if r.snapped_at else None,
            }
            for r in rows
        ]
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Async wrappers — run sync DB calls in a thread pool so FastAPI stays non-blocking
# ---------------------------------------------------------------------------

async def async_insert_complaint(data: dict[str, Any]) -> dict[str, Any]:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, partial(db_insert_complaint, data))


async def async_list_complaints(
    city: Optional[str] = None,
    ward: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None, partial(db_list_complaints, city, ward, status, limit, offset)
    )


async def async_get_complaint(ref_id: str) -> Optional[dict[str, Any]]:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, partial(db_get_complaint, ref_id))


async def async_count_complaints(city: Optional[str] = None) -> int:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, partial(db_count_complaints, city))


async def async_insert_event(data: dict[str, Any]) -> None:
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, partial(db_insert_event, data))


async def async_insert_ward_snapshot(data: dict[str, Any]) -> None:
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, partial(db_insert_ward_snapshot, data))


async def async_ward_trend(ward_id: str, city: str = "Jaipur", limit: int = 50) -> list[dict[str, Any]]:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, partial(db_ward_trend, ward_id, city, limit))
