"""
CityPulse — Unified Data Models
Pydantic v2 schemas for all civic event types and API responses.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Core civic event schema
# ---------------------------------------------------------------------------

EventCategory = Literal["weather", "transit", "incident_311", "air_quality"]


class Coordinates(BaseModel):
    lat: float = Field(..., ge=-90, le=90)
    lng: float = Field(..., ge=-180, le=180)


class CivicEvent(BaseModel):
    """Unified normalized schema for all incoming civic data feeds."""

    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    ward_id: str = Field(..., description="Ward or district identifier")
    category: EventCategory
    severity: float = Field(..., ge=0.0, le=1.0, description="0 = normal, 1 = critical")
    coordinates: Coordinates
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    title: str
    description: str
    raw_payload: dict[str, Any] = Field(default_factory=dict)

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}

    def age_seconds(self) -> float:
        """How many seconds ago this event was ingested."""
        now = datetime.now(timezone.utc)
        ts = self.timestamp if self.timestamp.tzinfo else self.timestamp.replace(tzinfo=timezone.utc)
        return (now - ts).total_seconds()


# ---------------------------------------------------------------------------
# Correlation / anomaly output
# ---------------------------------------------------------------------------

class CorrelationCluster(BaseModel):
    """A detected multi-feed correlation cluster within a geographic proximity."""

    cluster_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    ward_id: str
    event_ids: list[str]
    categories_involved: list[EventCategory]
    confidence: float = Field(..., ge=0.0, le=1.0, description="Statistical confidence 0–1")
    description: str = Field(..., description="Human-readable cluster description")
    disclaimer: str = Field(
        default="This represents a probable correlation, NOT a confirmed causal link.",
        description="Epistemic honesty disclaimer"
    )
    detected_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    severity_aggregate: float = Field(..., ge=0.0, le=1.0)

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


# ---------------------------------------------------------------------------
# Ward health
# ---------------------------------------------------------------------------

class WardStatus(BaseModel):
    ward_id: str
    name: str
    health_score: float = Field(..., ge=0.0, le=100.0, description="0 = critical, 100 = perfect")
    status: Literal["normal", "moderate", "critical"]
    active_event_count: int = 0
    coordinates: Coordinates
    radius_km: float = 2.5
    dominant_category: Optional[EventCategory] = None
    last_updated: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


# ---------------------------------------------------------------------------
# API response envelopes
# ---------------------------------------------------------------------------

class HealthPulseResponse(BaseModel):
    city_health_score: float
    status: Literal["normal", "moderate", "critical"]
    active_alert_count: int
    total_events_last_hour: int
    wards: list[WardStatus]
    top_correlations: list[CorrelationCluster]
    ai_summary: str
    summary_confidence: float
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


class RecentEventsResponse(BaseModel):
    events: list[CivicEvent]
    total: int
    filtered_by_category: Optional[EventCategory] = None
    filtered_by_min_severity: float = 0.0


class SimulateIncidentRequest(BaseModel):
    scenario: Literal["flash_flood", "power_outage", "transit_failure", "air_quality_spike", "custom"] = "flash_flood"
    ward_id: Optional[str] = None
    custom_events: Optional[list[dict[str, Any]]] = None


class WebSocketMessage(BaseModel):
    type: Literal["new_event", "score_update", "new_summary", "correlation_detected", "ping"]
    payload: dict[str, Any]
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


# ---------------------------------------------------------------------------
# Complaint schemas
# ---------------------------------------------------------------------------

class ComplaintResponse(BaseModel):
    """API response shape for a persisted complaint."""
    id: int
    ref_id: str
    city: str
    submitter_name: Optional[str]
    contact: Optional[str]
    ward: str
    issue_type: str
    description: str
    location: Optional[str]
    image_path: Optional[str]
    image_filename: Optional[str]
    status: str
    ward_id: Optional[str]
    lat: Optional[float]
    lng: Optional[float]
    created_at: Optional[str]
    updated_at: Optional[str]


class ComplaintListResponse(BaseModel):
    complaints: list[ComplaintResponse]
    total: int
    page: int
    limit: int
