"""
CityPulse Jaipur — Spatial-Temporal Correlation & Anomaly Engine

Responsibilities:
  1. Maintain a rolling 60-minute in-memory event window.
  2. Per ward, compute a Civic Health Score (0–100).
  3. Detect multi-feed correlation clusters within geographic proximity.
  4. Attach confidence scores and epistemic disclaimers to every finding.
"""
from __future__ import annotations

import logging
import math
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Optional

from models import CivicEvent, CorrelationCluster, EventCategory, WardStatus
from simulator import WARDS

logger = logging.getLogger("citypulse.correlation")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

WINDOW_MINUTES: int = 60          # Rolling window size
MAX_EVENTS: int = 2000            # Hard cap on in-memory events
PROXIMITY_KM: float = 3.0         # Geo proximity threshold for clustering

# Per-category severity weights for health score calculation
CATEGORY_WEIGHTS: dict[EventCategory, float] = {
    "weather":      0.30,
    "transit":      0.25,
    "incident_311": 0.30,
    "air_quality":  0.15,
}

# Correlation detection thresholds (average severity in window for that category)
CORRELATION_THRESHOLDS: dict[EventCategory, float] = {
    "weather":      0.50,
    "transit":      0.45,
    "incident_311": 0.50,
    "air_quality":  0.55,
}

# Pre-defined correlation rules: list of (categories, description template, base_confidence)
CORRELATION_RULES: list[tuple[list[EventCategory], str, float]] = [
    (
        ["weather", "transit", "incident_311"],
        "Heavy monsoon rainfall in {ward} is likely causing RSRTC delays and a surge of JMC waterlogging/infrastructure complaints.",
        0.84,
    ),
    (
        ["weather", "incident_311"],
        "Storm conditions in {ward} correlate with a spike in JMC 311 complaints (waterlogged roads, neem trees down, sewer overflows).",
        0.76,
    ),
    (
        ["weather", "air_quality"],
        "Aandhi/dust storm in {ward} is possibly trapping particulate matter, worsening RSPCB AQI readings.",
        0.70,
    ),
    (
        ["transit", "incident_311"],
        "RSRTC disruptions in {ward} coincide with road infrastructure complaints — possible shared root cause (monsoon flooding).",
        0.71,
    ),
    (
        ["air_quality", "incident_311"],
        "Elevated AQI in {ward} correlates with health-related complaints reported to JMC 311 — possibly Diwali smog or industrial emissions.",
        0.67,
    ),
    (
        ["weather", "transit", "air_quality", "incident_311"],
        "Multiple Jaipur civic systems are under simultaneous stress in {ward} — compound monsoon/dust storm emergency event probable.",
        0.92,
    ),
]

DISCLAIMER = (
    "NOTE: This represents a *probable correlation / possible link* detected from co-occurring "
    "Jaipur civic events. It is NOT a confirmed causal relationship. Data is simulated for demonstration."
)


# ---------------------------------------------------------------------------
# Haversine distance
# ---------------------------------------------------------------------------

def _haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Return great-circle distance in km between two (lat,lng) points."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlng / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


# ---------------------------------------------------------------------------
# Event store
# ---------------------------------------------------------------------------

class EventStore:
    """Thread-safe (asyncio single-threaded) rolling window event store."""

    def __init__(self, window_minutes: int = WINDOW_MINUTES, max_events: int = MAX_EVENTS) -> None:
        self._events: deque[CivicEvent] = deque(maxlen=max_events)
        self.window_minutes = window_minutes

    def add(self, event: CivicEvent) -> None:
        self._events.append(event)

    def get_window(self) -> list[CivicEvent]:
        """Return events within the rolling time window."""
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=self.window_minutes)
        result: list[CivicEvent] = []
        for e in self._events:
            ts = e.timestamp if e.timestamp.tzinfo else e.timestamp.replace(tzinfo=timezone.utc)
            if ts >= cutoff:
                result.append(e)
        return result

    def get_recent(self, n: int = 100, category: Optional[EventCategory] = None, min_severity: float = 0.0) -> list[CivicEvent]:
        """Return the n most recent events with optional filters."""
        events = list(self._events)
        events.reverse()  # most recent first
        if category:
            events = [e for e in events if e.category == category]
        if min_severity > 0.0:
            events = [e for e in events if e.severity >= min_severity]
        return events[:n]

    def count_last_hour(self) -> int:
        return len(self.get_window())


# ---------------------------------------------------------------------------
# Correlation Engine
# ---------------------------------------------------------------------------

class CorrelationEngine:
    """
    Analyzes the event store to compute ward health scores and detect
    spatial-temporal correlation clusters.
    """

    def __init__(self, store: EventStore) -> None:
        self.store = store

    # --- Ward health score ------------------------------------------------

    def compute_ward_scores(self) -> dict[str, WardStatus]:
        """
        For each ward, compute a Civic Health Score 0–100.
        100 = everything fine, 0 = catastrophic.
        """
        window = self.store.get_window()
        # Group by ward
        by_ward: dict[str, list[CivicEvent]] = defaultdict(list)
        for e in window:
            by_ward[e.ward_id].append(e)

        ward_statuses: dict[str, WardStatus] = {}
        for ward_id, info in WARDS.items():
            events = by_ward.get(ward_id, [])
            score = self._health_score(events)
            status = (
                "critical" if score < 40
                else "moderate" if score < 70
                else "normal"
            )
            # Find dominant category
            cat_counts: dict[str, int] = defaultdict(int)
            for e in events:
                cat_counts[e.category] += 1
            dominant = max(cat_counts, key=cat_counts.get) if cat_counts else None

            ward_statuses[ward_id] = WardStatus(
                ward_id=ward_id,
                name=info["name"],
                health_score=round(score, 1),
                status=status,
                active_event_count=len(events),
                coordinates={"lat": info["lat"], "lng": info["lng"]},  # type: ignore[arg-type]
                dominant_category=dominant,
            )
        return ward_statuses

    def _health_score(self, events: list[CivicEvent]) -> float:
        """
        Calculate a 0–100 health score.
        Start at 100, subtract weighted penalties for severity.
        """
        if not events:
            return 88.0 + (hash(str(datetime.now(timezone.utc).minute)) % 8)  # small variance for empty wards

        penalty = 0.0
        for e in events:
            weight = CATEGORY_WEIGHTS.get(e.category, 0.25)
            penalty += e.severity * weight * 15  # each event can deduct up to 15 points

        # Normalize by number of events (diminishing returns beyond ~10 events)
        normalized_penalty = min(100.0, penalty / max(1, math.log1p(len(events)) * 1.5))
        score = max(0.0, 100.0 - normalized_penalty)
        return score

    def city_health_score(self, ward_scores: dict[str, WardStatus]) -> float:
        """Aggregate all ward scores into a single city-level score."""
        if not ward_scores:
            return 80.0
        scores = [ws.health_score for ws in ward_scores.values()]
        # Weighted towards worst wards (min has higher pull)
        avg = sum(scores) / len(scores)
        minimum = min(scores)
        return round((avg * 0.65 + minimum * 0.35), 1)

    # --- Correlation detection --------------------------------------------

    def detect_correlations(self) -> list[CorrelationCluster]:
        """
        For each ward, check each correlation rule. If average severity for
        all listed categories exceeds threshold → emit a cluster.
        """
        window = self.store.get_window()
        by_ward: dict[str, list[CivicEvent]] = defaultdict(list)
        for e in window:
            by_ward[e.ward_id].append(e)

        clusters: list[CorrelationCluster] = []
        seen_rules: set[tuple[str, str]] = set()  # (ward_id, rule_key) dedup

        for ward_id, events in by_ward.items():
            for cats, desc_tmpl, base_conf in CORRELATION_RULES:
                rule_key = (ward_id, "_".join(sorted(cats)))
                if rule_key in seen_rules:
                    continue

                cat_set = set(cats)
                # Compute per-category average severity in this ward
                cat_events: dict[str, list[CivicEvent]] = defaultdict(list)
                for e in events:
                    if e.category in cat_set:
                        cat_events[e.category].append(e)

                # All required categories must have events above threshold
                if not all(cat in cat_events for cat in cats):
                    continue

                all_above = all(
                    sum(e.severity for e in cat_events[cat]) / len(cat_events[cat])
                    >= CORRELATION_THRESHOLDS.get(cat, 0.4)
                    for cat in cats
                )
                if not all_above:
                    continue

                # Confidence: base + boost from event count + severity
                event_ids = [e.event_id for cat in cats for e in cat_events[cat]]
                avg_sev = sum(
                    sum(e.severity for e in cat_events[cat]) / len(cat_events[cat])
                    for cat in cats
                ) / len(cats)

                count_boost = min(0.10, len(event_ids) * 0.005)
                sev_boost = avg_sev * 0.08
                confidence = min(0.97, base_conf + count_boost + sev_boost)

                ward_name = WARDS.get(ward_id, {}).get("name", ward_id)
                description = desc_tmpl.format(ward=ward_name)

                clusters.append(CorrelationCluster(
                    ward_id=ward_id,
                    event_ids=event_ids[:20],  # cap for payload size
                    categories_involved=list(cats),
                    confidence=round(confidence, 3),
                    description=description,
                    disclaimer=DISCLAIMER,
                    severity_aggregate=round(min(1.0, avg_sev * 1.1), 3),
                ))
                seen_rules.add(rule_key)

        # Sort by confidence descending, return top 10
        clusters.sort(key=lambda c: c.confidence, reverse=True)
        logger.debug("Detected %d correlation clusters.", len(clusters))
        return clusters[:10]

    # --- Anomaly detection ------------------------------------------------

    def detect_anomalies(self) -> list[dict]:
        """
        Simple spike detection: compare last 5 min vs previous 25 min.
        Returns list of anomaly dicts for the pulse endpoint.
        """
        now = datetime.now(timezone.utc)
        recent_cutoff = now - timedelta(minutes=5)
        baseline_cutoff = now - timedelta(minutes=30)

        window = self.store.get_window()
        recent_events = [e for e in window if (e.timestamp if e.timestamp.tzinfo else e.timestamp.replace(tzinfo=timezone.utc)) >= recent_cutoff]
        baseline_events = [e for e in window if baseline_cutoff <= (e.timestamp if e.timestamp.tzinfo else e.timestamp.replace(tzinfo=timezone.utc)) < recent_cutoff]

        anomalies: list[dict] = []
        for cat in ["weather", "transit", "incident_311", "air_quality"]:
            r_count = sum(1 for e in recent_events if e.category == cat)
            b_count = sum(1 for e in baseline_events if e.category == cat)
            b_rate = b_count / 5  # per-minute baseline rate
            r_rate = r_count / 1  # per-minute recent rate (1-minute window effective)
            if b_rate > 0 and r_rate > b_rate * 2.5:
                anomalies.append({
                    "category": cat,
                    "recent_rate": r_rate,
                    "baseline_rate": b_rate,
                    "spike_factor": round(r_rate / b_rate, 1),
                    "description": f"Spike detected in {cat.replace('_', ' ')} events: {r_count} in last 5 min vs {b_count} in previous 25 min.",
                })
        return anomalies

    # --- Full pulse snapshot ----------------------------------------------

    def full_pulse(self) -> dict:
        """
        Returns a comprehensive snapshot dict for the /health-pulse endpoint
        and WebSocket updates.
        """
        ward_scores = self.compute_ward_scores()
        city_score = self.city_health_score(ward_scores)
        correlations = self.detect_correlations()
        anomalies = self.detect_anomalies()
        active_alerts = sum(1 for ws in ward_scores.values() if ws.status != "normal")

        return {
            "city_health_score": city_score,
            "ward_scores": ward_scores,
            "correlations": correlations,
            "anomalies": anomalies,
            "active_alert_count": active_alerts,
            "total_events_last_hour": self.store.count_last_hour(),
        }
