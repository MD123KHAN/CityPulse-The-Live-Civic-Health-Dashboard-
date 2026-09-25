"""
CityPulse — Async Mock Ingestion Workers
Simulates 4 distinct civic data feeds at different intervals:
  - Weather feed:    every 30s
  - Transit feed:    every  5s
  - 311 Incidents:   every 10s
  - Air Quality:     every 20s

Each worker catches its own exceptions so one failing feed never
stops the others (graceful degradation).
"""
from __future__ import annotations

import asyncio
import logging
import random
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Callable, Awaitable

from models import CivicEvent, Coordinates

if TYPE_CHECKING:
    pass

logger = logging.getLogger("citypulse.simulator")

# ---------------------------------------------------------------------------
# Ward geography (Downtown core + surrounding districts)
# ---------------------------------------------------------------------------

# Jaipur, Rajasthan, India — real ward centroids
WARDS: dict[str, dict] = {
    "W01": {"name": "Walled City (Heritage)",   "lat": 26.9260, "lng": 75.8235},
    "W02": {"name": "Malviya Nagar",            "lat": 26.8600, "lng": 75.8045},
    "W03": {"name": "Vaishali Nagar",           "lat": 26.9160, "lng": 75.7380},
    "W04": {"name": "Mansarovar",               "lat": 26.8490, "lng": 75.7740},
    "W05": {"name": "Sanganer",                 "lat": 26.8045, "lng": 75.8360},
    "W06": {"name": "Civil Lines (Bani Park)",  "lat": 26.9190, "lng": 75.7980},
    "W07": {"name": "Tonk Road (Jhotwara)",     "lat": 26.9500, "lng": 75.7500},
    "W08": {"name": "Sindhi Camp / Station",    "lat": 26.9190, "lng": 75.7874},
}

def _ward_coords(ward_id: str, jitter: float = 0.008) -> Coordinates:
    """Return ward centroid with small random jitter so markers don't overlap."""
    w = WARDS.get(ward_id, WARDS["W01"])
    return Coordinates(
        lat=w["lat"] + random.uniform(-jitter, jitter),
        lng=w["lng"] + random.uniform(-jitter, jitter),
    )

def _random_ward() -> str:
    return random.choice(list(WARDS.keys()))


# ---------------------------------------------------------------------------
# Weather feed — interval 30 s
# ---------------------------------------------------------------------------

# Jaipur-specific weather events
WEATHER_CONDITIONS = [
    ("Monsoon Rainfall",      "Intense monsoon showers. Waterlogging risk in low-lying areas.",         0.55),
    ("Heavy Monsoon",         "Severe rainfall — Jawahar Circle underpass at risk. Avoid old city.",    0.75),
    ("Flash Flood Alert",     "Flash flood advisory. Avoid Walled City & MI Road underpasses.",         0.95),
    ("Dust Storm (Aandhi)",   "Aandhi approaching from west. Visibility below 200m. Stay indoors.",     0.70),
    ("Heatwave Advisory",     "Loo winds. Humidex 46°C. Cooling centres open. Avoid noon travel.",      0.65),
    ("Thunderstorm Warning",  "Severe thunderstorm with lightning. Seek shelter immediately.",           0.80),
    ("Light Drizzle",         "Light drizzle — minor traffic slowdown expected on Ajmer Road.",         0.18),
    ("Clear Skies",           "No significant weather events. Conditions normal across Jaipur.",        0.05),
    ("Wildfire Smoke",        "Crop burning smoke from neighbouring districts affecting AQI.",           0.60),
]

async def weather_worker(callback: Callable[[CivicEvent], Awaitable[None]]) -> None:
    """Emit one weather event every ~30 seconds."""
    logger.info("Weather feed worker started.")
    while True:
        try:
            ward = _random_ward()
            title, desc, base_sev = random.choice(WEATHER_CONDITIONS)
            sev = min(1.0, max(0.0, base_sev + random.uniform(-0.1, 0.1)))
            event = CivicEvent(
                ward_id=ward,
                category="weather",
                severity=round(sev, 3),
                coordinates=_ward_coords(ward),
                title=title,
                description=desc,
                raw_payload={
                    "source": "Environment Canada (simulated)",
                    "precipitation_mm": round(random.uniform(0, 40), 1),
                    "wind_kph": round(random.uniform(10, 90), 1),
                    "visibility_km": round(random.uniform(0.1, 10.0), 1),
                    "temperature_c": round(random.uniform(-5, 38), 1),
                },
            )
            await callback(event)
            logger.debug("Weather event emitted: %s ward=%s sev=%.2f", title, ward, sev)
        except asyncio.CancelledError:
            logger.info("Weather feed worker cancelled.")
            return
        except Exception as exc:  # noqa: BLE001
            logger.error("Weather feed error (continuing): %s", exc)
        await asyncio.sleep(random.uniform(25, 35))


# ---------------------------------------------------------------------------
# Transit feed — interval 5 s
# ---------------------------------------------------------------------------

# Jaipur RSRTC-specific transit events
TRANSIT_EVENTS = [
    ("RSRTC Bus Delayed",         "Route {n} delayed {d} min — congestion on Ajmer Road.",        0.35),
    ("Route Suspended — Flood",   "Route {n} suspended. Road waterlogged near Sanganer.",          0.80),
    ("On-Time Operations",        "All RSRTC routes on schedule. No incidents reported.",          0.05),
    ("Minor Delay — Peak Hour",   "Route {n} running {d} min behind due to peak-hour traffic.",   0.22),
    ("Sindhi Camp Congestion",    "Major congestion at Sindhi Camp terminal. Delays on all routes.",0.65),
    ("Route Diverted",            "Route {n} diverted via Tonk Road due to road closure.",         0.45),
    ("Overcrowded Bus",           "Bus on route {n} dangerously overcrowded. Next service in {d}m.",0.50),
    ("Terminal Closure",          "Sindhi Camp platform {n} closed — emergency maintenance.",      0.85),
    ("E-Rickshaw Strike",         "Auto/e-rickshaw drivers on strike. Commuter pressure on {n}.",  0.40),
]

async def transit_worker(callback: Callable[[CivicEvent], Awaitable[None]]) -> None:
    """Emit one transit event every ~5 seconds."""
    logger.info("Transit feed worker started.")
    # Jaipur RSRTC route numbers
    route_numbers = ["2A", "5", "7", "12", "14", "16", "25", "32", "45", "60", "Vaishali Exp", "Airport Exp"]
    while True:
        try:
            ward = _random_ward()
            route = random.choice(route_numbers)
            delay = random.randint(3, 25)
            tmpl, desc_tmpl, base_sev = random.choice(TRANSIT_EVENTS)
            desc = desc_tmpl.format(n=route, d=delay)
            title = tmpl.replace("{n}", route)
            sev = min(1.0, max(0.0, base_sev + random.uniform(-0.08, 0.08)))
            event = CivicEvent(
                ward_id=ward,
                category="transit",
                severity=round(sev, 3),
                coordinates=_ward_coords(ward),
                title=title,
                description=desc,
                raw_payload={
                    "source": "TTC / Transit Authority (simulated)",
                    "route_id": route,
                    "delay_minutes": delay,
                    "vehicle_type": random.choice(["bus", "streetcar", "subway"]),
                    "affected_stops": random.randint(1, 8),
                },
            )
            await callback(event)
            logger.debug("Transit event emitted: %s ward=%s", title, ward)
        except asyncio.CancelledError:
            logger.info("Transit feed worker cancelled.")
            return
        except Exception as exc:
            logger.error("Transit feed error (continuing): %s", exc)
        await asyncio.sleep(random.uniform(4, 7))


# ---------------------------------------------------------------------------
# 311 Incidents feed — interval 10 s
# ---------------------------------------------------------------------------

# Jaipur-specific 311 complaint types
INCIDENT_311 = [
    ("Pothole Reported",        "Large pothole on {st} causing vehicle damage.",                 0.28),
    ("Waterlogging Complaint",  "Street waterlogging on {st} — water entering homes.",          0.72),
    ("Stray Animal Complaint",  "Stray dogs/cattle blocking traffic on {st}.",                   0.20),
    ("Illegal Dumping",         "Garbage dump reported near {st}. JMC notified.",               0.22),
    ("Tree Down",               "Fallen neem tree blocking {st} after storm.",                   0.48),
    ("Water Supply Cut",        "PHED water supply cut at {st} — 3rd day without water.",       0.75),
    ("Power Outage — JVVNL",   "Power cut on {st}. JVVNL crews en route.",                     0.80),
    ("Gas Leak Suspected",      "LPG pipeline suspected leak near {st}. Residents evacuating.", 0.95),
    ("Sewer Overflow",          "Sewer manhole overflowing on {st} — health hazard.",           0.68),
    ("Road Cave-in",            "Road collapsed on {st} after heavy rain. Dangerous.",          0.85),
    ("Traffic Light Outage",    "Signal malfunction at {st}. Causing major jams.",              0.42),
    ("Heritage Wall Damage",    "Stone wall of heritage structure cracked near {st}.",          0.55),
    ("Encroachment",            "Illegal encroachment blocking footpath at {st}.",               0.18),
]

STREET_NAMES = [
    "MI Road", "Ajmer Road", "Tonk Road", "Amber Road",
    "Gopalpura Bypass", "Sikar Road", "Sanganer Road", "Mansarovar Sector 7",
    "Jawahar Circle", "JLN Marg", "C-Scheme", "Vaishali Nagar Sector 4",
    "Malviya Nagar Station Road", "Lal Kothi", "Gandhi Nagar", "Bani Park",
]

async def incidents_311_worker(callback: Callable[[CivicEvent], Awaitable[None]]) -> None:
    """Emit one 311 complaint every ~10 seconds."""
    logger.info("311 Incidents feed worker started.")
    while True:
        try:
            ward = _random_ward()
            street = random.choice(STREET_NAMES)
            tmpl, desc_tmpl, base_sev = random.choice(INCIDENT_311)
            desc = desc_tmpl.format(st=street)
            title = tmpl
            sev = min(1.0, max(0.0, base_sev + random.uniform(-0.1, 0.1)))
            event = CivicEvent(
                ward_id=ward,
                category="incident_311",
                severity=round(sev, 3),
                coordinates=_ward_coords(ward),
                title=title,
                description=desc,
                raw_payload={
                    "source": "311 Citizen Portal (simulated)",
                    "street": street,
                    "complaint_type": tmpl,
                    "ticket_number": f"311-{random.randint(100000, 999999)}",
                    "priority": random.choice(["low", "medium", "high", "emergency"]),
                    "reported_by": "anonymous",
                },
            )
            await callback(event)
            logger.debug("311 event emitted: %s ward=%s", title, ward)
        except asyncio.CancelledError:
            logger.info("311 Incidents feed worker cancelled.")
            return
        except Exception as exc:
            logger.error("311 feed error (continuing): %s", exc)
        await asyncio.sleep(random.uniform(8, 13))


# ---------------------------------------------------------------------------
# Air Quality feed — interval 20 s
# ---------------------------------------------------------------------------

AQI_CONDITIONS = [
    ("Good Air Quality",        "AQI {aqi} — Air quality is satisfactory.",             0.05),
    ("Moderate Air Quality",    "AQI {aqi} — Acceptable but sensitive groups take care.",0.30),
    ("Unhealthy for Sensitive", "AQI {aqi} — Children and elderly should limit outdoors.",0.55),
    ("Unhealthy Air Quality",   "AQI {aqi} — Everyone may experience health effects.",   0.75),
    ("Very Unhealthy",          "AQI {aqi} — Health alert. Avoid prolonged outdoor activity.",0.88),
    ("Hazardous",               "AQI {aqi} — Emergency: Health warning for entire population.",0.97),
    ("Wildfire Smoke Advisory", "AQI {aqi} — Wildfire smoke detected. N95 masks advised.",0.80),
]

async def air_quality_worker(callback: Callable[[CivicEvent], Awaitable[None]]) -> None:
    """Emit one air quality reading every ~20 seconds (Jaipur RSPCB monitoring)."""
    logger.info("Air Quality feed worker started.")
    # Jaipur baseline AQI is higher (industrial + dust) — start 80-180
    aqi = random.uniform(80, 180)
    while True:
        try:
            ward = _random_ward()
            # Random walk for realistic AQI evolution
            aqi = max(0, min(500, aqi + random.uniform(-15, 20)))
            aqi_int = int(aqi)

            # Map AQI to severity and template
            if aqi_int <= 50:
                tmpl, desc_tmpl, base_sev = AQI_CONDITIONS[0]
            elif aqi_int <= 100:
                tmpl, desc_tmpl, base_sev = AQI_CONDITIONS[1]
            elif aqi_int <= 150:
                tmpl, desc_tmpl, base_sev = AQI_CONDITIONS[2]
            elif aqi_int <= 200:
                tmpl, desc_tmpl, base_sev = AQI_CONDITIONS[3]
            elif aqi_int <= 300:
                tmpl, desc_tmpl, base_sev = AQI_CONDITIONS[4]
            else:
                tmpl, desc_tmpl, base_sev = AQI_CONDITIONS[5]

            # Occasionally inject wildfire smoke event
            if random.random() < 0.05:
                tmpl, desc_tmpl, base_sev = AQI_CONDITIONS[6]

            desc = desc_tmpl.format(aqi=aqi_int)
            title = tmpl
            sev = min(1.0, max(0.0, base_sev + random.uniform(-0.05, 0.05)))

            event = CivicEvent(
                ward_id=ward,
                category="air_quality",
                severity=round(sev, 3),
                coordinates=_ward_coords(ward),
                title=title,
                description=desc,
                raw_payload={
                    "source": "RSPCB Jaipur (simulated)",
                    "aqi": aqi_int,
                    "pm25": round(random.uniform(15, 250), 1),
                    "pm10": round(random.uniform(30, 400), 1),
                    "o3_ppb": round(random.uniform(20, 120), 1),
                    "no2_ppb": round(random.uniform(10, 120), 1),
                    "monitoring_station": random.choice(["Mansarovar", "Sanganer", "Civil Lines", "Vaishali", "Sitapura"]),
                },
            )
            await callback(event)
            logger.debug("AQI event emitted: AQI=%d ward=%s sev=%.2f", aqi_int, ward, sev)
        except asyncio.CancelledError:
            logger.info("Air Quality feed worker cancelled.")
            return
        except Exception as exc:
            logger.error("Air Quality feed error (continuing): %s", exc)
        await asyncio.sleep(random.uniform(18, 24))


# ---------------------------------------------------------------------------
# Scenario injection (for demo endpoint)
# ---------------------------------------------------------------------------

# Jaipur-specific demo scenarios
SCENARIOS: dict[str, list[dict]] = {
    "flash_flood": [
        {
            "ward_id": "W01",
            "category": "weather",
            "severity": 0.95,
            "title": "Monsoon Flash Flood — Walled City EMERGENCY",
            "description": "Jawahar Circle underpass submerged. Avoid old city area immediately.",
            "raw_payload": {"precipitation_mm": 92.0, "source": "IMD Jaipur (simulated)"},
        },
        {
            "ward_id": "W01",
            "category": "transit",
            "severity": 0.92,
            "title": "RSRTC Routes 2A, 5, 12 Suspended — Flooding",
            "description": "Routes suspended. MI Road waterlogged. Buses diverted via Gopalpura.",
            "raw_payload": {"source": "RSRTC (simulated)", "affected_routes": ["2A","5","12"]},
        },
        {
            "ward_id": "W01",
            "category": "incident_311",
            "severity": 0.93,
            "title": "Waterlogging Surge — Walled City",
            "description": "52 waterlogging complaints in last 30 mins — basements, roads, underpasses.",
            "raw_payload": {"source": "JMC 311 Portal (simulated)", "complaint_count": 52},
        },
    ],
    "power_outage": [
        {
            "ward_id": "W05",
            "category": "incident_311",
            "severity": 0.94,
            "title": "JVVNL Substation Failure — Sanganer",
            "description": "18,000+ residents without power. JVVNL repair crews deployed.",
            "raw_payload": {"source": "JVVNL (simulated)", "affected_customers": 18000},
        },
        {
            "ward_id": "W05",
            "category": "transit",
            "severity": 0.82,
            "title": "Traffic Signal Outage — Sanganer Crossroads",
            "description": "Signal failure causing massive jams. Traffic police deployed.",
            "raw_payload": {"source": "Jaipur Traffic Police (simulated)"},
        },
        {
            "ward_id": "W05",
            "category": "weather",
            "severity": 0.68,
            "title": "Dust Storm (Aandhi) Caused Outage",
            "description": "80 km/h Aandhi gusts brought down power lines in Sanganer industrial belt.",
            "raw_payload": {"wind_kph": 82.0, "source": "IMD Jaipur (simulated)"},
        },
    ],
    "transit_failure": [
        {
            "ward_id": "W08",
            "category": "transit",
            "severity": 0.96,
            "title": "RSRTC Unplanned Strike — All Services Halted",
            "description": "RSRTC drivers on unplanned strike. All Sindhi Camp services suspended.",
            "raw_payload": {"source": "RSRTC Authority (simulated)"},
        },
        {
            "ward_id": "W08",
            "category": "incident_311",
            "severity": 0.68,
            "title": "Overcrowding — Sindhi Camp Terminal",
            "description": "Massive crowd at Sindhi Camp. Police deployed for crowd management.",
            "raw_payload": {"source": "JMC 311 (simulated)"},
        },
    ],
    "air_quality_spike": [
        {
            "ward_id": "W03",
            "category": "air_quality",
            "severity": 0.97,
            "title": "Hazardous AQI 420 — Diwali Smog",
            "description": "AQI 420: all outdoor activity prohibited. Schools closed across Jaipur.",
            "raw_payload": {"aqi": 420, "pm25": 310.0, "source": "RSPCB Jaipur (simulated)"},
        },
        {
            "ward_id": "W03",
            "category": "weather",
            "severity": 0.45,
            "title": "Calm Winds Trapping Diwali Smog",
            "description": "No wind movement — firecracker smoke accumulating over Vaishali Nagar.",
            "raw_payload": {"source": "IMD Jaipur (simulated)", "wind_speed_kmh": 2.0},
        },
    ],
}


async def inject_scenario(
    scenario: str,
    ward_id: str | None,
    callback: Callable[[CivicEvent], Awaitable[None]],
) -> list[CivicEvent]:
    """Immediately inject a predefined scenario cluster and fire events."""
    events_cfg = SCENARIOS.get(scenario, SCENARIOS["flash_flood"])
    injected: list[CivicEvent] = []
    for cfg in events_cfg:
        w = ward_id or cfg.get("ward_id", _random_ward())
        ward_info = WARDS.get(w, WARDS["W01"])
        event = CivicEvent(
            ward_id=w,
            category=cfg["category"],
            severity=cfg["severity"],
            coordinates=Coordinates(
                lat=ward_info["lat"] + random.uniform(-0.004, 0.004),
                lng=ward_info["lng"] + random.uniform(-0.004, 0.004),
            ),
            title=cfg["title"],
            description=cfg["description"],
            raw_payload=cfg.get("raw_payload", {}),
        )
        await callback(event)
        injected.append(event)
        await asyncio.sleep(0.05)  # small stagger for realism
    return injected


# ---------------------------------------------------------------------------
# Worker registry — start / stop all workers
# ---------------------------------------------------------------------------

WORKERS = [weather_worker, transit_worker, incidents_311_worker, air_quality_worker]


async def start_all_workers(
    callback: Callable[[CivicEvent], Awaitable[None]]
) -> list[asyncio.Task]:
    """Launch all feed workers as asyncio Tasks. Returns task list for cancellation."""
    tasks: list[asyncio.Task] = []
    for worker_fn in WORKERS:
        task = asyncio.create_task(worker_fn(callback), name=worker_fn.__name__)
        tasks.append(task)
        logger.info("Started worker: %s", worker_fn.__name__)
    return tasks
