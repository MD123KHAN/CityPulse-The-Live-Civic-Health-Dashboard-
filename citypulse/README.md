# 🏙 CityPulse — Live Civic Health Dashboard

> *"One glance should tell a resident what's really happening in their neighborhood — and why it matters."*

**AmiHacks 2026 · Track B — Industry / Open Innovation**

---

## 🚀 Quick Start (2 minutes)

### Option A — Frontend Only (Zero dependencies, open instantly)

```bash
# Just open the file in any browser:
open citypulse/frontend/index.html
# or on Windows:
start citypulse\frontend\index.html
```

The frontend runs entirely standalone with a built-in simulation engine. No backend, no npm, no install needed.

---

### Option B — Full Stack (Backend + Live WebSocket)

#### 1. Backend Setup

```bash
cd citypulse/backend

# Create virtual environment
python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# (Optional) Configure LLM API key for AI summaries
cp .env.example .env
# Edit .env and add OPENAI_API_KEY, ANTHROPIC_API_KEY, GOOGLE_API_KEY, or GEMINI_API_KEY

# Start the server
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

The API will be live at **http://localhost:8000**  
Interactive API docs at **http://localhost:8000/docs**

#### 2. Open the Dashboard

```bash
# In a new terminal — just open the HTML file:
start citypulse\frontend\index.html
```

The frontend auto-detects the backend WebSocket at `ws://localhost:8000/ws/pulse` and switches from simulation mode to live data mode automatically.

---

## 📁 Project Structure

```
citypulse/
│
├── frontend/
│   ├── index.html              ← Standalone dashboard (open directly, no build needed)
│   ├── mylogo.jpeg             ← Brand logo used in the nav bar
│   └── package.json            ← React 18 + Vite + Tailwind CSS (optional build variant)
│
└── backend/
    ├── main.py                 ← FastAPI app — REST + WebSocket + static file serving
    ├── models.py               ← Pydantic v2 schemas (CivicEvent, WardStatus, Complaint, etc.)
    ├── simulator.py            ← Async mock feed workers (Weather, Transit, 311, Air Quality)
    │                             Jaipur-specific ward geography (W01–W08) and events
    ├── correlation.py          ← EventStore (rolling 60-min window) + CorrelationEngine
    │                             (ward health scores, cluster detection, anomaly spikes)
    ├── summarizer.py           ← AI plain-language summaries (OpenAI → Anthropic → Gemini
    │                             → deterministic Jaipur-specific fallback template)
    ├── database.py             ← SQLite persistence via SQLAlchemy (complaints,
    │                             civic_events, ward_snapshots tables)
    ├── requirements.txt        ← Python dependencies
    ├── .env.example            ← Environment variable template
    └── pyproject.toml
```

---

## 🔌 API Reference

### Dashboard

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/v1/health-pulse` | City score, ward metrics, top correlations, AI summary |
| `GET` | `/api/v1/wards` | GeoJSON FeatureCollection of ward data for map rendering |
| `GET` | `/api/v1/events/recent` | Recent normalized events (filter by `category`, `min_severity`, `limit`) |
| `GET` | `/api/v1/correlations` | Current detected correlation clusters with confidence scores |
| `GET` | `/api/v1/anomalies` | Current spike/anomaly detections (5-min vs 25-min baseline) |

### Demo

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/simulate/incident` | **Demo:** inject a pre-built or custom scenario cluster |

### 311 Complaints Portal

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/complaints` | Submit a civic complaint (`multipart/form-data`, optional image upload) |
| `GET` | `/api/v1/complaints` | List complaints (filter by `city`, `ward`, `status`; supports pagination) |
| `GET` | `/api/v1/complaints/{ref_id}` | Fetch a single complaint by reference ID (e.g. `CP-A1B2C3D4`) |

### Database Query

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/v1/db/events` | Query persisted civic events from SQLite |
| `GET` | `/api/v1/db/ward-trend/{ward_id}` | Ward health-score snapshots for trend visualisation |

### WebSocket

| Method | Endpoint | Description |
|--------|----------|-------------|
| `WS` | `/ws/pulse` | Live event + score + AI summary stream |

### Static Assets

- Frontend files are served from the parent `citypulse/` directory at `/static/`
- Uploaded complaint images are served at `/uploads/<filename>`

---

### Demo Scenario Injection

```bash
# Monsoon Flash Flood (Walled City — W01)
curl -X POST http://localhost:8000/api/v1/simulate/incident \
  -H "Content-Type: application/json" \
  -d '{"scenario": "flash_flood"}'

# JVVNL Power Grid Outage (Sanganer — W05)
curl -X POST http://localhost:8000/api/v1/simulate/incident \
  -H "Content-Type: application/json" \
  -d '{"scenario": "power_outage"}'

# RSRTC Transit Strike (Sindhi Camp — W08)
curl -X POST http://localhost:8000/api/v1/simulate/incident \
  -H "Content-Type: application/json" \
  -d '{"scenario": "transit_failure"}'

# Hazardous Diwali Smog / AQI Spike (Vaishali Nagar — W03)
curl -X POST http://localhost:8000/api/v1/simulate/incident \
  -H "Content-Type: application/json" \
  -d '{"scenario": "air_quality_spike"}'

# Custom event cluster
curl -X POST http://localhost:8000/api/v1/simulate/incident \
  -H "Content-Type: application/json" \
  -d '{"scenario": "custom", "custom_events": [{"ward_id": "W02", "category": "incident_311", "severity": 0.8, "title": "Road Cave-in", "description": "Road collapsed after heavy rain."}]}'
```

---

## 🧠 Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      CITYPULSE SYSTEM ARCHITECTURE                      │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ASYNC FEED WORKERS (simulator.py)                                      │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐  │
│  │ Weather Feed │ │ Transit Feed │ │ 311 Incidents│ │ Air Quality  │  │
│  │    ~30s      │ │     ~5s      │ │    ~10s      │ │    ~20s      │  │
│  │ (IMD/Jaipur) │ │   (RSRTC)    │ │  (JMC/JVVNL) │ │   (RSPCB)    │  │
│  └──────┬───────┘ └──────┬───────┘ └──────┬───────┘ └──────┬───────┘  │
│         └────────────────┴────────────────┴────────────────┘           │
│                                    │                                    │
│                          ┌─────────▼─────────┐                         │
│                          │  CivicEvent        │  ← Unified Pydantic     │
│                          │  Normalizer        │    schema (models.py)   │
│                          └─────────┬──────────┘                        │
│                                    │                                    │
│               ┌────────────────────┼─────────────────────┐             │
│               │                    │                      │             │
│     ┌─────────▼──────────┐         │             ┌────────▼──────────┐ │
│     │  SQLite Database   │         │             │    EventStore     │ │
│     │  (database.py)     │         │             │  Rolling 60-min   │ │
│     │  complaints        │         │             │  window (in-mem)  │ │
│     │  civic_events      │         │             └────────┬──────────┘ │
│     │  ward_snapshots    │         │                      │            │
│     └────────────────────┘         │      ┌───────────────┼──────────┐ │
│                                    │      │               │          │ │
│                          ┌─────────▼──────┐  ┌───────────▼──┐  ┌────▼─┐│
│                          │  Correlation   │  │   Anomaly    │  │  AI  ││
│                          │  Engine        │  │  Detector    │  │Summ. ││
│                          │  (Ward scores) │  │  (Spikes)    │  │      ││
│                          └─────────┬──────┘  └──────┬───────┘  └──┬───┘│
│                                    └────────────────┼─────────────┘    │
│                                                     │                  │
│                                          ┌──────────▼──────────┐       │
│                                          │  FastAPI             │       │
│                                          │  REST + WebSocket    │       │
│                                          │  + Static Files      │       │
│                                          └──────────┬───────────┘       │
│                                                     │ ws://…/ws/pulse   │
│                                          ┌──────────▼───────────┐       │
│                                          │  Vanilla HTML/CSS/JS │       │
│                                          │  Dashboard           │       │
│                                          │  (frontend/index.html│       │
│                                          │   no build needed)   │       │
│                                          └──────────────────────┘       │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 🗺 Jaipur Ward Map (W01–W08)

| Ward ID | Name | Coordinates |
|---------|------|-------------|
| W01 | Walled City (Heritage) | 26.926°N, 75.824°E |
| W02 | Malviya Nagar | 26.860°N, 75.805°E |
| W03 | Vaishali Nagar | 26.916°N, 75.738°E |
| W04 | Mansarovar | 26.849°N, 75.774°E |
| W05 | Sanganer | 26.805°N, 75.836°E |
| W06 | Civil Lines (Bani Park) | 26.919°N, 75.798°E |
| W07 | Tonk Road (Jhotwara) | 26.950°N, 75.750°E |
| W08 | Sindhi Camp / Station | 26.919°N, 75.787°E |

---

## ✅ Feature Checklist

### Core Requirements
- [x] **3+ distinct civic feeds** — Weather (~30s), Transit (~5s), 311 Incidents (~10s), Air Quality (~20s)
- [x] **Unified CivicEvent schema** — Pydantic v2 model: `event_id`, `ward_id`, `category`, `severity`, `coordinates`, `timestamp`, `title`, `description`, `raw_payload`
- [x] **Rolling-window correlation** — 60-minute in-memory EventStore (`deque`, maxlen 2000), multi-feed cluster detection with 6 pre-defined correlation rules
- [x] **Civic Health Score** — 0–100 per ward + city-level weighted aggregate (65% avg + 35% min)
- [x] **Epistemic honesty** — Every correlation has a confidence score + explicit "probable correlation" disclaimer
- [x] **AI Summary** — LLM integration (OpenAI GPT-4o-mini → Anthropic Claude Haiku → Gemini 1.5 Flash) with Jaipur-specific deterministic fallback
- [x] **REST API** — `/health-pulse`, `/wards`, `/events/recent`, `/correlations`, `/anomalies`, `/simulate/incident`
- [x] **WebSocket** — `/ws/pulse` live push stream (5 message types: `new_event`, `score_update`, `new_summary`, `correlation_detected`, `ping`)
- [x] **Interactive map** — Canvas-based ward zones with color-coded severity markers + hover tooltip
- [x] **10-Second Rule** — Dashboard readable at a glance by non-technical viewers
- [x] **Demo Controls** — Slide-up drawer with 4 pre-built Jaipur scenario injectors
- [x] **Graceful degradation** — Each feed worker isolated; frontend falls back to full simulation if backend unavailable
- [x] **Responsive design** — Works on desktop, tablet, mobile

### Backend Additions (beyond initial spec)
- [x] **SQLite persistence** — Three tables: `complaints`, `civic_events`, `ward_snapshots` (SQLAlchemy + WAL mode)
- [x] **311 Complaints Portal** — `POST/GET /api/v1/complaints` with multipart image upload (JPEG/PNG/GIF/WebP, max 10 MB), reference ID generation (`CP-XXXXXXXX`), city/ward/status filtering, pagination
- [x] **Ward trend snapshots** — Health scores saved every 60 s; queryable via `/api/v1/db/ward-trend/{ward_id}`
- [x] **Anomaly spike detection** — 5-min vs 25-min baseline comparison per feed category (`/api/v1/anomalies`)
- [x] **Custom scenario injection** — `{"scenario":"custom", "custom_events":[…]}` payload allows arbitrary event clusters
- [x] **Image file serving** — Uploaded images served at `/uploads/<filename>` via FastAPI `StaticFiles`
- [x] **WAL-mode SQLite** — `PRAGMA journal_mode=WAL` + `PRAGMA foreign_keys=ON` for concurrent read safety
- [x] **Async DB wrappers** — All SQLAlchemy (sync) calls run in `loop.run_in_executor` so the event loop stays non-blocking
- [x] **Ward snapshot background loop** — Runs every 60 s in a separate `asyncio.Task`

### Frontend Additions (beyond initial spec)
- [x] **Dark / Light theme toggle** — Full CSS custom-property theme switch persisted across sessions
- [x] **Multi-city selector** — Nav dropdown to switch between Jaipur and other cities (state preserved)
- [x] **Skeleton loaders** — Shimmer placeholders during initial data fetch
- [x] **Scroll-reveal animations** — Cards animate in from bottom/left/right with staggered delays
- [x] **Page-load entrance animations** — Nav slides down, sidebars slide in from sides on load
- [x] **Styled scrollbars** — Custom cyan-to-violet gradient scrollbar (WebKit)
- [x] **API source badges** — `LIVE` / `SIM` / `CACHED` pill badges indicating data source per widget
- [x] **Sticky API source bar** — Persistent top bar showing feed mode for each data category
- [x] **Live dot pulse states** — Green (normal), amber (warn), red (critical) animated pulse indicators
- [x] **Toast notifications** — Non-intrusive alerts for high-severity events
- [x] **Animated health ring** — Smooth SVG transitions, rotating halo glow, bounce pop on score change
- [x] **Animated shimmer ring track** — Secondary SVG track on health ring for visual depth
- [x] **Live heartbeat canvas** — Feed activity visualization
- [x] **Confidence bar charts** — Visual representation of correlation certainty
- [x] **Ward drill-down** — Per-ward health scores with color-coded status bars
- [x] **Map tooltip** — Hover over markers for event detail
- [x] **Layer filtering** — Filter map by feed category
- [x] **Threshold toggles** — Per-category alert on/off controls
- [x] **311 Complaint submission UI** — In-page form with image attachment, ward selector, issue type, and reference ID display
- [x] **WebSocket status pill** — Connected/disconnected indicator in nav with city label

---

## 🎬 Live Demo Script (For Judges)

1. **Open `citypulse/frontend/index.html`** in Chrome/Firefox — dashboard is immediately live with simulated data
2. Click **⚡ Demo Controls** button (top right)
3. Click **🌊 Flash Flood** — watch Walled City (W01) turn red, RSRTC transit halt, 311 waterlogging complaints surge
4. Observe the **AI Narrative Banner** update with a grounded plain-language Jaipur-specific summary
5. Watch the **Correlation panel** surface: "Heavy monsoon rainfall in Walled City (Heritage) is likely causing RSRTC delays and JMC waterlogging complaints — Confidence: ~91%"
6. Click **⚡ Power Grid Outage** to layer another scenario (Sanganer — JVVNL substation failure)
7. See the **City Health Score ring** drop toward critical
8. If backend is running: all 4 WebSocket event types stream in real-time; check the **LIVE** badge in the API source bar
9. Submit a test **311 complaint** via the complaints panel (with or without an image)

---

## 🔑 Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `OPENAI_API_KEY` | Optional | GPT-4o-mini for AI summaries |
| `ANTHROPIC_API_KEY` | Optional | Claude 3 Haiku for AI summaries |
| `GOOGLE_API_KEY` / `GEMINI_API_KEY` | Optional | Gemini 1.5 Flash for AI summaries |
| `DB_PATH` | Optional | SQLite file path (default: `citypulse.db` next to `main.py`) |
| `HOST` | Optional | Server bind host (default: `0.0.0.0`) |
| `PORT` | Optional | Server bind port (default: `8000`) |

**None of the LLM keys are required.** The system uses a Jaipur-specific deterministic template fallback when no key is present.

---

## 🛡 Epistemic Honesty

Every detected correlation in CityPulse includes:

1. **Confidence score** (e.g., "Confidence: 84%") — calculated from event count, average severity, and rule base confidence
2. **Explicit disclaimer** — *"This represents a probable correlation / possible link detected from co-occurring Jaipur civic events. It is NOT a confirmed causal relationship. Data is simulated for demonstration."*
3. **Framing language** — AI summaries use "likely", "appears to be", "may be" — never absolute claims

This prevents civic misinformation while still surfacing actionable patterns.

---

## 📦 Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend runtime | Python 3.11+, FastAPI 0.115, AsyncIO |
| Data models | Pydantic v2 (2.10) |
| WebSocket | FastAPI WebSocket + uvicorn[standard] 0.32 |
| Database | SQLite via SQLAlchemy 2.0 (WAL mode) |
| AI/NLP | OpenAI GPT-4o-mini / Anthropic Claude 3 Haiku / Gemini 1.5 Flash (optional) |
| Fallback summaries | Deterministic Python template engine (Jaipur-specific) |
| Frontend | Vanilla HTML/CSS/JS (no build step) |
| Maps | HTML5 Canvas (custom renderer) |
| Charts | HTML5 Canvas (custom sparklines) |
| Optional frontend build | React 18 + Vite 6 + Tailwind CSS 3 |

---

*Built for AmiHacks 2026 — 24-hour hackathon challenge.*
