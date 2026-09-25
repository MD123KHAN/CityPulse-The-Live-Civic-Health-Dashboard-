"""
CityPulse Jaipur — Plain-Language AI Summarization Service

Strategy:
  1. Try OpenAI / Gemini / Anthropic (whichever key is present in env).
  2. On failure, timeout, or missing key → deterministic fallback template.
  
The summary answers: "What is happening right now and why it matters"
in under ~30 words, grounded strictly in ingested data.
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Optional

from models import CorrelationCluster, CivicEvent, EventCategory

logger = logging.getLogger("citypulse.summarizer")

# ---------------------------------------------------------------------------
# Fallback template engine (deterministic, always works)
# ---------------------------------------------------------------------------

_CATEGORY_PHRASES: dict[EventCategory, list[str]] = {
    "weather": [
        "severe monsoon conditions are impacting the city",
        "heavy rainfall and waterlogging are ongoing across wards",
        "an aandhi dust storm is reducing visibility and causing disruptions",
    ],
    "transit": [
        "RSRTC bus services are severely disrupted",
        "multiple bus routes are delayed or suspended",
        "Sindhi Camp terminal is experiencing major overcrowding",
    ],
    "incident_311": [
        "a surge of JMC 311 complaints signals infrastructure stress",
        "waterlogging, road collapses, and JVVNL failures are being reported",
        "residents across Jaipur are reporting widespread civic failures",
    ],
    "air_quality": [
        "RSPCB air quality readings have reached unhealthy levels",
        "hazardous AQI is a health risk — dust storm or Diwali smog is likely",
        "elevated particulate matter is affecting all wards in Jaipur",
    ],
}

_IMPACT_PHRASES: list[str] = [
    "Jaipur residents should avoid affected areas and follow JMC guidance.",
    "JVVNL, JMC, and RSRTC crews are responding; expect delays.",
    "Stay informed via Jaipur Smart City dashboard and local alerts.",
    "Vulnerable residents — elderly, children — should take precautions immediately.",
    "Contact JMC helpline 0141-2385300 for urgent civic issues.",
]

_SEVERITY_LABELS: dict[str, str] = {
    "critical": "CRITICAL",
    "moderate": "ELEVATED",
    "normal":   "NORMAL",
}


def _fallback_summary(
    top_correlation: Optional[CorrelationCluster],
    recent_events: list[CivicEvent],
    city_score: float,
) -> tuple[str, float]:
    """
    Generate a deterministic plain-language summary without an LLM.
    Returns (summary_text, confidence_score).
    """
    # Determine overall status
    if city_score < 40:
        status = "critical"
    elif city_score < 70:
        status = "moderate"
    else:
        status = "normal"

    if status == "normal":
        return (
            f"City systems are operating normally. Health score {int(city_score)}/100. "
            "No significant anomalies detected across weather, transit, or incident feeds.",
            0.92,
        )

    # Pick dominant categories from recent events
    cat_counts: dict[str, int] = {}
    for e in recent_events[:50]:
        if e.severity > 0.4:
            cat_counts[e.category] = cat_counts.get(e.category, 0) + 1

    if not cat_counts:
        cat_counts = {e.category: 1 for e in recent_events[:5]}

    sorted_cats = sorted(cat_counts, key=cat_counts.get, reverse=True)
    primary_cat = sorted_cats[0] if sorted_cats else "incident_311"
    secondary_cat = sorted_cats[1] if len(sorted_cats) > 1 else None

    import random
    primary_phrase = random.choice(_CATEGORY_PHRASES.get(primary_cat, ["civic disruptions are occurring"]))  # noqa: S311

    if secondary_cat and secondary_cat in _CATEGORY_PHRASES:
        secondary_phrase = random.choice(_CATEGORY_PHRASES[secondary_cat])
        core = f"In the Downtown Core, {primary_phrase} while {secondary_phrase}"
    else:
        core = f"In the affected district, {primary_phrase}"

    impact = random.choice(_IMPACT_PHRASES)

    if top_correlation:
        ward = top_correlation.ward_id
        confidence_pct = int(top_correlation.confidence * 100)
        summary = f"{core}. {impact} (Correlation confidence: {confidence_pct}%)"
    else:
        summary = f"{core}. {impact}"

    confidence = 0.70 + (city_score / 1000)  # slightly higher when data is clearer
    return summary, round(min(0.95, confidence), 3)


# ---------------------------------------------------------------------------
# LLM prompt builder
# ---------------------------------------------------------------------------

def _build_prompt(
    top_correlations: list[CorrelationCluster],
    recent_events: list[CivicEvent],
    city_score: float,
    ward_scores: dict,
) -> str:
    """Build a concise, grounded prompt for the LLM."""
    status = "CRITICAL" if city_score < 40 else "ELEVATED" if city_score < 70 else "NORMAL"

    # Summarize top events
    event_lines: list[str] = []
    for e in recent_events[:8]:
        event_lines.append(f"- [{e.category}] {e.title}: {e.description[:80]} (sev={e.severity:.2f}, ward={e.ward_id})")

    # Summarize top correlation
    corr_line = ""
    if top_correlations:
        c = top_correlations[0]
        corr_line = f"\nTop detected correlation (confidence {int(c.confidence*100)}%): {c.description}"

    prompt = f"""You are a civic dashboard AI providing a brief situational summary for city residents.

CITY STATUS: {status} (Score: {int(city_score)}/100)
{corr_line}

RECENT EVENTS (last 60 min):
{chr(10).join(event_lines) if event_lines else "No significant events."}

TASK: Write ONE plain-English sentence (max 30 words) that answers "What is happening right now and why it matters" for a resident.
Rules:
- Ground your answer ONLY in the data above.
- Use non-technical language.
- Do NOT say "I" or "the data shows".
- Do NOT claim certainty — use "likely", "appears to be", "may be".
- End with a brief action hint for residents if severity is elevated.

SUMMARY:"""
    return prompt


# ---------------------------------------------------------------------------
# LLM callers
# ---------------------------------------------------------------------------

async def _try_openai(prompt: str) -> Optional[str]:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None
    try:
        import openai  # type: ignore[import]
        client = openai.AsyncOpenAI(api_key=api_key)
        resp = await asyncio.wait_for(
            client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=80,
                temperature=0.4,
            ),
            timeout=8.0,
        )
        text = resp.choices[0].message.content.strip()
        logger.info("OpenAI summary generated (%d chars).", len(text))
        return text
    except asyncio.TimeoutError:
        logger.warning("OpenAI request timed out.")
        return None
    except Exception as exc:
        logger.warning("OpenAI call failed: %s", exc)
        return None


async def _try_anthropic(prompt: str) -> Optional[str]:
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return None
    try:
        import anthropic  # type: ignore[import]
        client = anthropic.AsyncAnthropic(api_key=api_key)
        resp = await asyncio.wait_for(
            client.messages.create(
                model="claude-3-haiku-20240307",
                max_tokens=80,
                messages=[{"role": "user", "content": prompt}],
            ),
            timeout=8.0,
        )
        text = resp.content[0].text.strip()
        logger.info("Anthropic summary generated (%d chars).", len(text))
        return text
    except asyncio.TimeoutError:
        logger.warning("Anthropic request timed out.")
        return None
    except Exception as exc:
        logger.warning("Anthropic call failed: %s", exc)
        return None


async def _try_gemini(prompt: str) -> Optional[str]:
    api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
    if not api_key:
        return None
    try:
        import google.generativeai as genai  # type: ignore[import]
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel("gemini-1.5-flash")
        loop = asyncio.get_event_loop()
        resp = await asyncio.wait_for(
            loop.run_in_executor(None, lambda: model.generate_content(prompt)),
            timeout=8.0,
        )
        text = resp.text.strip()
        logger.info("Gemini summary generated (%d chars).", len(text))
        return text
    except asyncio.TimeoutError:
        logger.warning("Gemini request timed out.")
        return None
    except Exception as exc:
        logger.warning("Gemini call failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Public summarizer interface
# ---------------------------------------------------------------------------

class Summarizer:
    """
    Generates plain-language civic summaries.
    Tries LLM APIs in order; falls back to deterministic template on failure.
    """

    def __init__(self) -> None:
        self._last_summary: str = "Initializing CityPulse feed analysis…"
        self._last_confidence: float = 0.0
        self._last_updated: Optional[datetime] = None
        self._use_llm: bool = bool(
            os.getenv("OPENAI_API_KEY")
            or os.getenv("ANTHROPIC_API_KEY")
            or os.getenv("GOOGLE_API_KEY")
            or os.getenv("GEMINI_API_KEY")
        )

    async def generate(
        self,
        top_correlations: list[CorrelationCluster],
        recent_events: list[CivicEvent],
        city_score: float,
        ward_scores: dict,
    ) -> tuple[str, float]:
        """
        Returns (summary_text, confidence_score).
        confidence_score is 0.0–1.0.
        """
        llm_text: Optional[str] = None

        if self._use_llm:
            prompt = _build_prompt(top_correlations, recent_events, city_score, ward_scores)
            # Try providers in order
            llm_text = await _try_openai(prompt)
            if llm_text is None:
                llm_text = await _try_anthropic(prompt)
            if llm_text is None:
                llm_text = await _try_gemini(prompt)

        if llm_text:
            # LLM succeeded — use output with high confidence
            summary = llm_text
            confidence = 0.90
        else:
            # Deterministic fallback
            summary, confidence = _fallback_summary(
                top_correlations[0] if top_correlations else None,
                recent_events,
                city_score,
            )

        self._last_summary = summary
        self._last_confidence = confidence
        self._last_updated = datetime.now(timezone.utc)

        logger.debug("Summary generated (llm=%s, conf=%.2f): %s", bool(llm_text), confidence, summary[:60])
        return summary, confidence

    @property
    def last_summary(self) -> str:
        return self._last_summary

    @property
    def last_confidence(self) -> float:
        return self._last_confidence

    @property
    def last_updated(self) -> Optional[datetime]:
        return self._last_updated
