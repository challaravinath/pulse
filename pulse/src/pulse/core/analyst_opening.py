"""
analyst_opening.py
Generates the opening analyst briefing from 7-day instant data.

Returns a dict with:
  summary:  one plain-English headline sentence
  insights: list of {text, direction, value, icon} — rendered as icon pills

Rules compute all numbers. LLM writes the language. Called once on connect.
"""
import logging
import json
import pandas as pd
import numpy as np
from typing import Dict, Optional, List

logger = logging.getLogger(__name__)


def generate_opening_statement(
    instant_data: Dict[str, pd.DataFrame],
    llm_client,
    llm_model: str,
    enricher=None,
    product_name: str = "your product",
) -> dict:
    """
    Returns dict: {summary, insights}
    Falls back to rule-based if LLM fails.
    Stored in session_state.analyst_opening as a dict.
    """
    try:
        facts = _extract_facts(instant_data, enricher, product_name)
        if not facts:
            return {}
        result = _llm_write(facts, llm_client, llm_model, product_name)
        if not result:
            result = _rule_write(facts, product_name)
        return result
    except Exception as e:
        logger.warning(f"Opening statement failed: {e}")
        return {}


def _extract_facts(instant_data, enricher, product_name) -> Optional[Dict]:
    daily   = instant_data.get('daily',   pd.DataFrame())
    regions = instant_data.get('regions', pd.DataFrame())
    top10   = instant_data.get('top10',   pd.DataFrame())
    facts   = {}

    # ── Active orgs + WoW ────────────────────────────────────────────────
    if not daily.empty:
        ao_col = next((c for c in daily.columns if c.lower() in
                      ('active_orgs','active_organizations','dcount_orgid')), None)
        ev_col = next((c for c in daily.columns if c.lower() in
                      ('events','event_count')), None)

        val_col = ao_col or ev_col
        metric_name = "active orgs" if ao_col else "events"
        if val_col:
            vals   = pd.to_numeric(daily[val_col], errors='coerce').fillna(0).tolist()
            recent = float(np.median(vals[-4:])) if len(vals) >= 4 else float(np.median(vals))
            prior  = float(np.median(vals[:-4])) if len(vals) >= 7 else recent
            wow    = ((recent - prior) / prior * 100) if prior > 0 else 0.0
            wow    = max(-200.0, min(200.0, wow))
            facts['metric_name']  = metric_name
            facts['active_orgs']  = int(round(recent))
            facts['wow_pct']      = round(wow, 1)

    # ── Regions ──────────────────────────────────────────────────────────
    if not regions.empty:
        g_col   = next((c for c in regions.columns if c.lower() in
                       ('geoname','geo','region','geography')), None)
        ao_col2 = next((c for c in regions.columns if c.lower() in
                       ('active_orgs','active_organizations','dcount_orgid')), None)
        ev_col2 = next((c for c in regions.columns if c.lower() in
                       ('events','event_count')), None)
        v_col   = ao_col2 or ev_col2
        if g_col and v_col:
            regions = regions.copy()
            regions[v_col] = pd.to_numeric(regions[v_col], errors='coerce').fillna(0)
            sorted_r = regions.nlargest(2, v_col)
            facts['top_region']    = str(sorted_r.iloc[0][g_col])
            facts['region_count']  = len(regions)
            # Smallest region for watch signal
            smallest = regions.nsmallest(1, v_col).iloc[0]
            facts['small_region']  = str(smallest[g_col])
            total_v = regions[v_col].sum()
            top_share = float(sorted_r.iloc[0][v_col]) / total_v * 100 if total_v > 0 else 0
            facts['top_region_share'] = round(top_share, 0)

    # ── Top org ──────────────────────────────────────────────────────────
    if not top10.empty:
        id_col  = next((c for c in top10.columns if c.lower() in
                       ('orgid','org_id','organizationid')), None)
        ev_col3 = next((c for c in top10.columns if c.lower() in
                       ('events','event_count')), None)
        if id_col and ev_col3:
            top10 = top10.copy()
            top10[ev_col3] = pd.to_numeric(top10[ev_col3], errors='coerce').fillna(0)
            top_row = top10.nlargest(1, ev_col3).iloc[0]
            oid  = str(top_row[id_col])
            name = oid
            if enricher and enricher.is_loaded:
                try:
                    r = enricher.resolve(oid)
                    if r and r.org_name:
                        name = r.display_name
                except Exception:
                    pass
            if name == oid and len(oid) > 20:
                name = oid[:16] + "…"
            facts['top_org']        = name
            facts['top_org_events'] = int(top_row[ev_col3])
            total_e = float(top10[ev_col3].sum())
            facts['top_org_share']  = round(float(top_row[ev_col3]) / total_e * 100, 0) if total_e > 0 else 0

    return facts if facts else None


def _fmt(n: float) -> str:
    if abs(n) >= 1e9: return f"{n/1e9:.1f}B"
    if abs(n) >= 1e6: return f"{n/1e6:.1f}M"
    if abs(n) >= 1e3: return f"{n/1e3:.0f}K"
    return f"{int(n):,}"


def _llm_write(facts: Dict, llm_client, llm_model: str, product_name: str) -> dict:
    """
    LLM receives structured facts, returns JSON with summary + insight bullets.
    Each insight has: text (string), direction (up|down|neutral), value (string).
    """
    try:
        wow = facts.get('wow_pct', 0)
        direction_word = "up" if wow > 2 else ("down" if wow < -2 else "flat")

        prompt = f"""Product: {product_name}
Active {facts.get('metric_name','orgs')} this week: {_fmt(facts.get('active_orgs', 0))}
Week-on-week change: {wow:+.1f}% ({direction_word})
Top region: {facts.get('top_region','unknown')} ({facts.get('top_region_share',0):.0f}% of activity)
Regions active: {facts.get('region_count','unknown')}
Top customer: {facts.get('top_org','unknown')} ({facts.get('top_org_share',0):.0f}% of top-10 events)
Smallest region: {facts.get('small_region','unknown')}

Return ONLY valid JSON, no markdown, no explanation:
{{
  "summary": "One headline sentence — overall health, direct, max 20 words.",
  "insights": [
    {{"text": "concise insight", "direction": "up|down|neutral", "value": "the key number"}},
    {{"text": "concise insight", "direction": "up|down|neutral", "value": "the key number"}},
    {{"text": "concise insight", "direction": "up|down|neutral", "value": "the key number"}}
  ]
}}

Rules:
- summary: state what is happening overall, no preamble
- insights: exactly 3 bullets, each max 12 words
- direction: "up" = good/growing, "down" = declining/concerning, "neutral" = informational
- value: the most important number for that insight (e.g. "+4%", "2,847", "NAM 50%")
- plain English, no jargon, no "Based on the data"
"""

        resp = llm_client.chat.completions.create(
            model=llm_model,
            messages=[
                {"role": "system", "content":
                 "You are a concise product analyst. Return ONLY valid JSON. No markdown. No explanation."},
                {"role": "user", "content": prompt},
            ],
            max_tokens=300,
            temperature=0.2,
        )
        raw = resp.choices[0].message.content.strip()
        # Strip possible markdown fences
        raw = raw.replace("```json", "").replace("```", "").strip()
        data = json.loads(raw)
        # Validate structure
        if "summary" in data and "insights" in data:
            return data
        logger.warning("LLM returned unexpected JSON structure")
        return {}
    except Exception as e:
        logger.warning(f"LLM opening failed: {e}")
        return {}


def _rule_write(facts: Dict, product_name: str) -> dict:
    """Fallback rule-based output — same structure as LLM output."""
    n   = facts.get('active_orgs', 0)
    wow = facts.get('wow_pct', 0)
    rgn = facts.get('top_region', '')
    top = facts.get('top_org', '')

    direction_word = "up" if wow > 2 else ("down" if wow < -2 else "holding steady")
    summary = f"{product_name} — {_fmt(n)} active orgs this week, {direction_word} {abs(wow):.0f}% week-on-week."

    insights = []
    wow_dir = "up" if wow > 2 else ("down" if wow < -2 else "neutral")
    insights.append({
        "text": f"Week-on-week {'growth' if wow > 0 else 'decline'} across active orgs",
        "direction": wow_dir,
        "value": f"{wow:+.1f}%",
    })
    if rgn:
        share = facts.get('top_region_share', 0)
        insights.append({
            "text": f"{rgn} is the most active region",
            "direction": "neutral",
            "value": f"{share:.0f}% of activity",
        })
    if top:
        share = facts.get('top_org_share', 0)
        insights.append({
            "text": f"{top} leads customer activity",
            "direction": "neutral",
            "value": f"{share:.0f}% of top-10",
        })
    # Pad to 3 if needed
    while len(insights) < 3:
        insights.append({"text": "Profile building — more signals soon", "direction": "neutral", "value": ""})

    return {"summary": summary, "insights": insights[:3]}
