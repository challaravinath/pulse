"""
AnalystVoice v1.0 — The "analyst who knows the data" layer
============================================================

Wraps every response before it renders. Gets three inputs:
  - original_question: what the user actually typed
  - result: the FastResult (what was actually queried)
  - df: the data that came back
  - conversation_history: recent turns

Does three things:

  1. MISMATCH CHECK — did we answer what was asked?
     If the router substituted data (e.g. ROW not found → all regions),
     say so upfront instead of narrating the wrong thing confidently.

  2. INTENT-AWARE OPENING — reframe the narrative to respond to the
     actual question, not just describe the data.

  3. PROACTIVE SIGNAL — one short observation the data suggests that
     the user didn't ask for but should probably know. Passive footnote,
     never overrides the main answer.

No LLM calls. Pure computation. Wraps the existing narrative.

Author: PULSE Team
"""

import re
import logging
import pandas as pd
import numpy as np
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Public API — called from _execute_fast_result before returning
# ─────────────────────────────────────────────────────────────────────────────

def apply(
    original_question: str,
    base_narrative: str,
    result,                          # FastResult dataclass
    df: pd.DataFrame,
    conversation_history: List = None,
    available_values: Dict[str, List[str]] = None,  # {table: [values]}
) -> str:
    """
    Wrap base_narrative with analyst framing.
    Returns the improved narrative string.
    """
    q = original_question.strip()
    fv = getattr(result, 'filter_value', '') or ''
    pattern = getattr(result, 'pattern', '') or ''
    table = getattr(result, 'table', '') or ''
    explanation = getattr(result, 'explanation', '') or ''

    parts = []

    # ── 1. Mismatch check ──────────────────────────────────────────────────
    mismatch = _detect_mismatch(q, fv, df, explanation, table, available_values)
    if mismatch:
        parts.append(mismatch)

    # ── 2. Intent-aware opening — only if NOT a mismatch situation ──────────
    if not mismatch:
        opener = _intent_opener(q, fv, pattern, df, conversation_history)
        if opener:
            # Replace the first sentence of base_narrative with the opener
            # to avoid double-stating the same thing
            base_narrative = opener + " " + _strip_first_sentence(base_narrative)

    # ── 3. Base narrative ──────────────────────────────────────────────────
    parts.append(base_narrative)

    # ── 4. Proactive signal (footnote) ────────────────────────────────────
    signal = _proactive_signal(q, df, pattern, table)
    if signal:
        parts.append(signal)

    return "\n\n".join(p for p in parts if p.strip())


# ─────────────────────────────────────────────────────────────────────────────
# 1. Mismatch detection
# ─────────────────────────────────────────────────────────────────────────────

# Words that are region/geo signals but not actual region names in most datasets
_GEO_SIGNAL_WORDS = {
    'row', 'apac', 'latam', 'mena', 'anz', 'sea', 'cee', 'dach',
    'nordics', 'benelux', 'weur', 'eeur',
}

# Stop-words that should never be treated as entity lookups
_STOP_WORDS = {
    'data', 'show', 'me', 'give', 'get', 'the', 'for', 'and', 'or',
    'plz', 'pls', 'please', 'all', 'my', 'our', 'its', 'top', 'a', 'an',
    'is', 'are', 'has', 'have', 'does', 'do', 'did', 'was', 'were',
    'how', 'what', 'when', 'where', 'which', 'who', 'why',
}


def _detect_mismatch(
    question: str,
    filter_value: str,
    df: pd.DataFrame,
    explanation: str,
    table: str,
    available_values: Dict[str, List[str]] = None,
) -> Optional[str]:
    """
    Detect when we answered something different from what was asked.
    Returns a transparent acknowledgment string, or None if no mismatch.
    """
    q_lower = question.lower()

    # Case 1: Explicit fallback signal from fast_router
    if filter_value and 'not found as exact match' in explanation:
        asked = filter_value
        label_col = _label_col(df)
        available_in_data = []
        if label_col and not df.empty:
            available_in_data = df[label_col].astype(str).tolist()[:10]

        suggestion = _suggest_closest(asked, available_in_data)
        avail_str = ", ".join(available_in_data[:6])

        msg = f"**\"{asked}\"** isn't a value in your data."
        if suggestion:
            msg += f" Closest match is **{suggestion}**."
        if avail_str:
            msg += f" Your actual values: {avail_str}."
        msg += " Showing the full breakdown instead."
        return msg

    # Case 2: filter_value set but not found in returned data
    if filter_value and not df.empty:
        label_col = _label_col(df)
        if label_col:
            data_values = df[label_col].astype(str).str.upper().tolist()
            if filter_value.upper() not in data_values:
                if _is_specific_lookup(question, filter_value):
                    avail = ", ".join(data_values[:6])
                    return (
                        f"**{filter_value}** doesn't appear in your data "
                        f"(available: {avail}). Showing full breakdown."
                    )

    # Case 3: ★ filter_value is empty but question contains a specific geo/entity word
    # that doesn't match anything in the data.
    # e.g. "row data plz" → filter_value='', but 'row' not in EMEA/GBR/FRA/NA...
    if not filter_value and not df.empty and ('profile_region' in table or 'profile_org' in table):
        label_col = _label_col(df)
        if label_col:
            # Extract candidate entity words from question
            words = re.findall(r'\b[a-zA-Z]{2,}\b', q_lower)
            candidates = [w for w in words if w not in _STOP_WORDS]

            data_values_upper = set(df[label_col].astype(str).str.upper().tolist())
            data_values_lower = set(df[label_col].astype(str).str.lower().tolist())

            for candidate in candidates:
                cand_up = candidate.upper()
                # Skip if it IS in the data (valid lookup)
                if cand_up in data_values_upper or candidate in data_values_lower:
                    continue
                # Only flag geo signal words that suggest user wanted a specific region
                if candidate in _GEO_SIGNAL_WORDS:
                    avail = ", ".join(sorted(data_values_upper)[:8])
                    suggestion = _suggest_closest(candidate, list(data_values_upper))
                    msg = f"**{candidate.upper()}** isn't a region in your data."
                    if suggestion:
                        msg += f" Closest is **{suggestion}**."
                    msg += f" Your regions: {avail}. Showing full breakdown instead."
                    return msg

    return None


def _is_specific_lookup(question: str, filter_value: str) -> bool:
    """Return True if the question was clearly asking about a specific value."""
    q = question.lower()
    fv = filter_value.lower()
    # Direct mention of the value
    if fv in q:
        return True
    # "how is X", "X status", "X data", "X performance"
    if re.search(r'how (?:is|are|has)\s+\w', q):
        return True
    return False


def _suggest_closest(asked: str, available: List[str]) -> Optional[str]:
    """Find the closest match by simple string overlap."""
    asked_l = asked.lower()
    best, best_score = None, 0
    for v in available:
        v_l = v.lower()
        # Substring match
        if asked_l in v_l or v_l in asked_l:
            score = len(asked_l) / max(len(v_l), 1)
            if score > best_score:
                best, best_score = v, score
        # Common prefix
        common = sum(1 for a, b in zip(asked_l, v_l) if a == b)
        score = common / max(len(asked_l), 1)
        if score > 0.6 and score > best_score:
            best, best_score = v, score
    return best if best_score > 0.4 else None


# ─────────────────────────────────────────────────────────────────────────────
# 2. Intent-aware opener
# ─────────────────────────────────────────────────────────────────────────────

def _intent_opener(
    question: str,
    filter_value: str,
    pattern: str,
    df: pd.DataFrame,
    history: List = None,
) -> Optional[str]:
    """
    Generate a conversational opening line that responds to the actual question,
    not just describes the data. Returns None if the base narrative is fine as-is.
    """
    q = question.lower().strip()

    # Follow-up question — reference conversation context
    if history and _is_followup(q):
        prev = _get_last_topic(history)
        if prev:
            return f"Following up on {prev} —"

    # "summarize for my manager" type questions — handled by executive_briefing
    # Don't add opener for those
    if any(w in q for w in ['manager', 'executive', 'brief', 'summary for']):
        return None

    # Casual/abbreviated queries — add context the user didn't spell out
    abbreviations = {
        'plz': True, 'pls': True, 'lol': True,
        'wut': True, 'wat': True, 'wot': True,
    }
    is_casual = any(w in q.split() for w in abbreviations)
    is_short = len(q.split()) <= 3

    if (is_casual or is_short) and filter_value:
        # e.g. "row data plz" → "Here's what your data shows for ROW:"
        return f"Here's what your data shows for **{filter_value.upper()}**:"

    # "how is X" / "how's X doing" pattern
    if re.search(r"how['\s]*(is|are|s)\s+", q) and filter_value:
        return None  # narrate_dimension_health already handles this well

    # Trend/time questions
    if any(w in q for w in ['trend', 'over time', 'daily', 'growing', 'declining']):
        return None  # trend narrative already good

    return None


def _is_followup(q: str) -> bool:
    """Detect follow-up references."""
    followup_signals = [
        'that', 'those', 'this', 'it', 'them', 'same',
        'more', 'also', 'now', 'and', 'what about',
        'break down', 'drill', 'zoom', 'dig',
    ]
    return any(q.startswith(s) or f' {s} ' in q for s in followup_signals)


def _get_last_topic(history) -> Optional[str]:
    """Get a short description of what the last turn was about."""
    if not history:
        return None
    last = history[-1]
    msg = getattr(last, 'user_message', '') or ''
    if len(msg) > 40:
        msg = msg[:40] + "…"
    return f'"{msg}"' if msg else None  # no ** — bold breaks in some Streamlit renders


# ─────────────────────────────────────────────────────────────────────────────
# 3. Proactive signal
# ─────────────────────────────────────────────────────────────────────────────

def _proactive_signal(
    question: str,
    df: pd.DataFrame,
    pattern: str,
    table: str,
) -> Optional[str]:
    """
    One short observation the data suggests that the user didn't explicitly ask for.
    Rendered as a subtle footnote. Returns None if nothing noteworthy.
    """
    if df is None or df.empty or len(df) < 3:
        return None

    q_lower = question.lower()
    signals = []

    val_col = _val_col(df)
    label_col = _label_col(df)

    if val_col is None:
        return None

    vals = df[val_col].dropna()
    if len(vals) < 2:
        return None

    # ── Signal A: Data quality — "unavailable" or nulls in label col ──────
    if label_col:
        labels = df[label_col].astype(str).str.lower()
        bad_labels = labels.isin(['unavailable', 'unknown', 'null', 'none', '', 'n/a'])
        if bad_labels.any():
            bad_count = df.loc[bad_labels, val_col].sum() if val_col else 0
            total = vals.sum()
            pct = bad_count / total * 100 if total > 0 else 0
            if pct > 1:
                signals.append(
                    f"⚠️ **{pct:.0f}% of events have no {label_col}** "
                    f"(labeled \"unavailable\") — worth investigating for data coverage."
                )

    # ── Signal B: Extreme concentration (didn't ask about it, but notable) ──
    if label_col and 'top' not in q_lower and 'concentration' not in q_lower:
        total = vals.sum()
        if total > 0 and len(df) >= 5:
            top1_pct = df[val_col].max() / total * 100
            if top1_pct > 70:
                top_name = str(df.loc[df[val_col].idxmax(), label_col])
                signals.append(
                    f"📌 **{top_name} alone accounts for {top1_pct:.0f}% of all activity** "
                    f"— single-entity dependency worth monitoring."
                )

    # ── Signal C: Long tail (most rows contribute almost nothing) ─────────
    if len(df) >= 8 and label_col:
        total = vals.sum()
        if total > 0:
            bottom_half_pct = df[val_col].nsmallest(len(df) // 2).sum() / total * 100
            if bottom_half_pct < 2:
                signals.append(
                    f"💡 The bottom {len(df)//2} entries together represent only "
                    f"**{bottom_half_pct:.1f}% of activity** — heavy long tail."
                )

    # Return the highest-priority signal only (avoid noise)
    if signals:
        return signals[0]
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────────────────────────────────────

def _val_col(df: pd.DataFrame) -> Optional[str]:
    for name in ['events', 'event_count', 'total_events', 'count', 'sessions']:
        for c in df.columns:
            if c.lower() == name:
                return c
    num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    return num_cols[0] if num_cols else None


def _label_col(df: pd.DataFrame) -> Optional[str]:
    for name in ['geoname', 'geo', 'region', 'country', 'orgid', 'org_id',
                 'organization', 'entityname', 'entity', 'browsername', 'browser']:
        for c in df.columns:
            if c.lower() == name:
                return c
    str_cols = [c for c in df.columns if not pd.api.types.is_numeric_dtype(df[c])]
    # Exclude date columns
    date_names = {'day', 'date', 'time', 'timestamp', 'eventinfo_time',
                  'first_seen', 'last_seen', 'first_event', 'last_event'}
    str_cols = [c for c in str_cols if c.lower() not in date_names]
    return str_cols[0] if str_cols else None


def _strip_first_sentence(text: str) -> str:
    """Remove the first sentence from a narrative string."""
    # Split on first sentence-ending punctuation followed by space
    m = re.search(r'[.!?]\s+', text)
    if m:
        return text[m.end():].strip()
    return text
