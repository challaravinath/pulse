"""
Narrative Engine v3.0 — Conversational, Question-Aware Insights
================================================================

v3.0 fundamental change: the narrative now knows what the user asked
and generates a DIRECT ANSWER first, then supporting context.

Before (v2):
  "**Top 6 by events**\n\nThis is a highly concentrated distribution..."

After (v3):
  "EMEA is your biggest region by far — 138.5B events, 74% of all activity.
   GBR comes second at 42.2B, but there's a huge drop-off after that."

Principles:
  1. First sentence = direct answer to what was asked
  2. Key numbers inline, not in headers
  3. Context only if it adds meaning ("this is unusual because...")
  4. No analyst jargon ("concentration risk", "power-law distribution")
  5. Short. If it fits in 2-3 sentences, don't add more.

Author: PULSE Team
"""

import re
import logging
from typing import Optional
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════════

def generate_smart_insight(
    df: pd.DataFrame,
    intent: str,
    explanation: str,
    question: str = "",
) -> str:
    """
    Generate a conversational, direct-answer insight.
    question: the original user question (used to personalise the response)
    """
    if df is None or df.empty:
        return "No data available for this query."

    try:
        q = question.lower() if question else ""

        handler = {
            'ranking':          narrate_ranking,
            'trend':            narrate_trend,
            'health':           narrate_health,
            'summary':          narrate_summary,
            'total':            narrate_total,
            'overview':         narrate_overview,
            'lookup':           narrate_lookup,
            'compare':          narrate_compare,
            'dimension_health': narrate_dimension_health,
        }.get(intent, narrate_generic)

        return handler(df, explanation, q)

    except Exception as e:
        logger.warning(f"Narrative generation failed: {e}")
        n = len(df)
        return f"Found **{n:,}** results." if n > 0 else "No results found."


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers used across narrators
# ═══════════════════════════════════════════════════════════════════════════════

def _val_col(df):
    skip = {'day', 'date', 'time', 'timestamp', 'eventinfo_time',
            'first_event', 'last_event', 'first_seen', 'last_seen'}
    for c in df.select_dtypes(include='number').columns:
        if c.lower() not in skip:
            return c
    return None

def _label_col(df):
    skip_types = ['datetime64', 'float64', 'float32', 'int64', 'int32']
    for c in df.columns:
        if str(df[c].dtype) not in skip_types and c.lower() not in ('day', 'date', 'time'):
            return c
    return None

def _time_col(df):
    for c in df.columns:
        if c.lower() in ('day', 'date', 'time', 'eventinfo_time', 'timestamp'):
            return c
        if pd.api.types.is_datetime64_any_dtype(df[c]):
            return c
    return None

def _label(df, col, i):
    if col is None:
        return f"#{i+1}"
    try:
        v = str(df[col].iloc[i])
        return v[:22] + "…" if len(v) > 22 else v
    except Exception:
        return f"#{i+1}"

def _fmt(val):
    if not isinstance(val, (int, float, np.integer, np.floating)):
        return str(val)
    if pd.isna(val):
        return "N/A"
    v = float(val)
    if abs(v) >= 1e9:
        return f"{v/1e9:.1f}B"
    elif abs(v) >= 1e6:
        return f"{v/1e6:.1f}M"
    elif abs(v) >= 1e3:
        return f"{v/1e3:.1f}K"
    elif isinstance(v, float) and v != int(v):
        return f"{v:.1f}"
    return f"{int(v):,}"

_NET_EPOCH_TICKS = 621_355_968_000_000_000

def _fmt_date(val):
    """Format a date value safely — handles .NET ticks and bad timestamps."""
    try:
        import pandas as _pd
        ts = _pd.Timestamp(val)
        # .NET ticks or far-future timestamp
        if ts.year > __import__('datetime').datetime.now().year + 1 or ts.year < 2000:
            try:
                numeric = int(ts.value)  # nanoseconds since epoch
                if numeric > 6e26:  # looks like .NET ticks in ns
                    unix_ns = (numeric // 100 - _NET_EPOCH_TICKS) * 100
                    ts = _pd.Timestamp(unix_ns, unit='ns')
            except Exception:
                pass
        import pandas as _pd2; return ts.strftime('%b %d, %Y') if ts.year != _pd2.Timestamp.now().year else ts.strftime('%b %d')
    except Exception:
        return str(val)[:10]

def _pct(part, total):
    return round(part / total * 100) if total else 0

# Detect whether the question is about a specific entity
def _extract_subject(q: str) -> str:
    """Extract what the user is asking about: 'eu', 'EMEA', org name, etc."""
    patterns = [
        r'how(?:z|\'s|s)?\s+(?:is|are)?\s+(.+?)(?:\s+doing|\s+performing|\s+looking)?\s*$',
        r'what about\s+(.+)',
        r'tell me about\s+(.+)',
        r'show\s+(?:me\s+)?(.+)',
    ]
    for pat in patterns:
        m = re.match(pat, q.strip())
        if m:
            return m.group(1).strip()
    return ""


# ═══════════════════════════════════════════════════════════════════════════════
# Dimension Health — "howz eu doing", "how is EMEA", "how is org X"
# ═══════════════════════════════════════════════════════════════════════════════

def narrate_dimension_health(df, explanation, q):
    """Direct answer for 'how is X doing' questions.
    If subject isn't found in data, says so clearly instead of narrating wrong data.
    """
    # Extract subject from explanation e.g. "Performance of ROW vs all regions"
    subject = ""
    m = re.search(r'(?:for|of|matching)\s+[\'"]?([A-Z][A-Z0-9\-]+)[\'"]?', explanation)
    if m:
        subject = m.group(1)
    if not subject:
        subject = _extract_subject(q).upper()

    val_c = _val_col(df)
    lbl_c = _label_col(df)

    if not val_c:
        return f"Here's the breakdown — {len(df)} entries in the data."

    total = df[val_c].sum()
    n     = len(df)

    if subject and lbl_c:
        # Try exact match first, then partial
        mask_exact   = df[lbl_c].astype(str).str.upper() == subject.upper()
        mask_partial = df[lbl_c].astype(str).str.upper().str.contains(subject.upper(), na=False)

        if mask_exact.any():
            match = df[mask_exact].iloc[0]
        elif mask_partial.any():
            match = df[mask_partial].iloc[0]
            subject = str(match[lbl_c])  # use the actual value found
        else:
            # Subject not in data — be transparent, show what IS available
            available = df[lbl_c].astype(str).tolist()
            avail_str = ", ".join(available[:8])
            if len(available) > 8:
                avail_str += f" (+{len(available)-8} more)"
            top_name  = str(df.iloc[0][lbl_c]) if not df.empty else "?"
            top_val   = _fmt(df.iloc[0][val_c]) if not df.empty else "?"
            pct_top   = f"{df.iloc[0][val_c] / total * 100:.0f}%" if total > 0 else "?"
            return (
                f"**{subject}** doesn't appear in the data — the regions we have are: {avail_str}. "
                f"**{top_name}** is the leader with **{top_val}** events ({pct_top} of total)."
            )

        val  = match[val_c]
        pct  = f"{val / total * 100:.0f}%" if total > 0 else "?"

        # Find rank
        ranked = df.sort_values(val_c, ascending=False).reset_index(drop=True)
        rank_matches = ranked.index[ranked[lbl_c].astype(str).str.upper() == subject.upper()].tolist()
        rank_n = rank_matches[0] + 1 if rank_matches else "?"

        dim_word = _detect_dim_word(df, explanation, q)

        if rank_n == 1:
            opening = f"**{subject}** is your #1 {dim_word} with **{_fmt(val)}** events — {pct} of all activity."
            if n > 1:
                second      = ranked.iloc[1]
                second_name = str(second[lbl_c])
                second_val  = second[val_c]
                if second_val > 0:
                    ratio = val / second_val
                    opening += f" That's **{ratio:.1f}x** more than {second_name} in second place."
        elif isinstance(rank_n, int) and rank_n <= 3:
            opening = f"**{subject}** ranks **#{rank_n}** with **{_fmt(val)}** events ({pct} of total across {n} {dim_word}s)."
        else:
            opening = f"**{subject}** is ranked **#{rank_n}** out of {n} {dim_word}s, contributing **{_fmt(val)}** events ({pct} of total)."

        # Add context: nearest competitors
        context = ""
        if isinstance(rank_n, int) and n > 1:
            if rank_n == 1 and n >= 3:
                third_val = ranked.iloc[2][val_c]
                rest_pct  = f"{(total - val) / total * 100:.0f}%"
                context   = f" The other {n-1} {dim_word}s share the remaining {rest_pct}."
            elif rank_n > 1:
                leader      = ranked.iloc[0]
                leader_name = str(leader[lbl_c])
                leader_val  = leader[val_c]
                if leader_val > 0:
                    ratio   = leader_val / val if val > 0 else 0
                    context = f" **{leader_name}** leads at {_fmt(leader_val)} ({ratio:.1f}x ahead)."

        return opening + context

    # No subject — just narrate the full ranking
    return narrate_ranking(df, explanation, q)


def narrate_ranking(df, explanation, q):
    val_c = _val_col(df)
    lbl_c = _label_col(df)
    n = len(df)

    if not val_c:
        return f"Found **{n:,}** results."

    total = df[val_c].sum()
    if total == 0:
        return f"**{n}** results returned, but all values are zero."

    top_label = _label(df, lbl_c, 0)
    top_val = df[val_c].iloc[0]
    top_pct = _pct(top_val, total)

    # Detect what kind of dimension this is for better language
    dim_word = _detect_dim_word(lbl_c, q)

    # ── Opening: direct answer ──
    if n == 1:
        return f"**{top_label}** accounts for **{_fmt(top_val)}** events."

    # Decide how to frame based on question intent
    asking_about_breakdown = any(w in q for w in [
        'region', 'geo', 'country', 'browser', 'entity', 'feature',
        'breakdown', 'split', 'by', 'distribution',
    ])

    # Detect if this is a "bottom / lowest" query
    asking_bottom = any(w in q for w in [
        'bottom', 'lowest', 'least', 'worst', 'least active', 'lowest activity',
        'trailing', 'smallest', 'quiet',
    ])

    if asking_about_breakdown and n <= 15:
        # "Events by region" style — describe the landscape
        opening = _describe_landscape(df, val_c, lbl_c, dim_word, top_label, top_val, top_pct, n, total)
    elif asking_bottom:
        # "Bottom 10 orgs" style — describe the laggards
        opening = _describe_laggards(df, val_c, lbl_c, dim_word, n, total)
    else:
        # "Top 10 orgs" style — describe the leaders
        opening = _describe_leaders(df, val_c, lbl_c, dim_word, top_label, top_val, top_pct, n, total)

    return opening


def _detect_dim_word(lbl_c, q):
    """Return human word for what's being ranked."""
    if lbl_c:
        col = lbl_c.lower()
        if 'geo' in col or 'region' in col:
            return "region"
        if 'org' in col:
            return "org"
        if 'entity' in col:
            return "entity type"
        if 'browser' in col:
            return "browser"
        if 'country' in col:
            return "country"
    if 'region' in q or 'geo' in q:
        return "region"
    if 'org' in q:
        return "org"
    if 'entity' in q or 'feature' in q:
        return "entity"
    if 'browser' in q:
        return "browser"
    return "entry"


def _describe_landscape(df, val_c, lbl_c, dim_word, top, top_val, top_pct, n, total):
    """Landscape framing: 'Here's how X is split across regions...'"""
    lines = []

    if top_pct > 60:
        lines.append(
            f"**{top}** dominates — **{_fmt(top_val)}** events, {top_pct}% of all activity."
        )
        if n > 1:
            rest_str = _fmt(total - top_val)
            lines.append(
                f"The other {n-1} {dim_word}s share the remaining {100-top_pct}% ({rest_str} events)."
            )
    elif top_pct > 35:
        second = _label(df, lbl_c, 1)
        second_val = df[val_c].iloc[1]
        lines.append(
            f"**{top}** leads with **{_fmt(top_val)}** events ({top_pct}%), "
            f"followed by {second} at {_fmt(second_val)} ({_pct(second_val, total)}%)."
        )
        if n > 2:
            lines.append(f"The remaining {n-2} {dim_word}s make up the rest.")
    else:
        # Fairly even distribution
        lines.append(
            f"Activity is spread fairly evenly across {n} {dim_word}s. "
            f"**{top}** leads with {_fmt(top_val)} events ({top_pct}%)."
        )

    return " ".join(lines)


def _describe_leaders(df, val_c, lbl_c, dim_word, top, top_val, top_pct, n, total):
    """Leaders framing: 'Your top N orgs are...'"""
    if n >= 2:
        second = _label(df, lbl_c, 1)
        second_val = df[val_c].iloc[1]
        gap = top_val / second_val if second_val > 0 else 0

        if gap > 3:
            opening = (
                f"**{top}** is way out in front — **{_fmt(top_val)}** events, "
                f"{gap:.1f}x more than {second} in second place."
            )
        elif gap > 1.5:
            opening = (
                f"**{top}** leads with **{_fmt(top_val)}** events ({top_pct}% of total). "
                f"{second} is second at {_fmt(second_val)}."
            )
        else:
            opening = (
                f"The top spots are competitive. "
                f"**{top}** leads with **{_fmt(top_val)}** events, "
                f"just ahead of {second} at {_fmt(second_val)}."
            )
    else:
        opening = f"**{top}** — **{_fmt(top_val)}** events ({top_pct}% of total)."

    # Add tail note only if very skewed
    if n >= 5:
        top3 = df[val_c].iloc[:3].sum()
        top3_pct = _pct(top3, total)
        if top3_pct > 80:
            opening += f" The top 3 together account for {top3_pct}% of all activity."

    return opening


def _describe_laggards(df, val_c, lbl_c, dim_word, n, total):
    """Laggards framing: 'These N orgs show the lowest activity...'"""
    bottom_label = _label(df, lbl_c, 0)   # first row = lowest (ASC sort)
    bottom_val   = df[val_c].iloc[0]
    bottom_pct   = _pct(bottom_val, total)

    if n >= 2:
        second_label = _label(df, lbl_c, 1)
        second_val   = df[val_c].iloc[1]
        opening = (
            f"These **{n}** {dim_word}s show the lowest activity. "
            f"**{bottom_label}** has the fewest events at **{_fmt(bottom_val)}** "
            f"({bottom_pct}% of total), followed by {second_label} at {_fmt(second_val)}."
        )
    else:
        opening = (
            f"**{bottom_label}** has very low activity — "
            f"only **{_fmt(bottom_val)}** events ({bottom_pct}% of total)."
        )

    # Flag if any are near zero
    near_zero = df[df[val_c] <= 100] if val_c else df.iloc[0:0]
    if len(near_zero) > 0:
        opening += f" {len(near_zero)} of these have fewer than 100 events — worth a check-in."

    return opening


# ═══════════════════════════════════════════════════════════════════════════════
# Trend — "daily trend", "events over time"
# ═══════════════════════════════════════════════════════════════════════════════

def narrate_trend(df, explanation, q):
    t_col = _time_col(df)
    v_col = _val_col(df)

    if not t_col or not v_col:
        return f"**{len(df)}** data points returned."

    vals = df[v_col].dropna()
    n = len(vals)

    if n < 3:
        return f"Only **{n}** data points — need more history for a trend."

    avg = vals.mean()
    peak = vals.max()
    trough = vals.min()
    peak_day = _fmt_date(df.loc[vals.idxmax(), t_col]) if vals.idxmax() in df.index else "?"
    trough_day = _fmt_date(df.loc[vals.idxmin(), t_col]) if vals.idxmin() in df.index else "?"

    # Direction
    first_half = vals.iloc[:n//2].mean()
    second_half = vals.iloc[n//2:].mean()
    change = (second_half - first_half) / first_half * 100 if first_half > 0 else 0

    # Recent momentum
    recent = vals.iloc[-3:].mean() if n >= 5 else None
    baseline = vals.iloc[:-3].mean() if n >= 5 else None
    recent_vs = (recent - baseline) / baseline * 100 if (recent and baseline and baseline > 0) else 0

    # WoW
    wow = None
    if n >= 14:
        last7 = vals.iloc[-7:].sum()
        prev7 = vals.iloc[-14:-7].sum()
        wow = (last7 - prev7) / prev7 * 100 if prev7 > 0 else None

    # ── Build conversational response ──
    if abs(change) > 20:
        direction = "growing" if change > 0 else "declining"
        first_line = f"Activity is **{direction}** — up **{abs(change):.0f}%** in the second half of this period."
    elif abs(change) > 8:
        direction = "trending up" if change > 0 else "trending down"
        first_line = f"Activity is **{direction}** by {abs(change):.0f}% across this period."
    else:
        first_line = f"Activity has been **steady** — averaging **{_fmt(avg)}/day** with no major swings."

    if wow is not None:
        wow_dir = "up" if wow > 0 else "down"
        first_line += f" Week-over-week it's **{wow_dir} {abs(wow):.0f}%**."

    parts = [first_line]

    if peak_day and trough_day and peak_day != trough_day:
        parts.append(
            f"Busiest day was {peak_day} at **{_fmt(peak)}**; quietest was {trough_day} at {_fmt(trough)}."
        )

    if recent is not None and abs(recent_vs) > 15:
        direction = "above" if recent_vs > 0 else "below"
        parts.append(
            f"The last 3 days are running **{abs(recent_vs):.0f}% {direction} average** — "
            + ("momentum is building." if recent_vs > 0 else "things are cooling off.")
        )

    # Volatility warning
    cv = vals.std() / vals.mean() * 100 if vals.mean() > 0 else 0
    if cv > 40:
        parts.append(f"⚠️ There's high day-to-day variability (CV={cv:.0f}%) — worth investigating.")

    return " ".join(parts)


# ═══════════════════════════════════════════════════════════════════════════════
# Health — "any issues?", "anomalies?"
# ═══════════════════════════════════════════════════════════════════════════════

def narrate_health(df, explanation, q):
    # Sanitize date columns before analysis
    from .insight_cards import _sanitize_dates
    df = _sanitize_dates(df)

    t_col = _time_col(df)
    v_col = _val_col(df)

    if not t_col or not v_col:
        return "Need time-series data for a health check."

    vals = df[v_col].dropna()
    if len(vals) < 5:
        return "Not enough data for a meaningful health check (need at least 5 days)."

    avg = vals.mean()
    std = vals.std()
    issues = []

    # Recent drop
    recent = vals.iloc[-3:].mean()
    baseline = vals.iloc[:-3].mean()
    if baseline > 0:
        drop = (recent - baseline) / baseline * 100
        if drop < -25:
            issues.append(f"🔴 Activity has **dropped {abs(drop):.0f}%** in the last 3 days (avg {_fmt(recent)}/day vs baseline {_fmt(baseline)}/day).")
        elif drop < -15:
            issues.append(f"🟡 Activity is **down {abs(drop):.0f}%** recently — worth monitoring.")
        elif drop > 40:
            issues.append(f"🟡 There's an unusual **spike of {drop:.0f}%** in the last 3 days — check if it's organic.")

    # Low outlier days
    if std > 0:
        low_days = vals[vals < avg - 2 * std]
        if len(low_days) > 0:
            worst_val = low_days.min()
            worst_idx = low_days.idxmin()
            worst_day = _fmt_date(df.loc[worst_idx, t_col]) if worst_idx in df.index else "?"
            issues.append(f"🟡 **{len(low_days)} unusually low day(s)** — worst was {worst_day} at {_fmt(worst_val)}.")

    # WoW
    if len(vals) >= 14:
        last7 = vals.iloc[-7:].sum()
        prev7 = vals.iloc[-14:-7].sum()
        if prev7 > 0:
            wow = (last7 - prev7) / prev7 * 100
            if wow < -20:
                issues.append(f"🔴 **Week-over-week down {abs(wow):.0f}%** — this week {_fmt(last7)} vs last week {_fmt(prev7)}.")

    if not issues:
        return (
            f"✅ No drop detected. Scanned {len(vals)} days — "
            f"activity is within normal range, averaging **{_fmt(avg)}/day**."
        )

    # Lead with the most severe issue as a direct answer
    return "\n\n".join(issues) + f"\n\n*Based on {len(vals)} days of data.*"


# ═══════════════════════════════════════════════════════════════════════════════
# Summary — "weekly summary", "recap"
# ═══════════════════════════════════════════════════════════════════════════════

def narrate_summary(df, explanation, q):
    t_col = _time_col(df)
    v_cols = [c for c in df.select_dtypes(include='number').columns
              if c.lower() not in {'day', 'date', 'time', 'eventinfo_time',
                                    'first_seen', 'last_seen', 'first_event', 'last_event'}]

    if not v_cols:
        return f"{len(df)} data points."

    v_col = v_cols[0]
    vals = df[v_col].dropna()
    n = len(vals)
    total = vals.sum()
    avg = vals.mean()

    # ── Drop/spike detection — answer "why did X drop/spike" directly ──
    _asking_about_drop  = any(w in q for w in ['drop', 'fell', 'decline', 'decrease', 'down', 'lower', 'less', 'shrink'])
    _asking_about_spike = any(w in q for w in ['spike', 'jump', 'increase', 'surge', 'rise', 'higher', 'more', 'up'])

    if (_asking_about_drop or _asking_about_spike) and n >= 3:
        recent = vals.iloc[-3:].mean() if n >= 3 else vals.iloc[-1]
        earlier = vals.iloc[:-3].mean() if n > 3 else vals.iloc[0]
        if earlier > 0:
            pct_change = (recent - earlier) / earlier * 100
            peak_val = vals.max()
            peak_idx = vals.idxmax()
            peak_day_str = ""
            if t_col and peak_idx in df.index:
                peak_day_str = f" — peak was {_fmt_date(df.loc[peak_idx, t_col])} at {_fmt(peak_val)}"

            if abs(pct_change) > 10:
                direction = "dropped" if pct_change < 0 else "increased"
                severity = "sharply" if abs(pct_change) > 40 else "moderately"
                return (
                    f"Events {direction} {severity} — the last 3 days averaged **{_fmt(recent)}/day**, "
                    f"down {abs(pct_change):.0f}% from the earlier average of **{_fmt(earlier)}/day**"
                    f"{peak_day_str}. "
                    f"Total over this window: **{_fmt(total)}**."
                )

    lines = [f"Over this period: **{_fmt(total)}** total events across {n} days, averaging **{_fmt(avg)}/day**."]

    if n >= 6:
        first_half = vals.iloc[:n//2].mean()
        second_half = vals.iloc[n//2:].mean()
        if first_half > 0:
            change = (second_half - first_half) / first_half * 100
            direction = "up" if change > 0 else "down"
            lines.append(f"The second half of the period trended **{direction} {abs(change):.0f}%** vs the first half.")

    peak_idx = vals.idxmax()
    if t_col and peak_idx in df.index:
        peak_day = _fmt_date(df.loc[peak_idx, t_col])
        lines.append(f"Peak day was **{peak_day}** at {_fmt(vals.max())}.")

    return " ".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# Total — "how many orgs", "total events"
# ═══════════════════════════════════════════════════════════════════════════════

def narrate_total(df, explanation, q):
    if df.empty:
        return explanation

    if len(df) == 1 and len(df.columns) == 1:
        val = df.iloc[0, 0]
        col = df.columns[0].replace('total_', '').replace('_', ' ')
        return f"**{_fmt(val)}** {col}."

    if len(df) == 1:
        parts = []
        for col in df.columns:
            val = df.iloc[0][col]
            label = col.replace('total_', '').replace('_', ' ').title()
            try:
                parts.append(f"**{label}:** {_fmt(float(val))}")
            except (ValueError, TypeError):
                pass
        if parts:
            return " · ".join(parts)

    return f"**{len(df):,}** results returned."


# ═══════════════════════════════════════════════════════════════════════════════
# Overview, Lookup, Compare, Generic
# ═══════════════════════════════════════════════════════════════════════════════

def narrate_overview(df, explanation, q):
    return narrate_total(df, explanation, q)


def narrate_lookup(df, explanation, q):
    return narrate_ranking(df, explanation, q)


def narrate_compare(df, explanation, q):
    return narrate_ranking(df, explanation, q)


def narrate_generic(df, explanation, q):
    v_col = _val_col(df)
    if v_col:
        total = df[v_col].sum()
        return f"**{len(df):,}** results — **{_fmt(total)}** total {v_col.lower()}."
    return f"**{len(df):,}** results."
