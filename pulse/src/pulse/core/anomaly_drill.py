"""
AnomalyDrill v1.0 — Automatic follow-up when health check fires red
=====================================================================

When a health check detects a significant drop or spike, a human analyst
would immediately ask: "Where? Which region? Which org? Was it sudden?"

This module does exactly that — automatically, before the user has to ask.

Flow:
  1. health check fires → returns findings with severity='red'
  2. AnomalyDrill.investigate() is called with those findings + profile access
  3. Runs up to 3 targeted DuckDB queries (instant, no Kusto)
  4. Returns DrillResult with structured findings + narrative

Design principles:
  - Zero Kusto calls — profile only (instant)
  - Zero LLM calls — pure computation
  - Silent failure — if drill fails, original health response is unchanged
  - Additive — appended below the health findings, never replaces them

Author: PULSE Team
"""

import logging
import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class DrillFinding:
    """Single drill-down finding."""
    dimension: str          # "region", "org", "trend"
    headline: str           # "NA dropped 80% — accounts for most of the decline"
    detail: str             # supporting numbers
    severity: str           # "red", "amber", "green"
    data: Optional[pd.DataFrame] = None


@dataclass
class DrillResult:
    """Complete auto-drill result."""
    triggered: bool                     # was drill actually needed?
    primary_signal: str                 # what triggered the drill
    findings: List[DrillFinding]        # drill-down findings
    narrative: str                      # full narrative to append
    drill_count: int                    # how many queries ran


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def investigate(
    health_findings: List[Dict],
    daily_df: Optional[pd.DataFrame],
    data_profile,          # DataProfile instance
) -> DrillResult:
    """
    Given red health findings, auto-drill into region + org + trend.

    Args:
        health_findings: list of finding dicts from build_health_analysis
        daily_df:        current daily time-series data
        data_profile:    DataProfile instance (for instant profile queries)

    Returns:
        DrillResult — always safe, findings=[] if nothing notable
    """
    # Only drill on red (critical) findings
    red = [f for f in health_findings if f.get('severity') == 'red']
    if not red:
        return DrillResult(
            triggered=False,
            primary_signal='',
            findings=[],
            narrative='',
            drill_count=0,
        )

    primary = red[0]
    signal = primary.get('title', 'Issue detected')
    logger.info(f"AnomalyDrill triggered: {signal}")

    findings = []
    drill_count = 0

    # ── Drill 1: What changed by region? ─────────────────────────────────────
    region_finding = _drill_region(data_profile)
    if region_finding:
        findings.append(region_finding)
        drill_count += 1

    # ── Drill 2: What changed by org? ────────────────────────────────────────
    org_finding = _drill_org(data_profile)
    if org_finding:
        findings.append(org_finding)
        drill_count += 1

    # ── Drill 3: Was it sudden or gradual? ───────────────────────────────────
    trend_finding = _drill_trend_shape(daily_df)
    if trend_finding:
        findings.append(trend_finding)
        drill_count += 1

    if not findings:
        return DrillResult(
            triggered=True,
            primary_signal=signal,
            findings=[],
            narrative='',
            drill_count=0,
        )

    narrative = _build_narrative(signal, findings)

    return DrillResult(
        triggered=True,
        primary_signal=signal,
        findings=findings,
        narrative=narrative,
        drill_count=drill_count,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Drill implementations
# ─────────────────────────────────────────────────────────────────────────────

def _drill_region(data_profile) -> Optional[DrillFinding]:
    """
    Check profile_region for concentration / standout changes.
    Looks for: one region dominating drop, or one region going to zero.
    """
    try:
        if not data_profile or not data_profile.has_table('profile_region'):
            return None

        df = data_profile.query_safe(
            "SELECT * FROM profile_region ORDER BY events DESC"
        )
        if df is None or df.empty or len(df) < 2:
            return None

        val_col = _find_col(df, ['events', 'event_count', 'total_events'])
        lbl_col = _find_col(df, ['geoname', 'geo', 'region', 'country'])
        if not val_col or not lbl_col:
            return None

        df[val_col] = pd.to_numeric(df[val_col], errors='coerce').fillna(0)
        total = df[val_col].sum()
        if total == 0:
            return None

        # Top region share
        top_row = df.iloc[0]
        top_name = str(top_row[lbl_col])
        top_val  = top_row[val_col]
        top_pct  = top_val / total * 100

        # Dead regions (non-zero names with 0 or near-0 events)
        dead = df[(df[val_col] <= 1) & (~df[lbl_col].astype(str).str.lower().isin(
            ['unavailable', 'unknown', 'none', 'n/a', '']
        ))]

        if top_pct > 85:
            severity = 'red'
            headline = (
                f"**{top_name}** is carrying {top_pct:.0f}% of all activity "
                f"— the rest are near zero"
            )
            detail = _format_top5(df, val_col, lbl_col, total)
        elif len(dead) >= 2:
            dead_names = ", ".join(dead[lbl_col].astype(str).tolist()[:4])
            severity = 'red'
            headline = (
                f"{len(dead)} regions have essentially zero events: {dead_names}"
            )
            detail = f"This could indicate a data pipeline issue or genuine inactivity."
        else:
            top3 = df.head(3)
            top3_pct = top3[val_col].sum() / total * 100
            if top3_pct > 75:
                severity = 'amber'
                names = ", ".join(top3[lbl_col].astype(str).tolist())
                headline = f"Top 3 regions ({names}) hold {top3_pct:.0f}% of activity"
                detail = _format_top5(df, val_col, lbl_col, total)
            else:
                return None  # Nothing notable

        return DrillFinding(
            dimension='region',
            headline=headline,
            detail=detail,
            severity=severity,
            data=df,
        )

    except Exception as e:
        logger.debug(f"Region drill failed: {e}")
        return None


def _drill_org(data_profile) -> Optional[DrillFinding]:
    """
    Check profile_organization for single-org dominance or sudden absence.
    """
    try:
        if not data_profile or not data_profile.has_table('profile_organization'):
            return None

        df = data_profile.query_safe(
            "SELECT * FROM profile_organization ORDER BY events DESC LIMIT 20"
        )
        if df is None or df.empty or len(df) < 2:
            return None

        val_col = _find_col(df, ['events', 'event_count', 'total_events'])
        lbl_col = _find_col(df, ['orgid', 'org_id', 'organization', 'tenantid', 'tenant_id'])
        if not val_col or not lbl_col:
            return None

        df[val_col] = pd.to_numeric(df[val_col], errors='coerce').fillna(0)
        total = df[val_col].sum()
        if total == 0:
            return None

        top_row = df.iloc[0]
        top_name = str(top_row[lbl_col])
        if len(top_name) > 30:
            top_name = top_name[:28] + "…"
        top_val  = top_row[val_col]
        top_pct  = top_val / total * 100

        # Check for extreme single-org dominance
        if top_pct > 60:
            severity = 'red'
            headline = (
                f"**{top_name}** alone accounts for {top_pct:.0f}% of events "
                f"({_fmt(top_val)} of {_fmt(total)} total)"
            )
            detail = (
                f"If this org goes quiet, your numbers will collapse. "
                f"Next largest: {_fmt(df.iloc[1][val_col])} events "
                f"({df.iloc[1][val_col]/total*100:.0f}%)."
            )
            return DrillFinding(
                dimension='org',
                headline=headline,
                detail=detail,
                severity=severity,
                data=df.head(5),
            )

        # Check for cliff — big drop between #1 and #2
        v1, v2 = df[val_col].iloc[0], df[val_col].iloc[1]
        if v2 > 0:
            gap = (v1 - v2) / v2 * 100
            if gap > 200:
                severity = 'amber'
                n2 = str(df[lbl_col].iloc[1])[:28]
                headline = (
                    f"**{top_name}** is {gap:.0f}% ahead of #{2} ({n2}) "
                    f"— extreme top-heaviness"
                )
                detail = _format_top5(df, val_col, lbl_col, total)
                return DrillFinding(
                    dimension='org',
                    headline=headline,
                    detail=detail,
                    severity=severity,
                    data=df.head(5),
                )

        return None

    except Exception as e:
        logger.debug(f"Org drill failed: {e}")
        return None


def _drill_trend_shape(daily_df: Optional[pd.DataFrame]) -> Optional[DrillFinding]:
    """
    Analyse the shape of the drop: sudden cliff vs gradual slide.
    Uses the daily data already in memory — no extra query.
    """
    try:
        if daily_df is None or daily_df.empty or len(daily_df) < 6:
            return None

        t_col = _find_col(daily_df, ['day', 'date', 'eventinfo_time', 'timestamp'])
        v_col = _find_col(daily_df, ['events', 'event_count', 'total_events'])
        if not t_col or not v_col:
            return None

        df = daily_df.copy()
        try:
            df[t_col] = pd.to_datetime(df[t_col], errors='coerce', utc=True).dt.tz_convert(None)
        except Exception:
            df[t_col] = pd.to_datetime(df[t_col], errors='coerce')
        df = df.dropna(subset=[t_col]).sort_values(t_col)
        df[v_col] = pd.to_numeric(df[v_col], errors='coerce').fillna(0)

        vals = df[v_col].values
        n    = len(vals)

        # Baseline: first 60% of data
        baseline_end = int(n * 0.6)
        baseline_mean = vals[:baseline_end].mean()
        if baseline_mean <= 0:
            return None

        recent = vals[baseline_end:]
        recent_mean = recent.mean()
        overall_change_pct = (recent_mean - baseline_mean) / baseline_mean * 100

        if abs(overall_change_pct) < 20:
            return None  # Not significant enough to call out

        # Is it a sudden cliff or a slow slide?
        # Cliff: last 2 days are much lower than the 5 before them
        last_2  = vals[-2:].mean()
        prev_5  = vals[-7:-2].mean() if n >= 7 else vals[:-2].mean()

        if prev_5 > 0:
            recent_drop = (last_2 - prev_5) / prev_5 * 100
        else:
            recent_drop = 0

        # Find when the change started (first day significantly below baseline)
        threshold = baseline_mean * 0.6  # 40% below baseline = notable
        change_point_idx = None
        for i in range(baseline_end, n):
            if vals[i] < threshold:
                change_point_idx = i
                break

        if change_point_idx is not None:
            change_date = df[t_col].iloc[change_point_idx]
            try:
                change_date_str = pd.Timestamp(change_date).strftime('%b %d')
            except Exception:
                change_date_str = "recently"
        else:
            change_date_str = None

        if recent_drop < -40:
            # Sudden cliff in the last 2 days
            severity = 'red'
            headline = (
                f"Sharp drop in the last 2 days — {_fmt(last_2)}/day vs "
                f"{_fmt(prev_5)}/day the week before ({recent_drop:.0f}%)"
            )
            detail = (
                "This looks like a sudden event (outage, pipeline gap, or external change), "
                "not a gradual trend. Check for data pipeline issues first."
            )
        elif overall_change_pct < -40:
            # Sustained decline
            severity = 'amber'
            headline = (
                f"Sustained decline: {overall_change_pct:.0f}% below earlier baseline"
            )
            if change_date_str:
                detail = f"Decline started around {change_date_str}. Avg now {_fmt(recent_mean)}/day vs {_fmt(baseline_mean)}/day before."
            else:
                detail = f"Activity has been trending down. Current avg {_fmt(recent_mean)}/day vs {_fmt(baseline_mean)}/day earlier."
        elif overall_change_pct > 40:
            severity = 'green'
            headline = (
                f"Strong growth: {overall_change_pct:+.0f}% above earlier baseline"
            )
            detail = f"Current avg {_fmt(recent_mean)}/day vs {_fmt(baseline_mean)}/day earlier."
        else:
            return None

        return DrillFinding(
            dimension='trend',
            headline=headline,
            detail=detail,
            severity=severity,
        )

    except Exception as e:
        logger.debug(f"Trend shape drill failed: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Narrative builder
# ─────────────────────────────────────────────────────────────────────────────

def _build_narrative(primary_signal: str, findings: List[DrillFinding]) -> str:
    """Build the auto-drill narrative to append after health findings."""
    if not findings:
        return ''

    lines = ["\n---\n**🔍 Auto-drill — here's why:**\n"]

    dim_labels = {
        'trend':  '📉 How it happened',
        'region': '🌍 Where it happened',
        'org':    '🏢 Who\'s involved',
    }

    # Order: trend first (what happened), then region (where), then org (who)
    ordered = sorted(findings, key=lambda f: {'trend': 0, 'region': 1, 'org': 2}.get(f.dimension, 3))

    for f in ordered:
        icon = {'red': '🔴', 'amber': '🟡', 'green': '🟢'}.get(f.severity, '⚪')
        label = dim_labels.get(f.dimension, f.dimension.title())
        lines.append(f"**{label}**")
        lines.append(f"{icon} {f.headline}")
        if f.detail:
            lines.append(f"*{f.detail}*")
        lines.append("")

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────────────────────────────────────

def _find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for name in candidates:
        for c in df.columns:
            if c.lower() == name:
                return c
    return None


def _fmt(n) -> str:
    if n is None or (isinstance(n, float) and np.isnan(n)):
        return "—"
    n = float(n)
    if abs(n) >= 1e12: return f"{n/1e12:.1f}T"
    if abs(n) >= 1e9:  return f"{n/1e9:.1f}B"
    if abs(n) >= 1e6:  return f"{n/1e6:.1f}M"
    if abs(n) >= 1e3:  return f"{n/1e3:.1f}K"
    return f"{n:,.0f}"


def _format_top5(df: pd.DataFrame, val_col: str, lbl_col: str, total: float) -> str:
    """Format top 5 rows as a compact breakdown string."""
    lines = []
    for _, row in df.head(5).iterrows():
        val  = row[val_col]
        name = str(row[lbl_col])[:25]
        pct  = val / total * 100 if total > 0 else 0
        lines.append(f"{name}: {_fmt(val)} ({pct:.0f}%)")
    return "  ·  ".join(lines)
