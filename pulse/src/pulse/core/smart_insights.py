"""
Smart Insights v1.0 — Intent-Aware Analysis
=============================================

Different questions get different analysis:
  - "top 10 orgs"     → ranking: concentration, dominance, long tail
  - "trend last week"  → trend: slope, acceleration, WoW change
  - "any issues"       → health: drops, spikes, missing data, anomalies
  - "weekly summary"   → summary: key changes, period comparison
  - "how many orgs"    → total: formatted number with context

Zero LLM calls. Pure computation.

Author: PULSE Team
"""

import logging
from typing import Dict, Optional
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def _get_metric_cols(df: pd.DataFrame) -> list:
    """Get numeric columns that are actual metrics, not dates."""
    skip = {'day', 'date', 'time', 'timestamp', 'eventinfo_time', 'first_event', 'last_event',
            'first_seen', 'last_seen'}
    return [c for c in df.select_dtypes(include='number').columns.tolist()
            if c.lower() not in skip]


def generate_smart_insight(df: pd.DataFrame, intent: str, explanation: str) -> str:
    """
    Generate intent-specific insight text.
    Returns markdown string.
    """
    if df is None or df.empty:
        return "No data available."

    try:
        handler = {
            'ranking': _insight_ranking,
            'trend': _insight_trend,
            'health': _insight_health,
            'summary': _insight_summary,
            'total': _insight_total,
            'overview': _insight_overview,
            'lookup': _insight_lookup,
            'compare': _insight_compare,
        }.get(intent, _insight_generic)

        return handler(df, explanation)

    except Exception as e:
        logger.warning(f"Insight generation failed: {e}")
        return f"**{len(df):,}** results. {explanation}"


# -- Ranking: top N, bottom N --

def _insight_ranking(df: pd.DataFrame, explanation: str) -> str:
    """Insights for ranked data: concentration, dominance, long tail."""
    parts = [f"**{len(df):,}** results | {explanation}"]

    num_cols = _get_metric_cols(df)
    if not num_cols:
        return parts[0]

    val_col = num_cols[0]
    total = df[val_col].sum()
    if total == 0:
        return parts[0]

    # Top entity share
    if len(df) >= 2:
        top1_pct = df[val_col].iloc[0] / total * 100
        top3_pct = df[val_col].iloc[:3].sum() / total * 100 if len(df) >= 3 else top1_pct
        parts.append(f"Top 1 accounts for **{top1_pct:.1f}%** of total")
        if len(df) >= 3:
            parts.append(f"Top 3 account for **{top3_pct:.1f}%** of total")

    # Spread / concentration
    if len(df) >= 5:
        top_half = df[val_col].iloc[:len(df)//2].sum()
        concentration = top_half / total * 100
        if concentration > 80:
            parts.append(f"Highly concentrated: top half holds {concentration:.0f}% of total")
        elif concentration > 60:
            parts.append(f"Moderately concentrated: top half holds {concentration:.0f}%")

    # Gap between #1 and #2
    if len(df) >= 2:
        gap = df[val_col].iloc[0] / df[val_col].iloc[1] if df[val_col].iloc[1] > 0 else 0
        if gap > 2:
            parts.append(f"#1 is **{gap:.1f}x** larger than #2")

    return "\n\n".join(parts)


# -- Trend: daily/weekly data over time --

def _insight_trend(df: pd.DataFrame, explanation: str) -> str:
    """Insights for time-series data: slope, WoW change, direction."""
    parts = [f"**{len(df)} days** of data | {explanation}"]

    # Find the time column and value column
    time_col = _find_time_col(df)
    num_cols = _get_metric_cols(df)
    if not time_col or not num_cols:
        return parts[0]

    val_col = num_cols[0]  # Primary metric (events)
    values = df[val_col].dropna()
    if len(values) < 3:
        return parts[0]

    # Overall direction
    first_half = values.iloc[:len(values)//2].mean()
    second_half = values.iloc[len(values)//2:].mean()
    if first_half > 0:
        change_pct = (second_half - first_half) / first_half * 100
        if change_pct > 10:
            parts.append(f"Trending **up** — second half is {change_pct:.1f}% higher than first half")
        elif change_pct < -10:
            parts.append(f"Trending **down** — second half is {abs(change_pct):.1f}% lower")
        else:
            parts.append(f"Relatively **stable** (within {abs(change_pct):.1f}% variation)")

    # Last 7 days vs previous 7 days (week-over-week)
    if len(values) >= 14:
        last7 = values.iloc[-7:].sum()
        prev7 = values.iloc[-14:-7].sum()
        if prev7 > 0:
            wow = (last7 - prev7) / prev7 * 100
            direction = "up" if wow > 0 else "down"
            parts.append(f"Week-over-week: **{direction} {abs(wow):.1f}%** ({_fmt(last7)} vs {_fmt(prev7)})")

    # Peak and trough
    max_val = values.max()
    min_val = values.min()
    max_idx = values.idxmax()
    min_idx = values.idxmin()
    if time_col:
        max_day = df.loc[max_idx, time_col] if max_idx in df.index else "?"
        min_day = df.loc[min_idx, time_col] if min_idx in df.index else "?"
        parts.append(f"Peak: **{_fmt(max_val)}** ({_fmt_date(max_day)}) | Low: **{_fmt(min_val)}** ({_fmt_date(min_day)})")

    # Volatility
    cv = values.std() / values.mean() * 100 if values.mean() > 0 else 0
    if cv > 30:
        parts.append(f"High volatility (CV={cv:.0f}%) — consider investigating spikes/drops")
    elif cv > 15:
        parts.append(f"Moderate volatility (CV={cv:.0f}%)")

    return "\n\n".join(parts)


# -- Health: anomaly detection --

def _insight_health(df: pd.DataFrame, explanation: str) -> str:
    """Insights for health check: drops, spikes, missing data."""
    parts = ["**Health Check** — scanning for issues"]

    time_col = _find_time_col(df)
    num_cols = _get_metric_cols(df)
    if not time_col or not num_cols:
        parts.append("Insufficient data for health analysis")
        return "\n\n".join(parts)

    val_col = num_cols[0]
    values = df[val_col].dropna()
    issues_found = 0

    if len(values) < 3:
        parts.append("Not enough data points to analyze")
        return "\n\n".join(parts)

    mean_val = values.mean()
    std_val = values.std()

    # Check for recent drops (last 3 days vs average)
    if len(values) >= 7:
        recent_avg = values.iloc[-3:].mean()
        baseline_avg = values.iloc[:-3].mean()
        if baseline_avg > 0:
            drop_pct = (recent_avg - baseline_avg) / baseline_avg * 100
            if drop_pct < -20:
                parts.append(f"**DROP DETECTED**: Last 3 days are **{abs(drop_pct):.0f}% below** average ({_fmt(recent_avg)} vs {_fmt(baseline_avg)})")
                issues_found += 1
            elif drop_pct > 30:
                parts.append(f"**SPIKE DETECTED**: Last 3 days are **{drop_pct:.0f}% above** average")
                issues_found += 1

    # Check for zero/near-zero days
    if std_val > 0:
        low_threshold = mean_val - 2 * std_val
        zero_days = (values < max(low_threshold, 1)).sum()
        if zero_days > 0:
            parts.append(f"**{zero_days} unusually low day(s)** detected (below 2 std deviations)")
            issues_found += 1

    # Check for extreme outliers
    if std_val > 0:
        high_threshold = mean_val + 3 * std_val
        outliers = (values > high_threshold).sum()
        if outliers > 0:
            parts.append(f"**{outliers} extreme spike(s)** detected (above 3 std deviations)")
            issues_found += 1

    # Week-over-week drop
    if len(values) >= 14:
        last7 = values.iloc[-7:].sum()
        prev7 = values.iloc[-14:-7].sum()
        if prev7 > 0:
            wow = (last7 - prev7) / prev7 * 100
            if wow < -15:
                parts.append(f"**Week-over-week decline**: {wow:.1f}% ({_fmt(last7)} vs {_fmt(prev7)})")
                issues_found += 1

    if issues_found == 0:
        parts.append("No significant issues detected. Activity levels appear normal.")

    parts.append(f"Analyzed {len(values)} data points | avg: {_fmt(mean_val)}/day")

    return "\n\n".join(parts)


# -- Summary: period overview --

def _insight_summary(df: pd.DataFrame, explanation: str) -> str:
    """Insights for summary: key metrics, period comparison."""
    parts = [f"**{explanation}**"]

    time_col = _find_time_col(df)
    # Only use actual numeric columns, skip datetime-like ones
    num_cols = [c for c in df.select_dtypes(include='number').columns.tolist()
                if c.lower() not in ('day', 'date', 'time', 'timestamp', 'eventinfo_time')]

    if not num_cols:
        return f"{len(df)} rows returned"

    # Summarize each numeric column
    for col in num_cols[:3]:
        values = df[col].dropna()
        if values.empty:
            continue
        parts.append(f"**{col}**: total {_fmt(values.sum())} | avg {_fmt(values.mean())}/day | peak {_fmt(values.max())}")

    # Period change (first half vs second half)
    if len(df) >= 6 and num_cols:
        val_col = num_cols[0]
        first_half = df[val_col].iloc[:len(df)//2].mean()
        second_half = df[val_col].iloc[len(df)//2:].mean()
        if first_half > 0:
            change = (second_half - first_half) / first_half * 100
            direction = "up" if change > 0 else "down"
            parts.append(f"Period trend: **{direction} {abs(change):.1f}%** (first half vs second half)")

    return "\n\n".join(parts)


# -- Total: single number with context --

def _insight_total(df: pd.DataFrame, explanation: str) -> str:
    """Insights for total/count: formatted number."""
    if df.empty:
        return explanation

    # Single value result (e.g. SELECT total_active_orgs FROM profile_totals)
    if len(df) == 1 and len(df.columns) == 1:
        val = df.iloc[0, 0]
        return f"**{_fmt(val)}** | {explanation}"

    # Totals row with multiple columns
    if len(df) == 1:
        parts = []
        for col in df.columns:
            val = df.iloc[0][col]
            try:
                num = float(val)
                label = col.replace('total_', '').replace('_', ' ').title()
                parts.append(f"**{label}**: {_fmt(num)}")
            except (ValueError, TypeError):
                # Skip non-numeric (dates, strings)
                if hasattr(val, 'strftime'):
                    label = col.replace('_', ' ').title()
                    parts.append(f"**{label}**: {_fmt_date(val)}")
        return " | ".join(parts) if parts else explanation

    return f"**{len(df):,}** results | {explanation}"


# -- Overview --

def _insight_overview(df: pd.DataFrame, explanation: str) -> str:
    """Insights for overview: all key totals."""
    return _insight_total(df, explanation)


# -- Lookup: dimension listing --

def _insight_lookup(df: pd.DataFrame, explanation: str) -> str:
    """Insights for dimension lookup: count + top entries."""
    parts = [f"**{len(df):,}** results | {explanation}"]

    num_cols = df.select_dtypes(include='number').columns.tolist()
    if num_cols:
        val_col = num_cols[0]
        total = df[val_col].sum()
        parts.append(f"Total {val_col}: **{_fmt(total)}** across {len(df)} entries")

    return "\n\n".join(parts)


# -- Compare --

def _insight_compare(df: pd.DataFrame, explanation: str) -> str:
    """Insights for comparison: highlight differences."""
    return _insight_ranking(df, explanation)


# -- Generic fallback --

def _insight_generic(df: pd.DataFrame, explanation: str) -> str:
    return f"**{len(df):,}** results | {explanation}"


# -- Helpers --

def _find_time_col(df: pd.DataFrame) -> Optional[str]:
    """Find the time/date column."""
    for col in df.columns:
        if col.lower() in ('day', 'date', 'time', 'eventinfo_time', 'timestamp'):
            return col
        if df[col].dtype == 'datetime64[ns]':
            return col
    return None


def _fmt(val) -> str:
    """Format a number for display."""
    if not isinstance(val, (int, float)):
        return str(val)
    if pd.isna(val):
        return "N/A"
    if abs(val) >= 1e9:
        return f"{val/1e9:.1f}B"
    elif abs(val) >= 1e6:
        return f"{val/1e6:.1f}M"
    elif abs(val) >= 1e3:
        return f"{val/1e3:.1f}K"
    elif isinstance(val, float):
        return f"{val:.1f}"
    else:
        return f"{val:,}"


def _fmt_date(val) -> str:
    """Format a date for display."""
    try:
        if hasattr(val, 'strftime'):
            return val.strftime('%b %d')
        return str(val)[:10]
    except Exception:
        return str(val)
