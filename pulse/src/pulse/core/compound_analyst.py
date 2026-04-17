"""
CompoundAnalyst v1.0 — Multi-Table Intelligence Brain
========================================================

The missing piece: every other intelligence module works on ONE table.
This module pulls MULTIPLE tables simultaneously and cross-references
them to produce real analytical insights.

                    "any anomaly?"
                          │
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
    profile_daily    profile_region   profile_organization
    (WoW trend)      (regional Δ)     (org movers)
          │               │               │
          └───────────────┼───────────────┘
                          ▼
                   COMPOUND FINDINGS
                   [3-5 structured signals]
                          │
                          ▼
                   NARRATIVE (LLM or template)

Design:
  - Zero Kusto calls — profile DuckDB only (instant)
  - Deterministic stats — Python computes, LLM narrates
  - Structured output — frontend can render cards OR prose
  - Silent failure — if any table missing, skip that signal

Author: PULSE Team
"""

import logging
import time
import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Signal:
    """Single analytical signal."""
    category: str        # 'trend', 'anomaly', 'mover', 'concentration', 'health'
    severity: str        # 'red', 'amber', 'green', 'info'
    headline: str        # "EMEA dropped 23% WoW"
    detail: str          # supporting numbers
    metric_value: float = 0.0    # primary number (for sorting)
    metric_label: str = ""       # "+8% WoW"
    dimension: str = ""          # 'region', 'org', 'overall', 'activity'


@dataclass
class CompoundResult:
    """Full compound analysis output."""
    signals: List[Signal]
    narrative: str                  # Synthesized prose for chat
    scorecard: Dict[str, Any]      # Structured data for dashboard cards
    tables_consulted: List[str]    # Which profile tables were used
    compute_ms: float = 0.0        # How long computation took


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def analyze(
    data_profile,
    question: str = "",
    mode: str = "full",
    enricher=None,
    scope_days: int = 0,
) -> CompoundResult:
    """
    Run compound analysis across all available profile tables.

    Args:
        data_profile: DataProfile instance (for DuckDB queries)
        question: user question (influences which signals to prioritize)
        mode: 'full' (all signals), 'scorecard' (connect-time dashboard),
              'anomaly' (anomaly-focused), 'takeaways' (summary-focused)

    Returns:
        CompoundResult with structured signals + narrative
    """
    t0 = time.time()

    if not data_profile or not data_profile.is_built:
        return CompoundResult(
            signals=[], narrative="Profile not ready yet.",
            scorecard={}, tables_consulted=[], compute_ms=0,
        )

    signals: List[Signal] = []
    scorecard: Dict[str, Any] = {}
    tables_used: List[str] = []

    # ── 1. Overall trend (profile_daily) ──────────────────────────────────
    daily = _safe_query(data_profile, "profile_daily",
                        "SELECT * FROM profile_daily ORDER BY day ASC")
    if daily is not None:
        tables_used.append('profile_daily')
        trend_signals, trend_card = _analyze_daily_trend(daily)
        signals.extend(trend_signals)
        scorecard['trend'] = trend_card

    # ── 2. Regional breakdown (profile_region) ────────────────────────────
    region = _safe_query(data_profile, "profile_region",
                         "SELECT * FROM profile_region ORDER BY events DESC")
    if region is not None:
        tables_used.append('profile_region')
        region_signals, region_card = _analyze_regions(region)
        signals.extend(region_signals)
        scorecard['regions'] = region_card

    # ── 3. Org movers (profile_organization) ──────────────────────────────
    orgs = _safe_query(data_profile, "profile_organization",
                       "SELECT * FROM profile_organization ORDER BY events DESC LIMIT 20")
    if orgs is not None:
        tables_used.append('profile_organization')
        org_signals, org_card = _analyze_orgs(orgs, enricher=enricher)
        signals.extend(org_signals)
        scorecard['orgs'] = org_card

    # ── 4. Activity breakdown (profile_activity) ──────────────────────────
    activity = _safe_query(data_profile, "profile_activity",
                           "SELECT * FROM profile_activity ORDER BY events DESC")
    if activity is not None:
        tables_used.append('profile_activity')
        act_signals, act_card = _analyze_activity(activity)
        signals.extend(act_signals)
        scorecard['activity'] = act_card

    # ── 5. Grand totals (profile_totals) ──────────────────────────────────
    totals = _safe_query(data_profile, "profile_totals",
                         "SELECT * FROM profile_totals")
    if totals is not None:
        tables_used.append('profile_totals')
        scorecard['totals'] = _extract_totals(totals)

    # ── Sort signals: red first, then amber, then green ───────────────────
    severity_order = {'red': 0, 'amber': 1, 'info': 2, 'green': 3}
    signals.sort(key=lambda s: severity_order.get(s.severity, 4))

    compute_ms = (time.time() - t0) * 1000

    # ── Build narrative based on mode ─────────────────────────────────────
    narrative = _build_narrative(signals, scorecard, question, mode)

    logger.info(
        f"CompoundAnalyst: {len(signals)} signals from {len(tables_used)} tables "
        f"in {compute_ms:.0f}ms (mode={mode})"
    )

    return CompoundResult(
        signals=signals,
        narrative=narrative,
        scorecard=scorecard,
        tables_consulted=tables_used,
        compute_ms=compute_ms,
    )


def build_scorecard_payload(result: CompoundResult) -> Dict[str, Any]:
    """
    Convert CompoundResult into a WebSocket-ready scorecard payload.
    Frontend renders this as dashboard cards immediately after connect.
    """
    sc = result.scorecard

    # Anomaly signals for the alert card
    anomalies = [
        {
            'signal': s.headline,
            'severity': s.severity,
            'dimension': s.dimension,
            'detail': s.detail,
        }
        for s in result.signals
        if s.severity in ('red', 'amber')
    ]

    trend = sc.get('trend', {})
    regions = sc.get('regions', {})
    orgs = sc.get('orgs', {})

    # ★ iter13: Top mover = highest-volume org
    top_orgs = orgs.get('top_5', [])
    top_mover = None
    if top_orgs:
        top_mover = {
            'name': top_orgs[0].get('name', '—'),
            'value': top_orgs[0].get('value_fmt', '—'),
            'pct': top_orgs[0].get('pct', 0),
        }

    # ★ iter15: Ingestion health — use scope_days if available, else data_points
    ingestion_health = 'green'
    ingestion_label = 'Healthy'
    data_points = trend.get('data_points', 0)
    display_days = scope_days if scope_days > 0 else data_points
    if data_points < 7:
        ingestion_health = 'red'
        ingestion_label = f'Only {data_points} days of data'
    elif display_days > 0:
        ingestion_health = 'green'
        ingestion_label = f'{display_days} days of data'

    return {
        'type': 'scorecard',
        # Card 1: Total Events + WoW
        'total_events': sc.get('totals', {}).get('total_events_fmt', '—'),
        'total_events_raw': trend.get('total', 0),
        'wow_change': trend.get('wow_pct_label', '—'),
        'wow_pct': trend.get('wow_pct', 0),
        'wow_status': trend.get('status', 'neutral'),
        'trend_direction': trend.get('trend_word', 'flat'),
        'this_week': trend.get('this_week', 0),
        'last_week': trend.get('last_week', 0),
        # Card 2: Active Orgs + Sparkline
        'active_orgs': sc.get('totals', {}).get('active_orgs_fmt', '—'),
        'daily_sparkline': trend.get('daily_sparkline', []),
        'org_sparkline': trend.get('org_sparkline', []),
        # Card 3: Top Mover
        'top_mover': top_mover,
        'top_orgs': top_orgs,
        # Card 4: Anomaly Alert
        'anomaly_count': len(anomalies),
        'anomalies': anomalies[:5],
        # Card 5: Regional Heatmap
        'top_regions': regions.get('top_3', []),
        'all_regions': regions.get('all_regions', []),
        'region_count': regions.get('region_count', 0),
        # Card 6: Ingestion Health
        'ingestion_health': ingestion_health,
        'ingestion_label': ingestion_label,
        'avg_daily': trend.get('avg_daily_fmt', '—'),
        'peak_date': trend.get('peak_date', '—'),
        'peak_val': trend.get('peak_val', 0),
        # Meta
        'tables_consulted': result.tables_consulted,
        'compute_ms': round(result.compute_ms, 0),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Per-table analyzers
# ─────────────────────────────────────────────────────────────────────────────

def _analyze_daily_trend(df: pd.DataFrame) -> tuple:
    """Analyze profile_daily for WoW trend, slope, anomalies."""
    signals = []
    card = {}

    t_col = _find_col(df, ['day', 'date', 'eventinfo_time', 'timestamp'])
    v_col = _find_col(df, ['events', 'event_count', 'total_events'])
    ao_col = _find_col(df, ['active_orgs'])

    if not t_col or not v_col:
        return signals, card

    df = df.copy()
    df[t_col] = pd.to_datetime(df[t_col], errors='coerce')
    df = df.dropna(subset=[t_col]).sort_values(t_col)
    df[v_col] = pd.to_numeric(df[v_col], errors='coerce').fillna(0)

    # Filter to valid date range
    now = pd.Timestamp.now().normalize()
    min_valid = now - pd.DateOffset(years=2)
    df = df[(df[t_col] >= min_valid) & (df[t_col] <= now + pd.DateOffset(days=1))]

    if len(df) < 4:
        return signals, card

    vals = df[v_col].values
    total = float(vals.sum())

    # ── WoW computation ───────────────────────────────────────────────────
    max_date = df[t_col].max()
    cutoff_w1 = max_date - timedelta(days=7)
    cutoff_w2 = max_date - timedelta(days=14)

    this_week = float(df[df[t_col] > cutoff_w1][v_col].sum())
    last_week = float(df[(df[t_col] > cutoff_w2) & (df[t_col] <= cutoff_w1)][v_col].sum())

    wow_pct = ((this_week - last_week) / last_week * 100) if last_week > 0 else 0
    avg_daily = this_week / max(1, len(df[df[t_col] > cutoff_w1]))

    if wow_pct >= 10:
        status = 'positive'
    elif wow_pct > -10:
        status = 'neutral'
    elif wow_pct > -30:
        status = 'warning'
    else:
        status = 'critical'

    wow_label = f"{wow_pct:+.0f}%"

    # ── Trend slope ───────────────────────────────────────────────────────
    trend_word = 'flat'
    if len(df) >= 7:
        try:
            xs = np.arange(len(df))
            ys = vals.astype(float)
            slope = np.polyfit(xs, ys, 1)[0]
            slope_pct = slope / (ys.mean() + 1e-9) * 100
            if slope_pct > 2:
                trend_word = 'growing'
            elif slope_pct < -2:
                trend_word = 'declining'
        except Exception:
            pass

    # ── Peak and low ──────────────────────────────────────────────────────
    peak_idx = int(np.argmax(vals))
    low_idx = int(np.argmin(vals))
    peak_val = float(vals[peak_idx])
    low_val = float(vals[low_idx])
    try:
        peak_date = df[t_col].iloc[peak_idx].strftime('%b %d')
        low_date = df[t_col].iloc[low_idx].strftime('%b %d')
    except Exception:
        peak_date = low_date = "—"

    # ── Z-score anomaly detection ─────────────────────────────────────────
    if len(vals) > 7:
        mean_val = float(np.mean(vals))
        std_val = float(np.std(vals))
        if std_val > 0:
            z_scores = (vals - mean_val) / std_val
            anomaly_days = np.where(np.abs(z_scores) > 2.0)[0]
            for idx in anomaly_days[-3:]:  # Last 3 anomalous days
                z = float(z_scores[idx])
                day_val = float(vals[idx])
                try:
                    day_str = df[t_col].iloc[idx].strftime('%b %d')
                except Exception:
                    day_str = f"day {idx}"
                direction = "spike" if z > 0 else "drop"
                signals.append(Signal(
                    category='anomaly', severity='amber' if abs(z) < 3 else 'red',
                    headline=f"{day_str}: {direction} of {_fmt(day_val)} ({z:+.1f}σ)",
                    detail=f"Expected ~{_fmt(mean_val)}/day ± {_fmt(std_val)}",
                    metric_value=abs(z), dimension='overall',
                ))

    # ── WoW signal ────────────────────────────────────────────────────────
    if abs(wow_pct) > 15:
        direction = "up" if wow_pct > 0 else "down"
        sev = 'green' if wow_pct > 15 else ('amber' if wow_pct > -30 else 'red')
        signals.append(Signal(
            category='trend', severity=sev,
            headline=f"Events {direction} {wow_label} week-over-week",
            detail=f"This week: {_fmt(this_week)} vs last week: {_fmt(last_week)}",
            metric_value=abs(wow_pct), metric_label=wow_label,
            dimension='overall',
        ))

    # ── Build card ────────────────────────────────────────────────────────
    # ── Sparkline data (last 14 days) ───────────────────────────────────
    spark_df = df.tail(14)
    daily_sparkline = spark_df[v_col].tolist()
    org_sparkline = []
    if ao_col and ao_col in spark_df.columns:
        org_sparkline = pd.to_numeric(spark_df[ao_col], errors='coerce').fillna(0).astype(int).tolist()

    card = {
        'total': total,
        'total_fmt': _fmt(total),
        'this_week': this_week,
        'last_week': last_week,
        'wow_pct': round(wow_pct, 1),
        'wow_pct_label': wow_label,
        'status': status,
        'avg_daily': avg_daily,
        'avg_daily_fmt': _fmt(avg_daily),
        'trend_word': trend_word,
        'peak_val': peak_val,
        'peak_date': peak_date,
        'low_val': low_val,
        'low_date': low_date,
        'data_points': len(df),
        'daily_sparkline': daily_sparkline,
        'org_sparkline': org_sparkline,
    }

    return signals, card


def _analyze_regions(df: pd.DataFrame) -> tuple:
    """Analyze profile_region for concentration and dead regions."""
    signals = []
    card = {}

    v_col = _find_col(df, ['events', 'event_count', 'total_events'])
    l_col = _find_col(df, ['geoname', 'geo', 'region', 'country'])
    if not v_col or not l_col:
        return signals, card

    df = df.copy()
    df[v_col] = pd.to_numeric(df[v_col], errors='coerce').fillna(0)
    df = df.sort_values(v_col, ascending=False)
    total = float(df[v_col].sum())
    if total == 0:
        return signals, card

    # Top 3 for card
    top_3 = []
    for _, row in df.head(3).iterrows():
        val = float(row[v_col])
        name = str(row[l_col])
        pct = val / total * 100
        top_3.append({'name': name, 'value': val, 'value_fmt': _fmt(val), 'pct': round(pct, 1)})

    # Concentration check
    top_1_pct = float(df[v_col].iloc[0]) / total * 100 if len(df) > 0 else 0
    if top_1_pct > 70:
        name = str(df[l_col].iloc[0])
        signals.append(Signal(
            category='concentration', severity='amber',
            headline=f"{name} carries {top_1_pct:.0f}% of all events",
            detail=f"Heavy concentration in one region — risk if it goes down",
            metric_value=top_1_pct, dimension='region',
        ))

    # Dead regions
    dead = df[(df[v_col] <= 1) & (~df[l_col].astype(str).str.lower().isin(
        ['unavailable', 'unknown', 'none', 'n/a', '', 'other']
    ))]
    if len(dead) >= 2:
        dead_names = ", ".join(dead[l_col].astype(str).tolist()[:4])
        signals.append(Signal(
            category='health', severity='amber',
            headline=f"{len(dead)} regions have zero events: {dead_names}",
            detail="Could indicate pipeline issues or genuine inactivity",
            metric_value=len(dead), dimension='region',
        ))

    # ★ iter13: All regions for heatmap (top 3 still separate for quick display)
    all_regions = []
    for _, row in df.iterrows():
        val_r = float(row[v_col])
        name_r = str(row[l_col])
        pct_r = val_r / total * 100 if total > 0 else 0
        all_regions.append({'name': name_r, 'value': val_r, 'value_fmt': _fmt(val_r), 'pct': round(pct_r, 1)})

    card = {
        'total': total,
        'total_fmt': _fmt(total),
        'top_3': top_3,
        'all_regions': all_regions,
        'region_count': len(df[df[v_col] > 0]),
    }

    return signals, card


def _analyze_orgs(df: pd.DataFrame, enricher=None) -> tuple:
    """Analyze profile_organization for top movers and concentration."""
    signals = []
    card = {}

    v_col = _find_col(df, ['events', 'event_count', 'total_events'])
    l_col = _find_col(df, ['orgname', 'org_name', 'organizationname', 'tenantname',
                           'orgid', 'org_id', 'organization', 'tenantid'])
    if not v_col or not l_col:
        return signals, card

    df = df.copy()
    df[v_col] = pd.to_numeric(df[v_col], errors='coerce').fillna(0)
    total = float(df[v_col].sum())
    if total == 0:
        return signals, card

    # ★ iter14: Resolve org GUIDs to display names via enricher
    def _resolve_name(raw_id) -> str:
        raw_str = str(raw_id)[:30]
        if enricher and hasattr(enricher, 'is_loaded') and enricher.is_loaded:
            try:
                r = enricher.resolve(raw_str)
                if r and r.org_name:
                    return r.display_name[:30]
            except Exception:
                pass
        return raw_str

    # Top 5 for card
    top_5 = []
    for _, row in df.head(5).iterrows():
        val = float(row[v_col])
        name = _resolve_name(row[l_col])
        pct = val / total * 100
        top_5.append({'name': name, 'value': val, 'value_fmt': _fmt(val), 'pct': round(pct, 1)})

    # Single-org dominance
    if len(df) > 1:
        top_pct = float(df[v_col].iloc[0]) / total * 100
        if top_pct > 50:
            name = _resolve_name(df[l_col].iloc[0])
            signals.append(Signal(
                category='concentration', severity='amber',
                headline=f"{name} accounts for {top_pct:.0f}% of all events",
                detail=f"If this org goes quiet, total numbers will collapse",
                metric_value=top_pct, dimension='org',
            ))

    # Pareto concentration
    if len(df) > 5:
        sorted_vals = df[v_col].sort_values(ascending=False)
        cumsum = sorted_vals.cumsum()
        n_for_80 = int((cumsum <= total * 0.8).sum()) + 1
        pct_for_80 = n_for_80 / len(df) * 100
        if pct_for_80 < 10:
            signals.append(Signal(
                category='concentration', severity='info',
                headline=f"Top {n_for_80} of {len(df)} orgs ({pct_for_80:.0f}%) drive 80% of events",
                detail="Highly concentrated customer base — typical for enterprise telemetry",
                metric_value=pct_for_80, dimension='org',
            ))

    card = {
        'total': total,
        'total_fmt': _fmt(total),
        'top_5': top_5,
        'org_count': len(df),
    }

    return signals, card


def _analyze_activity(df: pd.DataFrame) -> tuple:
    """Analyze profile_activity for event type distribution."""
    signals = []
    card = {}

    v_col = _find_col(df, ['events', 'event_count', 'count'])
    l_col = _find_col(df, ['eventinfo_name', 'event_name', 'action', 'activity'])
    if not v_col or not l_col:
        return signals, card

    df = df.copy()
    df[v_col] = pd.to_numeric(df[v_col], errors='coerce').fillna(0)
    total = float(df[v_col].sum())

    # Single event dominance
    if len(df) >= 2 and total > 0:
        top_pct = float(df[v_col].iloc[0]) / total * 100
        if top_pct > 95:
            name = str(df[l_col].iloc[0])[:30]
            signals.append(Signal(
                category='health', severity='info',
                headline=f"Activity dominated by '{name}' ({top_pct:.0f}%)",
                detail=f"Only {len(df)} distinct event types recorded",
                metric_value=top_pct, dimension='activity',
            ))

    card = {
        'event_types': len(df),
        'total': total,
        'total_fmt': _fmt(total),
    }

    return signals, card


def _extract_totals(df: pd.DataFrame) -> Dict:
    """Extract grand totals for scorecard header."""
    card = {}
    for col in df.columns:
        val = df[col].iloc[0] if len(df) > 0 else 0
        if pd.api.types.is_numeric_dtype(df[col]):
            card[col] = float(val)
            card[f"{col}_fmt"] = _fmt(float(val))
        else:
            card[col] = str(val)

    # Ensure standard keys exist
    if 'total_events' not in card:
        ev_col = _find_col(df, ['events', 'total_events', 'event_count'])
        if ev_col and ev_col in card:
            card['total_events'] = card[ev_col]
            card['total_events_fmt'] = card.get(f"{ev_col}_fmt", _fmt(card[ev_col]))

    ao_col = _find_col(df, ['active_orgs', 'total_active_orgs', 'orgs'])
    if ao_col and ao_col in card:
        card['active_orgs'] = card[ao_col]
        card['active_orgs_fmt'] = card.get(f"{ao_col}_fmt", _fmt(card[ao_col]))

    return card


# ─────────────────────────────────────────────────────────────────────────────
# Narrative builder
# ─────────────────────────────────────────────────────────────────────────────

def _build_narrative(
    signals: List[Signal],
    scorecard: Dict,
    question: str,
    mode: str,
) -> str:
    """Build the synthesized narrative from computed signals."""
    if not signals and not scorecard:
        return "No profile data available yet."

    parts = []
    trend = scorecard.get('trend', {})
    totals = scorecard.get('totals', {})

    # ── Opening line — always grounded in numbers ─────────────────────────
    if mode == 'scorecard':
        total_fmt = totals.get('total_events_fmt', trend.get('total_fmt', ''))
        wow = trend.get('wow_pct_label', '')
        if total_fmt:
            parts.append(f"**{total_fmt}** total events tracked.")
        if wow and abs(trend.get('wow_pct', 0)) > 2:
            parts.append(f"Week-over-week: **{wow}**.")

    elif mode == 'anomaly':
        red = [s for s in signals if s.severity == 'red']
        amber = [s for s in signals if s.severity == 'amber']
        if red:
            parts.append(f"**{len(red)} critical signal{'s' if len(red) > 1 else ''}** detected:")
        elif amber:
            parts.append(f"**{len(amber)} item{'s' if len(amber) > 1 else ''}** to watch:")
        else:
            parts.append("**No anomalies detected** — all signals healthy.")

    elif mode == 'takeaways':
        parts.append("**Key takeaways:**\n")

    else:  # full
        total_fmt = totals.get('total_events_fmt', trend.get('total_fmt', ''))
        if total_fmt:
            parts.append(f"Analyzing {total_fmt} events across {len(scorecard.get('regions', {}).get('top_3', []))} regions.")

    # ── Signals as bullet points ──────────────────────────────────────────
    icons = {'red': '🔴', 'amber': '🟡', 'green': '🟢', 'info': 'ℹ️'}

    # For anomaly mode, show all red/amber. For takeaways, show top 5. For others, top 4.
    if mode == 'anomaly':
        show = [s for s in signals if s.severity in ('red', 'amber')]
        if not show:
            show = signals[:3]
    elif mode == 'takeaways':
        show = signals[:5]
    else:
        show = signals[:4]

    for s in show:
        icon = icons.get(s.severity, '•')
        parts.append(f"{icon} {s.headline}")
        if s.detail and mode != 'scorecard':
            parts.append(f"  *{s.detail}*")

    # ── Footer with context ───────────────────────────────────────────────
    if trend.get('trend_word') and trend['trend_word'] != 'flat':
        parts.append(f"\nOverall trend: **{trend['trend_word']}** (peak {_fmt(trend.get('peak_val', 0))} on {trend.get('peak_date', '—')}).")

    return "\n\n".join(parts)


# ─────────────────────────────────────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────────────────────────────────────

def _safe_query(data_profile, table_name: str, sql: str) -> Optional[pd.DataFrame]:
    """Query a profile table safely. Returns None if not available."""
    try:
        if not data_profile.has_table(table_name):
            return None
        df = data_profile.query_safe(sql)
        if df is None or df.empty:
            return None
        return df
    except Exception:
        return None


def _find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """Find first matching column (case-insensitive)."""
    for name in candidates:
        for c in df.columns:
            if c.lower() == name.lower():
                return c
    return None


def _fmt(n) -> str:
    """Format large numbers with K/M/B suffixes."""
    if n is None or (isinstance(n, float) and np.isnan(n)):
        return "—"
    n = float(n)
    if abs(n) >= 1e12: return f"{n/1e12:.1f}T"
    if abs(n) >= 1e9:  return f"{n/1e9:.1f}B"
    if abs(n) >= 1e6:  return f"{n/1e6:.1f}M"
    if abs(n) >= 1e3:  return f"{n/1e3:.1f}K"
    return f"{n:,.0f}"
