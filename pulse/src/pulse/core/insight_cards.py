"""
Insight Cards v3 — Rich Visual Health Analysis
==================================================

Fixes vs v2:
  - Date columns never used as label_col (was showing "2090-01-05" as entity names)
  - .NET ticks / bad timestamps sanitized before any analysis
  - Time-series and dimensional data routed separately
  - Entity checks never fire on time-series data
  - Message text uses plain markdown, no headings (no more giant text)
  - Line chart for time-series, bar chart for dimensional

Author: PULSE Team
"""

import logging
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

GREEN       = "#10B981"
AMBER       = "#F59E0B"
RED         = "#EF4444"
BLUE        = "#2563EB"
GRAY        = "#94A3B8"
DARK        = "#0F172A"
LIGHT_GREEN = "#D1FAE5"
LIGHT_AMBER = "#FEF3C7"
LIGHT_RED   = "#FEE2E2"

_DATE_COLS = {
    'day', 'date', 'time', 'timestamp', 'eventinfo_time',
    'first_seen', 'last_seen', 'first_event', 'last_event',
    'created_at', 'updated_at', 'period',
}

_NET_EPOCH_TICKS = 621_355_968_000_000_000


# =============================================================================
# Date sanitization
# =============================================================================

def _safe_parse_dates(series: pd.Series) -> pd.Series:
    """Parse dates robustly — handles .NET ticks, unix ms/s, ISO strings."""
    s = series.copy()

    if pd.api.types.is_datetime64_any_dtype(s):
        try:
            s = s.dt.tz_convert(None)
        except TypeError:
            pass
        try:
            years = s.dt.year.dropna()
            if len(years) > 0 and (years.max() > __import__('datetime').datetime.now().year + 1 or years.min() < 2000):
                numeric = s.astype('int64')
                if float(numeric.median()) > 6e17:
                    unix_ns = (numeric - _NET_EPOCH_TICKS) * 100
                    s = pd.to_datetime(unix_ns, unit='ns', errors='coerce')
        except Exception:
            pass
        return s

    numeric = pd.to_numeric(s, errors='coerce')
    if numeric.notna().sum() > len(s) * 0.5:
        med = float(numeric.median())
        if med > 6e17:
            unix_ns = (numeric - _NET_EPOCH_TICKS) * 100
            return pd.to_datetime(unix_ns, unit='ns', errors='coerce')
        elif med > 1e12:
            return pd.to_datetime(numeric, unit='ms', errors='coerce')
        elif med > 1e9:
            return pd.to_datetime(numeric, unit='s', errors='coerce')

    try:
        parsed = pd.to_datetime(s, errors='coerce', utc=True)
        try:
            return parsed.dt.tz_convert(None)
        except TypeError:
            return parsed
    except Exception:
        return s


def _sanitize_dates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.columns:
        if col.lower() in _DATE_COLS:
            try:
                df[col] = _safe_parse_dates(df[col])
            except Exception as e:
                logger.debug(f"Date sanitize failed for {col}: {e}")
    return df


# =============================================================================
# Column detection
# =============================================================================

def _find_primary_metric(df: pd.DataFrame, num_cols: List[str]) -> Optional[str]:
    for name in ['events', 'event_count', 'total_events', 'sessions', 'active_orgs', 'count']:
        for c in num_cols:
            if c.lower() == name:
                return c
    candidates = [c for c in num_cols if c.lower() not in _DATE_COLS]
    return candidates[0] if candidates else None


def _find_label_col(df: pd.DataFrame) -> Optional[str]:
    """Best entity label column — NEVER a date column."""
    preferred = [
        'geoname', 'geo', 'region', 'country',
        'orgid', 'org_id', 'organization', 'tenantid',
        'entityname', 'entity', 'browsername', 'browser',
    ]
    for name in preferred:
        for c in df.columns:
            if c.lower() == name:
                return c
    for c in df.columns:
        if (not pd.api.types.is_numeric_dtype(df[c])
                and c.lower() not in _DATE_COLS):
            return c
    return None


def _find_time_col(df: pd.DataFrame) -> Optional[str]:
    for c in df.columns:
        if c.lower() in _DATE_COLS:
            return c
    return None


def _is_time_series(df: pd.DataFrame) -> bool:
    """True if this is a time-series with no entity label column."""
    time_col  = _find_time_col(df)
    label_col = _find_label_col(df)
    return time_col is not None and label_col is None


# =============================================================================
# Public API
# =============================================================================

def build_health_analysis(df: pd.DataFrame, question: str = "") -> Dict:
    if df is None or df.empty:
        return {
            'chart': None, 'type': 'health_analysis',
            'findings': [], 'headline': 'No data',
            'message': 'No data available for health analysis.',
        }

    df       = _sanitize_dates(df)
    num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    time_col  = _find_time_col(df)
    label_col = _find_label_col(df)
    primary   = _find_primary_metric(df, num_cols)
    is_ts     = _is_time_series(df)

    logger.debug(
        f"Health: is_ts={is_ts} time={time_col} label={label_col} "
        f"metric={primary} rows={len(df)}"
    )

    findings = []

    if is_ts or (time_col and not label_col):
        # Time-series only — trend checks
        if primary and time_col:
            findings.extend(_check_trend(df, primary, time_col))
    else:
        # Dimensional — entity checks
        if primary and label_col:
            findings.extend(_check_leader_gap(df, primary, label_col))
            findings.extend(_check_concentration(df, primary, label_col))
            findings.extend(_check_outliers(df, primary, label_col))
            findings.extend(_check_dropoff(df, primary, label_col))
            findings.extend(_check_inactive(df, primary, label_col))
        # Also trend if time col present
        if primary and time_col:
            findings.extend(_check_trend(df, primary, time_col))

    # Deduplicate
    seen, unique = set(), []
    for f in findings:
        if f['title'] not in seen:
            seen.add(f['title'])
            unique.append(f)
    findings = unique

    if not findings:
        findings.append({
            'severity': 'green', 'title': 'All Clear',
            'detail': f'No anomalies across {len(df)} data points.',
        })

    findings.sort(key=lambda f: {'red': 0, 'amber': 1, 'green': 2}.get(f['severity'], 3))

    chart = _build_chart(df, findings, primary, label_col, time_col, is_ts)

    red_n   = sum(1 for f in findings if f['severity'] == 'red')
    amber_n = sum(1 for f in findings if f['severity'] == 'amber')

    if red_n:
        headline = f"⚠️ **{red_n} critical** finding{'s' if red_n > 1 else ''} — action needed"
    elif amber_n:
        headline = f"⚡ **{amber_n} item{'s' if amber_n > 1 else ''}** to keep an eye on"
    else:
        headline = "✅ **Healthy** — no concerns detected"

    # Plain text — no heading syntax, no giant rendered text
    parts = [headline, ""]
    for f in findings:
        icon = {'red': '🔴', 'amber': '🟡', 'green': '🟢'}.get(f['severity'], '⚪')
        parts.append(f"{icon} **{f['title']}** — {f['detail']}")

    return {
        'chart': chart, 'type': 'health_analysis',
        'findings': findings, 'headline': headline,
        'message': "\n\n".join(parts),
    }


# =============================================================================
# Dimensional health checks
# =============================================================================

def _check_leader_gap(df, metric, label_col) -> List[Dict]:
    if len(df) < 2:
        return []
    s  = df.sort_values(metric, ascending=False)
    v1 = float(s[metric].iloc[0])
    v2 = float(s[metric].iloc[1])
    n1 = _name(s[label_col].iloc[0])
    n2 = _name(s[label_col].iloc[1])
    if v2 <= 0:
        return []
    gap = (v1 - v2) / v2 * 100
    if gap > 100:
        return [{'severity': 'red', 'title': 'Single-Point Dependency',
                 'detail': f'**{n1}** ({_fmt(v1)}) is {gap:.0f}% ahead of '
                           f'**{n2}** ({_fmt(v2)}). Losing {n1} would be major impact.'}]
    elif gap > 40:
        return [{'severity': 'amber', 'title': 'Top-Heavy Leader',
                 'detail': f'**{n1}** leads at {_fmt(v1)}, {gap:.0f}% above **{n2}** ({_fmt(v2)}).'}]
    return []


def _check_concentration(df, metric, label_col) -> List[Dict]:
    if len(df) < 5:
        return []
    s     = df.sort_values(metric, ascending=False)
    total = float(s[metric].sum())
    if total <= 0:
        return []
    top3  = float(s[metric].head(3).sum())
    pct   = top3 / total * 100
    names = ", ".join(_name(str(n), 20) for n in s[label_col].head(3))
    if pct > 80:
        return [{'severity': 'red', 'title': f'Heavy Concentration ({pct:.0f}%)',
                 'detail': f'**{names}** hold {pct:.0f}% of all events '
                           f'({_fmt(top3)} of {_fmt(total)}). '
                           f'Remaining {len(df)-3} share just {100-pct:.0f}%.'}]
    elif pct > 55:
        return [{'severity': 'amber', 'title': f'Moderate Concentration ({pct:.0f}%)',
                 'detail': f'Top 3 (**{names}**) hold {pct:.0f}% of events. '
                           f'Monitor for increasing concentration.'}]
    return [{'severity': 'green', 'title': f'Well-Distributed ({pct:.0f}% top 3)',
             'detail': f'Activity spread across {len(df)} entities. No dependency risk.'}]


def _check_outliers(df, metric, label_col) -> List[Dict]:
    vals = df[metric].dropna()
    if len(vals) < 5:
        return []
    mean = float(vals.mean())
    std  = float(vals.std())
    if std == 0:
        return []
    s    = df.sort_values(metric, ascending=False)
    high = s[s[metric] > mean + 2.5 * std]
    if len(high) == 0:
        return []
    names = [f"{_name(str(r[label_col]), 22)} ({_fmt(r[metric])})"
             for _, r in high.head(3).iterrows()]
    return [{'severity': 'amber',
             'title': f'{len(high)} High Outlier{"s" if len(high) > 1 else ""} (>2.5σ)',
             'detail': f'{", ".join(names)} far exceed average of {_fmt(mean)}. '
                       f'Could be power users or data issues.'}]


def _check_dropoff(df, metric, label_col) -> List[Dict]:
    if len(df) < 4:
        return []
    s     = df.sort_values(metric, ascending=False)
    vals  = s[metric].values
    names = s[label_col].values
    max_drop, pos = 0, 0
    for i in range(min(len(vals) - 1, 8)):
        if vals[i] > 0:
            d = (vals[i] - vals[i + 1]) / vals[i] * 100
            if d > max_drop:
                max_drop, pos = d, i
    if max_drop > 50 and pos < len(vals) - 1:
        return [{'severity': 'amber', 'title': f'Cliff Drop at #{pos+1} → #{pos+2}',
                 'detail': f'{max_drop:.0f}% drop between '
                           f'**{_name(str(names[pos]))}** ({_fmt(vals[pos])}) and '
                           f'**{_name(str(names[pos+1]))}** ({_fmt(vals[pos+1])}). '
                           f'Clear tier boundary in your data.'}]
    return []


def _check_inactive(df, metric, label_col) -> List[Dict]:
    vals  = df[metric].fillna(0)
    zeros = df[vals == 0]
    if len(zeros) == 0:
        return []
    pct   = len(zeros) / len(df) * 100
    names = ", ".join(_name(str(n), 20) for n in zeros[label_col].head(3))
    more  = f" +{len(zeros)-3} more" if len(zeros) > 3 else ""
    if pct > 30:
        return [{'severity': 'red', 'title': f'{len(zeros)} Dead Entities ({pct:.0f}%)',
                 'detail': f'**{names}**{more} have zero events. '
                           f'Likely abandoned or broken integrations.'}]
    return [{'severity': 'amber', 'title': f'{len(zeros)} Inactive',
             'detail': f'**{names}**{more} showing zero events. Check if expected.'}]


# =============================================================================
# Time-series health check
# =============================================================================

def _check_trend(df, metric, time_col) -> List[Dict]:
    try:
        s    = df.sort_values(time_col).copy()
        vals = pd.to_numeric(s[metric], errors='coerce').fillna(0).values
        n    = len(vals)
        if n < 4:
            return []

        mid         = n // 2
        first_half  = float(vals[:mid].mean())
        second_half = float(vals[mid:].mean())
        overall_avg = float(vals.mean())
        if first_half <= 0:
            return []

        change_pct = (second_half - first_half) / first_half * 100
        last_3     = float(vals[-3:].mean())
        recent_pct = (last_3 - overall_avg) / overall_avg * 100 if overall_avg > 0 else 0

        # Build a human-readable date span
        try:
            dates   = _safe_parse_dates(s[time_col])
            t_min   = pd.Timestamp(dates.min())
            t_max   = pd.Timestamp(dates.max())
            span_d  = (t_max - t_min).days
            if span_d >= 60:
                span_str = f"{t_min.strftime('%b %Y')} – {t_max.strftime('%b %Y')}"
            else:
                span_str = f"{t_min.strftime('%b %d')} – {t_max.strftime('%b %d, %Y')}"
        except Exception:
            span_str = f"{n} periods"

        findings = []

        if change_pct < -30:
            findings.append({
                'severity': 'red',
                'title': f'Steep Decline ({change_pct:.0f}%)',
                'detail': (
                    f'Events dropped from {_fmt(first_half)}/day to '
                    f'{_fmt(second_half)}/day over {span_str}. Investigate urgently.'
                ),
            })
        elif change_pct < -10:
            findings.append({
                'severity': 'amber',
                'title': f'Gradual Decline ({change_pct:.0f}%)',
                'detail': (
                    f'Events down from {_fmt(first_half)} to '
                    f'{_fmt(second_half)}/day over {span_str}. Monitor closely.'
                ),
            })
        elif change_pct > 50:
            findings.append({
                'severity': 'amber',
                'title': f'Unusual Surge (+{change_pct:.0f}%)',
                'detail': (
                    f'Events surged from {_fmt(first_half)} to '
                    f'{_fmt(second_half)}/day. Verify organic growth vs anomaly.'
                ),
            })
        else:
            findings.append({
                'severity': 'green',
                'title': f'Stable Trend ({change_pct:+.0f}%)',
                'detail': (
                    f'Averaging {_fmt(overall_avg)}/day over {span_str}. '
                    f'Healthy and consistent.'
                ),
            })

        if recent_pct < -40:
            try:
                last_date = _safe_parse_dates(s[time_col]).iloc[-1]
                cliff_str = pd.Timestamp(last_date).strftime('%b %d')
            except Exception:
                cliff_str = "recently"
            findings.append({
                'severity': 'red',
                'title': 'Recent Cliff',
                'detail': (
                    f'Last 3 data points avg {_fmt(last_3)}/day — '
                    f'{abs(recent_pct):.0f}% below the period average. '
                    f'Something changed around {cliff_str}.'
                ),
            })

        return findings

    except Exception as e:
        logger.debug(f"Trend check failed: {e}")
        return []


# =============================================================================
# Chart builder
# =============================================================================

def _build_chart(df, findings, metric, label_col, time_col, is_ts) -> go.Figure:
    show_line = is_ts and time_col and metric and len(df) >= 4
    show_bars = (not is_ts) and metric and label_col and len(df) >= 3

    if show_line or show_bars:
        fig = make_subplots(
            rows=1, cols=2, column_widths=[0.55, 0.45],
            specs=[[{"type": "scatter" if show_line else "bar"}, {"type": "table"}]],
            horizontal_spacing=0.06,
        )
    else:
        fig = make_subplots(rows=1, cols=1, specs=[[{"type": "table"}]])

    # -- Line chart (time-series) ---------------------------------------------
    if show_line:
        s      = df.sort_values(time_col).copy()
        dates  = _safe_parse_dates(s[time_col])
        vals   = pd.to_numeric(s[metric], errors='coerce').fillna(0).values
        avg    = float(vals.mean())

        fig.add_trace(go.Scatter(
            x=dates, y=vals,
            mode='lines+markers',
            line=dict(color=BLUE, width=2),
            marker=dict(size=4, color=BLUE),
            fill='tozeroy', fillcolor='rgba(37,99,235,0.08)',
            showlegend=False,
        ), row=1, col=1)

        fig.add_trace(go.Scatter(
            x=[dates.iloc[0], dates.iloc[-1]], y=[avg, avg],
            mode='lines',
            line=dict(color=GRAY, width=1, dash='dot'),
            showlegend=False,
        ), row=1, col=1)

        peak_idx = int(np.argmax(vals))
        low_idx  = int(np.argmin(vals))
        for idx, label, color in [
            (peak_idx, f'Peak: {_fmt(vals[peak_idx])}', GREEN),
            (low_idx,  f'Low: {_fmt(vals[low_idx])}',   RED),
        ]:
            fig.add_annotation(
                x=dates.iloc[idx], y=float(vals[idx]),
                text=label, showarrow=True, arrowhead=2,
                font=dict(size=10, color=color),
                bgcolor='white', bordercolor=color, borderwidth=1,
                row=1, col=1,
            )

        try:
            span_d   = (dates.max() - dates.min()).days
            tick_fmt = '%b %Y' if span_d > 60 else '%b %d'
        except Exception:
            tick_fmt = '%b %d'

        fig.update_xaxes(tickformat=tick_fmt, type='date',
                         nticks=7, showgrid=False, row=1, col=1)
        fig.update_yaxes(showgrid=True, gridcolor='#F1F5F9', row=1, col=1)

    # -- Bar chart (dimensional) -----------------------------------------------
    elif show_bars:
        s      = df.sort_values(metric, ascending=True).tail(10)
        vals   = pd.to_numeric(s[metric], errors='coerce').fillna(0).values
        labels = [_name(str(v), 28) for v in s[label_col].values]
        mean   = float(df[metric].mean())
        std    = float(df[metric].std()) if len(df) > 1 else 0

        colors = []
        for v in vals:
            if std > 0 and v > mean + 2.5 * std: colors.append(RED)
            elif std > 0 and v > mean + 1.5 * std: colors.append(AMBER)
            elif v == 0: colors.append(GRAY)
            else: colors.append(BLUE)

        fig.add_trace(go.Bar(
            y=labels, x=vals, orientation='h', marker_color=colors,
            text=[_fmt(v) for v in vals], textposition='outside',
            textfont=dict(size=11, color=DARK), showlegend=False,
        ), row=1, col=1)

        fig.update_xaxes(showgrid=False, showticklabels=False, row=1, col=1)
        fig.update_yaxes(showgrid=False, row=1, col=1)

    # -- Findings table --------------------------------------------------------
    sev_bg = {'red': LIGHT_RED, 'amber': LIGHT_AMBER, 'green': LIGHT_GREEN}
    indicators, details, fills = [], [], []
    for f in findings[:6]:
        icon = {'red': '🔴', 'amber': '🟡', 'green': '🟢'}.get(f['severity'], '⚪')
        indicators.append(f"{icon} {f['title']}")
        d = f['detail'].replace('**', '')
        details.append(d[:120] + "…" if len(d) > 120 else d)
        fills.append(sev_bg.get(f['severity'], '#F8FAFC'))

    col_idx = 2 if (show_line or show_bars) else 1
    fig.add_trace(go.Table(
        columnwidth=[35, 65],
        header=dict(
            values=['<b>Finding</b>', '<b>Detail</b>'],
            fill_color='#F1F5F9', font=dict(size=12, color=DARK),
            align='left', height=32,
        ),
        cells=dict(
            values=[indicators, details],
            fill_color=[fills, fills],
            font=dict(size=11, color='#1E293B'),
            align='left', height=40,
            line=dict(color='#E2E8F0', width=1),
        ),
    ), row=1, col=col_idx)

    fig.update_layout(
        height=max(320, len(findings) * 55 + 100),
        margin=dict(l=0, r=0, t=30, b=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
        title=dict(text="Health Analysis", font=dict(size=14, color=DARK), x=0),
    )
    return fig


# =============================================================================
# Helpers
# =============================================================================

def _fmt(n) -> str:
    if n is None or (isinstance(n, float) and np.isnan(n)):
        return "—"
    n = float(n)
    if abs(n) >= 1e9: return f"{n/1e9:.1f}B"
    if abs(n) >= 1e6: return f"{n/1e6:.1f}M"
    if abs(n) >= 1e3: return f"{n/1e3:.1f}K"
    return f"{n:,.0f}"


def _name(val: str, max_len: int = 30) -> str:
    s = str(val)
    return s[:max_len - 1] + "…" if len(s) > max_len else s
