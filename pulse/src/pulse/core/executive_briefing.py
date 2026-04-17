"""
Executive Briefing v1.0 — LT-Ready Summary with Real Stats
============================================================

Replaces the bullet-point "narrate_summary" with a real executive view:
  - This week vs last week (actual numbers, not percentages only)
  - Top movers (orgs/regions with biggest change)
  - Status signal: green / amber / red with clear reason
  - Designed to be rendered as HTML cards in app.py

Author: PULSE Team
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def _fmt(n) -> str:
    if n is None or (isinstance(n, float) and np.isnan(n)):
        return "—"
    n = float(n)
    if abs(n) >= 1e12: return f"{n/1e12:.1f}T"
    if abs(n) >= 1e9:  return f"{n/1e9:.1f}B"
    if abs(n) >= 1e6:  return f"{n/1e6:.1f}M"
    if abs(n) >= 1e3:  return f"{n/1e3:.1f}K"
    return f"{n:,.0f}"


def _pct_change(new_val: float, old_val: float) -> Optional[float]:
    if old_val == 0:
        return None
    return (new_val - old_val) / old_val * 100


def _signal(pct: Optional[float]) -> Tuple[str, str, str]:
    """Returns (color_class, arrow, description)"""
    if pct is None:
        return "neutral", "→", "no prior data"
    if pct >= 10:
        return "positive", "↑", f"+{pct:.0f}%"
    elif pct > -10:
        return "neutral", "→", f"{pct:+.0f}%"
    elif pct > -30:
        return "warning", "↓", f"{pct:.0f}%"
    else:
        return "critical", "↓↓", f"{pct:.0f}%"


def build_executive_briefing(
    profile_daily: Optional[pd.DataFrame],
    profile_region: Optional[pd.DataFrame],
    profile_org: Optional[pd.DataFrame],
    question: str = "",
) -> Dict:
    """
    Build an executive briefing with real WoW comparisons.

    Returns a dict with:
      - 'html': rich HTML card layout for st.html()
      - 'summary_text': plain-text fallback
      - 'stats': structured data for follow-up use
    """
    stats = {}
    sections = []

    # ── 1. Week-over-week from daily data ──────────────────────────────────
    wow_section = None
    if profile_daily is not None and not profile_daily.empty:
        wow_section = _build_wow_section(profile_daily)
        if wow_section:
            stats['wow'] = wow_section

    # ── 2. Regional snapshot ───────────────────────────────────────────────
    region_section = None
    if profile_region is not None and not profile_region.empty:
        region_section = _build_region_section(profile_region)
        if region_section:
            stats['regions'] = region_section

    # ── 3. Top contributors ────────────────────────────────────────────────
    org_section = None
    if profile_org is not None and not profile_org.empty:
        org_section = _build_org_section(profile_org)
        if org_section:
            stats['orgs'] = org_section

    # ── Compose HTML ───────────────────────────────────────────────────────
    html = _render_html(wow_section, region_section, org_section)
    text = _render_text(wow_section, region_section, org_section)

    return {
        'html': html,
        'summary_text': text,
        'stats': stats,
        'type': 'executive_briefing',
    }


# ─────────────────────────────────────────────────────────────────────────────
# Section builders
# ─────────────────────────────────────────────────────────────────────────────

def _build_wow_section(df: pd.DataFrame) -> Optional[Dict]:
    """Last 7 days vs the 7 before that."""
    # Find time and events columns
    t_col = next((c for c in df.columns
                  if c.lower() in ('day', 'date', 'eventinfo_time', 'timestamp')), None)
    v_col = next((c for c in df.columns
                  if c.lower() in ('events', 'event_count', 'total_events', 'count')), None)

    if not t_col or not v_col:
        return None

    df = df.copy()
    try:
        df[t_col] = pd.to_datetime(df[t_col], errors='coerce', utc=True).dt.tz_convert(None)
    except Exception:
        try:
            df[t_col] = pd.to_datetime(df[t_col], errors='coerce')
        except Exception:
            return None

    df = df.dropna(subset=[t_col]).sort_values(t_col)
    df[v_col] = pd.to_numeric(df[v_col], errors='coerce').fillna(0)

    if len(df) < 4:
        return None

    # Use last 7 calendar days vs the 7 before
    max_date = df[t_col].max()
    cutoff_now  = max_date
    cutoff_w1   = max_date - timedelta(days=7)
    cutoff_w2   = max_date - timedelta(days=14)

    this_week = df[df[t_col] > cutoff_w1][v_col].sum()
    last_week = df[(df[t_col] > cutoff_w2) & (df[t_col] <= cutoff_w1)][v_col].sum()
    all_time  = df[v_col].sum()

    pct = _pct_change(this_week, last_week)
    color, arrow, label = _signal(pct)

    # Daily average this week
    days_this_week = max(1, len(df[df[t_col] > cutoff_w1]))
    avg_this_week  = this_week / days_this_week

    # Peak day
    peak_row = df.loc[df[v_col].idxmax()]
    peak_date = peak_row[t_col]
    peak_val  = peak_row[v_col]
    try:
        peak_date_str = pd.Timestamp(peak_date).strftime('%b %d')
    except Exception:
        peak_date_str = "—"

    # Trend direction over full period (linear regression slope)
    trend_word = "flat"
    if len(df) >= 7:
        xs = np.arange(len(df))
        ys = df[v_col].values.astype(float)
        try:
            slope = np.polyfit(xs, ys, 1)[0]
            slope_pct = slope / (ys.mean() + 1e-9) * 100
            if slope_pct > 2:
                trend_word = "growing"
            elif slope_pct < -2:
                trend_word = "declining"
        except Exception:
            pass

    return {
        'this_week': this_week,
        'last_week': last_week,
        'pct': pct,
        'color': color,
        'arrow': arrow,
        'label': label,
        'all_time': all_time,
        'avg_daily': avg_this_week,
        'peak_val': peak_val,
        'peak_date': peak_date_str,
        'trend_word': trend_word,
        'data_points': len(df),
    }


def _build_region_section(df: pd.DataFrame) -> Optional[Dict]:
    """Top regions with share."""
    v_col = next((c for c in df.columns
                  if c.lower() in ('events', 'event_count', 'total_events')), None)
    l_col = next((c for c in df.columns
                  if c.lower() in ('geoname', 'geo', 'region', 'country')), None)

    if not v_col or not l_col:
        return None

    df = df.copy()
    df[v_col] = pd.to_numeric(df[v_col], errors='coerce').fillna(0)
    df = df.sort_values(v_col, ascending=False).head(5)
    total = df[v_col].sum()

    regions = []
    for _, row in df.iterrows():
        val  = row[v_col]
        name = str(row[l_col])
        pct  = val / total * 100 if total > 0 else 0
        regions.append({'name': name, 'val': val, 'pct': pct})

    return {'regions': regions, 'total': total}


def _build_org_section(df: pd.DataFrame) -> Optional[Dict]:
    """Top 5 orgs by volume."""
    v_col = next((c for c in df.columns
                  if c.lower() in ('events', 'event_count', 'total_events')), None)
    l_col = next((c for c in df.columns
                  if c.lower() in ('orgid', 'org_id', 'organization', 'tenantid')), None)

    if not v_col or not l_col:
        return None

    df = df.copy()
    df[v_col] = pd.to_numeric(df[v_col], errors='coerce').fillna(0)
    df = df.sort_values(v_col, ascending=False).head(5)
    total = df[v_col].sum()

    orgs = []
    for _, row in df.iterrows():
        name = str(row[l_col])
        name = name[:28] + "…" if len(name) > 28 else name
        val  = row[v_col]
        pct  = val / total * 100 if total > 0 else 0
        orgs.append({'name': name, 'val': val, 'pct': pct})

    return {'orgs': orgs, 'total': total}


# ─────────────────────────────────────────────────────────────────────────────
# HTML renderer
# ─────────────────────────────────────────────────────────────────────────────

_COLOR_MAP = {
    'positive': ('#D1FAE5', '#065F46', '#10B981'),   # bg, text, accent
    'neutral':  ('#F1F5F9', '#334155', '#64748B'),
    'warning':  ('#FEF3C7', '#92400E', '#F59E0B'),
    'critical': ('#FEE2E2', '#991B1B', '#EF4444'),
}

def _render_html(wow, region, org) -> str:
    parts = []

    # ── WoW headline card ──────────────────────────────────────────────────
    if wow:
        bg, txt, accent = _COLOR_MAP[wow['color']]
        arrow_html = f'<span style="font-size:20px;color:{accent};">{wow["arrow"]}</span>'
        status_line = {
            'growing':  '📈 Activity is trending upward',
            'declining':'📉 Activity is trending down — worth investigating',
            'flat':     '➡️ Activity is broadly flat',
        }.get(wow['trend_word'], '')

        parts.append(f"""
<div style="background:{bg};border:1px solid {accent}33;border-radius:12px;
            padding:20px 24px;margin-bottom:16px;">
  <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:12px;">

    <div>
      <div style="font-size:12px;font-weight:600;color:{accent};text-transform:uppercase;
                  letter-spacing:1px;margin-bottom:4px;">This Week vs Last Week</div>
      <div style="font-size:32px;font-weight:800;color:#0F172A;letter-spacing:-1px;">
        {_fmt(wow['this_week'])} {arrow_html} {_fmt(wow['last_week'])}
      </div>
      <div style="font-size:14px;color:{txt};margin-top:4px;">
        <strong>{wow['label']}</strong> week-over-week &nbsp;·&nbsp;
        {_fmt(wow['avg_daily'])}/day average this week
      </div>
    </div>

    <div style="text-align:right;">
      <div style="font-size:12px;color:#64748B;margin-bottom:6px;">All-time total</div>
      <div style="font-size:22px;font-weight:700;color:#0F172A;">{_fmt(wow['all_time'])}</div>
      <div style="font-size:12px;color:#64748B;margin-top:2px;">
        Peak: <strong>{_fmt(wow['peak_val'])}</strong> on {wow['peak_date']}
      </div>
    </div>

  </div>
  {f'<div style="margin-top:12px;font-size:13px;color:{txt};">{status_line}</div>' if status_line else ''}
</div>""")

    # ── Two-column: regions + orgs ─────────────────────────────────────────
    left_col = ""
    right_col = ""

    if region:
        total = region['total']
        rows_html = ""
        for r in region['regions']:
            bar_w = max(4, int(r['pct'] * 0.9))
            rows_html += f"""
      <div style="margin-bottom:10px;">
        <div style="display:flex;justify-content:space-between;margin-bottom:3px;">
          <span style="font-size:13px;font-weight:600;color:#0F172A;">{r['name']}</span>
          <span style="font-size:13px;color:#64748B;">{_fmt(r['val'])} &nbsp;<span style="color:#94A3B8;">{r['pct']:.0f}%</span></span>
        </div>
        <div style="background:#E2E8F0;border-radius:4px;height:6px;">
          <div style="background:#2563EB;border-radius:4px;height:6px;width:{bar_w}%;"></div>
        </div>
      </div>"""

        left_col = f"""
<div style="background:#FFFFFF;border:1px solid #E2E8F0;border-radius:12px;padding:18px 20px;">
  <div style="font-size:12px;font-weight:600;color:#64748B;text-transform:uppercase;
              letter-spacing:1px;margin-bottom:14px;">Regional Breakdown</div>
  {rows_html}
</div>"""

    if org:
        rows_html = ""
        for i, o in enumerate(org['orgs']):
            bar_w = max(4, int(o['pct'] * 0.9))
            medal = ["🥇","🥈","🥉","",""][i]
            rows_html += f"""
      <div style="margin-bottom:10px;">
        <div style="display:flex;justify-content:space-between;margin-bottom:3px;">
          <span style="font-size:13px;font-weight:600;color:#0F172A;">{medal} {o['name']}</span>
          <span style="font-size:13px;color:#64748B;">{_fmt(o['val'])} &nbsp;<span style="color:#94A3B8;">{o['pct']:.0f}%</span></span>
        </div>
        <div style="background:#E2E8F0;border-radius:4px;height:6px;">
          <div style="background:#8B5CF6;border-radius:4px;height:6px;width:{bar_w}%;"></div>
        </div>
      </div>"""

        right_col = f"""
<div style="background:#FFFFFF;border:1px solid #E2E8F0;border-radius:12px;padding:18px 20px;">
  <div style="font-size:12px;font-weight:600;color:#64748B;text-transform:uppercase;
              letter-spacing:1px;margin-bottom:14px;">Top Contributors</div>
  {rows_html}
</div>"""

    if left_col or right_col:
        parts.append(f"""
<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:16px;">
  {left_col}
  {right_col}
</div>""")

    # ── Footer note ───────────────────────────────────────────────────────
    parts.append("""
<div style="font-size:11px;color:#94A3B8;margin-top:4px;text-align:right;">
  Based on profile data · data refreshes on reconnect
</div>""")

    return "\n".join(parts)


# ─────────────────────────────────────────────────────────────────────────────
# Plain-text fallback
# ─────────────────────────────────────────────────────────────────────────────

def _render_text(wow, region, org) -> str:
    lines = []

    if wow:
        direction = {"growing": "up", "declining": "down", "flat": "flat"}.get(wow['trend_word'], "")
        lines.append(
            f"**This week:** {_fmt(wow['this_week'])} events {wow['arrow']} {wow['label']} vs last week "
            f"({_fmt(wow['last_week'])}). Overall trend is {direction}."
        )
        lines.append(f"Peak was {_fmt(wow['peak_val'])} on {wow['peak_date']}.")

    if region and region['regions']:
        top = region['regions'][0]
        lines.append(
            f"**Top region:** {top['name']} with {_fmt(top['val'])} events "
            f"({top['pct']:.0f}% of total)."
        )

    if org and org['orgs']:
        top = org['orgs'][0]
        lines.append(
            f"**Top org:** {top['name']} with {_fmt(top['val'])} events "
            f"({top['pct']:.0f}% of total)."
        )

    return "\n\n".join(lines) if lines else "No summary data available."
