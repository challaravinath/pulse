"""
Business Overview v1.0 — VP-Ready Product Health Card
=======================================================

Replaces the engineering-focused hero card with a business narrative.

What it answers (in order):
  1. Are we growing or shrinking?       → active orgs WoW
  2. Are our customers engaged?         → events per active org (depth)
  3. Who should I be worried about?     → at-risk customers
  4. Where is adoption happening?       → org count by region
  5. Anything to flag to leadership?    → concentration risk

Design principles:
  - Every number traces to a real column. No inference. No hallucination.
  - ⓘ bubble on every metric that has a caveat
  - 📋 clipboard on every section with runnable KQL
  - Dates formatted cleanly: "Feb 17 – Feb 23, 2025"
  - Handles 7d snapshot vs 30d full profile gracefully
  - Org IDs shown as-is if enrichment not loaded (no fake names)

Author: PULSE Team
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

_NET_EPOCH = 621_355_968_000_000_000


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def build_business_overview(
    profile_daily:  Optional[pd.DataFrame],
    profile_org:    Optional[pd.DataFrame],
    profile_region: Optional[pd.DataFrame],
    profile_totals: Optional[pd.DataFrame],
    config=None,          # SemanticLayer config — for KQL generation
    data_scope: str = "30d",
    product_name: str = "Product",
) -> Dict:
    """
    Build the business overview card.

    Returns:
      html:         full HTML string → st.html()
      summary_text: plain-text fallback
      metrics:      dict of computed values for downstream use
    """
    # ── Sanitize all date columns first ──────────────────────────────────────
    daily  = _sanitize(profile_daily)
    orgs   = _sanitize(profile_org)
    region = _sanitize(profile_region)
    totals = profile_totals  # single row, no dates

    # ── Compute each metric block ─────────────────────────────────────────────
    active_orgs_block = _compute_active_orgs(daily)
    new_orgs_block    = _compute_new_orgs(orgs, data_scope)
    depth_block       = _compute_usage_depth(totals, orgs)
    at_risk_block     = _compute_at_risk(orgs, data_scope)
    region_block      = _compute_region_adoption(region)
    concentration     = _compute_concentration(orgs, totals)
    date_range        = _compute_date_range(daily, data_scope)

    # ── KQL snippets for clipboard ────────────────────────────────────────────
    kql = _build_kql_snippets(config)

    metrics = {
        'active_orgs':  active_orgs_block,
        'new_orgs':     new_orgs_block,
        'depth':        depth_block,
        'at_risk':      at_risk_block,
        'region':       region_block,
        'concentration':concentration,
        'date_range':   date_range,
    }

    html = _render_html(
        metrics, kql, product_name, data_scope, date_range
    )

    return {
        'html':         html,
        'summary_text': _render_text(metrics),
        'metrics':      metrics,
        'type':         'business_overview',
    }


# ─────────────────────────────────────────────────────────────────────────────
# Metric computers
# ─────────────────────────────────────────────────────────────────────────────

def _compute_active_orgs(daily: Optional[pd.DataFrame]) -> Dict:
    """
    Avg active orgs/day: last 7 days vs previous 7 days.
    Uses profile_daily.active_orgs column.

    Caveat: daily active_orgs = distinct orgs that day.
    Averaging 7 days is an approximation of weekly unique orgs.
    """
    empty = {'value': None, 'prev': None, 'delta': None, 'delta_pct': None,
             'available': False, 'caveat': 'active_orgs column not found in profile_daily'}

    if daily is None or daily.empty:
        return empty

    t_col = _col(daily, ['day', 'date', 'eventinfo_time', 'timestamp'])
    v_col = _col(daily, ['active_orgs'])  # must be active_orgs specifically

    if not t_col or not v_col:
        return {**empty, 'caveat': 'active_orgs not in profile_daily — only events tracked'}

    df = daily.copy().sort_values(t_col)
    df[v_col] = pd.to_numeric(df[v_col], errors='coerce').fillna(0)
    n = len(df)

    last7  = df[v_col].tail(7).mean()
    prev7  = df[v_col].iloc[max(0, n-14):max(0, n-7)].mean() if n >= 8 else None

    delta     = round(last7 - prev7) if prev7 else None
    delta_pct = (last7 - prev7) / prev7 * 100 if prev7 and prev7 > 0 else None

    return {
        'value':     round(last7),
        'prev':      round(prev7) if prev7 else None,
        'delta':     delta,
        'delta_pct': delta_pct,
        'available': True,
        'caveat':    (
            'Avg distinct orgs/day over last 7 days vs prior 7 days. '
            'This is an approximation — exact weekly unique count requires a separate query.'
        ),
    }


def _compute_new_orgs(orgs: Optional[pd.DataFrame], data_scope: str) -> Dict:
    """
    Orgs whose first_seen is within the last 30 days (or data_scope window).
    Uses profile_organization.first_seen.
    """
    empty = {'value': None, 'available': False,
             'caveat': 'first_seen column not in profile_organization'}

    if orgs is None or orgs.empty:
        return empty

    fs_col = _col(orgs, ['first_seen'])
    if not fs_col:
        return empty

    scope_days = _parse_days(data_scope)
    # "new" = first seen in the last scope_days
    # Use relative to max date in data (not today) to be robust
    try:
        max_date = orgs[fs_col].max()
        cutoff   = max_date - timedelta(days=scope_days)
        new_df   = orgs[orgs[fs_col] >= cutoff]
        value    = len(new_df)
    except Exception as e:
        logger.debug(f"new_orgs compute failed: {e}")
        return empty

    return {
        'value':     value,
        'available': True,
        'caveat':    (
            f'Organisations whose first recorded event is within the last {scope_days} days (this quarter). '
            f'"New" means first seen in your telemetry — not necessarily newly signed up.'
        ),
    }


def _compute_usage_depth(
    totals: Optional[pd.DataFrame],
    orgs:   Optional[pd.DataFrame],
) -> Dict:
    """
    Events per active org over the profile period.
    Uses profile_totals.total_events / total_active_orgs.
    Falls back to sum(org.events) / count(orgs).
    """
    empty = {'value': None, 'available': False,
             'caveat': 'Cannot compute — need both event counts and org counts'}

    try:
        total_events = None
        total_orgs   = None

        if totals is not None and not totals.empty:
            ev_col  = _col(totals, ['total_events'])
            org_col = _col(totals, ['total_active_orgs'])
            if ev_col:
                total_events = float(pd.to_numeric(totals[ev_col].iloc[0], errors='coerce'))
            if org_col:
                total_orgs = float(pd.to_numeric(totals[org_col].iloc[0], errors='coerce'))

        # Fallback: compute from org table
        if total_events is None and orgs is not None and not orgs.empty:
            ev_col = _col(orgs, ['events', 'event_count'])
            if ev_col:
                total_events = float(pd.to_numeric(orgs[ev_col], errors='coerce').sum())
                total_orgs   = len(orgs)

        if not total_events or not total_orgs or total_orgs == 0:
            return empty

        depth = total_events / total_orgs

        return {
            'value':       depth,
            'total_events': total_events,
            'total_orgs':   total_orgs,
            'available':   True,
            'caveat':      (
                'Total events divided by total active organisations over the profile period. '
                'Higher = customers using the product more intensively. '
                'Does not account for org size or contract tier.'
            ),
        }
    except Exception as e:
        logger.debug(f"depth compute failed: {e}")
        return empty


def _compute_at_risk(orgs: Optional[pd.DataFrame], data_scope: str) -> Dict:
    """
    Orgs that were active early in the period but silent in the last 7 days.
    Uses profile_organization.first_seen + last_seen.

    "At risk" definition:
      - first_seen older than 7 days (not brand new)
      - last_seen older than 7 days (gone quiet)
    """
    empty = {'value': None, 'orgs': [], 'available': False,
             'caveat': 'first_seen / last_seen columns not in profile_organization'}

    if orgs is None or orgs.empty:
        return empty

    fs_col = _col(orgs, ['first_seen'])
    ls_col = _col(orgs, ['last_seen'])
    if not fs_col or not ls_col:
        return empty

    try:
        max_date    = orgs[ls_col].max()
        quiet_since = max_date - timedelta(days=7)
        established = max_date - timedelta(days=7)

        at_risk = orgs[
            (orgs[ls_col] < quiet_since) &      # gone quiet
            (orgs[fs_col] < established)         # not brand new
        ].copy()

        # Sort by last_seen ascending (longest quiet first)
        at_risk = at_risk.sort_values(ls_col, ascending=True)

        # Build display list
        id_col = _col(orgs, ['orgid', 'org_id', 'organizationid', 'tenantid'])
        ev_col = _col(orgs, ['events', 'event_count'])

        risk_list = []
        for _, row in at_risk.head(5).iterrows():
            org_id     = str(row[id_col])[:32] if id_col else "—"
            last_active = _fmt_date(row[ls_col])
            events_val  = _fmt(float(row[ev_col])) if ev_col else "—"
            risk_list.append({
                'id':          org_id,
                'last_seen':   last_active,
                'events':      events_val,
            })

        return {
            'value':     len(at_risk),
            'orgs':      risk_list,
            'available': True,
            'caveat':    (
                '"At risk" = established organisations (active > 7 days ago) '
                'with no events in the last 7 days. '
                '"Gone quiet" — not necessarily churned. '
                'Org IDs shown — enable org enrichment to see friendly names.'
            ),
        }
    except Exception as e:
        logger.debug(f"at_risk compute failed: {e}")
        return empty


def _compute_region_adoption(region: Optional[pd.DataFrame]) -> Dict:
    """
    Org count by region. Uses profile_region.active_orgs — NOT event volume.
    """
    empty = {'regions': [], 'available': False,
             'caveat': 'active_orgs column not in profile_region'}

    if region is None or region.empty:
        return empty

    v_col = _col(region, ['active_orgs'])
    l_col = _col(region, ['geoname', 'geo', 'region', 'country'])

    # Fall back to events if active_orgs missing
    using_events = False
    if not v_col:
        v_col = _col(region, ['events', 'event_count'])
        using_events = True
    if not v_col or not l_col:
        return empty

    df = region.copy()
    df[v_col] = pd.to_numeric(df[v_col], errors='coerce').fillna(0)
    df = df.sort_values(v_col, ascending=False).head(6)
    total = df[v_col].sum()

    regions = []
    for _, row in df.iterrows():
        name = str(row[l_col])
        val  = float(row[v_col])
        pct  = val / total * 100 if total > 0 else 0
        if name.lower() in ('unavailable', 'unknown', 'none', 'nan', ''):
            continue
        regions.append({'name': name, 'val': float(val), 'pct': float(pct)})

    caveat = (
        'Showing active organisation count per region — breadth of adoption, not event volume. '
        'One large org can generate more events than 50 small orgs.'
    ) if not using_events else (
        'active_orgs not available in profile_region — showing event volume instead. '
        'Upgrade profile to include dcount(OrgId) for true adoption breadth.'
    )

    return {
        'regions':       regions,
        'total':         total,
        'using_events':  using_events,
        'available':     True,
        'caveat':        caveat,
    }


def _compute_concentration(
    orgs:   Optional[pd.DataFrame],
    totals: Optional[pd.DataFrame],
) -> Dict:
    """Top org's share of total events — concentration risk."""
    empty = {'top_pct': None, 'top_name': None, 'available': False}

    if orgs is None or orgs.empty:
        return empty

    ev_col = _col(orgs, ['events', 'event_count'])
    id_col = _col(orgs, ['orgid', 'org_id', 'organizationid', 'tenantid'])
    if not ev_col:
        return empty

    df = orgs.copy()
    df[ev_col] = pd.to_numeric(df[ev_col], errors='coerce').fillna(0)
    df = df.sort_values(ev_col, ascending=False)

    top_val  = float(df[ev_col].iloc[0])
    top_name = str(df[id_col].iloc[0])[:28] if id_col else "Top org"

    # Get total from totals table if available
    total = None
    if totals is not None and not totals.empty:
        t_col = _col(totals, ['total_events'])
        if t_col:
            total = float(pd.to_numeric(totals[t_col].iloc[0], errors='coerce'))
    if total is None:
        total = float(df[ev_col].sum())

    top_pct  = top_val / total * 100 if total > 0 else 0
    top3_pct = float(df[ev_col].head(3).sum()) / total * 100 if total > 0 else 0

    return {
        'top_pct':  float(top_pct),
        'top3_pct': float(top3_pct),
        'top_name': top_name,
        'top_val':  float(top_val),
        'total':    float(total),
        'available': True,
    }


def _compute_date_range(
    daily: Optional[pd.DataFrame],
    data_scope: str,
) -> Dict:
    """Compute the actual date range covered by the data."""
    try:
        if daily is not None and not daily.empty:
            t_col = _col(daily, ['day', 'date', 'eventinfo_time', 'timestamp'])
            if t_col:
                dates = daily[t_col].dropna()
                if len(dates) > 0:
                    t_min = pd.Timestamp(dates.min())
                    t_max = pd.Timestamp(dates.max())
                    return {
                        'start': t_min,
                        'end':   t_max,
                        'label': _fmt_date_range(t_min, t_max),
                        'days':  (t_max - t_min).days + 1,
                    }
    except Exception as e:
        logger.debug(f"date_range failed: {e}")

    scope_days = _parse_days(data_scope)
    end   = datetime.now()
    start = end - timedelta(days=scope_days)
    return {
        'start': start,
        'end':   end,
        'label': f"Last {scope_days} days",
        'days':  scope_days,
    }


# ─────────────────────────────────────────────────────────────────────────────
# KQL snippet generator
# ─────────────────────────────────────────────────────────────────────────────

def _build_kql_snippets(config) -> Dict[str, str]:
    """
    Generate runnable KQL snippets for each metric.
    Uses actual table/column names from config if available.
    """
    # Defaults — work for standard app_telemetry schema
    table      = "YOUR_TABLE"
    time_col   = "EventInfo_Time"
    org_col    = "OrgId"
    geo_col    = "GeoName"

    try:
        if config:
            if isinstance(config, dict):
                table    = config.get('table',    table)
                time_col = config.get('time_col', time_col)
                org_col  = config.get('org_col',  org_col)
                geo_col  = config.get('geo_col',  geo_col)
            else:
                clusters = getattr(config, 'clusters', [])
                if clusters:
                    table = clusters[0].table
                dims = getattr(config, 'dimensions', {})
                if 'time' in dims:
                    time_col = dims['time'].column
                if 'organization' in dims:
                    org_col  = dims['organization'].column
                if 'region' in dims:
                    geo_col  = dims['region'].column
    except Exception:
        pass

    return {
        'active_orgs': (
            f"// Active orgs per day — last 90 days (PULSE quarter scope)\n"
            f"{table}\n"
            f"| where {time_col} > ago(90d)\n"
            f"| summarize active_orgs = dcount({org_col})\n"
            f"         by Day = bin({time_col}, 1d)\n"
            f"| order by Day asc"
        ),
        'new_orgs': (
            f"// New organisations — first seen in last 90 days\n"
            f"{table}\n"
            f"| summarize first_seen = min({time_col}) by {org_col}\n"
            f"| where first_seen > ago(90d)\n"
            f"| count"
        ),
        'usage_depth': (
            f"// Usage depth: events per active org\n"
            f"{table}\n"
            f"| where {time_col} > ago(90d)\n"
            f"| summarize events = count(),\n"
            f"           active_orgs = dcount({org_col})\n"
            f"| extend events_per_org = todouble(events) / active_orgs"
        ),
        'at_risk': (
            f"// At-risk customers: active before, silent last 7 days\n"
            f"{table}\n"
            f"| summarize first_seen = min({time_col}),\n"
            f"           last_seen  = max({time_col}),\n"
            f"           events     = count()\n"
            f"         by {org_col}\n"
            f"| where first_seen < ago(7d)   // established org\n"
            f"| where last_seen  < ago(7d)   // gone quiet\n"
            f"| order by last_seen asc"
        ),
        'region_adoption': (
            f"// Regional adoption: org count per region\n"
            f"{table}\n"
            f"| where {time_col} > ago(90d)\n"
            f"| summarize active_orgs = dcount({org_col})\n"
            f"         by {geo_col}\n"
            f"| order by active_orgs desc"
        ),
        'concentration': (
            f"// Customer concentration: top org share\n"
            f"{table}\n"
            f"| where {time_col} > ago(90d)\n"
            f"| summarize events = count() by {org_col}\n"
            f"| order by events desc\n"
            f"| extend total = toscalar(\n"
            f"    {table} | where {time_col} > ago(30d) | count)\n"
            f"| extend share_pct = todouble(events) / total * 100\n"
            f"| top 10 by events desc"
        ),
    }


# ─────────────────────────────────────────────────────────────────────────────
# HTML renderer
# ─────────────────────────────────────────────────────────────────────────────

def _render_html(
    m: Dict,
    kql: Dict,
    product_name: str,
    data_scope: str,
    date_range: Dict,
) -> str:

    scope_days = date_range.get('days', _parse_days(data_scope))
    is_partial = scope_days < 28
    scope_label = f"{scope_days}-day snapshot" if is_partial else f"{scope_days}-day profile"
    scope_color = "#F59E0B" if is_partial else "#10B981"
    scope_note  = (
        f'<span style="color:{scope_color};font-weight:600;">'
        f'⚡ {scope_label}</span>'
        f'{"&nbsp;· Full profile building in background" if is_partial else ""}'
    )

    now_str = datetime.now().strftime("%b %d, %Y · %H:%M")

    parts = []

    # ── JS for clipboard ─────────────────────────────────────────────────────
    parts.append("""
<script>
function copyKQL(id) {
  var el = document.getElementById(id);
  if (!el) return;
  var text = el.textContent;
  var btn = document.querySelector('[data-copy="' + id + '"]');

  function onSuccess() {
    if (btn) {
      btn.textContent = '✓ Copied';
      btn.style.color = '#10B981';
      setTimeout(function() {
        btn.textContent = '📋';
        btn.style.color = '#64748B';
      }, 1800);
    }
  }

  // Try modern clipboard API first
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(text).then(onSuccess).catch(function() {
      fallbackCopy(text, onSuccess);
    });
  } else {
    fallbackCopy(text, onSuccess);
  }
}

function fallbackCopy(text, onSuccess) {
  // execCommand fallback — works in sandboxed iframes where clipboard API is blocked
  var ta = document.createElement('textarea');
  ta.value = text;
  ta.style.cssText = 'position:fixed;top:-9999px;left:-9999px;opacity:0;';
  document.body.appendChild(ta);
  ta.focus();
  ta.select();
  try {
    var ok = document.execCommand('copy');
    if (ok && onSuccess) onSuccess();
  } catch(e) {
    // Last resort: show the text so user can copy manually
    prompt('Copy this KQL (Ctrl+A, Ctrl+C):', text);
  }
  document.body.removeChild(ta);
}
</script>""")

    # ── Header ────────────────────────────────────────────────────────────────
    parts.append(f"""
<div style="font-family:'SF Pro Display',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
            max-width:900px;margin:0 auto;">

<div style="display:flex;justify-content:space-between;align-items:center;
            margin-bottom:20px;padding-bottom:14px;
            border-bottom:2px solid #0F172A;">
  <div>
    <div style="font-size:20px;font-weight:700;color:#0F172A;letter-spacing:-0.5px;">
      {product_name}
    </div>
    <div style="font-size:13px;color:#64748B;margin-top:2px;">
      {date_range.get('label','—')} &nbsp;·&nbsp; {scope_note}
    </div>
  </div>
  <div style="text-align:right;font-size:11px;color:#94A3B8;">
    Updated {now_str}
  </div>
</div>""")

    # ── 4 metric cards ────────────────────────────────────────────────────────
    ao = m['active_orgs']
    no = m['new_orgs']
    de = m['depth']
    ar = m['at_risk']

    cards_html = ""

    # Card: Active Orgs
    if ao['available']:
        delta     = ao['delta']
        delta_pct = ao['delta_pct']
        if delta is not None:
            d_col = "#10B981" if delta >= 0 else "#EF4444"
            sign  = "+" if delta > 0 else ""
            abs_d = abs(int(delta))
            abs_p = abs(delta_pct) if delta_pct else 0
            delta_html = (
                f'<div style="font-size:13px;color:{d_col};margin-top:4px;font-weight:600;">'
                f'{sign}{abs_d} vs last week &nbsp;'
                f'<span style="font-weight:400;color:{d_col};opacity:0.8;">'
                f'({sign}{abs_p:.0f}%)</span></div>'
            )
        else:
            delta_html = '<div style="font-size:12px;color:#94A3B8;margin-top:4px;">no prior week data</div>'

        cards_html += _metric_card(
            label="ACTIVE ORGS",
            value=f"{ao['value']:,}",
            sub=delta_html,
            info=ao['caveat'],
            kql_id="kql_active_orgs",
            kql_text=kql.get('active_orgs',''),
            accent="#2563EB",
        )
    else:
        cards_html += _metric_card_na("ACTIVE ORGS", ao['caveat'])

    # Card: New This Month
    if no['available']:
        cards_html += _metric_card(
            label=f"NEW THIS PERIOD",
            value=f"+{no['value']:,}",
            sub='<div style="font-size:12px;color:#64748B;margin-top:4px;">orgs seen for first time</div>',
            info=no['caveat'],
            kql_id="kql_new_orgs",
            kql_text=kql.get('new_orgs',''),
            accent="#8B5CF6",
        )
    else:
        cards_html += _metric_card_na("NEW THIS PERIOD", no['caveat'])

    # Card: Usage Depth
    if de['available']:
        cards_html += _metric_card(
            label="USAGE DEPTH",
            value=_fmt(de['value']),
            sub='<div style="font-size:12px;color:#64748B;margin-top:4px;">events per active org</div>',
            info=de['caveat'],
            kql_id="kql_depth",
            kql_text=kql.get('usage_depth',''),
            accent="#0891B2",
        )
    else:
        cards_html += _metric_card_na("USAGE DEPTH", de['caveat'])

    # Card: At Risk
    if ar['available']:
        risk_col = "#EF4444" if ar['value'] > 0 else "#10B981"
        cards_html += _metric_card(
            label="AT RISK",
            value=f"{ar['value']:,}",
            sub=f'<div style="font-size:12px;color:{risk_col};margin-top:4px;">'
                f'{"orgs gone quiet >7 days" if ar["value"] > 0 else "no at-risk orgs"}</div>',
            info=ar['caveat'],
            kql_id="kql_at_risk",
            kql_text=kql.get('at_risk',''),
            accent="#EF4444",
        )
    else:
        cards_html += _metric_card_na("AT RISK", ar['caveat'])

    parts.append(f"""
<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px;">
  {cards_html}
</div>""")

    # ── Regional adoption + Concentration ─────────────────────────────────────
    reg = m['region']
    con = m['concentration']

    left_html  = _render_region_block(reg, kql)
    right_html = _render_concentration_block(con, kql)

    parts.append(f"""
<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:20px;">
  {left_html}
  {right_html}
</div>""")

    # ── At-risk customer list ─────────────────────────────────────────────────
    if ar['available'] and ar['value'] > 0:
        parts.append(_render_at_risk_block(ar, kql))

    # ── Default chips ──────────────────────────────────────────────────────────
    chips = [
        "who's at risk",
        "top customers",
        "regional adoption",
        "new orgs this month",
        "usage depth",
        "weekly summary for my manager",
    ]
    chip_html = " ".join(
        f'<span onclick="window.parent.postMessage({{type:\'streamlit:setComponentValue\','
        f'value:\'{c}\'}},\'*\')" '
        f'style="display:inline-block;background:#F1F5F9;border:1px solid #CBD5E1;'
        f'border-radius:20px;padding:6px 14px;font-size:13px;color:#334155;'
        f'font-weight:500;cursor:pointer;margin:3px 4px 3px 0;'
        f'transition:background 0.15s;"'
        f'onmouseover="this.style.background=\'#E2E8F0\'" '
        f'onmouseout="this.style.background=\'#F1F5F9\'">'
        f'{c}</span>'
        for c in chips
    )
    parts.append(f"""
<div style="margin-bottom:16px;">
  <div style="font-size:11px;font-weight:600;color:#94A3B8;
              text-transform:uppercase;letter-spacing:1px;margin-bottom:8px;">
    Ask about
  </div>
  {chip_html}
</div>""")

    # ── Footer disclaimer ──────────────────────────────────────────────────────
    parts.append(f"""
<div style="font-size:11px;color:#94A3B8;border-top:1px solid #F1F5F9;
            padding-top:10px;margin-top:4px;line-height:1.6;">
  All metrics computed from your telemetry profile — last {scope_label} of data.
  Numbers marked ⓘ have caveats — hover to read.
  Click 📋 on any section to copy the underlying KQL query.
</div>

</div>""")  # close main wrapper

    return "\n".join(parts)


# ─────────────────────────────────────────────────────────────────────────────
# Sub-renderers
# ─────────────────────────────────────────────────────────────────────────────

def _metric_card(
    label: str, value: str, sub: str,
    info: str, kql_id: str, kql_text: str,
    accent: str = "#2563EB",
) -> str:
    escaped_info = info.replace('"', '&quot;').replace("'", "&#39;")
    kql_escaped  = kql_text.replace('`', '&#96;').replace('<', '&lt;').replace('>', '&gt;')
    return f"""
<div style="background:#FFFFFF;border:1px solid #E2E8F0;border-radius:12px;
            padding:18px 16px;position:relative;">
  <div style="display:flex;justify-content:space-between;align-items:flex-start;">
    <div style="font-size:10px;font-weight:700;color:#94A3B8;
                text-transform:uppercase;letter-spacing:1.2px;">
      {label}
    </div>
    <div style="display:flex;gap:6px;align-items:center;">
      <span title="{escaped_info}"
            style="font-size:12px;color:#94A3B8;cursor:help;
                   border:1px solid #E2E8F0;border-radius:50%;
                   width:16px;height:16px;display:inline-flex;
                   align-items:center;justify-content:center;line-height:1;">
        ⓘ
      </span>
      <span data-copy="{kql_id}"
            onclick="copyKQL('{kql_id}')"
            style="font-size:12px;color:#94A3B8;cursor:pointer;"
            title="Copy KQL query">📋</span>
    </div>
  </div>
  <div style="font-size:30px;font-weight:800;color:{accent};
              margin:8px 0 0 0;letter-spacing:-1px;line-height:1.1;">
    {value}
  </div>
  {sub}
  <pre id="{kql_id}" style="display:none;">{kql_escaped}</pre>
</div>"""


def _metric_card_na(label: str, reason: str) -> str:
    escaped = reason.replace('"', '&quot;')
    return f"""
<div style="background:#F8FAFC;border:1px dashed #CBD5E1;border-radius:12px;
            padding:18px 16px;">
  <div style="font-size:10px;font-weight:700;color:#94A3B8;
              text-transform:uppercase;letter-spacing:1.2px;">{label}</div>
  <div style="font-size:22px;font-weight:700;color:#CBD5E1;margin:8px 0 4px;">—</div>
  <div style="font-size:11px;color:#94A3B8;" title="{escaped}">
    Not available ⓘ
  </div>
</div>"""


def _render_region_block(reg: Dict, kql: Dict) -> str:
    kql_id      = "kql_region"
    kql_text    = kql.get('region_adoption', '')
    kql_escaped = kql_text.replace('`', '&#96;').replace('<', '&lt;').replace('>', '&gt;')
    info        = reg.get('caveat', '').replace('"', '&quot;').replace("'", "&#39;")
    metric_label = "Active Orgs" if not reg.get('using_events') else "Events (org count n/a)"

    if not reg.get('available') or not reg.get('regions'):
        return f"""
<div style="background:#F8FAFC;border:1px dashed #CBD5E1;border-radius:12px;padding:18px 20px;">
  <div style="font-size:12px;font-weight:600;color:#94A3B8;">REGIONAL ADOPTION</div>
  <div style="font-size:13px;color:#94A3B8;margin-top:12px;">No region data available</div>
</div>"""

    rows = ""
    for r in reg['regions'][:6]:
        bar_w   = max(3, int(r['pct'] * 0.85))
        bar_col = "#2563EB" if not reg.get('using_events') else "#64748B"
        rows += f"""
<div style="margin-bottom:11px;">
  <div style="display:flex;justify-content:space-between;
              align-items:baseline;margin-bottom:4px;">
    <span style="font-size:13px;font-weight:600;color:#0F172A;">{r['name']}</span>
    <span style="font-size:12px;color:#64748B;">
      {_fmt(r['val'])}
      <span style="color:#94A3B8;margin-left:4px;">{r['pct']:.0f}%</span>
    </span>
  </div>
  <div style="background:#F1F5F9;border-radius:4px;height:5px;">
    <div style="background:{bar_col};border-radius:4px;
                height:5px;width:{bar_w}%;"></div>
  </div>
</div>"""

    return f"""
<div style="background:#FFFFFF;border:1px solid #E2E8F0;border-radius:12px;padding:18px 20px;">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;">
    <div>
      <div style="font-size:10px;font-weight:700;color:#94A3B8;
                  text-transform:uppercase;letter-spacing:1.2px;">
        Regional Adoption
      </div>
      <div style="font-size:10px;color:#94A3B8;margin-top:2px;">
        by {metric_label}
      </div>
    </div>
    <div style="display:flex;gap:6px;">
      <span title="{info}"
            style="font-size:12px;color:#94A3B8;cursor:help;
                   border:1px solid #E2E8F0;border-radius:50%;
                   width:16px;height:16px;display:inline-flex;
                   align-items:center;justify-content:center;">ⓘ</span>
      <span data-copy="{kql_id}" onclick="copyKQL('{kql_id}')"
            style="font-size:12px;color:#94A3B8;cursor:pointer;"
            title="Copy KQL">📋</span>
    </div>
  </div>
  {rows}
  <pre id="{kql_id}" style="display:none;">{kql_escaped}</pre>
</div>"""


def _render_concentration_block(con: Dict, kql: Dict) -> str:
    kql_id      = "kql_concentration"
    kql_text    = kql.get('concentration', '')
    kql_escaped = kql_text.replace('`', '&#96;').replace('<', '&lt;').replace('>', '&gt;')

    if not con.get('available'):
        return f"""
<div style="background:#F8FAFC;border:1px dashed #CBD5E1;border-radius:12px;padding:18px 20px;">
  <div style="font-size:12px;font-weight:600;color:#94A3B8;">CUSTOMER CONCENTRATION</div>
  <div style="font-size:13px;color:#94A3B8;margin-top:12px;">No org data available</div>
</div>"""

    top_pct  = con['top_pct']
    top3_pct = con['top3_pct']
    top_name = con['top_name']

    risk_level = "critical" if top_pct > 50 else ("warning" if top_pct > 30 else "healthy")
    risk_colors = {
        "critical": ("#FEE2E2", "#EF4444", "#991B1B"),
        "warning":  ("#FEF3C7", "#F59E0B", "#92400E"),
        "healthy":  ("#D1FAE5", "#10B981", "#065F46"),
    }
    bg, accent, txt = risk_colors[risk_level]

    risk_note = {
        "critical": "⚠️ High concentration — if this customer goes quiet, numbers drop significantly",
        "warning":  "Monitor: top customer has significant share",
        "healthy":  "✓ Healthy spread across customers",
    }[risk_level]

    info = (
        "Based on event share over the profile period. "
        "High concentration is not necessarily bad — a large enterprise customer should dominate. "
        "Context depends on your customer mix."
    ).replace('"', '&quot;')

    return f"""
<div style="background:#FFFFFF;border:1px solid #E2E8F0;border-radius:12px;padding:18px 20px;">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;">
    <div style="font-size:10px;font-weight:700;color:#94A3B8;
                text-transform:uppercase;letter-spacing:1.2px;">
      Customer Concentration
    </div>
    <div style="display:flex;gap:6px;">
      <span title="{info}"
            style="font-size:12px;color:#94A3B8;cursor:help;
                   border:1px solid #E2E8F0;border-radius:50%;
                   width:16px;height:16px;display:inline-flex;
                   align-items:center;justify-content:center;">ⓘ</span>
      <span data-copy="{kql_id}" onclick="copyKQL('{kql_id}')"
            style="font-size:12px;color:#94A3B8;cursor:pointer;"
            title="Copy KQL">📋</span>
    </div>
  </div>

  <div style="background:{bg};border-radius:8px;padding:12px 14px;margin-bottom:12px;">
    <div style="font-size:13px;font-weight:600;color:{txt};margin-bottom:2px;">
      {top_name}
    </div>
    <div style="font-size:28px;font-weight:800;color:{accent};letter-spacing:-1px;">
      {top_pct:.0f}%
    </div>
    <div style="font-size:11px;color:{txt};margin-top:2px;">
      of all events · top customer
    </div>
  </div>

  <div style="font-size:12px;color:#64748B;margin-bottom:10px;">
    Top 3 customers combined: <strong style="color:#0F172A;">{top3_pct:.0f}%</strong>
  </div>

  <div style="font-size:12px;color:{txt};background:{bg};
              border-radius:6px;padding:8px 10px;line-height:1.5;">
    {risk_note}
  </div>

  <pre id="{kql_id}" style="display:none;">{kql_escaped}</pre>
</div>"""


def _render_at_risk_block(ar: Dict, kql: Dict) -> str:
    kql_id      = "kql_at_risk_list"
    kql_text    = kql.get('at_risk', '')
    kql_escaped = kql_text.replace('`', '&#96;').replace('<', '&lt;').replace('>', '&gt;')
    info        = ar.get('caveat', '').replace('"', '&quot;').replace("'", "&#39;")

    rows = ""
    for o in ar['orgs']:
        rows += f"""
<div style="display:flex;justify-content:space-between;align-items:center;
            padding:10px 0;border-bottom:1px solid #F1F5F9;">
  <div>
    <div style="font-size:13px;font-weight:600;color:#0F172A;
                font-family:'SF Mono',Monaco,'Courier New',monospace;
                font-size:12px;">
      {o['id']}
    </div>
    <div style="font-size:11px;color:#94A3B8;margin-top:2px;">
      Last active: {o['last_seen']}
    </div>
  </div>
  <div style="text-align:right;">
    <div style="font-size:13px;font-weight:600;color:#64748B;">{o['events']}</div>
    <div style="font-size:10px;color:#94A3B8;">events (period)</div>
  </div>
</div>"""

    more_note = ""
    if ar['value'] > len(ar['orgs']):
        more_note = (
            f'<div style="font-size:12px;color:#94A3B8;margin-top:10px;">'
            f'+ {ar["value"] - len(ar["orgs"])} more — run the KQL query for full list</div>'
        )

    return f"""
<div style="background:#FFFFFF;border:1px solid #FEE2E2;border-radius:12px;
            padding:18px 20px;margin-bottom:20px;">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;">
    <div>
      <div style="font-size:10px;font-weight:700;color:#EF4444;
                  text-transform:uppercase;letter-spacing:1.2px;">
        At-Risk Customers
      </div>
      <div style="font-size:12px;color:#64748B;margin-top:2px;">
        Active previously · silent last 7 days · hand to customer success
      </div>
    </div>
    <div style="display:flex;gap:6px;">
      <span title="{info}"
            style="font-size:12px;color:#94A3B8;cursor:help;
                   border:1px solid #E2E8F0;border-radius:50%;
                   width:16px;height:16px;display:inline-flex;
                   align-items:center;justify-content:center;">ⓘ</span>
      <span data-copy="{kql_id}" onclick="copyKQL('{kql_id}')"
            style="font-size:12px;color:#94A3B8;cursor:pointer;"
            title="Copy KQL">📋</span>
    </div>
  </div>
  {rows}
  {more_note}
  <pre id="{kql_id}" style="display:none;">{kql_escaped}</pre>
</div>"""


# ─────────────────────────────────────────────────────────────────────────────
# Plain-text fallback
# ─────────────────────────────────────────────────────────────────────────────

def _render_text(m: Dict) -> str:
    lines = []

    ao = m['active_orgs']
    if ao['available']:
        delta_str = ""
        if ao['delta'] is not None:
            sign = "+" if ao['delta'] > 0 else "-"
            delta_str = f" ({sign}{abs(int(ao['delta']))} vs last week)"
        lines.append(f"Active orgs: {ao['value']:,}{delta_str}")

    ar = m['at_risk']
    if ar['available'] and ar['value'] > 0:
        lines.append(f"At-risk customers: {ar['value']} orgs gone quiet in last 7 days")

    de = m['depth']
    if de['available']:
        lines.append(f"Usage depth: {_fmt(de['value'])} events per active org")

    reg = m['region']
    if reg['available'] and reg['regions']:
        top = reg['regions'][0]
        lines.append(f"Top region: {top['name']} — {_fmt(top['val'])} active orgs ({top['pct']:.0f}%)")

    return "\n".join(lines) if lines else "Business overview not available — profile data loading."


# ─────────────────────────────────────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────────────────────────────────────

def _sanitize(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """Sanitize date columns using insight_cards logic."""
    if df is None or df.empty:
        return df
    try:
        from .insight_cards import _sanitize_dates
        return _sanitize_dates(df)
    except Exception:
        return df


def _col(df: pd.DataFrame, candidates: list) -> Optional[str]:
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


def _fmt_date(val) -> str:
    """Format a date value cleanly: 'Feb 10, 2025'"""
    try:
        ts = pd.Timestamp(val)
        if ts.year > __import__('datetime').datetime.now().year + 1 or ts.year < 2000:
            return "unknown date"
        now = datetime.now()
        if ts.year == now.year:
            return ts.strftime('%b %d')
        return ts.strftime('%b %d, %Y')
    except Exception:
        return str(val)[:10]


def _fmt_date_range(start: pd.Timestamp, end: pd.Timestamp) -> str:
    """Format a date range cleanly: 'Jan 24 – Feb 23, 2025'"""
    try:
        if start.year == end.year:
            if start.month == end.month:
                return f"{start.strftime('%b %d')} – {end.strftime('%d, %Y')}"
            return f"{start.strftime('%b %d')} – {end.strftime('%b %d, %Y')}"
        return f"{start.strftime('%b %d, %Y')} – {end.strftime('%b %d, %Y')}"
    except Exception:
        return "—"


def _parse_days(scope: str) -> int:
    try:
        return int(str(scope).lower().replace('d', '').strip())
    except Exception:
        return 30
