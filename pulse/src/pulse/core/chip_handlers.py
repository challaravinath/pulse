"""
Chip Handlers — 4 purpose-built answer machines for quick-action chips.
═══════════════════════════════════════════════════════════════════════
Rules fetch data deterministically. LLM interprets. KQL attached.

Extracted from ai_orchestrator.py (iter 11.0 god-file refactor).
"""

import logging
from typing import Dict, Optional
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def _safe_parse_dates(s):
    """Inline date parser — avoids circular imports."""
    if s is None:
        return pd.Series([], dtype='datetime64[ns]')
    if not isinstance(s, pd.Series):
        s = pd.Series(s)
    if pd.api.types.is_datetime64_any_dtype(s):
        try:
            if hasattr(s.dt, 'tz') and s.dt.tz is not None:
                s = s.dt.tz_convert(None)
        except Exception:
            pass
        return s
    try:
        numeric = pd.to_numeric(s, errors='coerce')
        if numeric.notna().all():
            med = float(numeric.median())
            NET_EPOCH = 621355968000000000
            if med > 6e17:
                unix_ns = (numeric - NET_EPOCH) * 100
                return pd.to_datetime(unix_ns, unit='ns', errors='coerce')
    except Exception:
        pass
    return pd.to_datetime(s, errors='coerce')


def _fmt_big(n):
    """Format large numbers: 1.2B, 3.4M, 5.6K."""
    n = float(n)
    if n >= 1e9: return f"{n/1e9:.1f}B"
    if n >= 1e6: return f"{n/1e6:.1f}M"
    if n >= 1e3: return f"{n/1e3:.1f}K"
    return str(int(n))


class ChipHandler:
    """
    Handles the 4 quick-action chip types:
      - __chip_trending__
      - __chip_top_customers__
      - __chip_bottom_customers__
      - __chip_manager_summary__

    Each method: rules fetch data → compute stats → LLM writes one sentence → attach KQL.
    """

    def __init__(self, orch):
        """
        Args:
            orch: AIOrchestrator instance (provides data_profile, kusto_client,
                  semantic_layer, enricher, _enrich, _llm_interpret)
        """
        self.o = orch

    # ── KQL templates ───────────────────────────────────────────────────────

    def chip_kql(self, query_type: str) -> str:
        """Return ready-to-run KQL for each chip type."""
        try:
            cl  = self.o.kusto_client.config.clusters[0]
            tbl = cl.table
            src = f"cluster('{cl.url}').database('{cl.database}').{tbl}"
            sl  = self.o.semantic_layer.model if self.o.semantic_layer else None
            t   = sl.time_column if sl else 'EventInfo_Time'
            o   = next((d.column for did, d in (sl.dimensions or {}).items()
                        if did == 'organization'), 'OrgId') if sl else 'OrgId'
            g   = next((d.column for did, d in (sl.dimensions or {}).items()
                        if did in ('region', 'geo')), 'GeoName') if sl else 'GeoName'
        except Exception:
            src, t, o, g = 'YOUR_TABLE', 'EventInfo_Time', 'OrgId', 'GeoName'

        kqls = {
            'trending': (
                f"// Active org trend — last 14 days\n"
                f"{src}\n"
                f"| where {t} > ago(14d)\n"
                f"| summarize active_orgs=dcount({o})\n"
                f"         by Day=bin({t}, 1d)\n"
                f"| order by Day asc"
            ),
            'top_customers': (
                f"// Top customers by activity — last 14 days\n"
                f"{src}\n"
                f"| where {t} > ago(14d)\n"
                f"| summarize events=count(),\n"
                f"           active_days=dcount(bin({t},1d))\n"
                f"         by {o}\n"
                f"| order by events desc\n"
                f"| take 8"
            ),
            'bottom_customers': (
                f"// Bottom customers by activity — last 14 days\n"
                f"{src}\n"
                f"| where {t} > ago(14d)\n"
                f"| summarize events=count(),\n"
                f"           active_days=dcount(bin({t},1d))\n"
                f"         by {o}\n"
                f"| order by events asc\n"
                f"| take 10"
            ),
            'manager_summary': (
                f"// Manager summary: top 3, bottom 3, anomaly check\n"
                f"{src}\n"
                f"| where {t} > ago(14d)\n"
                f"| summarize events=count() by {o}\n"
                f"| order by events desc"
            ),
        }
        return kqls.get(query_type, '')

    # ── Trending ────────────────────────────────────────────────────────────

    def handle_chip_trending(self) -> Optional[Dict]:
        """📈 How are we trending? — rules fetch, LLM writes the one honest sentence."""
        import time as _t
        start = _t.time()
        if not self.o.data_profile or not self.o.data_profile.has_table('profile_daily'):
            return None
        try:
            df = self.o.data_profile.query_safe(
                "SELECT * FROM profile_daily ORDER BY day ASC")
            if df is None or df.empty:
                return None

            t_col = next((c for c in df.columns
                          if c.lower() in ('day', 'date', 'eventinfo_time')), None)
            ao_col = next((c for c in df.columns
                           if c.lower() in ('active_orgs', 'active_organizations', 'dcount_orgid')), None)
            ev_col = next((c for c in df.columns
                           if c.lower() in ('events', 'event_count')), None)
            val_col = ao_col or ev_col
            if not t_col or not val_col:
                return None

            df = df.copy()
            df[t_col] = _safe_parse_dates(df[t_col])
            df[val_col] = pd.to_numeric(df[val_col], errors='coerce').fillna(0)
            df = df.dropna(subset=[t_col])
            _now = pd.Timestamp.now()
            df = df[(df[t_col] >= _now - pd.DateOffset(years=5)) &
                    (df[t_col] <= _now + pd.DateOffset(months=1))]
            df = df.sort_values(t_col)

            recent = df[val_col].tail(7).median()
            prior = df[val_col].iloc[max(0, len(df)-14):max(0, len(df)-7)].median()
            wow_pct = ((recent - prior) / prior * 100) if prior > 0 else 0
            wow_pct = max(-200, min(200, wow_pct))

            chart_data = {
                'x': [str(d.date()) for d in df[t_col]],
                'y': df[val_col].tolist(),
                'label': 'Active Orgs' if ao_col else 'Events',
            }

            direction = "up" if wow_pct > 3 else ("down" if wow_pct < -3 else "flat")
            llm_sentence = self.o._llm_interpret(
                system_prompt=(
                    "You are a concise data analyst. Write ONE sentence (max 20 words) "
                    "describing this trend. Be direct. No preamble. No 'Based on the data'. "
                    "Just the insight."
                ),
                user_content=(
                    f"14-day active org trend. Last 7d median: {recent:.0f}. "
                    f"Prior 7d median: {prior:.0f}. "
                    f"Week-on-week: {wow_pct:+.1f}% ({direction}). "
                    f"Write one honest sentence."
                ),
                max_tokens=60,
            )
            if not llm_sentence:
                sign = "+" if wow_pct > 0 else ""
                llm_sentence = (
                    f"Active orgs {'grew' if wow_pct > 3 else 'declined' if wow_pct < -3 else 'held steady'} "
                    f"{sign}{wow_pct:.0f}% week-on-week."
                )

            return {
                'content': llm_sentence,
                'intent': 'chip_trending',
                'chart_data': chart_data,
                'kql': self.chip_kql('trending'),
                'source': 'profile',
                'elapsed': _t.time() - start,
                'data_scope': 'Last 14 days',
                'follow_up_suggestions': [
                    '__chip_top_customers__',
                    '__chip_bottom_customers__',
                    '__chip_manager_summary__',
                ],
            }
        except Exception as e:
            logger.warning(f"chip_trending failed: {e}")
            return None

    # ── Top customers ───────────────────────────────────────────────────────

    def handle_chip_top_customers(self) -> Optional[Dict]:
        """🏆 Top customers — rules build chart, LLM spots concentration story."""
        import time as _t
        start = _t.time()
        if not self.o.data_profile or not self.o.data_profile.has_table('profile_organization'):
            return None
        try:
            df = self.o.data_profile.query_safe(
                "SELECT * FROM profile_organization ORDER BY events DESC LIMIT 8")
            if df is None or df.empty:
                return None

            id_col = next((c for c in df.columns
                           if c.lower() in ('orgid', 'org_id', 'organizationid')), None)
            ev_col = next((c for c in df.columns
                           if c.lower() in ('events', 'event_count')), None)
            if not id_col or not ev_col:
                return None

            df = df.copy()
            df[ev_col] = pd.to_numeric(df[ev_col], errors='coerce').fillna(0)
            total_events = df[ev_col].sum()

            display_df_top = self.o._enrich(df, for_display=True)
            name_col_top = "Organization" if "Organization" in display_df_top.columns else id_col

            names = []
            for idx, row in df.iterrows():
                disp_row = display_df_top.loc[idx]
                name = str(disp_row.get(name_col_top, row[id_col]))
                if len(name) > 36 and '-' in name:
                    name = name[:14] + "…"
                names.append(name)

            events_list = df[ev_col].tolist()
            top3_share = sum(events_list[:3]) / total_events * 100 if total_events > 0 else 0

            chart_data = {
                'names': names,
                'events': [_fmt_big(e) for e in events_list],
                'values': events_list,
                'pcts': [e / total_events * 100 if total_events > 0 else 0 for e in events_list],
            }

            llm_line = self.o._llm_interpret(
                system_prompt=(
                    "You are a concise customer analyst. Write ONE sentence (max 25 words). "
                    "Focus on concentration risk or health. No preamble."
                ),
                user_content=(
                    f"Top 8 customers account for all data shown. "
                    f"Top 3 combined share: {top3_share:.0f}% of total activity. "
                    f"Top customer: {names[0]} with {_fmt_big(events_list[0])} events. "
                    f"Is this healthy concentration or a risk?"
                ),
                max_tokens=60,
            )
            if not llm_line:
                llm_line = f"Top 3 customers account for {top3_share:.0f}% of all activity."

            return {
                'content': llm_line,
                'intent': 'chip_top_customers',
                'chart_data': chart_data,
                'kql': self.chip_kql('top_customers'),
                'source': 'profile',
                'elapsed': _t.time() - start,
                'data_scope': 'Last 14 days',
                'follow_up_suggestions': [
                    '__chip_trending__',
                    '__chip_bottom_customers__',
                    '__chip_manager_summary__',
                ],
            }
        except Exception as e:
            logger.warning(f"chip_top_customers failed: {e}")
            return None

    # ── Bottom customers ────────────────────────────────────────────────────

    def handle_chip_bottom_customers(self) -> Optional[Dict]:
        """⚠️ Bottom 10 — lowest activity, LLM suggests why."""
        import time as _t
        start = _t.time()
        if not self.o.data_profile or not self.o.data_profile.has_table('profile_organization'):
            return None
        _src = self.o.data_profile._table_sources.get('profile_organization', '')
        if _src == '⚡ instant':
            return None
        try:
            df = self.o.data_profile.query_safe(
                "SELECT * FROM profile_organization ORDER BY events ASC LIMIT 10")
            if df is None or df.empty:
                return None

            id_col = next((c for c in df.columns
                           if c.lower() in ('orgid', 'org_id', 'organizationid')), None)
            ev_col = next((c for c in df.columns
                           if c.lower() in ('events', 'event_count')), None)
            ls_col = next((c for c in df.columns
                           if c.lower() in ('last_seen', 'last_event', 'lastseen')), None)
            if not id_col or not ev_col:
                return None

            df = df.copy()
            df[ev_col] = pd.to_numeric(df[ev_col], errors='coerce').fillna(0)

            display_df = self.o._enrich(df, for_display=True)
            name_col = "Organization" if "Organization" in display_df.columns else id_col

            rows = []
            for idx, row in df.iterrows():
                disp_row = display_df.loc[idx]
                name = str(disp_row.get(name_col, row[id_col]))
                if len(name) > 36 and '-' in name:
                    name = name[:14] + "…"
                ev = float(row[ev_col])
                last = ""
                if ls_col and pd.notna(row.get(ls_col)):
                    try:
                        d = _safe_parse_dates(pd.Series([row[ls_col]])).iloc[0]
                        if pd.notna(d):
                            last = d.strftime('%b %d')
                    except Exception:
                        pass
                rows.append({'name': name, 'events': ev, 'last_seen': last})

            rows_summary = "; ".join(
                f"{r['name']} ({_fmt_big(r['events'])} events"
                + (f", last seen {r['last_seen']}" if r['last_seen'] else "") + ")"
                for r in rows[:5]
            )

            llm_line = self.o._llm_interpret(
                system_prompt=(
                    "You are a customer success analyst. Write ONE sentence (max 25 words). "
                    "These are the lowest-activity customers. Are they new, inactive, or at risk? "
                    "Be specific. No preamble."
                ),
                user_content=(
                    f"Bottom 10 customers by activity in last 14 days: {rows_summary}. "
                    f"What should the team know about these?"
                ),
                max_tokens=70,
            )
            if not llm_line:
                llm_line = f"{len(rows)} customers showing lowest activity — worth a check-in."

            return {
                'content': llm_line,
                'intent': 'chip_bottom_customers',
                'table_rows': rows,
                'kql': self.chip_kql('bottom_customers'),
                'source': 'profile',
                'elapsed': _t.time() - start,
                'data_scope': 'Last 14 days',
                'follow_up_suggestions': [
                    '__chip_top_customers__',
                    '__chip_trending__',
                    '__chip_manager_summary__',
                ],
            }
        except Exception as e:
            logger.warning(f"chip_bottom_customers failed: {e}")
            return None

    # ── Manager summary ─────────────────────────────────────────────────────

    def handle_chip_manager_summary(self) -> Optional[Dict]:
        """📋 Manager summary — top 3, bottom 3, anomaly. LLM writes bullets."""
        import time as _t
        import datetime
        start = _t.time()
        if not self.o.data_profile:
            return None
        try:
            has_org = self.o.data_profile.has_table('profile_organization')
            has_daily = self.o.data_profile.has_table('profile_daily')

            org_df = self.o.data_profile.query_safe(
                "SELECT * FROM profile_organization ORDER BY events DESC"
            ) if has_org else pd.DataFrame()

            daily_df = self.o.data_profile.query_safe(
                "SELECT * FROM profile_daily ORDER BY day ASC"
            ) if has_daily else pd.DataFrame()

            if org_df is None: org_df = pd.DataFrame()
            if daily_df is None: daily_df = pd.DataFrame()

            def _resolve_name(oid):
                name = str(oid)
                if self.o.enricher and self.o.enricher.is_loaded:
                    try:
                        r = self.o.enricher.resolve(name)
                        if r and r.org_name:
                            return r.display_name
                    except Exception:
                        pass
                return name[:18] + "…" if len(name) > 18 else name

            # ── Top 3
            top3_summary = "not available"
            id_col = ev_col = None
            if not org_df.empty:
                id_col = next((c for c in org_df.columns
                               if c.lower() in ('orgid', 'org_id', 'organizationid')), None)
                ev_col = next((c for c in org_df.columns
                               if c.lower() in ('events', 'event_count')), None)
                if id_col and ev_col:
                    org_df[ev_col] = pd.to_numeric(org_df[ev_col], errors='coerce').fillna(0)
                    top3 = org_df.nlargest(3, ev_col)
                    top3_items = [
                        f"{_resolve_name(r[id_col])} ({_fmt_big(r[ev_col])})"
                        for _, r in top3.iterrows()
                    ]
                    top3_summary = ", ".join(top3_items)

            # ── Bottom 3
            bottom3_summary = "not available"
            if not org_df.empty and id_col and ev_col:
                bot3 = org_df.nsmallest(3, ev_col)
                bot3_items = [
                    f"{_resolve_name(r[id_col])} ({_fmt_big(r[ev_col])})"
                    for _, r in bot3.iterrows()
                ]
                bottom3_summary = ", ".join(bot3_items)

            # ── Anomaly
            anomaly_summary = "no anomalies detected"
            if not daily_df.empty:
                t_col = next((c for c in daily_df.columns
                              if c.lower() in ('day', 'date', 'eventinfo_time')), None)
                val_col = next((c for c in daily_df.columns
                                if c.lower() in ('active_orgs', 'active_organizations',
                                                  'events', 'event_count')), None)
                if t_col and val_col:
                    daily_df = daily_df.copy()
                    daily_df[val_col] = pd.to_numeric(daily_df[val_col], errors='coerce').fillna(0)
                    daily_df[t_col] = _safe_parse_dates(daily_df[t_col])
                    daily_df = daily_df[
                        (daily_df[t_col].dt.year >= 2000) &
                        (daily_df[t_col].dt.year <= datetime.datetime.now().year + 1)
                    ]
                    vals = daily_df[val_col].values
                    mean = float(np.mean(vals))
                    std = float(np.std(vals))
                    if std > 0:
                        z_scores = (vals - mean) / std
                        spikes = daily_df[np.abs(z_scores) > 2.0]
                        if not spikes.empty:
                            spike_row = spikes.iloc[-1]
                            spike_val = float(spike_row[val_col])
                            try:
                                spike_date = spike_row[t_col].strftime('%b %d')
                            except Exception:
                                spike_date = "recently"
                            anomaly_summary = (
                                f"Unusual activity on {spike_date}: "
                                f"{_fmt_big(spike_val)} vs avg {_fmt_big(mean)} "
                                f"(z={z_scores[spikes.index.get_loc(spikes.index[-1])]:.1f})"
                            )

            # ── LLM writes three bullets
            llm_bullets = self.o._llm_interpret(
                system_prompt=(
                    "You are a product analyst preparing a manager briefing. "
                    "Write EXACTLY 3 bullet points. Each bullet: one sentence, plain English, "
                    "no jargon. Format as:\n• [bullet 1]\n• [bullet 2]\n• [bullet 3]\n"
                    "No preamble. No headers. Just the 3 bullets."
                ),
                user_content=(
                    f"14-day product summary:\n"
                    f"Top 3 customers: {top3_summary}\n"
                    f"Bottom 3 customers: {bottom3_summary}\n"
                    f"Anomaly signal: {anomaly_summary}\n\n"
                    f"Write 3 bullets a manager would want to read before a meeting."
                ),
                max_tokens=200,
            )
            if not llm_bullets:
                llm_bullets = (
                    f"• Top performers: {top3_summary}\n"
                    f"• Lowest activity: {bottom3_summary}\n"
                    f"• Signals: {anomaly_summary}"
                )

            return {
                'content': llm_bullets,
                'intent': 'chip_manager_summary',
                'chart': None,
                'kql': self.chip_kql('manager_summary'),
                'source': 'profile',
                'elapsed': _t.time() - start,
                'data_scope': 'Last 14 days',
                'follow_up_suggestions': [
                    '__chip_top_customers__',
                    '__chip_bottom_customers__',
                    '__chip_trending__',
                ],
            }
        except Exception as e:
            logger.warning(f"chip_manager_summary failed: {e}")
            return None
