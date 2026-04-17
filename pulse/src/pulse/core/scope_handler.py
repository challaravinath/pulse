"""
Scope Handler — Beyond-scope detection, at-risk queries, KQL builders.
═══════════════════════════════════════════════════════════════════════
Handles:
  - Questions >180 days → KQL clipboard handoff
  - At-risk customer detection (silent orgs)
  - Building live KQL equivalents for profile queries
  - Drop investigation KQL

Extracted from ai_orchestrator.py (iter 11.0 god-file refactor).
"""

import logging
import re
from typing import Dict, Optional
import pandas as pd

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


class ScopeHandler:
    """
    Handles beyond-scope detection (>180 days), at-risk customer queries,
    and KQL generation for profile query transparency.
    """

    def __init__(self, orch):
        self.o = orch

    # ═══════════════════════════════════════════════════════════════════════
    # Beyond-scope detection
    # ═══════════════════════════════════════════════════════════════════════

    def detect_beyond_scope(self, question: str) -> Optional[int]:
        """
        Detect if the question implies a time window beyond 180 days.
        Returns the requested days if beyond scope, None otherwise.

        ★ iter13.1: Calculates ACTUAL date range instead of keyword→fixed days.
        "this year" in Feb = ~57 days (IN scope).
        "this year" in Aug = ~240 days (beyond scope).
        "in 2025" = within scope if recent enough.
        """
        from datetime import date
        q = question.lower()
        SCOPE_LIMIT = 180
        today = date.today()

        # ── Try the smart date range approach first ──────────────────────
        # Import the fast router's method if available
        try:
            from pulse.core.fast_router import FastRouter
            _fr = FastRouter.__new__(FastRouter)
            date_range = _fr._extract_date_range(q)
            if date_range:
                start, end = date_range
                # Clamp end to today
                end = min(end, today)
                # If the requested range overlaps with our data window,
                # let it through — the SQL will naturally filter to available data
                data_start = today - __import__('datetime').timedelta(days=SCOPE_LIMIT)
                if end >= data_start:
                    return None  # Overlaps with our data — answer with what we have
                # Entire range is before our data window
                return (today - start).days
        except Exception:
            pass

        # ── Fallback: explicit large ranges only ─────────────────────────
        day_match = re.search(r'([0-9]+)\s*day', q)
        if day_match and int(day_match.group(1)) > SCOPE_LIMIT:
            return int(day_match.group(1))

        week_match = re.search(r'([0-9]+)\s*week', q)
        if week_match and int(week_match.group(1)) > (SCOPE_LIMIT // 7):
            return int(week_match.group(1)) * 7

        # Only block unambiguously large ranges
        large_ranges = {
            '12 month': 365, 'last year': 365, 'past year': 365,
            'full year': 365, '10 month': 300, '11 month': 330,
        }
        for phrase, days in large_ranges.items():
            if phrase in q:
                return days

        return None

    def build_beyond_scope_response(self, question: str, days: int) -> Dict:
        """Build a helpful response for queries beyond 180-day scope."""
        table = 'YOUR_TABLE'
        time_col = 'EventInfo_Time'
        org_col = 'OrgId'
        geo_col = 'GeoName'
        clusters = []

        try:
            if hasattr(self.o, 'kusto_client') and self.o.kusto_client:
                kc = self.o.kusto_client
                if hasattr(kc, 'config') and kc.config:
                    cfg = kc.config
                    for cl in getattr(cfg, 'clusters', []):
                        clusters.append({
                            'url': cl.url, 'database': cl.database, 'table': cl.table,
                        })
                        table = cl.table
            if hasattr(self.o, 'semantic_layer') and self.o.semantic_layer:
                sl = self.o.semantic_layer
                if sl.model:
                    if sl.model.time_column:
                        time_col = sl.model.time_column
                    for did, dim in (sl.model.dimensions or {}).items():
                        if did in ('organization', 'org', 'tenant'):
                            org_col = dim.column
                        elif did in ('region', 'geo', 'geography'):
                            geo_col = dim.column
        except Exception as e:
            logger.debug(f"Config extract failed: {e}")

        def _cluster_kql(cl):
            return f"cluster('{cl['url']}').database('{cl['database']}').{cl['table']}"

        if len(clusters) > 1:
            union_sources = ",\n  ".join(_cluster_kql(cl) for cl in clusters)
            from_clause = f"union\n  {union_sources}"
        elif clusters:
            from_clause = _cluster_kql(clusters[0])
        else:
            from_clause = table

        q = question.lower()
        is_org_query = any(w in q for w in ['org', 'customer', 'tenant', 'company'])
        is_region = any(w in q for w in ['region', 'geo', 'emea', 'na', 'apac'])

        if is_org_query:
            kql = (
                f"// {question}\n// Copy and run in Azure Data Explorer\n"
                f"{from_clause}\n| where {time_col} > ago({days}d)\n"
                f"| summarize events     = count(),\n"
                f"           active_orgs = dcount({org_col})\n"
                f"         by Day = bin({time_col}, 1d)\n| order by Day asc"
            )
        elif is_region:
            kql = (
                f"// {question}\n// Copy and run in Azure Data Explorer\n"
                f"{from_clause}\n| where {time_col} > ago({days}d)\n"
                f"| summarize events     = count(),\n"
                f"           active_orgs = dcount({org_col})\n"
                f"         by {geo_col}\n| order by active_orgs desc"
            )
        else:
            kql = (
                f"// {question}\n// Copy and run in Azure Data Explorer\n"
                f"{from_clause}\n| where {time_col} > ago({days}d)\n"
                f"| summarize events     = count(),\n"
                f"           active_orgs = dcount({org_col})\n"
                f"         by Day = bin({time_col}, 1d)\n| order by Day asc"
            )

        months = round(days / 30)
        period_str = f"{months} months" if months > 1 else f"{days} days"

        message = (
            f"PULSE maintains a **180-day** analytical profile — covering 6 months of data.\n\n"
            f"For a **{period_str}** view, here's the exact KQL ready to run in your cluster:\n\n"
            f"```kql\n{kql}\n```\n\n"
            f"Open [Azure Data Explorer](https://dataexplorer.azure.com), "
            f"paste the query, and run. "
            f"The query is pre-configured with your cluster, database, and table."
        )

        return {
            'message': message, 'kql_snippet': kql, 'days': days, 'type': 'beyond_scope',
        }

    # ═══════════════════════════════════════════════════════════════════════
    # At-risk customer handler
    # ═══════════════════════════════════════════════════════════════════════

    def handle_at_risk_query(self, message: str) -> Optional[Dict]:
        """At-risk handler: orgs active before but silent >7 days."""
        import time as _time
        start = _time.time()

        if not self.o.data_profile:
            return None

        if not self.o.data_profile.has_table('profile_organization'):
            n_tables = len(self.o.data_profile.tables) if self.o.data_profile else 0
            hint = ""
            try:
                instant = self.o.data_profile.query_safe(
                    "SELECT * FROM profile_organization ORDER BY events DESC LIMIT 5")
                if instant is not None and not instant.empty:
                    id_col = next((c for c in instant.columns
                                   if c.lower() in ("orgid", "org_id", "organizationid")), None)
                    if id_col:
                        names = []
                        for _, row in instant.head(3).iterrows():
                            oid = str(row[id_col])
                            if self.o.enricher and self.o.enricher.is_loaded:
                                try:
                                    r = self.o.enricher.resolve(oid)
                                    if r and r.org_name:
                                        oid = r.display_name
                                except Exception:
                                    pass
                            names.append(oid[:20] if len(oid) > 20 else oid)
                        if names:
                            hint = (
                                f"\n\nFrom the 7-day snapshot I can see "
                                f"{', '.join(names)} are among the most active — "
                                f"but I need the full profile to identify which ones have gone quiet."
                            )
            except Exception:
                pass

            return {
                'content': (
                    f"⏳ Still building the 90-day org-level profile "
                    f"({n_tables} tables ready so far).\n\n"
                    f"At-risk detection needs per-org last-seen dates which come "
                    f"from the full profile build. This usually takes 30–60 seconds "
                    f"after connect.{hint}\n\n"
                    f"Ask me again in a moment — or ask about trends and regions "
                    f"which are already available."
                ),
                'intent': 'at_risk_pending',
                'chart': None, 'dataframe': None,
                'source': 'profile', 'elapsed': 0.0,
                'follow_up_suggestions': [
                    '__chip_trending__', '__chip_top_customers__', '__chip_manager_summary__',
                ],
            }

        try:
            df = self.o.data_profile.query(
                "SELECT * FROM profile_organization ORDER BY last_seen ASC")
            if df is None or df.empty:
                return None

            ls_col = next((c for c in df.columns
                           if c.lower() in ('last_seen', 'last_event', 'lastseen')), None)
            fs_col = next((c for c in df.columns
                           if c.lower() in ('first_seen', 'first_event', 'firstseen')), None)
            id_col = next((c for c in df.columns
                           if c.lower() in ('orgid', 'org_id', 'organizationid')), None)
            ev_col = next((c for c in df.columns
                           if c.lower() in ('events', 'event_count')), None)

            if not ls_col or not id_col:
                return None

            ls = _safe_parse_dates(df[ls_col])
            if ls.dropna().empty:
                return None

            max_date = ls.dropna().max()
            cutoff_7d = max_date - pd.Timedelta(days=7)

            at_risk_mask = ls < cutoff_7d
            if fs_col:
                fs = _safe_parse_dates(df[fs_col])
                established_mask = fs < cutoff_7d
                at_risk_mask = at_risk_mask & established_mask

            at_risk_df = df[at_risk_mask].copy()
            if at_risk_df.empty:
                elapsed = _time.time() - start
                return {
                    'content': (
                        "✅ No at-risk customers right now — all active organisations "
                        "have had activity in the last 7 days."
                    ),
                    'intent': 'at_risk',
                    'chart': None, 'dataframe': None,
                    'source': 'profile', 'elapsed': elapsed,
                    'kql': self.build_at_risk_kql(),
                    'scope_label': 'Last 180 days',
                    'follow_up_suggestions': [
                        'Show top customers', 'Who are the most active orgs?', 'Regional adoption',
                    ],
                }

            display_cols = [id_col]
            if fs_col: display_cols.append(fs_col)
            display_cols.append(ls_col)
            if ev_col: display_cols.append(ev_col)

            result_df = at_risk_df[display_cols].copy()
            result_df[ls_col] = _safe_parse_dates(result_df[ls_col])
            if fs_col:
                result_df[fs_col] = _safe_parse_dates(result_df[fs_col])

            n = len(result_df)
            days_quiet = int((max_date - result_df[ls_col].min()).days)

            enricher = getattr(self.o, 'enricher', None)
            top_name = str(result_df.iloc[0][id_col])
            if enricher and enricher.is_loaded:
                try:
                    r = enricher.resolve(top_name)
                    if r and r.org_name:
                        top_name = r.display_name
                except Exception:
                    pass
            if len(top_name) > 20:
                top_name = top_name[:16] + "…"

            last_date = result_df.iloc[0][ls_col]
            last_str = last_date.strftime('%b %d') if pd.notna(last_date) else "unknown"

            plural = "s" if n != 1 else ""
            content = (
                f"**{n} organisation{plural} at risk** — "
                "established customers with no activity in the last 7 days.\n\n"
                f"Longest gap: **{top_name}**, last active **{last_str}** "
                f"({days_quiet} days ago). "
                "Hand this list to customer success now."
            )

            elapsed = _time.time() - start
            return {
                'content': content, 'intent': 'at_risk',
                'chart': None, 'dataframe': result_df.to_dict('records'),
                'source': 'profile', 'elapsed': elapsed,
                'kql': self.build_at_risk_kql(),
                'scope_label': 'Last 180 days',
                'follow_up_suggestions': [
                    'Show top customers', "What is the overall trend?", 'Show usage depth',
                ],
            }
        except Exception as e:
            logger.warning(f"at_risk handler failed: {e}")
            return None

    # ═══════════════════════════════════════════════════════════════════════
    # KQL builders
    # ═══════════════════════════════════════════════════════════════════════

    def build_at_risk_kql(self) -> str:
        """Build the at-risk KQL with real cluster/table/column names."""
        table = 'YOUR_TABLE'
        time_col = 'EventInfo_Time'
        org_col = 'OrgId'
        try:
            if hasattr(self.o, 'kusto_client') and self.o.kusto_client:
                cfg = getattr(self.o.kusto_client, 'config', None)
                if cfg and cfg.clusters:
                    cl = cfg.clusters[0]
                    table = f"cluster('{cl.url}').database('{cl.database}').{cl.table}"
            if self.o.semantic_layer and self.o.semantic_layer.model:
                if self.o.semantic_layer.model.time_column:
                    time_col = self.o.semantic_layer.model.time_column
                for did, dim in (self.o.semantic_layer.model.dimensions or {}).items():
                    if did in ('organization', 'org', 'tenant'):
                        org_col = dim.column
        except Exception:
            pass

        return (
            f"// At-risk customers — established, silent last 7 days\n"
            f"// Paste into Azure Data Explorer and run\n"
            f"{table}\n"
            f"| summarize first_seen = min({time_col}),\n"
            f"           last_seen  = max({time_col}),\n"
            f"           events     = count()\n"
            f"         by {org_col}\n"
            f"| where first_seen < ago(7d)   // established org\n"
            f"| where last_seen  < ago(7d)   // gone quiet\n"
            f"| order by last_seen asc"
        )

    @staticmethod
    def drop_investigator_kql(table, time_col, org_col, geo_col):
        """KQL showing which orgs and regions drove a drop."""
        return (
            f"// Investigate event drop — compare last 7 days vs prior 7 days by org\n"
            f"let recent  = {table} | where {time_col} between (ago(7d) .. now()) "
            f"| summarize recent_events=count() by {org_col};\n"
            f"let prior   = {table} | where {time_col} between (ago(14d) .. ago(7d)) "
            f"| summarize prior_events=count() by {org_col};\n"
            f"prior\n"
            f"| join kind=fullouter recent on {org_col}\n"
            f"| extend delta = recent_events - prior_events,\n"
            f"         pct_change = round((todouble(recent_events - prior_events) "
            f"/ prior_events) * 100, 1)\n"
            f"| order by delta asc\n"
            f"| take 20\n\n"
            f"// By region\n"
            f"let recent_geo  = {table} | where {time_col} between (ago(7d) .. now()) "
            f"| summarize recent_events=count() by {geo_col};\n"
            f"let prior_geo   = {table} | where {time_col} between (ago(14d) .. ago(7d)) "
            f"| summarize prior_events=count() by {geo_col};\n"
            f"prior_geo\n"
            f"| join kind=fullouter recent_geo on {geo_col}\n"
            f"| extend delta = recent_events - prior_events\n"
            f"| order by delta asc"
        )

    def build_live_kql_for_profile(self, table: str, profile_sql: str, question: str) -> str:
        """Return the ACTUAL KQL that built this profile table."""
        try:
            kql_table = 'YOUR_TABLE'
            time_col = 'EventInfo_Time'

            if hasattr(self.o, 'kusto_client') and self.o.kusto_client:
                cfg = getattr(self.o.kusto_client, 'config', None)
                if cfg and cfg.clusters:
                    if len(cfg.clusters) > 1:
                        parts = [f"cluster('{c.url}').database('{c.database}').{c.table}"
                                 for c in cfg.clusters]
                        kql_table = "union " + ", ".join(parts)
                    else:
                        cl = cfg.clusters[0]
                        kql_table = f"cluster('{cl.url}').database('{cl.database}').{cl.table}"
            elif self.o.semantic_layer and self.o.semantic_layer.model:
                time_col = self.o.semantic_layer.model.time_column

            real_kql = None
            if self.o.data_profile and self.o.data_profile._all_queries:
                pq = self.o.data_profile._all_queries.get(table)
                if pq and hasattr(pq, 'kql') and pq.kql:
                    real_kql = pq.kql

            if not real_kql and '_x_' in table:
                base = table.split('_x_')[0]
                if self.o.data_profile and self.o.data_profile._all_queries:
                    pq = self.o.data_profile._all_queries.get(base)
                    if pq and hasattr(pq, 'kql') and pq.kql:
                        real_kql = pq.kql

            if real_kql:
                scope_days = getattr(
                    self.o.data_profile._all_queries.get(table), 'scope_days', 180
                ) if self.o.data_profile and self.o.data_profile._all_queries.get(table) else 180
                desc = table.replace('profile_', '').replace('_', ' ').title()
                return (
                    f"// {desc} — last {scope_days} days\n"
                    f"// Source: PULSE profile query for '{table}'\n"
                    f"{kql_table}\n{real_kql}"
                )

            _drop_words = ('drop', 'fell', 'decline', 'decrease', 'down', 'lower',
                           'why', 'issue', 'problem', 'wrong', 'happen')
            _is_drop_q = any(w in (question or '').lower() for w in _drop_words)
            if table == 'profile_daily' and _is_drop_q:
                org_col = 'OrgId'
                geo_col = 'GeoName'
                if self.o.semantic_layer and self.o.semantic_layer.model:
                    for did, dim in (self.o.semantic_layer.model.dimensions or {}).items():
                        if did in ('organization', 'org', 'tenant'):
                            org_col = dim.column
                        elif did in ('region', 'geo', 'geography'):
                            geo_col = dim.column
                return self.drop_investigator_kql(kql_table, time_col, org_col, geo_col)

            return (
                f"// {question}\n{kql_table}\n"
                f"| where {time_col} > ago(180d)\n"
                f"| summarize events = count() by Day = bin({time_col}, 1d)\n"
                f"| order by Day asc"
            )
        except Exception as e:
            logger.debug(f"build_live_kql_for_profile failed: {e}")
            return ""
