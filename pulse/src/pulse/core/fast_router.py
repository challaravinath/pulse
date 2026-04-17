"""
Fast Router v3.0 — Universal Profile Query Compiler
=====================================================

v3.0 replaces the patchwork of specific patterns with a UNIVERSAL resolver.

Architecture:
  resolve()  ←── specific patterns (top N, trend, health, etc.)
      ↓ if none match
  universal_resolve()  ←── scores ALL profile tables against any question
      ↓ always returns a result if any profile table exists

Key design: instead of pattern-matching specific phrases and fixing each one
individually, the universal resolver scores every profile table against the
question using keyword signals, then builds appropriate SQL.

This means ANY question containing ANY dimension reference (or none at all)
gets served from DuckDB instead of hitting Kusto.

Author: PULSE Team
"""

import re
import logging
from typing import Optional, Dict, Tuple, List
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class FastResult:
    """Result from fast-path resolution."""
    table: str
    sql: str
    explanation: str
    pattern: str
    intent: str = ''
    filter_value: str = ''
    _fallback_sql: str = ''
    confidence: float = 1.0       # ★ iter14.1: 0.0–1.0, below 0.5 = add caveat
    caveat: str = ''              # ★ iter14.1: honest note when answer is approximate


class FastRouter:
    """
    Zero-LLM query router.  Two-stage resolution:

    Stage 1 — specific patterns (fast, precise):
      top_n, bottom_n, trend, health, summary, total, overview,
      per_dimension, compare, how_is_doing

    Stage 2 — universal resolver (catches everything else):
      Scores every profile table by relevance to the question.
      Builds appropriate SQL (ranking/lookup/trend/total).
      Never returns None if any profile table exists.
    """

    # ── Geo alias map ────────────────────────────────────────────────────────

    GEO_ALIASES = {
        'eu': 'EMEA', 'europe': 'EMEA', 'emea': 'EMEA', 'eur': 'EMEA', 'european': 'EMEA',
        'na': 'NA',   'nam': 'NAM',     'north america': 'NAM', 'us': 'NAM', 'usa': 'NAM',
        'apac': 'APAC', 'asia': 'APAC', 'pacific': 'APAC', 'ap': 'APAC', 'asia pacific': 'APAC',
        'row': 'ROW', 'rest of world': 'ROW', 'other': 'ROW',
        'latam': 'LATAM', 'latin america': 'LATAM',
        'worldwide': 'global', 'global': 'global',
    }

    # Table keyword signals for universal scoring
    _TABLE_SIGNALS: Dict[str, List[str]] = {
        'profile_region':       ['region', 'geo', 'geography', 'emea', 'apac', 'nam', 'na',
                                  'latam', 'row', 'eu', 'europe', 'asia', 'americas',
                                  'country', 'where', 'geoname', 'worldwide'],
        'profile_organization': ['org', 'organization', 'organisation', 'company', 'customer',
                                  'tenant', 'account', 'orgid', 'who', 'contributor'],
        'profile_entity':       ['entity', 'entities', 'feature', 'features', 'module', 'modules',
                                  'capability', 'function', 'table', 'object'],
        'profile_browser':      ['browser', 'chrome', 'edge', 'firefox', 'safari', 'ie',
                                  'internet explorer', 'chromium', 'opera'],
        'profile_country':      ['country', 'countries', 'nation', 'locale'],
        'profile_daily':        ['daily', 'day', 'date', 'trend', 'over time', 'growth',
                                  'timeline', 'per day', 'weekly', 'monthly', 'time series',
                                  'when', 'history', 'historical', 'past', 'recent'],
        'profile_totals':       ['total', 'overall', 'how many', 'count', 'all orgs',
                                  'all events', 'aggregate', 'grand', 'sum',
                                  'number of', 'overview', 'snapshot'],
        # ★ Enhanced profile tables
        'profile_activity':     ['what are users doing', 'what are people doing',
                                  'user actions', 'event type', 'event name', 'action',
                                  'actions', 'doing', 'activity', 'activities',
                                  'event breakdown', 'common actions', 'popular actions',
                                  'what actions', 'what events', 'which events',
                                  'eventinfo_name', 'event_name'],
        'profile_ingestion_lag': ['lag', 'ingestion', 'pipeline', 'delay', 'latency',
                                   'freshness', 'data delay', 'pipeline health',
                                   'ingestion lag', 'how fresh', 'stale'],
        'profile_engagement':   ['engagement', 'engaged', 'sessions per user',
                                  'session depth', 'user engagement', 'how engaged',
                                  'active users', 'session count', 'sessions per day',
                                  'users per day'],
        'profile_version':      ['version', 'app version', 'appversion', 'rollout',
                                  'adoption', 'which version', 'version adoption',
                                  'client version', 'release', 'build'],
    }

    def __init__(self, semantic_model):
        self.model = semantic_model
        self._dim_lookup: Dict[str, Tuple[str, str]] = {}
        self._metric_lookup: Dict[str, str] = {}
        self._browser_values = {
            'chrome', 'edge', 'firefox', 'safari', 'ie', 'opera',
            'internet explorer', 'chromium',
        }
        self._build_lookups()

    def _build_lookups(self):
        for dim_id, dim in self.model.dimensions.items():
            if dim_id == 'time':
                continue
            table_name = f"profile_{dim_id}"
            sql_col = dim.sql_column
            self._dim_lookup[dim_id] = (table_name, sql_col)
            for word in dim.display_name.lower().split():
                if len(word) > 2:
                    self._dim_lookup[word] = (table_name, sql_col)
            self._dim_lookup[dim.column.lower()] = (table_name, sql_col)
            aliases = {
                'organization': ['org', 'orgs', 'organizations', 'organisation'],
                'region':       ['regions', 'geo', 'geography', 'geoname'],
                'entity':       ['entities', 'entity_type', 'entity_types', 'features', 'feature'],
                'browser':      ['browsers'],
                'country':      ['countries', 'clientcountry'],
                'app_module':   ['modules', 'module', 'appmodule'],
                'activity':     ['activities', 'event_name', 'eventinfo_name', 'actions', 'action'],
                'version':      ['versions', 'appversion', 'app_version', 'clientversion'],
            }
            if dim_id in aliases:
                for alias in aliases[dim_id]:
                    self._dim_lookup[alias] = (table_name, sql_col)
        for m_id, metric in self.model.metrics.items():
            self._metric_lookup[m_id] = m_id
            for word in metric.display_name.lower().split():
                if len(word) > 2:
                    self._metric_lookup[word] = m_id
            metric_aliases = {
                'events':       ['event', 'count', 'total', 'activity'],
                'active_orgs':  ['orgs', 'org', 'organizations'],
                'active_users': ['users', 'user'],
                'sessions':     ['session'],
                'entity_types': ['entities', 'entity'],
            }
            if m_id in metric_aliases:
                for alias in metric_aliases[m_id]:
                    self._metric_lookup[alias] = m_id

    # ═══════════════════════════════════════════════════════════════════════════
    # Public API
    # ═══════════════════════════════════════════════════════════════════════════

    def resolve(self, question: str, available_tables: set) -> Optional[FastResult]:
        """
        Stage 1: specific pattern matching.
        Returns a FastResult if matched, None otherwise.
        Callers should then try universal_resolve() before giving up.
        """
        q = self._clean(question)

        result = (
            self._try_how_is_doing(q, available_tables)
            or self._try_top_n(q, available_tables)
            or self._try_bottom_n(q, available_tables)
            or self._try_per_dimension(q, available_tables)
            or self._try_time_filtered(q, available_tables)
            or self._try_trend(q, available_tables)
            or self._try_summary(q, available_tables)
            or self._try_health_check(q, available_tables)
            or self._try_total(q, available_tables)
            or self._try_overview(q, available_tables)
            or self._try_compare_dimensions(q, available_tables)
        )

        if result:
            logger.info(f"FastRouter stage1 [{result.pattern}]: {result.sql[:80]}")
        return result

    def universal_resolve(self, question: str, available_tables: set) -> Optional[FastResult]:
        """
        Stage 2: universal resolver — handles ANY question about profile data.

        Scores every available profile table against the question using keyword
        signals, then builds appropriate SQL based on detected intent.

        This is the architectural guarantee that no question about pre-aggregated
        data ever falls through to Kusto.
        """
        if not available_tables:
            return None

        q = self._clean(question)

        # ── Score every available table ──────────────────────────────────────
        scores: Dict[str, int] = {}
        for table in available_tables:
            if not table.startswith('profile_'):
                continue
            score = self._score_table(q, table)
            if score > 0:
                scores[table] = score

        if not scores:
            # No keyword signals matched — this question needs the LLM, not a guess.
            # Returning None here lets Stage 3 (semantic LLM) handle it properly.
            return None

        best_table = max(scores, key=scores.get)
        logger.info(f"Universal resolver: '{question[:50]}' → {best_table} (score={scores[best_table]})")

        # ── Extract intent components ────────────────────────────────────────
        n = self._extract_n(q)
        metric = self._resolve_metric(q) or 'events'
        is_top = any(w in q for w in [
            'top', 'most', 'highest', 'largest', 'biggest', 'leading', 'best',
            'ranking', 'rank', 'ranked', 'greatest',
        ])
        is_bottom = any(w in q for w in [
            'bottom', 'lowest', 'least', 'worst', 'smallest', 'trailing',
            'lowest', 'minimum', 'min',
        ])
        is_trend = best_table == 'profile_daily'
        is_total = best_table == 'profile_totals'

        # ── Build SQL ────────────────────────────────────────────────────────
        if is_total:
            sql = f"SELECT * FROM {best_table}"
            intent = 'total'
            explanation = "Overall metrics"
        elif is_trend:
            # ★ iter13.1: Try date range first
            date_range = self._extract_date_range(q)
            if date_range:
                start, end = date_range
                sql = self._date_range_sql(start, end)
                explanation = f"Activity ({start.strftime('%b %d')} – {end.strftime('%b %d, %Y')})"
            else:
                days = self._extract_days(q) or 30
                sql = (
                    f"SELECT * FROM (SELECT * FROM {best_table} ORDER BY day DESC LIMIT {days}) "
                    f"sub ORDER BY day ASC"
                )
                explanation = f"Activity trend ({days} days)"
            intent = 'trend'
        elif is_bottom:
            limit = n or 10
            sql = f"SELECT * FROM {best_table} ORDER BY {metric} ASC LIMIT {limit}"
            intent = 'ranking'
            explanation = f"Bottom {limit} by {metric}"
        elif is_top:
            limit = n or 10
            sql = f"SELECT * FROM {best_table} ORDER BY {metric} DESC LIMIT {limit}"
            intent = 'ranking'
            explanation = f"Top {limit} by {metric}"
        else:
            sql = f"SELECT * FROM {best_table} ORDER BY {metric} DESC"
            intent = 'lookup'
            explanation = f"All data from {best_table.replace('profile_', '')}"

        return FastResult(
            table=best_table, sql=sql, explanation=explanation,
            pattern='universal', intent=intent,
        )

    def _score_table(self, q: str, table: str) -> int:
        """Score a table's relevance to the question."""
        signals = self._TABLE_SIGNALS.get(table, [])
        score = sum(3 if ' ' in sig else 1 for sig in signals if sig in q)

        # Bonus: if the table's dimension name directly appears
        dim_id = table.replace('profile_', '')
        if dim_id in q:
            score += 5

        return score

    # ═══════════════════════════════════════════════════════════════════════════
    # Stage 1 patterns
    # ═══════════════════════════════════════════════════════════════════════════

    def _try_how_is_doing(self, q: str, tables: set) -> Optional[FastResult]:
        """
        Handles: howz eu doing, how is APAC, how's ROW performing,
                 how is org 12345, how is Chrome doing
        """
        q_norm = re.sub(r"\bhow[sz']?\b", "how is", q)
        q_norm = re.sub(r"\bwhat about\b", "how is", q_norm)
        q_norm = re.sub(r"\bwhat'?s (?:the status of|happening with|going on with)\b",
                        "how is", q_norm)

        m = re.match(
            r'how (?:is|are|has)\s+(.+?)(?:\s+(?:doing|performing|looking|going|trending|behav\w*))?\s*$',
            q_norm.strip()
        )
        if not m:
            m = re.match(r'how\s+(.+?)(?:\s+(?:doing|performing|looking|going))?\s*$', q.strip())
            if not m:
                return None

        value_raw = m.group(1).strip()
        skip_words = {'many', 'much', 'often', 'long', 'far', 'big', 'large',
                      'the', 'we', 'you', 'our', 'i', 'it'}
        if value_raw.lower() in skip_words:
            return None
        if value_raw.startswith('the '):
            value_raw = value_raw[4:].strip()

        value_lower = value_raw.lower()

        # Geo region
        if 'profile_region' in tables:
            canonical = self.GEO_ALIASES.get(value_lower)
            is_geo = (canonical is not None or value_lower in
                      {'emea', 'apac', 'nam', 'na', 'row', 'latam', 'global'}
                      or len(value_lower) <= 5)
            if is_geo:
                display = canonical or value_raw.upper()
                col = self._get_col('region')
                exact_sql = (
                    f"SELECT * FROM profile_region "
                    f"WHERE UPPER(CAST({col} AS VARCHAR)) = '{display}' "
                    f"ORDER BY events DESC"
                )
                return FastResult(
                    table='profile_region',
                    sql=exact_sql,
                    explanation=f"Performance of {display} vs all regions",
                    pattern='how_is_region', intent='dimension_health',
                    filter_value=display,
                    _fallback_sql="SELECT * FROM profile_region ORDER BY events DESC",
                )

        # Browser
        if 'profile_browser' in tables and value_lower in self._browser_values:
            col = self._get_col('browser') or 'deviceinfo_browsername'
            return FastResult(
                table='profile_browser',
                sql=f"SELECT * FROM profile_browser WHERE LOWER(CAST({col} AS VARCHAR)) LIKE '%{value_lower}%' ORDER BY events DESC",
                explanation=f"Activity for browser '{value_raw}'",
                pattern='how_is_browser', intent='dimension_health',
                filter_value=value_raw,
            )

        # Org GUID or org keyword
        if 'profile_organization' in tables:
            clean = re.sub(r'^org\s*', '', value_lower).strip()
            is_guid = bool(re.match(r'^[a-f0-9\-]{8,}$', value_lower))
            is_org = value_lower.startswith('org') or bool(re.match(r'^\d{4,}$', clean))
            if is_guid or is_org:
                lookup = clean if re.match(r'^\d+$', clean) else value_raw
                return FastResult(
                    table='profile_organization',
                    sql=f"SELECT * FROM profile_organization WHERE LOWER(CAST(orgid AS VARCHAR)) LIKE '%{lookup.lower()}%' ORDER BY events DESC",
                    explanation=f"Activity for org '{lookup}'",
                    pattern='how_is_org', intent='dimension_health',
                    filter_value=lookup,
                )

        # Entity
        if 'profile_entity' in tables and len(value_lower) > 3:
            col = self._get_col('entity') or 'entityname'
            return FastResult(
                table='profile_entity',
                sql=f"SELECT * FROM profile_entity WHERE LOWER(CAST({col} AS VARCHAR)) LIKE '%{value_lower}%' ORDER BY events DESC",
                explanation=f"Activity for entity '{value_raw}'",
                pattern='how_is_entity', intent='dimension_health',
                filter_value=value_raw,
            )

        return None

    def _try_top_n(self, q: str, tables: set) -> Optional[FastResult]:
        clean = re.sub(r'^(?:show\s+(?:me\s+)?|give\s+(?:me\s+)?|list\s+(?:the\s+)?)', '', q).strip()
        m = re.match(r'(?:the\s+)?top\s+(\d+)?\s*(.+?)(?:\s+by\s+(\w+))?\s*$', clean)
        if not m:
            return None
        n = int(m.group(1)) if m.group(1) else 10
        dim_text = (m.group(2) or '').strip()
        # ★ iter13: bare "top 10" → default to org table
        if not dim_text or dim_text in ('', 'all', 'everything'):
            if 'profile_organization' in tables:
                return FastResult(
                    table='profile_organization',
                    sql=f"SELECT * FROM profile_organization ORDER BY events DESC LIMIT {n}",
                    explanation=f"Top {n} organizations by events",
                    pattern=f"top_{n}", intent="ranking",
                )
        dim = self._find_dimension(dim_text, tables)
        if not dim:
            return None
        table, sql_col = dim
        order_col = self._resolve_metric(m.group(3)) if m.group(3) else 'events'
        return FastResult(
            table=table,
            sql=f"SELECT * FROM {table} ORDER BY {order_col} DESC LIMIT {n}",
            explanation=f"Top {n} by {order_col}",
            pattern=f"top_{n}", intent="ranking",
        )

    def _try_bottom_n(self, q: str, tables: set) -> Optional[FastResult]:
        m = re.match(
            r'(?:show\s+(?:me\s+)?)?(?:the\s+)?(?:bottom|lowest|least|worst)\s+(\d+)?\s*(.+?)(?:\s+by\s+(\w+))?\s*$',
            q
        )
        if not m:
            return None
        n = int(m.group(1)) if m.group(1) else 10
        dim = self._find_dimension(m.group(2).strip(), tables)
        if not dim:
            return None
        table, sql_col = dim
        order_col = self._resolve_metric(m.group(3)) if m.group(3) else 'events'
        return FastResult(
            table=table,
            sql=f"SELECT * FROM {table} ORDER BY {order_col} ASC LIMIT {n}",
            explanation=f"Bottom {n} by {order_col}",
            pattern=f"bottom_{n}", intent="ranking",
        )

    def _try_per_dimension(self, q: str, tables: set) -> Optional[FastResult]:
        """
        Handles: events per region, break down by org, now by entity,
                 show by browser, split by region, grouped by country,
                 break it/that down by X, drill down by X
        """
        clean = re.sub(r'^(?:show\s+(?:me\s+)?|give\s+(?:me\s+)?|get\s+(?:me\s+)?|list\s+)', '', q).strip()

        # break [it/that/them] down by X  |  drill down by X  |  drill into X
        m = re.match(
            r'(?:break\s+(?:it\s+|that\s+|them\s+)?down|drill\s+(?:down|into))\s+(?:by\s+)?(.+)',
            clean
        )
        if m:
            dim = self._find_dimension(m.group(1).strip(), tables)
            if dim:
                t, col = dim
                return FastResult(t, f"SELECT * FROM {t} ORDER BY events DESC",
                                  f"Breakdown by {col}", "break_down_by", "lookup")

        # now by X  |  now show X  |  split by X  |  group[ed] by X  |  segment by X
        m = re.match(
            r'(?:now\s+(?:by|show(?:\s+by)?)|split\s+by|grouped?\s+by|group\s+by|segment(?:ed)?\s+by|slice\s+by|pivot\s+(?:by|on))\s+(.+)',
            clean
        )
        if m:
            dim = self._find_dimension(m.group(1).strip(), tables)
            if dim:
                t, col = dim
                return FastResult(t, f"SELECT * FROM {t} ORDER BY events DESC",
                                  f"Grouped by {col}", "group_by", "lookup")

        # X per/by/for each Y
        m = re.match(r'(\w[\w\s]*?)\s+(?:per|by|for each|grouped by)\s+(.+)', clean)
        if m:
            dim = self._find_dimension(m.group(2).strip(), tables)
            if dim:
                t, col = dim
                return FastResult(t, f"SELECT * FROM {t} ORDER BY events DESC",
                                  f"All results by {col}", "per_dimension", "lookup")

        # X breakdown / X distribution / X split
        for pat in [
            r'(.+?)\s+(?:breakdown|distribution|split|by)',
            r'(?:all\s+)?(\w+)$',
            r'(.+?)\s+(?:data|stats|numbers|figures)',
        ]:
            m = re.match(pat, clean)
            if m:
                dim = self._find_dimension(m.group(1).strip(), tables)
                if dim:
                    t, col = dim
                    return FastResult(t, f"SELECT * FROM {t} ORDER BY events DESC",
                                      f"All results by {col}", "dimension_lookup", "lookup")

        return None

    def _try_total(self, q: str, tables: set) -> Optional[FastResult]:
        if 'profile_totals' not in tables:
            return None
        patterns = [
            r'how many\s+(.+)',
            r'(?:what(?:\'s|\s+is)\s+(?:the\s+)?)?total\s+(.+)',
            r'count\s+(?:of\s+)?(.+)',
            r'number\s+of\s+(.+)',
        ]
        for pat in patterns:
            m = re.match(pat, q)
            if m:
                target = m.group(1).strip().rstrip('?')
                metric = self._resolve_metric(target)
                if metric:
                    return FastResult(
                        'profile_totals',
                        f"SELECT total_{metric} FROM profile_totals",
                        f"Total {metric}", "total_metric", "total",
                    )
                dim = self._find_dimension(target, tables)
                if dim:
                    t, col = dim
                    return FastResult(
                        t, f"SELECT COUNT(*) as count FROM {t}",
                        f"Count of {col}", "total_dimension", "total",
                    )
        return None

    def _try_trend(self, q: str, tables: set) -> Optional[FastResult]:
        if 'profile_daily' not in tables:
            return None
        kw = ['trend', 'over time', 'daily', 'day by day', 'per day', 'per week',
              'per month', 'time series', 'timeline', 'growth', 'growing',
              'declining', 'increasing', 'decreasing', 'historical', 'history']
        if not any(k in q for k in kw):
            return None
        # ★ iter13.1: Try date range first, then day count
        date_range = self._extract_date_range(q)
        if date_range:
            start, end = date_range
            sql = self._date_range_sql(start, end)
            expl = f"Daily trend ({start.strftime('%b %d')} – {end.strftime('%b %d, %Y')})"
            return FastResult('profile_daily', sql, expl, "trend", "trend")
        days = self._extract_days(q)
        if days:
            sql = self._last_n_days_sql(days)
            expl = f"Daily trend (last {days} days)"
        else:
            sql = "SELECT * FROM profile_daily ORDER BY day ASC"
            expl = "Daily activity trend"
        return FastResult('profile_daily', sql, expl, "trend", "trend")

    def _try_summary(self, q: str, tables: set) -> Optional[FastResult]:
        if 'profile_daily' not in tables:
            return None
        if any(w in q for w in ['manager', 'email', 'report', 'presentation']):
            return None
        kw = ['summary', 'summarize', 'summarise', 'recap', 'what happened',
              'this week', 'last week', 'this month', 'last month',
              'recent activity', 'weekly', 'monthly']
        if not any(k in q for k in kw):
            return None
        days = self._extract_days(q) or 7
        return FastResult('profile_daily', self._last_n_days_sql(days),
                          f"Activity summary (last {days} days)", "summary", "summary")

    def _try_health_check(self, q: str, tables: set) -> Optional[FastResult]:
        if 'profile_daily' not in tables:
            return None
        kw = ['issue', 'issues', 'problem', 'problems', 'anomal', 'unusual',
              'wrong', 'weird', 'health', 'diagnos', 'drop', 'dropped',
              'spike', 'spiked', 'red flag', 'worry', 'worrying', 'concerned',
              'concern', 'alert', 'warning', 'bad', 'good', 'okay', 'ok',
              'how are we', 'how is it', 'how we doing', 'anything to',
              'what happened', 'what is happening', 'whats happening',
              'anything wrong', 'any problems', 'any issues', 'all good',
              'normal', 'abnormal', 'lately', 'recently', 'this week']
        if not any(k in q for k in kw):
            return None
        return FastResult('profile_daily', self._last_n_days_sql(14),
                          "Recent daily activity for issue detection", "health_check", "health")

    def _try_overview(self, q: str, tables: set) -> Optional[FastResult]:
        if 'profile_totals' not in tables:
            return None
        if any(s in q for s in ['manager', 'email', 'report', 'presentation', 'explain', 'why']):
            return None
        kw = ['overview', 'high level', 'big picture', 'snapshot', 'at a glance',
              'what data', 'what do you have', 'what metrics', 'what can you show',
              'say something', 'tell me something', 'show me something',
              'what can you tell', 'whats going on', "what's going on",
              'show me everything', 'show me all', 'give me a view', 'give me an idea']
        if any(k in q for k in kw):
            return FastResult('profile_totals', "SELECT * FROM profile_totals",
                              "High-level overview", "overview", "overview")
        return None

    def _try_compare_dimensions(self, q: str, tables: set) -> Optional[FastResult]:
        m = re.match(r'compare\s+(.+)', q)
        if not m:
            return None
        dim = self._find_dimension(m.group(1).strip(), tables)
        if dim:
            t, col = dim
            return FastResult(t, f"SELECT * FROM {t} ORDER BY events DESC",
                              f"Comparison across {col}", "compare", "compare")
        return None

    # ═══════════════════════════════════════════════════════════════════════════
    # Helpers
    # ═══════════════════════════════════════════════════════════════════════════

    def _clean(self, question: str) -> str:
        q = question.lower().strip().rstrip('?.!')
        q = re.sub(r'\b(please|plz|pls|thanks|thank you|can you|could you|would you)\b', '', q)
        return q.strip()

    def _find_dimension(self, text: str, available_tables: set) -> Optional[Tuple[str, str]]:
        text = text.lower().strip()
        if text in self._dim_lookup:
            table, col = self._dim_lookup[text]
            if table in available_tables:
                return table, col
        for word in text.split():
            word = word.strip('.,!?')
            if word in self._dim_lookup:
                table, col = self._dim_lookup[word]
                if table in available_tables:
                    return table, col
        for key, (table, col) in self._dim_lookup.items():
            if key in text and table in available_tables:
                return table, col
        return None

    def _get_col(self, dim_id: str) -> Optional[str]:
        if dim_id in self.model.dimensions:
            return self.model.dimensions[dim_id].sql_column
        return None

    def _resolve_metric(self, text: str) -> Optional[str]:
        if not text:
            return 'events'
        text = text.lower().strip()
        if text in self._metric_lookup:
            return self._metric_lookup[text]
        clean = text.replace('active ', 'active_').replace(' ', '_')
        if clean in self._metric_lookup:
            return self._metric_lookup[clean]
        for word in reversed(text.split()):
            if word in self._metric_lookup and word not in ('active', 'total', 'the', 'all'):
                return self._metric_lookup[word]
        return None

    def _extract_n(self, q: str) -> Optional[int]:
        m = re.search(r'\b(\d+)\b', q)
        if m:
            n = int(m.group(1))
            if 1 <= n <= 1000:
                return n
        return None

    def _extract_days(self, q: str) -> Optional[int]:
        m = re.search(r'(?:last|past)\s+(\d+)\s*(?:day|days)', q)
        if m:
            return int(m.group(1))
        m = re.search(r'(?:last|past)\s+(\d+)\s*(?:week|weeks)', q)
        if m:
            return int(m.group(1)) * 7
        if 'this week' in q or 'last week' in q:
            return 7
        if 'this month' in q or 'last month' in q:
            return 30
        m = re.search(r'(?:last|past)\s+(\d+)\s*(?:month|months)', q)
        if m:
            return int(m.group(1)) * 30
        return None

    @staticmethod
    def _last_n_days_sql(n: int) -> str:
        return (f"SELECT * FROM ("
                f"SELECT * FROM profile_daily ORDER BY day DESC LIMIT {n}"
                f") sub ORDER BY day ASC")

    # ═══════════════════════════════════════════════════════════════════════
    # ★ iter13.1: Comprehensive date range extraction
    # ═══════════════════════════════════════════════════════════════════════

    _MONTH_MAP = {
        'jan': 1, 'january': 1, 'feb': 2, 'february': 2,
        'mar': 3, 'march': 3, 'apr': 4, 'april': 4,
        'may': 5, 'jun': 6, 'june': 6, 'jul': 7, 'july': 7,
        'aug': 8, 'august': 8, 'sep': 9, 'september': 9, 'sept': 9,
        'oct': 10, 'october': 10, 'nov': 11, 'november': 11,
        'dec': 12, 'december': 12,
    }

    def _extract_date_range(self, q: str):
        """
        Extract a (start_date, end_date) tuple from temporal references.
        Returns None if no recognizable temporal pattern found.

        Handles:
          - "in 2025", "2026 data", "during 2025"
          - "how did 2026 start" → first 2 months of that year
          - "in January", "Jan data", "January 2025"
          - "Q1", "Q1 2025", "last quarter", "this quarter"
          - "year to date", "YTD", "this year"
          - "since October", "after November"
          - "before December", "until March"
          - "from Jan to March", "between Oct and Dec"
        """
        from datetime import date, timedelta
        import calendar

        today = date.today()
        current_year = today.year

        # ── "from X to Y" / "between X and Y" ───────────────────────────
        m = re.search(
            r'(?:from|between)\s+(\w+)\s+(?:to|and|through|thru|till|until)\s+(\w+)',
            q
        )
        if m:
            r1 = self._parse_single_time_ref(m.group(1), current_year)
            r2 = self._parse_single_time_ref(m.group(2), current_year)
            if r1 and r2:
                return (r1[0], r2[1])

        # ── "since X" / "after X" ───────────────────────────────────────
        m = re.search(r'(?:since|after|starting)\s+(\w+(?:\s+\d{4})?)', q)
        if m:
            ref = self._parse_single_time_ref(m.group(1).strip(), current_year)
            if ref:
                return (ref[0], today)

        # ── "before X" / "until X" ──────────────────────────────────────
        m = re.search(r'(?:before|until|up to|through|thru)\s+(\w+(?:\s+\d{4})?)', q)
        if m and 'from' not in q:  # avoid matching "from X to Y" again
            ref = self._parse_single_time_ref(m.group(1).strip(), current_year)
            if ref:
                return (date(current_year - 1, 1, 1), ref[1])

        # ── "how did YEAR start" / "beginning of YEAR" ──────────────────
        m = re.search(r'(?:how did|beginning of|start of)\s+(\d{4})\b', q)
        if m:
            yr = int(m.group(1))
            return (date(yr, 1, 1), date(yr, 2, 28))

        # ── "end of YEAR" ───────────────────────────────────────────────
        m = re.search(r'(?:end of|ending)\s+(\d{4})\b', q)
        if m:
            yr = int(m.group(1))
            return (date(yr, 10, 1), date(yr, 12, 31))

        # ── Quarter: "Q1", "Q1 2025", "this quarter", "last quarter" ───
        m = re.search(r'\bq([1-4])\s*(\d{4})?\b', q)
        if m:
            qn = int(m.group(1))
            yr = int(m.group(2)) if m.group(2) else current_year
            start_month = (qn - 1) * 3 + 1
            end_month = start_month + 2
            end_day = calendar.monthrange(yr, end_month)[1]
            return (date(yr, start_month, 1), date(yr, end_month, end_day))

        if 'this quarter' in q:
            qn = (today.month - 1) // 3 + 1
            start_month = (qn - 1) * 3 + 1
            end_month = start_month + 2
            end_day = calendar.monthrange(current_year, end_month)[1]
            return (date(current_year, start_month, 1),
                    min(today, date(current_year, end_month, end_day)))

        if 'last quarter' in q:
            qn = (today.month - 1) // 3 + 1
            prev_qn = qn - 1 if qn > 1 else 4
            yr = current_year if qn > 1 else current_year - 1
            start_month = (prev_qn - 1) * 3 + 1
            end_month = start_month + 2
            end_day = calendar.monthrange(yr, end_month)[1]
            return (date(yr, start_month, 1), date(yr, end_month, end_day))

        # ── "year to date" / "YTD" / "this year" ────────────────────────
        if any(p in q for p in ['year to date', 'ytd', 'this year']):
            return (date(current_year, 1, 1), today)

        # ── "Month YEAR" or "in Month" ──────────────────────────────────
        for month_name, month_num in self._MONTH_MAP.items():
            # "January 2025" or "in January 2025"
            m = re.search(rf'\b{month_name}\s+(\d{{4}})\b', q)
            if m:
                yr = int(m.group(1))
                end_day = calendar.monthrange(yr, month_num)[1]
                return (date(yr, month_num, 1), date(yr, month_num, end_day))
            # "in January" or "January data"
            if re.search(rf'\b(?:in\s+)?{month_name}\b', q):
                # Determine year: if the month is in the future this year,
                # assume last year
                yr = current_year
                if month_num > today.month:
                    yr = current_year - 1
                end_day = calendar.monthrange(yr, month_num)[1]
                return (date(yr, month_num, 1), date(yr, month_num, end_day))

        # ── Bare year: "in 2025", "2025 data", "during 2025" ────────────
        m = re.search(r'\b(20[0-9]{2})\b', q)
        if m:
            yr = int(m.group(1))
            # Distinguish "top 10" (has digits) from "year 2025"
            # Only match if the 4-digit number is actually a plausible year
            if 2020 <= yr <= current_year + 1:
                # Don't trigger on things like "top 10" where 10 isn't a year
                # Check the number isn't preceded by "top " or other count contexts
                pre = q[:m.start()].rstrip()
                if not re.search(r'(?:top|bottom|last|past|first|limit)\s*$', pre):
                    end = min(date(yr, 12, 31), today)
                    return (date(yr, 1, 1), end)

        return None

    def _parse_single_time_ref(self, text: str, current_year: int):
        """Parse a single time reference like 'January', 'Jan 2025', 'Q1', '2025'."""
        from datetime import date
        import calendar
        text = text.lower().strip()
        today = date.today()

        # Month name with optional year
        for mname, mnum in self._MONTH_MAP.items():
            m = re.match(rf'{mname}\s*(\d{{4}})?$', text)
            if m:
                yr = int(m.group(1)) if m.group(1) else current_year
                # If no explicit year and month is in the future, use last year
                if not m.group(1) and mnum > today.month:
                    yr = current_year - 1
                end_day = calendar.monthrange(yr, mnum)[1]
                return (date(yr, mnum, 1), date(yr, mnum, end_day))

        # Quarter
        m = re.match(r'q([1-4])\s*(\d{4})?$', text)
        if m:
            qn = int(m.group(1))
            yr = int(m.group(2)) if m.group(2) else current_year
            sm = (qn - 1) * 3 + 1
            em = sm + 2
            return (date(yr, sm, 1), date(yr, em, calendar.monthrange(yr, em)[1]))

        # Bare year
        m = re.match(r'(20\d{2})$', text)
        if m:
            yr = int(m.group(1))
            return (date(yr, 1, 1), date(yr, 12, 31))

        return None

    def _date_range_sql(self, start, end) -> str:
        """Build SQL for profile_daily with date range filter."""
        return (
            f"SELECT * FROM profile_daily "
            f"WHERE CAST(day AS DATE) >= '{start.isoformat()}' "
            f"AND CAST(day AS DATE) <= '{end.isoformat()}' "
            f"ORDER BY day ASC"
        )

    def _try_time_filtered(self, q: str, tables: set) -> Optional[FastResult]:
        """
        ★ iter13.1: Handle ANY time-scoped question about profile_daily.
        Catches: "in 2025", "how did 2026 start", "January data",
                 "Q1 trend", "since October", "this year", "YTD", etc.
        """
        if 'profile_daily' not in tables:
            return None

        date_range = self._extract_date_range(q)
        if not date_range:
            return None

        start, end = date_range
        sql = self._date_range_sql(start, end)

        # Build human-readable label
        from datetime import date
        if start.month == 1 and start.day == 1 and end.month == 12 and end.day == 31:
            label = f"{start.year}"
        elif start.month == 1 and start.day == 1 and end == date.today():
            label = f"{start.year} year to date"
        elif start.year == end.year and start.month == end.month:
            label = f"{start.strftime('%B %Y')}"
        else:
            label = f"{start.strftime('%b %d')} – {end.strftime('%b %d, %Y')}"

        return FastResult(
            table='profile_daily',
            sql=sql,
            explanation=f"Activity for {label}",
            pattern='time_filtered',
            intent='trend',
        )
