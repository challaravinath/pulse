"""
Profile Handler — Instant answers from pre-aggregated DuckDB data.
═══════════════════════════════════════════════════════════════════════
Three strategies tried in order:
  1. Fast router (zero LLM, specific patterns ~10ms)
  2. Universal resolver (zero LLM, dimension scoring ~10ms)
  3. Semantic LLM (profile SQL generation ~800ms)

Returns None only if all strategies fail → caller falls back to Kusto.

Extracted from ai_orchestrator.py (iter 11.0 god-file refactor).
"""

import logging
from typing import Dict, Optional
import pandas as pd

logger = logging.getLogger(__name__)


def _safe_parse_dates(s):
    """Inline date parser."""
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


class ProfileHandler:
    """
    Handles profile-based question answering.
    Tries fast router → universal resolver → semantic LLM before giving up.
    """

    def __init__(self, orch):
        self.o = orch

    def has_profile(self) -> bool:
        return (
            self.o.semantic_layer is not None
            and self.o.semantic_layer.is_loaded
            and self.o.data_profile is not None
            and self.o.data_profile.is_built
        )

    def try_exhaustively(self, message: str) -> Optional[Dict]:
        """
        Try ALL profile resolution strategies before giving up.
        Returns None only if all strategies fail or LLM says live_kusto.
        """
        if not self.o.data_profile:
            return None
        available = set(self.o.data_profile.list_tables())
        if not available:
            return None

        # Strategy 0: Dedicated at-risk handler
        q_lower = message.lower()
        _at_risk_kw = (
            'at risk', 'at-risk', "who's at risk", 'who is at risk',
            'gone quiet', 'gone silent', 'stopped using', 'no activity',
            'churned', 'churn', 'inactive', 'not active',
            'risk customers', 'risk orgs', 'risk org',
        )
        if any(kw in q_lower for kw in _at_risk_kw):
            _ar = self.o.scope_handler.handle_at_risk_query(message)
            if _ar is not None:
                return _ar

        # Strategy 1: specific pattern matching
        if self.o.fast_router:
            result = self.o.fast_router.resolve(message, available)
            if result:
                # ★ iter14.1: Pre-guard — impossible/action results
                if result.intent == 'impossible':
                    return self._honest_decline(result, message)
                if result.intent == 'action' and result.filter_value:
                    # Route to chip handler (e.g., manager summary)
                    return None  # Let orchestrator handle the chip

                logger.info(f"Strategy 1 (fast): {result.pattern}")
                outcome = self.execute_fast_result(result, message)
                if outcome is not None:
                    # ★ iter14.1: Update conversation anchor
                    self._update_anchor(result, outcome)
                    return outcome

        # Strategy 2: universal dimension-scoring resolver
        if self.o.fast_router:
            result = self.o.fast_router.universal_resolve(message, available)
            if result:
                logger.info(f"Strategy 2 (universal): {result.table} [{result.intent}]")
                outcome = self.execute_fast_result(result, message)
                if outcome is not None:
                    self._update_anchor(result, outcome)
                    return outcome

        # Strategy 3: LLM semantic resolution (profile SQL)
        result = self.handle_profile(message)
        return result  # None means LLM said live_kusto

    # ═══════════════════════════════════════════════════════════════════════
    # iter14.1: Honest routing helpers
    # ═══════════════════════════════════════════════════════════════════════

    def _honest_decline(self, result, message: str) -> Dict:
        """
        Build a helpful response when we KNOW we can't answer.
        Instead of silence or hallucination, explain what we DO have.
        """
        from . import analyst_voice as _av

        # Build a "here's what I CAN tell you" section
        available = []
        if 'profile_totals' in (self.o.data_profile.tables if self.o.data_profile else {}):
            try:
                row = self.o.duckdb.connection.execute(
                    "SELECT * FROM profile_totals LIMIT 1"
                ).fetchdf()
                if not row.empty:
                    for c in row.columns:
                        v = row.iloc[0][c]
                        if isinstance(v, (int, float)) and v > 0:
                            available.append(f"{c}: {_av.fmt_number(v)}")
            except Exception:
                pass

        alt = ""
        if available:
            alt = "\n\nHere's what your data does show:\n• " + "\n• ".join(available[:4])

        return {
            'intent': 'profile',
            'response_type': 'answer',
            'data': None,
            'message': result.explanation + alt,
            'visualization': None,
            'profile_sql': '',
            'suggestions': None,
            'follow_up_suggestions': [
                'Show the trend', 'Top 10 orgs', 'Health check'
            ],
        }

    def _update_anchor(self, result, outcome: Dict):
        """Update the fast router's conversation anchor after a successful answer."""
        if not self.o.fast_router or not hasattr(self.o.fast_router, 'update_anchor'):
            return
        try:
            df = outcome.get('data')
            cols = list(df.columns) if df is not None and hasattr(df, 'columns') else []
            self.o.fast_router.update_anchor(result, cols)
        except Exception:
            pass

    def execute_fast_result(self, result, message: str) -> Optional[Dict]:
        """Execute a FastResult against DuckDB and build the response dict."""
        import time as _time
        import traceback
        from .narrative_engine import generate_smart_insight
        from . import analyst_voice as _av
        start = _time.time()

        try:
            logger.info(f"Executing: [{result.pattern}] {result.sql[:100]}")

            if not self.o.data_profile.has_table(result.table):
                logger.info(f"Table '{result.table}' not built yet — trying next strategy")
                return None

            try:
                df = self.o.data_profile.query(result.sql)
            except Exception as sql_err:
                if result._fallback_sql:
                    logger.info(f"Primary SQL failed ({sql_err}), trying fallback")
                    df = self.o.data_profile.query(result._fallback_sql)
                else:
                    raise

            if df.empty and result._fallback_sql:
                logger.info(f"Exact match 0 rows for '{result.filter_value}', using fallback")
                try:
                    df = self.o.data_profile.query(result._fallback_sql)
                    if not df.empty:
                        from dataclasses import replace as _dc_replace
                        result = _dc_replace(
                            result,
                            explanation=f"All regions — '{result.filter_value}' not found as exact match",
                            _fallback_sql='',
                        )
                except Exception:
                    pass

            if df.empty:
                logger.info(f"0 rows from {result.table} — trying next strategy")
                return None

            elapsed_ms = (_time.time() - start) * 1000
            logger.info(f"DuckDB: {len(df)} rows in {elapsed_ms:.0f}ms [{result.pattern}]")

            # Parse date columns, coerce numeric
            _date_col_names = {'day', 'date', 'time', 'timestamp', 'eventinfo_time',
                               'first_seen', 'last_seen', 'first_event', 'last_event'}
            _today = pd.Timestamp.now().normalize()
            _min_valid = _today - pd.DateOffset(years=5)
            _max_valid = _today + pd.DateOffset(days=1)

            for col in df.columns:
                if col.lower() in _date_col_names:
                    if not pd.api.types.is_datetime64_any_dtype(df[col]):
                        try:
                            parsed = _safe_parse_dates(df[col])
                            valid = parsed[(parsed >= _min_valid) & (parsed <= _max_valid)]
                            if len(valid) >= len(df) * 0.5:
                                df[col] = parsed
                        except Exception:
                            pass
                    continue
                if not pd.api.types.is_numeric_dtype(df[col]):
                    try:
                        converted = pd.to_numeric(df[col], errors='coerce')
                        if converted.notna().sum() > len(df) * 0.5:
                            df[col] = converted
                    except Exception:
                        pass

            self.o.context.load_data(df, message, "Profile", kql=None)
            _skip_sync = result.table in ('profile_daily', 'profile_totals')
            if not _skip_sync:
                self.o._sync_to_telemetry(df)

            _has_entity_col = any(
                c.lower() in ('orgid', 'org_id', 'organizationid', 'orgsid')
                for c in df.columns
            )
            if _has_entity_col:
                df = self.o._enrich(df)
                display_df = self.o._enrich(df, for_display=True)
            else:
                display_df = df

            _clean_title = self.chart_title_for(result)
            _viz_intent = (
                'ranking_bottom'
                if result.intent == 'ranking' and 'ASC' in result.sql
                else result.intent
            )

            # ★ iter14.2: Schema-aware column hints — never let visualizer guess wrong
            _col_hint = self._column_hint_for(result.table, display_df)

            viz = self.o.visualizer.analyze_and_visualize(
                display_df, _clean_title, "", intent_hint=_viz_intent,
                columns_hint=_col_hint,
            )
            self.o.context.add_turn(
                message, 'profile',
                result_rows=len(df),
                result_columns=list(df.columns),
                visualization_type=viz.get('type'),
            )

            insight = self._build_insight(result, df, question=message)

            # AnalystVoice: mismatch check + proactive signal
            try:
                available_values = {}
                _ts_tables = {'profile_daily', 'profile_totals'}
                if (hasattr(self.o, 'data_profile') and self.o.data_profile
                        and result.table not in _ts_tables):
                    try:
                        _t = result.table
                        _av_df = self.o.data_profile.query(f"SELECT * FROM {_t} LIMIT 50")
                        if not _av_df.empty:
                            for _c in _av_df.columns:
                                if not pd.api.types.is_numeric_dtype(_av_df[_c]):
                                    available_values[_c] = _av_df[_c].astype(str).tolist()
                    except Exception:
                        pass

                history = self.o.context.get_recent_turns(5)
                insight = _av.apply(
                    original_question=message,
                    base_narrative=insight,
                    result=result,
                    df=df,
                    conversation_history=history,
                    available_values=available_values,
                )
            except Exception as _e:
                logger.debug(f"AnalystVoice skipped: {_e}")

            # ★ iter14.1: Append honest caveat if confidence < 1.0
            if hasattr(result, 'caveat') and result.caveat:
                insight = insight + f"\n\nℹ️ {result.caveat}" if insight else result.caveat

            _live_kql = self.o.scope_handler.build_live_kql_for_profile(
                result.table, result.sql, message)

            _scope_days = 180
            if self.o.data_profile and self.o.data_profile._table_scope_days:
                _scope_days = self.o.data_profile._table_scope_days.get(result.table, 180)

            return {
                'intent': 'profile',
                'response_type': 'data',
                'data': display_df,
                'kql': _live_kql,
                'profile_sql': result.sql,
                'visualization': viz,
                'message': insight,
                'suggestions': None,
                'scope_label': f'Last {_scope_days} days',
            }

        except Exception as e:
            logger.warning(f"execute_fast_result failed [{result.pattern}]: {e}")
            logger.debug(traceback.format_exc())
            return None

    def handle_fast_profile(self, message: str) -> Optional[Dict]:
        if not self.o.data_profile:
            return None
        available = set(self.o.data_profile.list_tables())
        if not available or not self.o.fast_router:
            return None
        r = self.o.fast_router.resolve(message, available)
        if r:
            return self.execute_fast_result(r, message)
        return None

    def handle_profile(self, message: str) -> Optional[Dict]:
        """LLM-based profile resolution (Strategy 3)."""
        try:
            if not self.o.data_profile or not self.o.semantic_layer:
                return None
            available = set(self.o.data_profile.list_tables())
            if not available:
                logger.info("Semantic: no profile tables built yet, skipping LLM call")
                return None

            summaries = self.o.data_profile.get_table_summaries()
            conv_ctx = self.o.context.format_context_for_llm()

            resolution = self.o.semantic_layer.resolve_question(
                question=message,
                llm_client=self.o.llm_service.client,
                llm_model=self.o.llm_service.model,
                profile_summaries=summaries,
                conversation_context=conv_ctx,
            )

            source = resolution.get('source', '')
            if source == 'live_kusto':
                logger.info(f"Semantic: needs live Kusto — {resolution.get('reason', '')}")
                return None

            sql = resolution.get('sql', '')
            table = resolution.get('table', '')
            explanation = resolution.get('explanation', '')

            if not sql:
                logger.warning("Semantic: returned profile but no SQL")
                return None

            if table and not self.o.data_profile.has_table(table):
                logger.info(f"Semantic: table '{table}' not built yet, skipping to Kusto")
                return None

            logger.info(f"Profile SQL: {sql[:120]}")
            try:
                df, audit = self.o.data_profile.validated_query(sql)
                if audit.get('corrections'):
                    logger.info(f"SQL auto-corrected: {audit['corrections']}")
            except ValueError as ve:
                logger.warning(f"Profile SQL validation failed: {ve}")
                return None

            if df.empty:
                logger.info("Profile returned 0 rows — falling back to Kusto")
                return None

            for col in df.columns:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    try:
                        converted = pd.to_numeric(df[col], errors='coerce')
                        if converted.notna().sum() > len(df) * 0.5:
                            df[col] = converted
                    except Exception:
                        pass

            self.o.context.load_data(df, message, "Profile", kql=None)
            self.o._sync_to_telemetry(df)

            df = self.o._enrich(df)
            display_df = self.o._enrich(df, for_display=True)

            _msg_lower = message.lower()
            _s3_intent = (
                'ranking_bottom' if any(w in _msg_lower for w in
                    ['bottom', 'lowest', 'least', 'worst', 'trailing'])
                else 'ranking' if any(w in _msg_lower for w in
                    ['top', 'most', 'highest', 'largest', 'best'])
                else ''
            )
            viz = self.o.visualizer.analyze_and_visualize(
                display_df, message, sql or "", intent_hint=_s3_intent,
                columns_hint=self._column_hint_for('', display_df))

            self.o.context.add_turn(
                message, 'profile',
                result_rows=len(df),
                result_columns=list(df.columns),
                visualization_type=viz.get('type'),
            )

            from .narrative_engine import generate_smart_insight
            q_lower = message.lower()
            intent_guess = 'lookup'
            if any(w in q_lower for w in ['top', 'bottom', 'highest', 'lowest', 'worst', 'best']):
                intent_guess = 'ranking'
            elif any(w in q_lower for w in ['trend', 'daily', 'over time', 'growth', 'week', 'month']):
                intent_guess = 'trend'
            elif any(w in q_lower for w in ['total', 'how many', 'count', 'number of']):
                intent_guess = 'total'
            elif any(w in q_lower for w in ['summary', 'overview', 'recap', 'health', 'worry', 'issue', 'anomal']):
                intent_guess = 'summary'

            msg = generate_smart_insight(
                display_df, intent_guess,
                explanation or f"{len(df):,} results",
                question=message,
            )

            staleness = self.o.data_profile.get_staleness_info() if self.o.data_profile else {}
            _source_kql = self.o.scope_handler.build_live_kql_for_profile(
                table or '', sql, message)

            return {
                'intent': 'profile',
                'response_type': 'data',
                'message': msg,
                'data': display_df,
                'visualization': viz,
                'kql': _source_kql,
                'profile_sql': sql,
                'suggestions': None,
                'staleness': staleness,
            }

        except Exception as e:
            logger.warning(f"Profile handler failed: {e}")
            return None

    # ── Helpers ──────────────────────────────────────────────────────────────

    # ═══════════════════════════════════════════════════════════════════════
    # iter14.2: Schema-aware column hints
    # ═══════════════════════════════════════════════════════════════════════

    # Known profile table → (value_candidates, label_candidates)
    # First match found in actual df.columns wins
    _COLUMN_MAP = {
        'profile_organization': {
            'value': ['events', 'event_count', 'total_events', 'active_orgs'],
            'label': ['organization', 'org_name', 'display_name', 'organizationname',
                       'orgname', 'orgid', 'org_id', 'organizationid', 'tenantid',
                       'tenant'],
        },
        'profile_region': {
            'value': ['events', 'event_count', 'total_events', 'active_orgs'],
            'label': ['geoname', 'region', 'geo', 'country', 'clientcountry'],
        },
        'profile_entity': {
            'value': ['events', 'event_count', 'total_events'],
            'label': ['entityname', 'entity', 'eventinfo_name', 'entity_type'],
        },
        'profile_browser': {
            'value': ['events', 'event_count', 'total_events'],
            'label': ['browsername', 'browser', 'client_browser'],
        },
        'profile_activity': {
            'value': ['events', 'event_count', 'total_events'],
            'label': ['eventinfo_name', 'activity', 'action', 'event_name'],
        },
        'profile_version': {
            'value': ['events', 'event_count', 'total_events'],
            'label': ['version', 'app_version', 'appversion', 'build'],
        },
    }

    @classmethod
    def _column_hint_for(cls, table: str, df) -> dict:
        """
        Return {'value': col, 'label': col} for known profile tables.
        Returns None if table is unknown or columns not found.
        """
        if not table or not hasattr(df, 'columns'):
            return None

        mapping = cls._COLUMN_MAP.get(table)
        if not mapping:
            return None

        cols_lower = {c.lower(): c for c in df.columns}

        val_col = None
        for candidate in mapping['value']:
            if candidate.lower() in cols_lower:
                val_col = cols_lower[candidate.lower()]
                break

        lbl_col = None
        for candidate in mapping['label']:
            if candidate.lower() in cols_lower:
                lbl_col = cols_lower[candidate.lower()]
                break

        if val_col and lbl_col:
            logger.info(f"Column hint for {table}: value={val_col}, label={lbl_col}")
            return {'value': val_col, 'label': lbl_col}

        return None

    @staticmethod
    def chart_title_for(result) -> str:
        """Clean, data-descriptive chart title based on table + intent."""
        _title_map = {
            'profile_daily': 'Daily Activity Trend',
            'profile_region': 'Events by Region',
            'profile_organization': 'Top Organizations by Events',
            'profile_activity': 'Activity Type Distribution',
            'profile_entity': 'Entity Breakdown',
            'profile_totals': 'Overall Metrics',
            'profile_browser': 'Browser Distribution',
            'profile_ingestion_lag': 'Ingestion Latency',
            'profile_engagement': 'Engagement Over Time',
            'profile_version': 'Version Distribution',
        }

        if result.table in _title_map:
            base = _title_map[result.table]
        elif '_x_' in result.table:
            parts = result.table.replace('profile_org_x_', '').replace('profile_', '')
            base = f"Organization × {parts.title()}"
        else:
            base = result.table.replace('profile_', '').replace('_', ' ').title()

        if result.intent == 'ranking' and 'ASC' in result.sql:
            base = base.replace('Top', 'Bottom')
            if 'Bottom' not in base:
                base = f"Bottom {base}"

        if result.filter_value:
            base = f"{base} — {result.filter_value}"

        return base

    def _build_insight(self, result, df, question: str = "") -> str:
        """Build narrative insight for a profile result."""
        from .narrative_engine import generate_smart_insight
        return generate_smart_insight(
            df, result.intent,
            result.explanation or f"{len(df):,} results",
            question=question,
        )
