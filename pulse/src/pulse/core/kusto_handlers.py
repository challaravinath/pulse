"""
Kusto Handlers — Live Kusto query execution (fallback path).
═══════════════════════════════════════════════════════════════════════
Only reached when profile tables can't answer the question.
Handles: FETCH_DATA, DIRECT_KQL, COMPLEX_QUERY, ANALYZE_CACHE,
         REFINE_CACHE, streaming analysis, health checks.

Extracted from ai_orchestrator.py (iter 11.0 god-file refactor).
"""

import logging
from typing import Dict, Optional
import pandas as pd

from .query_planner import QueryPlanner

logger = logging.getLogger(__name__)


class KustoHandler:
    """Handles all live Kusto query paths and cached data analysis."""

    def __init__(self, orch):
        self.o = orch

    # ═══════════════════════════════════════════════════════════════════════
    # Live Kusto query handlers
    # ═══════════════════════════════════════════════════════════════════════

    def handle_fetch(self, message: str, intent_result: Dict) -> Dict:
        """Fetch new data from Kusto."""
        if not self.o.kusto_client:
            return self.o._error(
                intent_result['intent'].value,
                "⏳ Kusto is still connecting in the background. "
                "This usually takes 10–20 seconds after startup. "
                "Try again in a moment, or rephrase your question to use the pre-built profile data."
            )
        try:
            schema_ctx = self.o.kusto_client.get_schema_context()
            conv_ctx = self.o.context.format_context_for_llm()
            kql = self.o.llm_service.generate_kql_filter(message, schema_ctx, conv_ctx)
            return self.validate_execute_visualize(kql, message, intent_result['intent'].value)
        except ValueError as e:
            return self.o._error(intent_result['intent'].value, str(e))
        except Exception as e:
            logger.error(f"Fetch error: {e}", exc_info=True)
            return self.o._error(intent_result['intent'].value,
                                 self.o._friendly_error("fetch data", e))

    def handle_direct_kql(self, message: str) -> Dict:
        try:
            kql = self.o.kql_detector.extract_kql(message)
            return self.validate_execute_visualize(kql, message, 'direct_kql')
        except Exception as e:
            logger.error(f"Direct KQL error: {e}", exc_info=True)
            return self.o._error('direct_kql', self.o._friendly_error("run KQL", e))

    def handle_complex(self, message: str, intent_result: Dict) -> Dict:
        """Multi-step query planning."""
        intent_val = intent_result['intent'].value
        if not self.o.kusto_client:
            return self.o._error(
                intent_val,
                "⏳ Kusto is still connecting in the background. "
                "Complex queries need a live Kusto connection. Try again in a moment."
            )
        try:
            schema_ctx = self.o.kusto_client.get_schema_context()
            conv_ctx = self.o.context.format_context_for_llm()
            steps = self.o.query_planner.create_plan(message, schema_ctx, conv_ctx)

            if len(steps) == 1 and steps[0].step_type.value == "kql":
                return self.validate_execute_visualize(steps[0].query, message, intent_val)

            result = self.o.query_planner.execute_plan(steps, self.o.schema_validator)

            if not result.success or result.final_df is None or result.final_df.empty:
                error_msg = result.error or "The multi-step query produced no results."
                step_log = QueryPlanner.format_plan_log(result.steps_executed)
                return self.o._error(intent_val,
                    f"⚠️ {error_msg}\n\n**Plan:**\n{step_log}\n\nTry rephrasing.")

            df = result.final_df
            last_kql = None
            for s in result.steps_executed:
                if s['type'] == 'kql' and s['status'] in ('success', 'retried'):
                    last_kql = s.get('query')

            self.o.context.load_data(df, message, "Multi-step plan", kql=last_kql)
            self.o._sync_to_telemetry(df)

            df = self.o._enrich(df)
            display_df = self.o._enrich(df, for_display=True)

            viz = self.o.visualizer.analyze_and_visualize(display_df, message, "")

            step_log = QueryPlanner.format_plan_log(result.steps_executed)
            self.o.context.add_turn(message, intent_val, kql_query=last_kql,
                result_rows=len(df), result_columns=list(df.columns),
                visualization_type=viz.get('type'))

            return {
                'intent': intent_val, 'response_type': 'data',
                'data': df, 'kql': last_kql, 'visualization': viz,
                'message': (
                    f"Found **{len(df):,}** results "
                    f"({len(result.steps_executed)} steps, {result.total_time_ms:.0f}ms)."
                    f"\n\n**Plan:**\n{step_log}"
                ),
                'suggestions': None,
            }

        except ValueError as e:
            return self.o._error(intent_val, str(e))
        except Exception as e:
            logger.error(f"Complex query error: {e}", exc_info=True)
            try:
                return self.handle_fetch(message, intent_result)
            except Exception:
                return self.o._error(intent_val, self.o._friendly_error("answer this question", e))

    def handle_analyze(self, message: str, intent_result: Dict) -> Dict:
        """Analyse cached data with LLM narration."""
        intent_val = intent_result['intent'].value

        if not self.o.context.has_data():
            return self.o._conversational(intent_val,
                "Nothing to analyse yet — ask a question first! "
                "Try **\"Top 10 orgs by events\"**.")

        try:
            df = self.o.duckdb_engine.get_current_data()
            if df is None or df.empty:
                return self.o._conversational(intent_val,
                    "The cache is empty — fetch some data first.")

            analysis = self.o.analysis_engine.generate_insights(
                df, message, self.o.context.format_context_for_llm())

            parts = []
            if analysis.get('summary'):
                parts.append(analysis['summary'])
            if analysis.get('insights'):
                parts.append("")
                parts.append("### 💡 Key Insights")
                for ins in analysis['insights']:
                    parts.append(f"- {ins}")
            if analysis.get('recommendations'):
                parts.append("")
                parts.append("### 📋 Recommendations")
                for rec in analysis['recommendations']:
                    parts.append(f"- {rec}")

            df = self.o._enrich(df)
            display_df = self.o._enrich(df, for_display=True)

            viz = self.o.visualizer.analyze_and_visualize(display_df, message, "")
            self.o.context.add_turn(message, intent_val, result_rows=len(df),
                visualization_type=viz.get('type'))

            return {
                'intent': intent_val, 'response_type': 'analysis',
                'data': df, 'kql': None, 'visualization': viz,
                'message': "\n".join(parts), 'suggestions': None,
            }
        except Exception as e:
            logger.error(f"Analysis error: {e}", exc_info=True)
            return self.o._error(intent_val, self.o._friendly_error("analyse the data", e))

    def handle_refine(self, message: str, intent_result: Dict) -> Dict:
        """Refine cached data with DuckDB SQL."""
        intent_val = intent_result['intent'].value
        if not self.o.context.has_data():
            return self.handle_fetch(message, intent_result)

        try:
            cached_df = self.o.duckdb_engine.get_current_data()
            if cached_df is None or cached_df.empty:
                return self.handle_fetch(message, intent_result)

            if self._refine_needs_fresh_data(
                    message.lower(), [c.lower() for c in cached_df.columns]):
                return self.handle_fetch(message, intent_result)

            conv_ctx = self.o.context.format_context_for_llm()
            sql = self.o.llm_service.generate_sql_refinement(
                message, list(cached_df.columns), conv_ctx)
            df = self.o.duckdb_engine.execute_sql(sql)
            if df.empty:
                return self.handle_fetch(message, intent_result)

            self.o.context.load_data(df, message, "DuckDB (refine)", kql=None)

            df = self.o._enrich(df)
            display_df = self.o._enrich(df, for_display=True)

            viz = self.o.visualizer.analyze_and_visualize(display_df, message, sql)
            self.o.context.add_turn(message, intent_val, result_rows=len(df),
                result_columns=list(df.columns), visualization_type=viz.get('type'))

            return {
                'intent': intent_val, 'response_type': 'data',
                'data': df, 'kql': None, 'visualization': viz,
                'message': f"Refined to **{len(df):,}** results.",
                'suggestions': None,
            }
        except Exception as e:
            logger.info(f"DuckDB refine failed: {e} → Kusto fallback")
            return self.handle_fetch(message, intent_result)

    # ═══════════════════════════════════════════════════════════════════════
    # Validate → Execute → Visualise (shared Kusto path)
    # ═══════════════════════════════════════════════════════════════════════

    def validate_execute_visualize(
        self, kql: str, user_message: str, intent: str, _retry: bool = False
    ) -> Dict:
        if not self.o.kusto_client:
            return self.o._error(intent, "⏳ Kusto is still connecting. Try again in a moment.")

        if self.o.schema_validator:
            is_valid, error_msg, suggestions = self.o.schema_validator.validate_kql(kql)
            if not is_valid:
                corrected = self.o.schema_validator.suggest_corrections(kql)
                if corrected:
                    kql = corrected
                else:
                    return {
                        'intent': intent, 'response_type': 'error',
                        'data': None, 'kql': kql, 'visualization': None,
                        'message': error_msg, 'suggestions': suggestions,
                    }

        df = self.o.kusto_client.execute_query(kql)

        if not _retry and self._result_looks_wrong(df, user_message, kql):
            logger.info(f"Result looks wrong ({len(df)} rows). Retrying…")
            retry_kql = self._retry_kql(user_message, kql, df)
            if retry_kql and retry_kql != kql:
                return self.validate_execute_visualize(
                    retry_kql, user_message, intent, _retry=True)

        self.o.context.load_data(df, user_message, "Kusto", kql=kql)
        self.o._sync_to_telemetry(df)

        df = self.o._enrich(df)
        display_df = self.o._enrich(df, for_display=True)

        viz = self.o.visualizer.analyze_and_visualize(display_df, user_message, kql)
        self.o.context.add_turn(user_message, intent, kql_query=kql,
            result_rows=len(df), result_columns=list(df.columns),
            visualization_type=viz.get('type'))

        return {
            'intent': intent, 'response_type': 'data',
            'data': df, 'kql': kql, 'visualization': viz,
            'message': f"Found **{len(df):,}** results.",
            'suggestions': None,
        }

    # ═══════════════════════════════════════════════════════════════════════
    # Streaming analysis support
    # ═══════════════════════════════════════════════════════════════════════

    def prepare_analysis_stream(self, message: str, force_mode: str = None):
        """
        Intercepts explicit analysis/health signals for streaming UI.
        Returns None for non-analysis questions → let pipeline handle.

        ★ iter16: force_mode='narrative'|'health' bypasses signal detection.
        Used by question_classifier to route COMPOUND/INVESTIGATE questions.
        """
        if not self.o.context.has_data():
            return None

        q = message.lower()

        narrative_signals = [
            'summary', 'summarize', 'summarise',
            'manager', 'executive', 'report',
            'explain', 'deep dive',
            'analyze this', 'analyse this',
            'what does this mean', 'what does this show',
            'tell me more about this', 'elaborate on this',
            'takeaway', 'key takeaway', 'highlight', 'recap',
            'overview', 'what happened', 'week in review',
            'what should i know', 'catch me up',
        ]
        health_signals = [
            'any issues', 'any problems', 'anything wrong',
            'health check', 'anomal', 'anamol', 'anom',
            'is anything broken',
            'what went wrong', 'why did it drop', 'why did it spike',
            'red flags', 'red flag', 'concerns', 'worry', 'worried',
            'investigate', 'diagnose',
            'health', 'any red',
        ]

        is_narrative = force_mode == 'narrative' or any(sig in q for sig in narrative_signals)
        is_health = force_mode == 'health' or any(sig in q for sig in health_signals)

        if not is_narrative and not is_health:
            logger.info("prepare_analysis_stream: not an analysis signal, passing to pipeline")
            return None

        # Load and prep current data
        try:
            if is_health and self.o.data_profile and self.o.data_profile.has_table('profile_daily'):
                df = self.o.data_profile.query_safe(
                    "SELECT * FROM profile_daily ORDER BY day ASC")
                if df is None or df.empty:
                    df = self.o.duckdb_engine.get_current_data()
            else:
                df = self.o.duckdb_engine.get_current_data()

            if df is None or df.empty:
                return None

            from .insight_cards import _find_label_col as _flc
            _has_entity = _flc(df) is not None and any(
                c.lower() in ('orgid', 'org_id', 'organizationid')
                for c in df.columns
            )
            if _has_entity:
                df = self.o._enrich(df)
                display_df = self.o._enrich(df, for_display=True)
            else:
                display_df = df
        except Exception as e:
            logger.warning(f"Data prep failed: {e}")
            return None

        # NARRATIVE mode
        if is_narrative:
            return self._handle_narrative_stream(message, q, df, display_df)

        # HEALTH mode
        return self._handle_health_stream(message, q, df, display_df)

    def _handle_narrative_stream(self, message, q, df, display_df):
        """Handle narrative/exec summary streaming."""
        from .insight_cards import build_health_analysis
        from .executive_briefing import build_executive_briefing

        q_lower = message.lower()
        is_exec_summary = any(w in q_lower for w in [
            'manager', 'management', 'executive', 'exec', 'leadership', 'lt ',
            'summary for', 'brief', 'briefing', 'weekly report', 'weekly summary',
            'what happened', 'last week', 'this week', 'week in review',
            'takeaway', 'key takeaway', 'highlight', 'recap',
            'overview', 'catch me up', 'what should i know',
        ])

        if is_exec_summary and self.o.data_profile and self.o.data_profile.is_built:
            try:
                from . import compound_analyst as _ca
                compound = _ca.analyze(self.o.data_profile, question=message, mode='takeaways')

                daily_df = self.o.data_profile.query_safe(
                    "SELECT * FROM profile_daily ORDER BY day ASC"
                ) if self.o.data_profile.has_table('profile_daily') else None
                region_df = self.o.data_profile.query_safe(
                    "SELECT * FROM profile_region ORDER BY events DESC"
                ) if self.o.data_profile.has_table('profile_region') else None
                org_df = self.o.data_profile.query_safe(
                    "SELECT * FROM profile_organization ORDER BY events DESC LIMIT 10"
                ) if self.o.data_profile.has_table('profile_organization') else None

                briefing = build_executive_briefing(daily_df, region_df, org_df, message)
                combined_text = compound.narrative if compound.signals else briefing['summary_text']

                self.o.context.add_turn(message, 'executive_briefing', result_rows=0)
                return {
                    'intent': 'executive_briefing',
                    'content': combined_text,
                    'executive_briefing_html': briefing['html'],
                    'chart': None, 'dataframe': None,
                    'source': (f"compound ({len(compound.tables_consulted)} tables)"
                               if compound.signals else 'profile'),
                }
            except Exception as _e:
                logger.warning(f"Executive briefing failed: {_e}, falling through to stream")

        try:
            viz = self.o.visualizer.analyze_and_visualize(display_df, message, "")
            context = self.o.context.format_context_for_llm()
            stream = self.o.analysis_engine.stream_insights(df, message, context)

            self.o.context.add_turn(message, 'analyze_cache', result_rows=len(df),
                visualization_type=viz.get('type'))

            return {
                'mode': 'narrative',
                'df': df, 'viz': viz, 'stream': stream,
                'intent': 'analyze_cache',
            }
        except Exception as e:
            logger.warning(f"Narrative prep failed: {e}")
            return None

    def _handle_health_stream(self, message, q, df, display_df):
        """Handle health/anomaly analysis streaming."""
        from .insight_cards import build_health_analysis
        from . import anomaly_drill as _drill

        # Try compound analyst first
        try:
            from . import compound_analyst as _ca
            import time as _t
            _t0 = _t.time()

            compound = _ca.analyze(
                self.o.data_profile,
                question=message,
                mode='anomaly' if any(w in q for w in
                    ['anomal', 'issue', 'problem', 'wrong', 'flag', 'concern']) else 'takeaways',
            )

            if compound.signals:
                logger.info(
                    f"CompoundAnalyst: {len(compound.signals)} signals from "
                    f"{len(compound.tables_consulted)} tables in {compound.compute_ms:.0f}ms"
                )

                result = build_health_analysis(display_df, message)
                chart = result.get('chart') if result else None

                drill_narrative = ''
                findings = result.get('findings', []) if result else []
                try:
                    drill = _drill.investigate(
                        health_findings=findings, daily_df=df,
                        data_profile=self.o.data_profile if hasattr(self.o, 'data_profile') else None,
                    )
                    if drill.triggered and drill.narrative:
                        drill_narrative = drill.narrative
                except Exception:
                    pass

                full_message = compound.narrative
                if result and result.get('message'):
                    full_message += "\n\n---\n" + result['message']
                if drill_narrative:
                    full_message += drill_narrative

                self.o.context.add_turn(message, 'compound_analysis',
                    result_rows=len(df), visualization_type='health_analysis')

                return {
                    'mode': 'health',
                    'df': display_df,
                    'intent': 'compound_analysis',
                    'content': full_message,
                    'chart': chart,
                    'findings': findings,
                    'compound_signals': len(compound.signals),
                    'source': f"compound ({len(compound.tables_consulted)} tables)",
                }

        except Exception as _compound_err:
            logger.warning(f"CompoundAnalyst failed, falling back to health: {_compound_err}")

        # Fallback: original single-table health analysis
        try:
            import time as _t
            _t0 = _t.time()

            result = build_health_analysis(display_df, message)
            logger.info(f"Health build_health_analysis: {(_t.time()-_t0)*1000:.0f}ms")
            if not result or not result.get('chart'):
                return None

            self.o.context.add_turn(message, 'analyze_cache',
                result_rows=len(df), visualization_type='health_analysis')

            findings = result.get('findings', [])

            drill_narrative = ''
            try:
                drill = _drill.investigate(
                    health_findings=findings, daily_df=df,
                    data_profile=self.o.data_profile if hasattr(self.o, 'data_profile') else None,
                )
                if drill.triggered and drill.narrative:
                    drill_narrative = drill.narrative
                    logger.info(f"Auto-drill: {drill.drill_count} queries, "
                                f"{len(drill.findings)} findings")
            except Exception as _de:
                logger.debug(f"Auto-drill skipped: {_de}")

            full_message = result.get('message', '')
            if drill_narrative:
                full_message = full_message + drill_narrative

            return {
                'mode': 'health',
                'df': display_df,
                'viz': {'chart': result['chart'], 'type': 'health_analysis'},
                'message': full_message,
                'intent': 'analyze_cache',
            }
        except Exception as e:
            logger.warning(f"Health analysis failed: {e}")
            return None

    # ═══════════════════════════════════════════════════════════════════════
    # Helpers
    # ═══════════════════════════════════════════════════════════════════════

    @staticmethod
    def _result_looks_wrong(df: pd.DataFrame, question: str, kql: str) -> bool:
        q = question.lower()
        rows = len(df)
        if rows == 0:
            return False
        per_kw = ["per org", "per region", "by org", "by region", "by entity",
                   "by day", "per day", "each org", "top 10", "top 5", "top 20", "ranking"]
        if any(k in q for k in per_kw) and rows <= 2:
            return True
        avg_kw = ["average", "avg", "mean", "median"]
        if any(k in q for k in avg_kw) and "per" in q and rows <= 2:
            return True
        return False

    def _retry_kql(self, question: str, failed_kql: str, bad_result: pd.DataFrame) -> str:
        try:
            if not self.o.kusto_client:
                return failed_kql
            schema_ctx = self.o.kusto_client.get_schema_context()
            enhanced = (
                f"{question}\n\n"
                f"[Previous KQL: {failed_kql}\n"
                f"Returned {len(bad_result)} rows (columns: {list(bad_result.columns)}).\n"
                f"That's WRONG — expected a detailed breakdown. Fix it.]"
            )
            conv_ctx = self.o.context.format_context_for_llm()
            return self.o.llm_service.generate_kql_filter(enhanced, schema_ctx, conv_ctx)
        except Exception as e:
            logger.error(f"Retry KQL failed: {e}")
            return failed_kql

    @staticmethod
    def _refine_needs_fresh_data(msg: str, cached_cols: list) -> bool:
        concept_cols = [
            (["emea", "apac", "na ", "us ", "eu ", "region", "geo", "country"],
             ["geoname", "region", "geo"]),
            (["org"], ["orgid"]),
            (["entity", "feature"], ["entityname"]),
            (["session"], ["sessionid", "sessions"]),
            (["day", "daily", "date", "trend"], ["eventinfo_time", "date", "day"]),
        ]
        for triggers, needed in concept_cols:
            if any(t in msg for t in triggers):
                if any(nc in cached_cols for nc in needed):
                    continue
                if any(any(nc in cc for cc in cached_cols) for nc in needed):
                    continue
                return True
        return False
