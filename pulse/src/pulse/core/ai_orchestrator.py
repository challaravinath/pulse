from pathlib import Path
from .insight_cards import build_health_analysis
from .executive_briefing import build_executive_briefing
from . import analyst_voice as _av
from . import anomaly_drill as _drill
from . import business_context as _bctx
from .memory_store import MemoryStore
"""
AI Orchestrator v4.0 — Thin Dispatcher Architecture
====================================================
Iter 11.0: God-file refactored into focused handler modules.

Handler modules:
  - chip_handlers.py   — 4 quick-action chip answers
  - scope_handler.py   — beyond-scope, at-risk, KQL builders
  - profile_handler.py — exhaustive profile resolution (3 strategies)
  - kusto_handlers.py  — live Kusto fallback + analysis streaming
"""

import logging
from typing import Dict, Any, Optional
import pandas as pd


def _safe_parse_dates(s):
    """Inline date parser — avoids importing pulse.ui.app."""
    import numpy as _np
    import pandas as _pd
    if s is None:
        return _pd.Series([], dtype='datetime64[ns]')
    if not isinstance(s, _pd.Series):
        s = _pd.Series(s)
    if _pd.api.types.is_datetime64_any_dtype(s):
        try:
            if hasattr(s.dt, 'tz') and s.dt.tz is not None:
                s = s.dt.tz_convert(None)
        except Exception:
            pass
        return s
    try:
        numeric = _pd.to_numeric(s, errors='coerce')
        if numeric.notna().all():
            med = float(numeric.median())
            NET_EPOCH = 621355968000000000
            if med > 6e17:
                unix_ns = (numeric - NET_EPOCH) * 100
                return _pd.to_datetime(unix_ns, unit='ns', errors='coerce')
    except Exception:
        pass
    return _pd.to_datetime(s, errors='coerce')

from .intent_router import IntentRouter, IntentType
from .context_manager import ConversationContext
from .schema_validator import SchemaValidator
from .analysis_engine import AnalysisEngine
from .llm_service import LLMService
from .visualizer import SmartVisualizer
from .query_planner import QueryPlanner
from .semantic_layer import SemanticLayer
from .data_profile import DataProfile
from .fast_router import FastRouter
from .fast_router_v2 import FastRouterV2
from .predictive_cache import PredictiveCache
from .org_enrichment import OrgEnricher, EnrichmentConfig

# Handler modules (extracted in iter 11.0)
from .chip_handlers import ChipHandler
from .scope_handler import ScopeHandler
from .profile_handler import ProfileHandler
from .kusto_handlers import KustoHandler

logger = logging.getLogger(__name__)


class AIOrchestrator:
    """Main AI agent — thin dispatcher delegating to focused handler modules."""

    def __init__(
        self,
        llm_service: LLMService,
        kusto_client,
        duckdb_engine,
        visualizer: SmartVisualizer,
        semantic_layer: SemanticLayer = None,
        data_profile: DataProfile = None,
        enrichment_config: dict = None,
        config_path: str = None,
    ):
        self.llm_service = llm_service
        self.kusto_client = kusto_client
        self.duckdb_engine = duckdb_engine
        self.visualizer = visualizer
        self.semantic_layer = semantic_layer
        self.data_profile = data_profile
        self.config_path = config_path

        self.biz_ctx = _bctx.load(config_path) if config_path else _bctx.BusinessContext()
        config_id = Path(config_path).stem if config_path else 'default'
        self.memory = MemoryStore(config_id)

        self.intent_router = IntentRouter(llm_service.client, model=llm_service.model)
        self.context = ConversationContext()
        self.analysis_engine = AnalysisEngine(llm_service.client)
        self.query_planner = QueryPlanner(llm_service, kusto_client, duckdb_engine)

        from .kql_detector import KQLDetector
        self.kql_detector = KQLDetector()

        self.fast_router = None
        if semantic_layer and semantic_layer.is_loaded:
            self.fast_router = FastRouterV2(semantic_layer.model)
            logger.info("FastRouterV2 initialized (85%+ pattern coverage)")

        self.predictive_cache: Optional[PredictiveCache] = None
        self.enricher = OrgEnricher(EnrichmentConfig.from_dict(enrichment_config or {}))
        self.schema_validator: Optional[SchemaValidator] = None

        # Handler modules
        self.chip_handler = ChipHandler(self)
        self.scope_handler = ScopeHandler(self)
        self.profile_handler = ProfileHandler(self)
        self.kusto_handler = KustoHandler(self)

    # ═══════════════════════════════════════════════════════════════════════
    # Setup
    # ═══════════════════════════════════════════════════════════════════════

    def set_schema(self, schema: Dict[str, str]):
        self.context.set_schema(schema)
        self.schema_validator = SchemaValidator(schema)

    def load_enrichment(self):
        if self.enricher.config.source == "none":
            return
        if self.enricher.config.source == "csv":
            success = self.enricher.load()
            if success:
                logger.info(f"Enrichment: loaded {self.enricher.org_count} orgs from CSV")
            else:
                logger.warning("Enrichment: CSV load failed")
        elif self.enricher.config.source == "kusto":
            logger.info("Enrichment ready: Kusto source, will resolve on-demand per query")

    def init_predictive_cache(self, visualizer=None):
        self.predictive_cache = PredictiveCache(
            data_profile=self.data_profile,
            visualizer=visualizer or self.visualizer,
        )
        logger.info("PredictiveCache enabled")

    # ═══════════════════════════════════════════════════════════════════════
    # Shared utilities (used by multiple handlers)
    # ═══════════════════════════════════════════════════════════════════════

    def _enrich(self, df: pd.DataFrame, for_display: bool = False) -> pd.DataFrame:
        try:
            if not self.enricher or not self.enricher.is_loaded:
                if self.enricher and self.enricher.config.source == "kusto":
                    pass
                else:
                    return df
            result_org_ids = set()
            for col in df.columns:
                if col.lower() in ('orgid', 'org_id', 'organizationid', 'orgsid'):
                    result_org_ids.update(df[col].dropna().astype(str).unique())
            if not result_org_ids:
                return df
            missing = [oid for oid in result_org_ids if not self.enricher.resolve(str(oid))]
            if missing:
                logger.info(f"Enrichment: resolving {len(missing)} new org names")
                self.enricher.load_for_orgs(missing)
            if for_display:
                return self.enricher.enrich_for_display(df)
            return self.enricher.enrich(df)
        except Exception as e:
            logger.warning(f"Enrichment failed: {e}")
            return df

    def _llm_interpret(self, system_prompt: str, user_content: str,
                       max_tokens: int = 200) -> str:
        try:
            resp = self.llm_service.client.chat.completions.create(
                model=self.llm_service.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_content},
                ],
                max_tokens=max_tokens,
                temperature=0.2,
            )
            return resp.choices[0].message.content.strip()
        except Exception as e:
            logger.warning(f"LLM interpret failed: {e}")
            return ""

    def _sync_to_telemetry(self, df: pd.DataFrame):
        try:
            _rwlock = getattr(self.data_profile, '_rwlock', None) if self.data_profile else None
            conn = self.duckdb_engine.connection
            if not conn:
                return
            def _do_sync():
                conn.execute("DROP TABLE IF EXISTS telemetry")
                conn.register('_tmp_sync', df)
                conn.execute("CREATE TABLE telemetry AS SELECT * FROM _tmp_sync")
                conn.unregister('_tmp_sync')
                self.duckdb_engine._columns = list(df.columns)
                self.duckdb_engine._row_count = len(df)
                self.duckdb_engine.loaded = True
            if _rwlock:
                with _rwlock.write_lock():
                    _do_sync()
            else:
                _do_sync()
        except Exception as e:
            logger.warning(f"Telemetry sync failed: {e}")

    # ═══════════════════════════════════════════════════════════════════════
    # Health analysis (direct, used by app.py)
    # ═══════════════════════════════════════════════════════════════════════

    def _handle_health_analysis(self, message: str) -> Optional[Dict]:
        try:
            df = self.duckdb_engine.get_current_data()
            if df is None or df.empty:
                return None
            df = self._enrich(df)
            display_df = self._enrich(df, for_display=True)
            result = build_health_analysis(display_df, message)
            if not result or not result.get('chart'):
                return None
            self.context.add_turn(message, 'analyze_cache',
                result_rows=len(df), visualization_type='health_analysis')
            findings = result.get('findings', [])
            drill_narrative = ''
            try:
                drill = _drill.investigate(
                    health_findings=findings, daily_df=df,
                    data_profile=self.data_profile if hasattr(self, 'data_profile') else None,
                )
                if drill.triggered and drill.narrative:
                    drill_narrative = drill.narrative
            except Exception:
                pass
            full_message = result.get('message', '')
            if drill_narrative:
                full_message = full_message + drill_narrative
            return {
                'intent': 'analyze_cache', 'response_type': 'data',
                'data': display_df, 'kql': None,
                'visualization': {'chart': result['chart'], 'type': 'health_analysis'},
                'message': full_message, 'suggestions': None,
            }
        except Exception as e:
            logger.warning(f"Health analysis failed: {e}")
            return None

    # ═══════════════════════════════════════════════════════════════════════
    # Main Entry Point
    # ═══════════════════════════════════════════════════════════════════════

    def process_message(self, user_message: str) -> Dict[str, Any]:
        if self.predictive_cache:
            cached = self.predictive_cache.get_if_ready(user_message)
            if cached:
                logger.info(f"Predictive cache HIT: {user_message[:50]}")
                if not cached.get("follow_up_suggestions"):
                    cached["follow_up_suggestions"] = self._build_suggestions(user_message, cached)
                return cached
        response = self._process_core(user_message)
        if not response.get("follow_up_suggestions"):
            response["follow_up_suggestions"] = self._build_suggestions(user_message, response)
        if self.predictive_cache:
            import threading
            threading.Thread(
                target=self.predictive_cache.prefetch_after,
                args=(response.get("intent", ""), user_message),
                daemon=True,
            ).start()
        return response

    def _build_suggestions(self, question: str, response: Dict) -> list:
        """
        ★ iter14: Context-aware chip suggestions.
        Reads actual data from the response to generate specific follow-ups.
        Tracks conversation history to avoid repeating what's been asked.
        """
        if response.get('response_type') == 'error':
            return []

        q = question.lower()
        suggestions = []

        # ── Extract data context ─────────────────────────────────────────
        df = response.get('data')
        if df is None:
            df = response.get('dataframe')
        if isinstance(df, list) and df:
            import pandas as _pd
            df = _pd.DataFrame(df)
        if isinstance(df, pd.DataFrame) and not df.empty:
            suggestions = self._data_aware_suggestions(q, df, response)

        # ── Fallback to intent-based if data extraction gave nothing ─────
        if not suggestions:
            suggestions = self._intent_based_suggestions(q)

        # ── Filter out already-asked questions ───────────────────────────
        asked = set()
        for turn in self.context.get_recent_turns(8):
            asked.add(turn.user_message.lower().strip().rstrip('?'))
        suggestions = [
            s for s in suggestions
            if s.lower().rstrip('?') not in asked
            and s.lower().rstrip('?') not in q
        ]

        return suggestions[:3]

    def _data_aware_suggestions(self, q: str, df: pd.DataFrame, response: Dict) -> list:
        """Generate suggestions based on actual data returned."""
        suggestions = []
        profile_sql = response.get('profile_sql', '')
        table = ''
        if profile_sql:
            import re as _re
            m = _re.search(r'FROM\s+(profile_\w+)', profile_sql, _re.IGNORECASE)
            if m:
                table = m.group(1)

        # ── Identify column types ────────────────────────────────────────
        org_col = next((c for c in df.columns
                        if c.lower() in ('orgid', 'org_id', 'organizationid',
                                         'org_name', 'organization')), None)
        region_col = next((c for c in df.columns
                           if c.lower() in ('geoname', 'region', 'geo',
                                            'country', 'clientcountry')), None)
        day_col = next((c for c in df.columns
                        if c.lower() in ('day', 'date', 'timestamp')), None)
        event_col = next((c for c in df.columns
                          if c.lower() in ('events', 'event_count', 'count')), None)

        # ── Org ranking → suggest drilling into top org + trend ──────────
        if org_col and table in ('profile_organization', ''):
            top_org = str(df.iloc[0][org_col]) if len(df) > 0 else None
            if top_org and len(top_org) < 40:
                display = top_org[:25] + '…' if len(top_org) > 25 else top_org
                suggestions.append(f"How is {display} doing?")
            suggestions.append("Show these over time")
            suggestions.append("Break down by region")

        # ── Region data → suggest top orgs in leading region ─────────────
        elif region_col and table in ('profile_region', ''):
            top_region = str(df.iloc[0][region_col]) if len(df) > 0 else None
            if top_region and len(top_region) < 20:
                suggestions.append(f"Top orgs in {top_region}")
            suggestions.append("Compare all regions")
            suggestions.append("Regional trend over time")

        # ── Trend data → suggest spike investigation ─────────────────────
        elif day_col and event_col and table in ('profile_daily', ''):
            try:
                if pd.api.types.is_numeric_dtype(df[event_col]):
                    peak_idx = df[event_col].idxmax()
                    peak_row = df.loc[peak_idx]
                    peak_day = peak_row[day_col]
                    if hasattr(peak_day, 'strftime'):
                        day_str = peak_day.strftime('%b %d')
                        suggestions.append(f"What caused the spike on {day_str}?")
                    # Check for drops
                    if len(df) > 7:
                        recent_avg = df[event_col].tail(7).mean()
                        prior_avg = df[event_col].iloc[-14:-7].mean() if len(df) > 14 else df[event_col].mean()
                        if prior_avg > 0 and recent_avg < prior_avg * 0.8:
                            suggestions.append("Why is activity declining?")
            except Exception:
                pass
            suggestions.append("Top orgs driving this")
            suggestions.append("Break down by region")

        # ── Entity data → suggest org breakdown ──────────────────────────
        elif table in ('profile_entity', 'profile_activity'):
            entity_col = next((c for c in df.columns
                               if c.lower() in ('entityname', 'entity', 'eventinfo_name')), None)
            if entity_col and len(df) > 0:
                top_entity = str(df.iloc[0][entity_col])[:30]
                suggestions.append(f"Which orgs use {top_entity}?")
            suggestions.append("Show entity trend over time")
            suggestions.append("Top 10 orgs")

        # ── Totals → suggest breakdown ───────────────────────────────────
        elif table == 'profile_totals':
            suggestions.append("Show the daily trend")
            suggestions.append("Break down by region")
            suggestions.append("Top 10 orgs")

        # ── Health/anomaly → suggest investigation ───────────────────────
        elif any(w in q for w in ['anomal', 'health', 'issue', 'spike', 'drop']):
            suggestions.append("Show the full trend")
            suggestions.append("Which orgs are affected?")
            suggestions.append("Compare regions")

        return suggestions

    def _intent_based_suggestions(self, q: str) -> list:
        """Fallback: intent-based suggestions when no data context available."""
        if any(w in q for w in ['top', 'ranking', 'biggest', 'most active', 'bottom', 'largest']):
            return ['Show these over time', 'Break down by region', 'Any anomalies?']
        elif any(w in q for w in ['trend', 'daily', 'over time', 'growing', 'declining']):
            return ['Top orgs driving this', 'Break down by region', 'Any anomalies?']
        elif any(w in q for w in ['summary', 'overview', 'recap', 'manager', 'snapshot']):
            return ['Daily trend', 'Top 10 orgs', 'Events by region']
        elif any(w in q for w in ['total', 'how many', 'count']):
            return ['Show the trend', 'Break down by region', 'Top 10 orgs']
        elif any(w in q for w in ['region', 'geo', 'emea', 'nam', 'apac']):
            return ['Top orgs in this region', 'Compare all regions', 'Regional trend']
        elif any(w in q for w in ['how is', 'howz', "how's"]):
            return ['Show the trend', 'Compare all regions', 'Top 10 orgs']
        return ['Show the trend', 'Top 10 orgs', 'Events by region']

    # ═══════════════════════════════════════════════════════════════════════
    # Core routing — thin dispatcher
    # ═══════════════════════════════════════════════════════════════════════

    def _process_core(self, user_message: str) -> Dict[str, Any]:
        logger.info(f"Processing: {user_message}")
        q = user_message.lower()

        # Chip dispatch
        _chip_map = {
            '__chip_trending__': self.chip_handler.handle_chip_trending,
            '__chip_top_customers__': self.chip_handler.handle_chip_top_customers,
            '__chip_bottom_customers__': self.chip_handler.handle_chip_bottom_customers,
            '__chip_manager_summary__': self.chip_handler.handle_chip_manager_summary,
        }
        if user_message.strip() in _chip_map:
            _result = _chip_map[user_message.strip()]()
            if _result is not None:
                return _result
            _chip_name = user_message.strip()
            if _chip_name == '__chip_bottom_customers__':
                _msg = "⏳ Bottom 10 needs the full org-level profile, which is still building. Try again in a moment."
            elif _chip_name == '__chip_trending__':
                _msg = "⏳ Trend data is still loading. Usually ready in 20–40 seconds."
            else:
                _msg = "⏳ Still loading data — usually ready in 15–30 seconds. Try again in a moment."
            return {'content': _msg, 'intent': 'chip_pending', 'chart': None,
                    'dataframe': None, 'source': 'profile', 'elapsed': 0.0}

        # Beyond-scope
        _beyond_days = self.scope_handler.detect_beyond_scope(user_message)
        if _beyond_days:
            _bs = self.scope_handler.build_beyond_scope_response(user_message, _beyond_days)
            return {
                'content': _bs['message'], 'intent': 'beyond_scope',
                'kql': _bs['kql_snippet'], 'chart': None, 'dataframe': None,
                'source': 'scope_limit', 'elapsed': 0,
                'follow_up_suggestions': ['Show last 180 days instead',
                    'Top customers this quarter', 'Regional trend this quarter'],
            }

        # Direct KQL
        is_kql, reason = self.kql_detector.is_kql(user_message)
        if is_kql:
            logger.info(f"Direct KQL: {reason}")
            return self.kusto_handler.handle_direct_kql(user_message)

        # Conversational / meta
        if self.intent_router._is_offtopic(q):
            return self._handle_redirect(user_message,
                {'intent': IntentType.REDIRECT, 'confidence': 0.9, 'reason': 'off-topic'})
        if any(k in q for k in self.intent_router.meta_kw):
            return self._handle_meta(user_message,
                {'intent': IntentType.META_QUESTION, 'confidence': 0.9, 'reason': 'meta'})

        # Profile path
        if self.profile_handler.has_profile():
            result = self.profile_handler.try_exhaustively(user_message)
            if result is not None:
                return result
            logger.info("Profile exhausted all strategies → Kusto fallback")

        # Intent router
        intent_result = self.intent_router.classify_intent(
            user_message, self.context.has_data(), self.context.conversation_history)
        intent = intent_result['intent']
        logger.info(f"Intent: {intent.value} (conf: {intent_result['confidence']:.2f})")

        if intent == IntentType.ANALYZE_CACHE:
            return self.kusto_handler.handle_analyze(user_message, intent_result)
        if intent == IntentType.REFINE_CACHE:
            return self.kusto_handler.handle_refine(user_message, intent_result)
        if intent == IntentType.META_QUESTION:
            return self._handle_meta(user_message, intent_result)
        if intent == IntentType.REDIRECT:
            return self._handle_redirect(user_message, intent_result)
        if intent == IntentType.COMPLEX_QUERY:
            return self.kusto_handler.handle_complex(user_message, intent_result)
        return self.kusto_handler.handle_fetch(user_message, intent_result)

    # ═══════════════════════════════════════════════════════════════════════
    # Small conversational handlers
    # ═══════════════════════════════════════════════════════════════════════

    def _handle_meta(self, message: str, intent_result: Dict) -> Dict:
        profile_info = ""
        if self.profile_handler.has_profile():
            profile_info = f"\n\n{self.data_profile.get_status_summary()}"
        return self._conversational(
            intent_result['intent'].value,
            "I can help you explore your telemetry data! Try:\n\n"
            "- **\"How many active orgs?\"** — totals and counts\n"
            "- **\"Top 10 orgs by events\"** — rankings\n"
            "- **\"Events per region\"** — breakdowns\n"
            "- **\"Daily event trend\"** — time series\n"
            "- **\"Give me a summary for my manager\"** — executive overview\n"
            + profile_info
        )

    def _handle_redirect(self, message: str, intent_result: Dict) -> Dict:
        return self._conversational(
            intent_result['intent'].value,
            "I'm best at analysing your telemetry data! Try asking:\n\n"
            "- **\"How many active orgs?\"**\n"
            "- **\"Top 10 orgs by events\"**\n"
            "- **\"Events per region\"**"
        )

    def _is_conversational_followup(self, message: str) -> bool:
        q = message.lower()
        new_data = ['top ', 'bottom ', 'show me', 'how many', 'count of',
                    'trend', 'daily', 'per day', 'by region', 'by org']
        if any(sig in q for sig in new_data):
            return False
        followup = ['critical', 'important', 'concern', 'anything', 'what should',
                    'normal', 'ok', 'good', 'bad', 'wrong', 'issue', 'flag', 'alert',
                    'worry', 'tell me more', 'elaborate', 'red flag']
        return any(sig in q for sig in followup)

    # ═══════════════════════════════════════════════════════════════════════
    # Memory & digest
    # ═══════════════════════════════════════════════════════════════════════

    def save_memory_snapshot(self) -> bool:
        if self.data_profile and self.data_profile.is_built:
            try:
                self.memory.current = self.memory._build_snapshot(self.data_profile)
                return self.memory.save_snapshot(self.data_profile)
            except Exception as e:
                logger.debug(f"save_memory_snapshot failed: {e}")
        return False

    def get_proactive_digest(self) -> str:
        parts = []
        if self.memory.has_previous() and self.memory.current:
            delta_narrative = self.memory.format_delta_narrative()
            if delta_narrative:
                parts.append(delta_narrative)
        if self.biz_ctx.loaded:
            upcoming = self.biz_ctx.get_upcoming_events(days_ahead=7)
            if upcoming:
                ev_lines = []
                for ev in upcoming[:2]:
                    name = ev.get('name', '')
                    days_away = ev.get('days_away', 0)
                    impact = ev.get('expected_impact', '')
                    day_str = "today" if days_away == 0 else f"in {days_away} day(s)"
                    ev_lines.append(f"📅 **{name}** {day_str}" +
                                    (f" — expected impact: {impact}" if impact else ""))
                if ev_lines:
                    parts.append("**Upcoming events:**\n" + "\n".join(ev_lines))
            if self.biz_ctx.known_quiet_periods:
                from datetime import datetime
                now = datetime.now()
                month = now.strftime('%B').lower()
                adj = self.biz_ctx._get_seasonal_adjustment()
                if adj <= -15:
                    parts.append(
                        f"📌 *Seasonality note: {month.title()} is typically "
                        f"{abs(adj):.0f}% slower — factor this into any WoW comparisons.*")
        return "\n\n".join(parts)

    # ═══════════════════════════════════════════════════════════════════════
    # Backward-compatible delegations (app.py calls these directly)
    # ═══════════════════════════════════════════════════════════════════════

    def handle_chip_trending(self) -> Optional[Dict]:
        return self.chip_handler.handle_chip_trending()
    def handle_chip_top_customers(self) -> Optional[Dict]:
        return self.chip_handler.handle_chip_top_customers()
    def handle_chip_bottom_customers(self) -> Optional[Dict]:
        return self.chip_handler.handle_chip_bottom_customers()
    def handle_chip_manager_summary(self) -> Optional[Dict]:
        return self.chip_handler.handle_chip_manager_summary()
    def prepare_analysis_stream(self, message: str, force_mode: str = None):
        return self.kusto_handler.prepare_analysis_stream(message, force_mode=force_mode)
    def _try_profile_exhaustively(self, message: str) -> Optional[Dict]:
        return self.profile_handler.try_exhaustively(message)
    def _execute_fast_result(self, result, message: str) -> Optional[Dict]:
        return self.profile_handler.execute_fast_result(result, message)
    def _handle_fast_profile(self, message: str) -> Optional[Dict]:
        return self.profile_handler.handle_fast_profile(message)
    def _handle_profile(self, message: str) -> Optional[Dict]:
        return self.profile_handler.handle_profile(message)
    def _has_profile(self) -> bool:
        return self.profile_handler.has_profile()
    def _detect_beyond_scope(self, question: str) -> Optional[int]:
        return self.scope_handler.detect_beyond_scope(question)
    def build_beyond_scope_response(self, question: str, days: int) -> Dict:
        return self.scope_handler.build_beyond_scope_response(question, days)
    def _handle_at_risk_query(self, message: str) -> Optional[Dict]:
        return self.scope_handler.handle_at_risk_query(message)
    def _build_live_kql_for_profile(self, table: str, profile_sql: str, question: str) -> str:
        return self.scope_handler.build_live_kql_for_profile(table, profile_sql, question)
    def _handle_fetch(self, message: str, intent_result: Dict) -> Dict:
        return self.kusto_handler.handle_fetch(message, intent_result)
    def _handle_direct_kql(self, message: str) -> Dict:
        return self.kusto_handler.handle_direct_kql(message)
    def _handle_complex(self, message: str, intent_result: Dict) -> Dict:
        return self.kusto_handler.handle_complex(message, intent_result)
    def _handle_analyze(self, message: str, intent_result: Dict) -> Dict:
        return self.kusto_handler.handle_analyze(message, intent_result)
    def _handle_refine(self, message: str, intent_result: Dict) -> Dict:
        return self.kusto_handler.handle_refine(message, intent_result)
    def _validate_execute_visualize(self, kql, user_message, intent, _retry=False) -> Dict:
        return self.kusto_handler.validate_execute_visualize(kql, user_message, intent, _retry)

    # ═══════════════════════════════════════════════════════════════════════
    # Static helpers
    # ═══════════════════════════════════════════════════════════════════════

    @staticmethod
    def _error(intent: str, message: str) -> Dict:
        return {'intent': intent, 'response_type': 'error', 'data': None,
                'kql': None, 'visualization': None, 'message': message, 'suggestions': None}

    @staticmethod
    def _conversational(intent: str, message: str) -> Dict:
        return {'intent': intent, 'response_type': 'conversational', 'data': None,
                'kql': None, 'visualization': None, 'message': message, 'suggestions': None}

    @staticmethod
    def _friendly_error(action: str, error: Exception) -> str:
        err_str = str(error)
        if 'connection' in err_str.lower() or 'timeout' in err_str.lower():
            return f"⚠️ Couldn't {action} — connection timed out.\nTry reconnecting."
        if 'unauthorized' in err_str.lower() or '401' in err_str:
            return f"⚠️ Authentication expired while trying to {action}.\nTry reconnecting."
        if 'syntax' in err_str.lower() or 'semantic' in err_str.lower():
            return f"⚠️ The generated query had a syntax issue.\nTry rephrasing.\n*Detail: {err_str[:200]}*"
        if 'throttl' in err_str.lower() or '429' in err_str:
            return "⚠️ Kusto is rate-limiting. Wait a moment and try again."
        if 'sql' in err_str.lower() or 'duckdb' in err_str.lower():
            return f"⚠️ The follow-up query didn't work.\nTry rephrasing.\n*Detail: {err_str[:200]}*"
        return f"⚠️ Something went wrong trying to {action}.\n*{err_str[:300]}*"
