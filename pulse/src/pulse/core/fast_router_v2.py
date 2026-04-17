"""
Fast Router v2.0 — 85%+ Zero-LLM Resolution
==============================================

Extends the original FastRouter with:
  - 25+ additional patterns covering common question variants
  - "Average X per Y" pattern (the #1 missed pattern in v1)
  - "Compare X vs Y" with specific dimensions
  - Negation: "excluding test", "not counting X"
  - Possessive: "EMEA's top orgs", "entity breakdown for NAM"
  - Conversational: "what about regions?", "and by entity?"

Pattern coverage target: 85%+ of questions resolved without LLM.

Inherits from FastRouter — all v1 patterns still work.

Author: PULSE Team
"""

import re
import logging
from typing import Optional, Tuple, Set
from dataclasses import dataclass, field

# Import the original
from pulse.core.fast_router import FastRouter, FastResult

logger = logging.getLogger(__name__)


class FastRouterV2(FastRouter):
    """
    ★ iter14.1: Honest Router — 4 layers of quality.

    Layer 1: PRE-GUARD   — Can we answer this at all? (impossible / action / missing data)
    Layer 2: CONTEXT     — Resolve pronouns & follow-ups from conversation anchor
    Layer 3: ROUTE       — Match question to table + SQL (existing patterns)
    Layer 4: VALIDATE    — Confidence scoring + honest caveats
    """

    # ═══════════════════════════════════════════════════════════════════════
    # Layer 1: Pre-guard — catch unanswerable questions BEFORE routing
    # ═══════════════════════════════════════════════════════════════════════

    _IMPOSSIBLE = [
        (r'\b(?:predict|forecast|will .* next|projection|estimate future)\b', 'prediction',
         "I can show you historical trends, but I can't predict the future from telemetry data."),
        (r'\b(?:revenue|price|budget|spend|roi|dollar|profit|margin|arpu|£|€|\$)\b', 'financial',
         "Your telemetry tracks events and usage — it doesn't include financial data."),
        (r'\b(?:competitors?|industry average|market share|versus (?:the )?market)\b', 'external',
         "I only have your product's telemetry — no competitor or market data."),
        (r'\b(?:sentiment|satisfaction|nps|csat|feedback|survey|rating)\b', 'sentiment',
         "Your data tracks usage events, not user sentiment or satisfaction scores."),
        (r'\b(?:personally identifiable|pii|email address|phone number|name of the user)\b', 'pii',
         "Telemetry data doesn't include personally identifiable information."),
    ]

    _ACTIONS = [
        (r'(?:send|email|forward|share).*(?:manager|boss|team|exec|stakeholder)', 'manager_summary',
         "__chip_manager_summary__"),
        (r'(?:manager|exec).*(?:summary|brief|report|email)', 'manager_summary',
         "__chip_manager_summary__"),
        (r'\bpresentation\b', 'manager_summary', "__chip_manager_summary__"),
        (r'\b(?:export|download|save as|to csv|to excel|to pdf)\b', 'export', None),
        (r'\b(?:set up.*alert|notify me|trigger when|threshold)\b', 'alert', None),
    ]

    # Negative signals: words that should PREVENT certain table matches
    _NEGATIVE_FOR_TABLE = {
        'profile_region': {'stopped', 'churned', 'churn', 'inactive', 'silent', 'quiet',
                           'competitor', 'benchmark'},
        'profile_totals': {'compare', 'versus', 'vs', 'per', 'by', 'breakdown'},
    }

    # ═══════════════════════════════════════════════════════════════════════
    # Layer 2: Conversation anchor — resolve follow-ups
    # ═══════════════════════════════════════════════════════════════════════

    @dataclass
    class Anchor:
        """Tracks what the last answer was about, so follow-ups work."""
        table: str = ''
        dimension: str = ''        # 'region', 'organization', 'entity'...
        metric: str = 'events'
        filter_value: str = ''
        n: int = 10
        intent: str = ''
        result_cols: list = field(default_factory=list)

    def __init__(self, semantic_model):
        super().__init__(semantic_model)
        self._add_extra_aliases()
        self._anchor = self.Anchor()
        self._available_columns: dict = {}  # table → set(col names)

    def update_anchor(self, result: FastResult, df_columns: list = None):
        """Called after every successful answer to track conversation state."""
        dim = result.table.replace('profile_', '')
        self._anchor = self.Anchor(
            table=result.table,
            dimension=dim,
            metric='events',
            filter_value=result.filter_value,
            n=10,
            intent=result.intent,
            result_cols=df_columns or [],
        )

    def set_available_columns(self, table_columns: dict):
        """Called after profile build for column validation."""
        self._available_columns = {
            k: set(c.lower() for c in v) for k, v in table_columns.items()
        }

    def _add_extra_aliases(self):
        """Add conversational aliases that v1 misses."""
        extra_dim = {
            "organization": ["tenant", "tenants", "customer", "customers", "client", "clients", "company", "companies"],
            "region": ["area", "areas", "geography", "geographies", "location", "locations"],
            "entity": ["table", "tables", "object", "objects", "type", "types"],
            "browser": ["chrome", "firefox", "safari", "edge"],
            "country": ["nation", "nations"],
            "activity": ["event type", "event name", "user action", "user actions"],
            "version": ["release", "build", "app version"],
        }
        for dim_id, aliases in extra_dim.items():
            table = f"profile_{dim_id}"
            if dim_id in self.model.dimensions:
                sql_col = self.model.dimensions[dim_id].sql_column
                for alias in aliases:
                    self._dim_lookup[alias] = (table, sql_col)

        extra_metric = {
            "events": ["activity", "actions", "usage", "traffic", "volume"],
            "active_orgs": ["organizations", "tenants", "customers"],
            "active_users": ["people", "individuals"],
            "sessions": ["visits"],
        }
        for m_id, aliases in extra_metric.items():
            if m_id in self._metric_lookup.values() or m_id in self.model.metrics:
                for alias in aliases:
                    self._metric_lookup[alias] = m_id

    def resolve(self, question: str, available_tables: set) -> Optional[FastResult]:
        """
        4-layer honest resolution.

        Layer 1: Pre-guard  → catch impossible/action/missing questions
        Layer 2: Context    → resolve follow-ups from conversation anchor
        Layer 3: Route      → V2 patterns then V1 patterns
        Layer 4: Validate   → confidence check + caveats
        """
        q = question.lower().strip().rstrip("?.!")
        q_clean = re.sub(r"\b(please|plz|pls|thanks|thank you|can you|could you|would you|show me|give me|get me|i want|i need|let me see)\b", "", q).strip()

        # ═══════════════════════════════════════════════════════════════════
        # Layer 1: Pre-guard — stop unanswerable questions early
        # ═══════════════════════════════════════════════════════════════════

        guard = self._pre_guard(q_clean)
        if guard is not None:
            return guard

        # ═══════════════════════════════════════════════════════════════════
        # Layer 2: Context — resolve pronouns and follow-ups
        # ═══════════════════════════════════════════════════════════════════

        context_result = self._resolve_from_context(q_clean, available_tables)
        if context_result:
            logger.info(f"Layer2 context [{context_result.pattern}]: {context_result.sql[:80]}")
            return self._validate(q_clean, context_result)

        # ═══════════════════════════════════════════════════════════════════
        # Layer 3: Route — V2 patterns, then V1 patterns
        # ═══════════════════════════════════════════════════════════════════

        result = (
            self._try_org_lookup(q, available_tables)
            or self._try_enhanced_table(q_clean, available_tables)
            or self._try_average_per(q_clean, available_tables)
            or self._try_filter_dimension(q_clean, available_tables)
            or self._try_conversational_followup(q_clean, available_tables)
            or self._try_specific_value_filter(q_clean, available_tables)
            or self._try_new_vs_returning(q_clean, available_tables)
            or self._try_diversity(q_clean, available_tables)
            or self._try_growth_decline(q_clean, available_tables)
        )

        if result:
            logger.info(f"FastRouterV2 [{result.pattern}]: {result.sql[:80]}")
            return self._validate(q_clean, result)

        # V1 patterns — pass cleaned question so "show me X" → "X"
        v1 = super().resolve(q_clean, available_tables)
        if v1:
            return self._validate(q_clean, v1)

        # V1 universal fallback
        v1u = super().universal_resolve(q_clean, available_tables)
        if v1u:
            return self._validate(q_clean, v1u)

        return None

    # ═══════════════════════════════════════════════════════════════════════
    # Layer 1 implementation
    # ═══════════════════════════════════════════════════════════════════════

    def _pre_guard(self, q: str) -> Optional[FastResult]:
        """
        Catch questions we cannot or should not answer from profile data.
        Returns a FastResult with intent='impossible' or 'action', or None to continue.
        """
        # Check impossible questions
        for pattern, category, message in self._IMPOSSIBLE:
            if re.search(pattern, q):
                logger.info(f"Pre-guard: impossible/{category} — {q[:50]}")
                return FastResult(
                    table='__none__', sql='', explanation=message,
                    pattern=f'guard_{category}', intent='impossible',
                    confidence=1.0, caveat='',
                )

        # Check action requests
        for pattern, action_type, chip_id in self._ACTIONS:
            if re.search(pattern, q):
                logger.info(f"Pre-guard: action/{action_type} — {q[:50]}")
                if chip_id:
                    # Route to the manager summary chip handler
                    return FastResult(
                        table='__action__', sql='', explanation=f'Action: {action_type}',
                        pattern=f'action_{action_type}', intent='action',
                        filter_value=chip_id,
                        confidence=1.0,
                    )
                else:
                    return FastResult(
                        table='__none__', sql='',
                        explanation=f"That's an action I can't do yet — {action_type} is on the roadmap.",
                        pattern=f'guard_{action_type}', intent='impossible',
                        confidence=1.0,
                    )

        return None  # Continue to routing

    # ═══════════════════════════════════════════════════════════════════════
    # Layer 2 implementation
    # ═══════════════════════════════════════════════════════════════════════

    _FOLLOWUP_PATTERNS = [
        # "and by region?" → switch dimension
        (r'^(?:and |now |also |then )?(?:by|per|break\w* (?:down )?by)\s+(\w+)', 'switch_dimension'),
        # "now show bottom 5" → flip sort
        (r'^(?:and |now |also |then )?(?:show |the )?bottom\s+(\d+)?', 'flip_to_bottom'),
        # "now top 10" → same dim, top
        (r'^(?:and |now |also |then )?(?:show |the )?top\s+(\d+)?', 'flip_to_top'),
        # "what about entities/regions/orgs?" → switch dimension
        (r'^(?:what about|how about|and)\s+(\w+)', 'switch_dimension'),
        # "show that over time" / "over time" → switch to daily
        (r'(?:over time|the trend|time series|daily)', 'switch_to_trend'),
        # "drill down" / "more detail" / "expand" → show more from same
        (r'(?:drill|more detail|expand|zoom in|deeper|elaborate)', 'drill_down'),
        # "the top one" / "first one" / "the biggest"  → filter to #1
        (r'(?:the (?:top|first|biggest|largest|#1) (?:one|org|customer|region))', 'filter_top_1'),
    ]

    def _resolve_from_context(self, q: str, tables: set) -> Optional[FastResult]:
        """
        Resolve follow-up questions using the conversation anchor.
        Only activates when the anchor has data AND the question looks like a follow-up.
        """
        if not self._anchor.table:
            return None  # No prior context

        for pattern, action in self._FOLLOWUP_PATTERNS:
            m = re.search(pattern, q)
            if not m:
                continue

            if action == 'switch_dimension':
                new_dim_text = m.group(1) if m.lastindex else ''
                dim = self._find_dimension(new_dim_text, tables)
                if dim:
                    table, sql_col = dim
                    metric = self._anchor.metric or 'events'
                    return FastResult(
                        table=table,
                        sql=f"SELECT * FROM {table} ORDER BY {metric} DESC",
                        explanation=f"Switching to {table.replace('profile_', '')} breakdown",
                        pattern='context_switch_dim', intent='lookup',
                    )

            elif action == 'flip_to_bottom':
                n = int(m.group(1)) if m.lastindex and m.group(1) else 5
                if self._anchor.table in tables:
                    metric = self._anchor.metric or 'events'
                    return FastResult(
                        table=self._anchor.table,
                        sql=f"SELECT * FROM {self._anchor.table} ORDER BY {metric} ASC LIMIT {n}",
                        explanation=f"Bottom {n} from {self._anchor.dimension}",
                        pattern='context_bottom', intent='ranking',
                    )

            elif action == 'flip_to_top':
                n = int(m.group(1)) if m.lastindex and m.group(1) else 10
                if self._anchor.table in tables:
                    metric = self._anchor.metric or 'events'
                    return FastResult(
                        table=self._anchor.table,
                        sql=f"SELECT * FROM {self._anchor.table} ORDER BY {metric} DESC LIMIT {n}",
                        explanation=f"Top {n} from {self._anchor.dimension}",
                        pattern='context_top', intent='ranking',
                    )

            elif action == 'switch_to_trend':
                if 'profile_daily' in tables:
                    return FastResult(
                        table='profile_daily',
                        sql="SELECT * FROM profile_daily ORDER BY day ASC",
                        explanation="Showing the same data over time",
                        pattern='context_trend', intent='trend',
                    )

            elif action == 'drill_down':
                # Show all columns from the same table, limited
                if self._anchor.table in tables:
                    return FastResult(
                        table=self._anchor.table,
                        sql=f"SELECT * FROM {self._anchor.table} ORDER BY events DESC LIMIT 5",
                        explanation=f"Detailed view of {self._anchor.dimension}",
                        pattern='context_drill', intent='lookup',
                    )

            elif action == 'filter_top_1':
                if self._anchor.table in tables:
                    return FastResult(
                        table=self._anchor.table,
                        sql=f"SELECT * FROM {self._anchor.table} ORDER BY events DESC LIMIT 1",
                        explanation=f"Top 1 from {self._anchor.dimension}",
                        pattern='context_top1', intent='lookup',
                    )

        return None

    # ═══════════════════════════════════════════════════════════════════════
    # Layer 4 implementation
    # ═══════════════════════════════════════════════════════════════════════

    def _validate(self, q: str, result: FastResult) -> FastResult:
        """
        Post-route validation: check for wrong-table signals, add caveats.
        Lowers confidence when the match is suspicious.
        """
        if result.intent in ('impossible', 'action'):
            return result  # Pre-guard results don't need validation

        # ── Check negative signals ───────────────────────────────────────
        neg_words = self._NEGATIVE_FOR_TABLE.get(result.table, set())
        q_words = set(q.split())
        collisions = neg_words & q_words
        if collisions:
            result.confidence *= 0.5
            result.caveat = (
                f"This might not fully answer your question — "
                f"I matched on table keywords but your question also mentions "
                f"'{', '.join(collisions)}' which suggests a different kind of analysis."
            )
            logger.info(f"Layer4: negative signal collision {collisions} on {result.table}")

        # ── Column validation ────────────────────────────────────────────
        if self._available_columns:
            cols = self._available_columns.get(result.table, set())
            if cols:
                # Check if asked metric exists
                asked_metric = self._guess_asked_metric(q)
                if asked_metric and asked_metric not in cols:
                    # Find closest available metric
                    available_metrics = [c for c in cols
                                         if c in ('events', 'active_orgs', 'active_users',
                                                   'sessions', 'entity_types')]
                    if available_metrics:
                        result.caveat = (
                            f"Showing '{available_metrics[0]}' — "
                            f"your data doesn't have a '{asked_metric}' column."
                        )
                        result.confidence *= 0.7

        # ── Single-row ranking check ─────────────────────────────────────
        if result.intent == 'ranking' and result.table == 'profile_totals':
            result.confidence *= 0.3
            result.caveat = "Totals table has aggregate data, not individual rankings."

        return result

    def _guess_asked_metric(self, q: str) -> Optional[str]:
        """Try to figure out what metric the user actually asked for."""
        metric_hints = {
            'users': 'active_users', 'user': 'active_users', 'dau': 'active_users',
            'sessions': 'sessions', 'session': 'sessions',
            'orgs': 'active_orgs', 'organizations': 'active_orgs',
            'entities': 'entity_types', 'features': 'entity_types',
        }
        for word, metric in metric_hints.items():
            if word in q:
                return metric
        return None

    # ═══════════════════════════════════════════════════════════════
    # ENHANCED TABLE PATTERNS — direct routing for new profile tables
    # ═══════════════════════════════════════════════════════════════

    _ENHANCED_PATTERNS = {
        'profile_activity': [
            r'what (?:are|were) (?:users?|people) doing',
            r'what (?:are|were) (?:the )?(?:most )?(?:common |popular |frequent )?(?:actions?|activit|events?)',
            r'(?:user|event|action)\s*(?:breakdown|types?|names?)',
            r'which (?:events?|actions?|activit)',
            r'top (?:events?|actions?|activit)',
        ],
        'profile_ingestion_lag': [
            r'(?:ingestion|pipeline)\s*(?:lag|delay|latency|health|issue)',
            r'(?:data|event)\s*(?:freshness|delay|stale)',
            r'how (?:fresh|stale|delayed)',
        ],
        'profile_engagement': [
            r'(?:user )?engagement',
            r'sessions? per (?:user|day)',
            r'how (?:engaged|active) are (?:users?|people)',
            r'(?:daily|active) users? (?:count|trend|over)',
        ],
        'profile_version': [
            r'(?:app|client)?\s*version\s*(?:adoption|rollout|breakdown|distribution)?',
            r'which version',
            r'version (?:trend|migration|update)',
            r'rollout (?:status|progress)',
        ],
    }

    def _try_org_lookup(self, q: str, tables: set) -> Optional[FastResult]:
        """
        ★ iter15: Catch org-specific questions and answer from profile_organization.
        Patterns:
          "how is dde25578-c8f1-... doing"
          "how is SCORE doing"
          "show me Acme Corp"
          "details on org XYZ"
          Raw GUID in question
        """
        if 'profile_organization' not in tables:
            return None

        import re as _re

        # Detect GUID pattern (8-4-4-4-12 or partial)
        guid_match = _re.search(
            r'([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{6,12})', q)
        if guid_match:
            guid = guid_match.group(1)
            return FastResult(
                table='profile_organization',
                sql=(
                    f"SELECT * FROM profile_organization "
                    f"WHERE LOWER(CAST(orgid AS VARCHAR)) LIKE '%{guid.lower()}%' "
                    f"OR LOWER(CAST(COALESCE(orgid, '') AS VARCHAR)) LIKE '%{guid.lower()}%'"
                ),
                explanation=f"Looking up organization {guid[:12]}...",
                pattern='org_lookup_guid',
                intent='lookup',
                filter_value=guid,
            )

        # Detect "how is [name] doing" / "show me [name]" / "details on [name]"
        # NOTE: We check against the ORIGINAL question (before cleanup strips verbs)
        name_patterns = [
            r'how is (.+?)(?:\s+doing|\s+performing|\s*\??$)',
            r'(?:show|tell|details|info|status)\s+(?:me\s+)?(?:on\s+|about\s+|for\s+)?(.+?)$',
            r'(?:what about|check on|look up|lookup)\s+(.+?)$',
        ]

        # Region/generic skip list
        skip = {'the data', 'this', 'that', 'it', 'them', 'today', 'yesterday',
                'last week', 'the trend', 'regions', 'entities', 'browsers',
                'top orgs', 'bottom orgs', 'all orgs', 'the orgs', 'orgs',
                'events', 'our data', 'my data', 'the daily trend',
                # Common query phrases — not org names
                'total events', 'events by region', 'events by org',
                'daily trend', 'the breakdown', 'all data', 'everything',
                'top 10', 'top 5', 'bottom 10', 'bottom 5',
                'active orgs', 'active users', 'active organizations',
                'entity breakdown', 'browser breakdown', 'region breakdown',
                'daily activity', 'the chart', 'the table', 'the dashboard',
                # Region names — let region handler deal with these
                'emea', 'nam', 'apac', 'latam', 'mea', 'eu', 'us', 'eur',
                'gbr', 'fra', 'deu', 'che', 'nor', 'swe', 'ita', 'esp',
                'nld', 'bel', 'aut', 'dnk', 'fin', 'irl', 'pol', 'jpn',
                'aus', 'nzl', 'sgp', 'ind', 'chn', 'kor', 'bra', 'mex',
                'can', 'zaf', 'are', '_na_'}
        # Also skip if query contains dimension/metric keywords
        _data_words = {'by region', 'by org', 'by entity', 'by browser',
                       'over time', 'per day', 'per week', 'daily', 'weekly',
                       'total', 'average', 'sum', 'count', 'breakdown'}

        for pat in name_patterns:
            m = _re.search(pat, q)
            if m:
                name = m.group(1).strip().strip('"\'')
                if len(name) < 3 or len(name) > 60:
                    continue
                if name.lower() in skip:
                    continue
                # Skip if the extracted name contains data query words
                if any(dw in name.lower() for dw in _data_words):
                    continue

                safe_name = name.replace("'", "''")
                return FastResult(
                    table='profile_organization',
                    sql=(
                        f"SELECT * FROM profile_organization "
                        f"WHERE LOWER(CAST(orgid AS VARCHAR)) LIKE '%{safe_name.lower()}%'"
                    ),
                    explanation=f"Looking up '{name}'",
                    pattern='org_lookup_name',
                    intent='lookup',
                    filter_value=name,
                    _fallback_sql=(
                        f"SELECT * FROM profile_organization "
                        f"ORDER BY events DESC LIMIT 10"
                    ),
                )

        return None

    def _try_enhanced_table(self, q: str, tables: set) -> Optional[FastResult]:
        """
        Direct routing for enhanced profile tables:
          "what are users doing?" → profile_activity
          "any pipeline lag?" → profile_ingestion_lag
          "user engagement?" → profile_engagement
          "which version?" → profile_version
        """
        for table_name, patterns in self._ENHANCED_PATTERNS.items():
            if table_name not in tables:
                continue
            for pat in patterns:
                if re.search(pat, q):
                    # Build appropriate SQL based on table type
                    if table_name in ('profile_ingestion_lag', 'profile_engagement'):
                        # Time-series tables — show last 30 days sorted by day
                        sql = (
                            f"SELECT * FROM (SELECT * FROM {table_name} "
                            f"ORDER BY day DESC LIMIT 30) sub ORDER BY day ASC"
                        )
                    else:
                        # Ranked tables — top entries by events
                        sql = f"SELECT * FROM {table_name} ORDER BY events DESC LIMIT 20"

                    return FastResult(
                        table=table_name,
                        sql=sql,
                        explanation=f"Data from {table_name.replace('profile_', '')} profile",
                        pattern=f'enhanced_{table_name}',
                        intent='enhanced_profile',
                    )

    # ═══════════════════════════════════════════════════════════════
    # NEW PATTERNS
    # ═══════════════════════════════════════════════════════════════

    def _try_average_per(self, q: str, tables: set) -> Optional[FastResult]:
        """
        "Average events per org"
        "Avg sessions per region"
        "Mean events per entity"
        "Median events per org"
        """
        m = re.match(
            r"(?:average|avg|mean|median)\s+(\w+)\s+(?:per|by|for each)\s+(.+)",
            q,
        )
        if not m:
            return None

        metric = self._resolve_metric(m.group(1).strip())
        dim = self._find_dimension(m.group(2).strip(), tables)
        if not metric or not dim:
            return None

        table, sql_col = dim
        agg = "MEDIAN" if "median" in q else "AVG"

        return FastResult(
            table=table,
            sql=(
                f"SELECT {agg}({metric}) AS avg_{metric}, "
                f"MIN({metric}) AS min_{metric}, "
                f"MAX({metric}) AS max_{metric}, "
                f"COUNT(*) AS count "
                f"FROM {table}"
            ),
            explanation=f"{agg.title()} {metric} per {sql_col}",
            pattern="average_per",
            intent="total",
        )

    def _try_filter_dimension(self, q: str, tables: set) -> Optional[FastResult]:
        """
        "Top orgs in EMEA"
        "Entities used in NAM"
        "APAC top 10 orgs"
        "Show orgs for US region"
        """
        # "{region_value}'s top N {dim}"
        m = re.match(
            r"(\w+?)(?:'s|s)?\s+top\s+(\d+)?\s*(.+)",
            q,
        )
        if m:
            region_val = m.group(1).upper()
            n = int(m.group(2)) if m.group(2) else 10
            dim_text = m.group(3).strip()

            if region_val in ("EMEA", "NAM", "APAC", "LATAM", "MEA", "EU", "US"):
                cross = f"profile_org_x_region"
                if cross in tables:
                    dim = self._find_dimension(dim_text, tables)
                    sort_col = "events"
                    return FastResult(
                        table=cross,
                        sql=f"SELECT * FROM {cross} WHERE UPPER(geoname) = '{region_val}' ORDER BY {sort_col} DESC LIMIT {n}",
                        explanation=f"Top {n} in {region_val}",
                        pattern="filter_region_cross",
                        intent="ranking",
                    )

        # "top N {dim} in {value}"
        m = re.match(
            r"top\s+(\d+)?\s*(\w+)\s+(?:in|from|for)\s+(\w+)",
            q,
        )
        if m:
            n = int(m.group(1)) if m.group(1) else 10
            dim_text = m.group(2).strip()
            filter_val = m.group(3).strip().upper()

            for cross_name in tables:
                if "org_x_" in cross_name:
                    return FastResult(
                        table=cross_name,
                        sql=f"SELECT * FROM {cross_name} WHERE UPPER(geoname) = '{filter_val}' ORDER BY events DESC LIMIT {n}",
                        explanation=f"Top {n} {dim_text} in {filter_val}",
                        pattern="filter_top_in",
                        intent="ranking",
                    )

        return None

    def _try_conversational_followup(self, q: str, tables: set) -> Optional[FastResult]:
        """
        "What about regions?"
        "And by entity?"
        "Now show entities"
        "How about the browsers?"
        """
        patterns = [
            r"(?:what about|how about|and (?:by|the)?)\s+(.+)",
            r"(?:now show|now|show)\s+(?:me\s+)?(?:the\s+)?(.+)",
            r"(?:what|how) (?:about|are) (?:the\s+)?(.+)",
            r"(?:and|also)\s+(?:by\s+)?(?:the\s+)?(.+?)(?:\s+too)?$",
            r"^and\s+(.+)$",
        ]
        for pattern in patterns:
            m = re.match(pattern, q)
            if m:
                target = m.group(1).strip()
                # Is it a dimension?
                dim = self._find_dimension(target, tables)
                if dim:
                    table, sql_col = dim
                    return FastResult(
                        table=table,
                        sql=f"SELECT * FROM {table} ORDER BY events DESC",
                        explanation=f"All {sql_col} values",
                        pattern="conversational_followup",
                        intent="lookup",
                    )
        return None

    def _try_specific_value_filter(self, q: str, tables: set) -> Optional[FastResult]:
        """
        "Show me EMEA"
        "Just NAM data"
        "Only Chrome users"
        "Filter to Account entity"
        """
        # Known region values
        regions = {"emea", "nam", "apac", "latam", "mea", "eu", "us", "eur"}
        import re as _re
        for region in regions:
            if _re.search(r'\b' + region + r'\b', q):
                table = "profile_region"
                if table in tables:
                    return FastResult(
                        table=table,
                        sql=f"SELECT * FROM {table} WHERE LOWER(geoname) = '{region}'",
                        explanation=f"Data for {region.upper()}",
                        pattern="specific_region",
                        intent="lookup",
                    )
                # Try cross-tab
                cross = "profile_org_x_region"
                if cross in tables:
                    return FastResult(
                        table=cross,
                        sql=f"SELECT * FROM {cross} WHERE LOWER(geoname) = '{region}' ORDER BY events DESC LIMIT 20",
                        explanation=f"Top orgs in {region.upper()}",
                        pattern="specific_region_cross",
                        intent="ranking",
                    )
        return None

    def _try_new_vs_returning(self, q: str, tables: set) -> Optional[FastResult]:
        """
        "New orgs this week"
        "Which orgs are new?"
        "Recently appeared orgs"
        "Orgs that disappeared"
        "Inactive orgs"
        """
        if "profile_organization" not in tables:
            return None

        if any(w in q for w in ["new org", "recently appeared", "just started", "new customer", "new tenant"]):
            return FastResult(
                table="profile_organization",
                sql=(
                    "SELECT * FROM profile_organization "
                    "WHERE first_seen IS NOT NULL "
                    "ORDER BY first_seen DESC LIMIT 20"
                ),
                explanation="Most recently appeared organizations",
                pattern="new_orgs",
                intent="ranking",
            )

        if any(w in q for w in ["disappear", "inactive", "churned", "stopped", "gone", "lost"]):
            return FastResult(
                table="profile_organization",
                sql=(
                    "SELECT * FROM profile_organization "
                    "WHERE last_seen IS NOT NULL "
                    "ORDER BY last_seen ASC LIMIT 20"
                ),
                explanation="Organizations with oldest last activity",
                pattern="inactive_orgs",
                intent="ranking",
            )

        return None

    def _try_diversity(self, q: str, tables: set) -> Optional[FastResult]:
        """
        "Most diverse orgs" (use most entity types)
        "Orgs using most features"
        "Which orgs use the most entities?"
        """
        if any(w in q for w in ["diverse", "most features", "most entities", "most entity types", "feature adoption"]):
            cross = "profile_org_x_entity"
            if cross in tables:
                return FastResult(
                    table=cross,
                    sql=(
                        f"SELECT orgid, COUNT(DISTINCT entityname) AS entity_count, SUM(events) AS total_events "
                        f"FROM {cross} GROUP BY orgid ORDER BY entity_count DESC LIMIT 15"
                    ),
                    explanation="Organizations by entity type diversity",
                    pattern="diversity",
                    intent="ranking",
                )

            # Fallback to org table
            if "profile_organization" in tables:
                return FastResult(
                    table="profile_organization",
                    sql="SELECT * FROM profile_organization ORDER BY events DESC LIMIT 15",
                    explanation="Top organizations (diversity requires cross-tab)",
                    pattern="diversity_fallback",
                    intent="ranking",
                )

        return None

    def _try_growth_decline(self, q: str, tables: set) -> Optional[FastResult]:
        """
        "Is usage growing?"
        "Are we declining?"
        "Activity going up or down?"
        "Week over week change"
        """
        if "profile_daily" not in tables:
            return None

        growth_words = [
            "growing", "growth", "increasing", "going up",
            "declining", "decreasing", "going down", "shrinking",
            "improving", "worsening",
            "week over week", "wow", "w/w", "week-over-week",
            "month over month", "mom", "m/m",
            "up or down", "direction",
        ]

        if any(w in q for w in growth_words):
            return FastResult(
                table="profile_daily",
                sql=self._last_n_days_sql(28),
                explanation="Last 28 days for growth analysis",
                pattern="growth_check",
                intent="trend",
            )

        return None
