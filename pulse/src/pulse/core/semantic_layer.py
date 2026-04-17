"""
Semantic Layer v3.0 — Config-Driven Intelligence
===================================================

Loads metrics + dimensions from the team's config file.
Auto-generates profile queries from metrics × dimensions.
Resolves user questions to SQL on profile tables.

Architecture:
  Config YAML → metrics + dimensions
  On connect → auto-generate profile queries → execute → DuckDB
  On question → LLM picks table + SQL → instant answer

Author: PULSE Team
"""

import yaml
import json
import logging
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple

logger = logging.getLogger(__name__)


# ── Data Structures ──────────────────────────────────────────────────────────

@dataclass
class Metric:
    id: str
    display_name: str
    kql: str
    description: str


@dataclass
class Dimension:
    id: str
    column: str
    display_name: str
    kql_group: str
    sql_column: str
    description: str
    granularities: Dict[str, str] = field(default_factory=dict)


@dataclass
class ProfileQuery:
    table_name: str
    kql: str
    description: str
    answers: List[str] = field(default_factory=list)
    tier: int = 1  # 1=essential (on connect), 2=background, 3=on-demand
    scope_days: int = 180  # ★ v7.0: actual scope for staleness labels


@dataclass
class SemanticModel:
    metrics: Dict[str, Metric]
    dimensions: Dict[str, Dimension]
    profile_queries: Dict[str, ProfileQuery]
    time_scope: str
    time_column: str


# ── LLM Resolution Prompt ────────────────────────────────────────────────────

SEMANTIC_RESOLVE_PROMPT = """You are a data analyst AI for a telemetry analytics platform called PULSE.
The user is a PM or Customer Success Manager asking questions about their product usage data.
Your job: translate ANY natural language question into a SQL query against the pre-aggregated profile tables.

AVAILABLE PROFILE TABLES (pre-aggregated in local DuckDB — use these first, they are instant):
{profile_tables}

AVAILABLE METRICS:
{metrics}

AVAILABLE DIMENSIONS:
{dimensions}

═══════════════════════════════════════════════════════════════
RULES
═══════════════════════════════════════════════════════════════
1. ALWAYS answer from profile tables. They are pre-aggregated and instant.
2. Output ONLY valid JSON — no markdown, no explanation, no backticks.
3. ALL column names in profile tables are LOWERCASE.
4. Only use "live_kusto" if the question genuinely requires raw event-level data
   that CANNOT be answered from the profile tables (e.g. filter by a specific user session).
5. For vague/open-ended questions, pick the MOST RELEVANT table and return useful data.

═══════════════════════════════════════════════════════════════
QUESTION → TABLE MAPPING
═══════════════════════════════════════════════════════════════
"anything to worry / any issues / health / anomalies / problems"
  → profile_daily  — look at recent trend, flag drops
  → SQL: SELECT * FROM profile_daily ORDER BY day DESC LIMIT 14

"how are we doing / overall / summary / what happened"
  → profile_totals or profile_daily
  → SQL: SELECT * FROM profile_totals

"top customers / biggest orgs / most active"
  → profile_organization ORDER BY events DESC LIMIT 10

"bottom / lowest / least active / at risk"
  → profile_organization ORDER BY events ASC LIMIT 10

"by region / regional breakdown / EMEA / NAM"
  → profile_region ORDER BY events DESC

"trend / over time / daily / growing / declining"
  → profile_daily ORDER BY day ASC

"how many orgs / count / total orgs"
  → profile_totals  (has pre-computed totals)

"what are users doing / actions / activity / event types / events breakdown"
  → profile_activity ORDER BY events DESC (if table exists)

"pipeline / ingestion / lag / delay / data freshness"
  → profile_ingestion_lag ORDER BY day ASC (if table exists)

"engagement / users / sessions / active users / sessions per user"
  → profile_engagement ORDER BY day ASC (if table exists)

"version / app version / rollout / adoption / which version"
  → profile_version ORDER BY events DESC (if table exists)

"say something / what can you tell me / show me data"
  → profile_totals — give the high-level overview

═══════════════════════════════════════════════════════════════
OUTPUT FORMAT (profile — preferred)
═══════════════════════════════════════════════════════════════
{{
  "source": "profile",
  "table": "profile_organization",
  "sql": "SELECT * FROM profile_organization ORDER BY events DESC LIMIT 10",
  "explanation": "Top 10 orgs by events"
}}

OUTPUT FORMAT (live fallback — only when truly necessary)
{{
  "source": "live_kusto",
  "reason": "Need raw event-level data for specific session ID"
}}

IMPORTANT:
- For ANY health/worry/issue question → use profile_daily, show last 14 days
- For ANY vague question → default to profile_totals for high-level view
- For ANY org/customer question → use profile_organization
- For ANY region question → use profile_region
- For ANY "what are users doing" / activity / action question → use profile_activity (if it exists in the table list above)
- For ANY engagement / sessions / active users question → use profile_engagement (if it exists)
- For ANY version / rollout question → use profile_version (if it exists)
- For ANY pipeline / lag / freshness question → use profile_ingestion_lag (if it exists)
- NEVER return live_kusto just because the question is vague — always try profile tables first
- NEVER return live_kusto when a matching profile table exists — profile tables are FASTER and more reliable
- Use standard SQL (DuckDB): SELECT, WHERE, ORDER BY, LIMIT, AVG, SUM, COUNT, ROUND
- Column names: events, active_orgs, orgid, geoname, day, entityname (all lowercase)
"""


class SemanticLayer:
    """Loads config, auto-generates profile queries, resolves questions."""

    def __init__(self, config_path: str = None, raw_config: Dict = None):
        """
        Load from either a file path or a raw config dict.
        The config is the MERGED format (cluster + metrics + dimensions in one file).
        """
        self.model: Optional[SemanticModel] = None

        if raw_config:
            self._parse_config(raw_config)
        elif config_path:
            self._load_from_file(config_path)

    def _load_from_file(self, path: str):
        p = Path(path)
        if not p.exists():
            logger.warning(f"Config not found: {p}")
            return
        with open(p, encoding='utf-8') as f:
            raw = yaml.safe_load(f)
        self._parse_config(raw)

    def _parse_config(self, raw: Dict):
        """Parse metrics + dimensions from merged config."""
        profile_cfg = raw.get('profile', {})
        time_scope = profile_cfg.get('time_scope', '365d')
        time_column = profile_cfg.get('time_column', 'EventInfo_Time')

        # Parse metrics
        metrics = {}
        for mid, m in raw.get('metrics', {}).items():
            metrics[mid] = Metric(
                id=mid,
                display_name=m.get('display_name', mid),
                kql=m['kql'],
                description=m.get('description', ''),
            )

        # Parse dimensions
        dimensions = {}
        for did, d in raw.get('dimensions', {}).items():
            dimensions[did] = Dimension(
                id=did,
                column=d['column'],
                display_name=d.get('display_name', did),
                kql_group=d.get('kql_group', d['column']),
                sql_column=d.get('sql_column', d['column']),
                description=d.get('description', ''),
                granularities=d.get('granularities', {}),
            )

        # Auto-generate profile queries from metrics × dimensions
        ingestion_time_column = profile_cfg.get('ingestion_time_column', None)
        profile_queries = self._generate_profile_queries(
            metrics, dimensions, time_scope, time_column,
            ingestion_time_column=ingestion_time_column,
        )

        self.model = SemanticModel(
            metrics=metrics,
            dimensions=dimensions,
            profile_queries=profile_queries,
            time_scope=time_scope,
            time_column=time_column,
        )

        logger.info(
            f"Semantic model loaded: {len(metrics)} metrics, "
            f"{len(dimensions)} dimensions → "
            f"{len(profile_queries)} auto-generated profile queries"
        )

    # ═══════════════════════════════════════════════════════════════════════════
    # ★ Auto-Generate Profile Queries
    # ═══════════════════════════════════════════════════════════════════════════

    def _generate_profile_queries(
        self,
        metrics: Dict[str, Metric],
        dimensions: Dict[str, Dimension],
        time_scope: str,
        time_column: str,
        ingestion_time_column: str = None,
    ) -> Dict[str, ProfileQuery]:
        """
        Generate LIGHTWEIGHT profile queries.

        Key optimizations vs v3.0:
          - Dimension queries use count() + dcount(OrgId) ONLY (not all 5 metrics)
          - Daily trend uses count() + dcount(OrgId) ONLY
          - Totals query keeps all metrics (just 1 row, always fast)
          - 180-day scope for most tables, 90-day for high-cardinality
          - Cross-tabs are tier 3 (on-demand) with minimal metrics

        This prevents the 5GB memory exceeded errors and timeouts.
        """
        queries = {}

        # ── Scope policy — PULSE owns 6 months (180 days) ────────────────
        # Data retention is ~1 year. We scope to 6 months — enough for
        # trend analysis, quarterly comparisons, and seasonal patterns.
        # Beyond 180d → clipboard KQL handoff.
        #
        #   profile_daily      → 180d (dates+counts only, ~180 rows, always fast)
        #   profile_region     → 180d (low cardinality, safe)
        #   profile_totals     → 180d (single row, always fast)
        #   profile_org        → 90d  (high cardinality 10K+ orgs, keep safe)
        #   profile_entity     → 180d (medium cardinality, capped)
        #   cross-tabs         → 60d  (tier 3 on-demand, keep cheap)
        #   profile_activity   → 90d  (scan-heavy: full event scan + group by)
        #   profile_lag        → 90d  (scan-heavy: percentile computation)
        #   profile_engagement → 180d (daily aggregate, safe)
        #   profile_version    → 90d  (high cardinality)

        DAILY_SCOPE  = '180d'
        REGION_SCOPE = '180d'
        ORG_SCOPE    = '90d'
        XTAB_SCOPE   = '60d'
        # ★ Scan-heavy queries (activity, ingestion_lag) scan every event row.
        # 180d on billion-event clusters causes timeouts.
        # 90d keeps them fast while still giving a full quarter view.
        SCAN_HEAVY_SCOPE = '90d'

        logger.info(f"Profile scopes — daily:{DAILY_SCOPE} region:{REGION_SCOPE} org:{ORG_SCOPE} scan-heavy:{SCAN_HEAVY_SCOPE}")

        # Light metrics for dimension queries: just count + primary dcount
        light_metrics = f"events={metrics['events'].kql}"
        if 'active_orgs' in metrics:
            # Use hll() for daily/region aggregates — approx dcount, 5-10x faster
            # on 100B+ event datasets. ~2% error vs exact dcount. Worth the trade.
            ao_kql = metrics['active_orgs'].kql
            # hll() wraps dcount expressions: dcount(X) → hll(X) then dcount_hll(hll(X))
            # But we store the hll sketch and compute at query time for max speed.
            # Use dcount() directly — approx_countdistinct not supported on all clusters
            if 'dcount(' in ao_kql:
                # Keep dcount() — works on all Kusto versions
                light_ao = f"active_orgs={ao_kql}"
            else:
                light_ao = f"active_orgs={ao_kql}"
            light_metrics += f", {light_ao}"

        # Full metrics only for totals (1 row, always fast)
        full_metric_exprs = [f"total_{m.id}={m.kql}" for m in metrics.values()]

        time_filter     = f"where {time_column} > ago({DAILY_SCOPE})"
        dim_time_filter = f"where {time_column} > ago({REGION_SCOPE})"
        org_time_filter = f"where {time_column} > ago({ORG_SCOPE})"
        xtab_time_filter = f"where {time_column} > ago({XTAB_SCOPE})"
        scan_heavy_filter = f"where {time_column} > ago({SCAN_HEAVY_SCOPE})"


        # -- 1. Grand totals (1 row, all metrics, full scope) --
        total_exprs = full_metric_exprs + [
            f"first_event=min({time_column})",
            f"last_event=max({time_column})",
        ]
        queries['profile_totals'] = ProfileQuery(
            table_name='profile_totals',
            kql=f"{time_filter}\n| summarize {', '.join(total_exprs)}",
            description="Grand totals",
            answers=["how many total events", "how many orgs", "overview"],
            tier=1,
            scope_days=180,
        )

        # -- 2. Daily trend (light metrics only, full scope) --
        non_time_dims = {k: v for k, v in dimensions.items() if k != 'time'}

        if 'time' in dimensions:
            time_dim = dimensions['time']
            queries['profile_daily'] = ProfileQuery(
                table_name='profile_daily',
                kql=(
                    f"{time_filter}\n"
                    f"| summarize {light_metrics} by {time_dim.kql_group}\n"
                    f"| order by {time_dim.sql_column} asc"
                ),
                description="Daily activity trends",
                answers=["events per day", "daily trends", "is usage growing"],
                tier=1,
                scope_days=180,
            )

        # -- 3. Per-dimension tables (light metrics, 180d/90d scope) --
        HIGH_CARD_DIMS = {'organization', 'country', 'activity', 'version', 'event_name'}

        for dim_id, dim in non_time_dims.items():
            table_name = f"profile_{dim_id}"

            extra = ""
            if dim_id == 'organization':
                extra = f", first_seen=min({time_column}), last_seen=max({time_column})"

            dim_tier = 3 if dim_id in HIGH_CARD_DIMS else 1  # high-card: on-demand only

            # ★ hint.shufflekey for high-cardinality dims (org, country)
            # Forces distributed execution on Kusto instead of single-node
            # group-by, significantly faster for 10k+ distinct values.
            if dim_id in HIGH_CARD_DIMS:
                summarize_clause = f"hint.shufflekey={dim.kql_group} {light_metrics}{extra}"
            else:
                summarize_clause = f"{light_metrics}{extra}"

            # High-card dims (org, country) use shorter scope to avoid timeout
            _q_filter = org_time_filter if dim_id in HIGH_CARD_DIMS else dim_time_filter

            queries[table_name] = ProfileQuery(
                table_name=table_name,
                kql=(
                    f"{_q_filter}\n"
                    f"| summarize {summarize_clause} by {dim.kql_group}\n"
                    f"| order by events desc"
                ),
                description=f"Metrics grouped by {dim.display_name}",
                answers=[
                    f"top N by {dim.display_name}",
                    f"events per {dim.display_name}",
                ],
                tier=dim_tier,
                scope_days=90 if dim_id in HIGH_CARD_DIMS else 180,
            )

        # -- 4. Cross-tabs (tier 3, on-demand, minimal) --
        if 'organization' in non_time_dims:
            for other_id in non_time_dims:
                if other_id == 'organization':
                    continue
                other_dim = non_time_dims[other_id]
                table_name = f"profile_org_x_{other_id}"

                queries[table_name] = ProfileQuery(
                    table_name=table_name,
                    kql=(
                        f"{xtab_time_filter}\n"
                        f"| summarize events={metrics['events'].kql} "
                        f"by OrgId, {other_dim.kql_group}"
                    ),
                    description=f"Organization x {other_dim.display_name}",
                    answers=[f"orgs per {other_dim.display_name}"],
                    tier=3,
                    scope_days=60,
                )

        if 'region' in non_time_dims and 'entity' in non_time_dims:
            r, e = non_time_dims['region'], non_time_dims['entity']
            queries['profile_region_x_entity'] = ProfileQuery(
                table_name='profile_region_x_entity',
                kql=(
                    f"{xtab_time_filter}\n"
                    f"| summarize events={metrics['events'].kql} "
                    f"by {r.kql_group}, {e.kql_group}"
                ),
                description="Region x Entity breakdown",
                answers=["entity usage by region"],
                tier=3,
                scope_days=60,
            )

        logger.info(f"Auto-generated {len(queries)} profile queries")

        # ══════════════════════════════════════════════════════════════════
        # ★ ENHANCED PROFILES — conditionally generated based on schema
        # ══════════════════════════════════════════════════════════════════
        # These provide deeper analytical context. Each checks whether the
        # required columns/metrics exist. Teams without them simply don't
        # get these tables — no errors, no fallbacks.

        _dim_cols = {d.column.lower() for d in dimensions.values()}
        _dim_ids = set(dimensions.keys())
        _metric_ids = set(metrics.keys())

        # ── profile_activity: top event names by count ────────────────────
        # Requires: EventInfo_Name or equivalent column
        # Answers: "what are users doing?", "which actions are most common?"
        _activity_dim = None
        for did, d in dimensions.items():
            if did in ('activity', 'event_name') or d.column.lower() == 'eventinfo_name':
                _activity_dim = d
                break
        if _activity_dim:
            queries['profile_activity'] = ProfileQuery(
                table_name='profile_activity',
                kql=(
                    f"{scan_heavy_filter}\n"
                    f"| summarize events={metrics['events'].kql} by {_activity_dim.kql_group}\n"
                    f"| order by events desc\n"
                    f"| take 30"
                ),
                description="Top event/action types by count",
                answers=["what are users doing", "most common actions", "event breakdown"],
                tier=1,
                scope_days=90,
            )
            logger.info(f"Enhanced: profile_activity (column: {_activity_dim.column})")

        # ── profile_ingestion_lag: pipeline health ────────────────────────
        # Requires: ingestion_time_column in profile config
        # Answers: "any pipeline issues?", "ingestion delay?"
        _ingestion_col = ingestion_time_column
        if _ingestion_col:
            queries['profile_ingestion_lag'] = ProfileQuery(
                table_name='profile_ingestion_lag',
                kql=(
                    f"{scan_heavy_filter}\n"
                    f"| extend lag_seconds = datetime_diff('second', {_ingestion_col}, {time_column})\n"
                    f"| summarize\n"
                    f"    avg_lag=avg(lag_seconds),\n"
                    f"    p50_lag=percentile(lag_seconds, 50),\n"
                    f"    p95_lag=percentile(lag_seconds, 95),\n"
                    f"    max_lag=max(lag_seconds),\n"
                    f"    events={metrics['events'].kql}\n"
                    f"  by Day=startofday({time_column})\n"
                    f"| order by Day asc"
                ),
                description="Daily ingestion lag (pipeline health)",
                answers=["pipeline issues", "ingestion delay", "data freshness", "lag"],
                tier=1,
                scope_days=90,
            )
            logger.info(f"Enhanced: profile_ingestion_lag (column: {_ingestion_col})")

        # ── profile_engagement: daily users + sessions ────────────────────
        # Requires: active_users AND sessions metrics
        # Answers: "are users engaged?", "how many users?", "sessions per user?"
        if 'active_users' in _metric_ids and 'sessions' in _metric_ids:
            queries['profile_engagement'] = ProfileQuery(
                table_name='profile_engagement',
                kql=(
                    f"{time_filter}\n"
                    f"| summarize\n"
                    f"    users={metrics['active_users'].kql},\n"
                    f"    sessions={metrics['sessions'].kql},\n"
                    f"    events={metrics['events'].kql}\n"
                    f"  by Day=startofday({time_column})\n"
                    f"| extend sessions_per_user=round(toreal(sessions)/toreal(users), 1)\n"
                    f"| order by Day asc"
                ),
                description="Daily user engagement (users, sessions, depth)",
                answers=["user engagement", "how many users", "sessions per user", "active users"],
                tier=2,
                scope_days=180,
            )
            logger.info("Enhanced: profile_engagement (users + sessions)")

        # ── profile_version: app version adoption ─────────────────────────
        # Requires: version dimension (AppVersion or ClientVersion)
        # Answers: "which version?", "version adoption?", "rollout?"
        _version_dim = None
        for did, d in dimensions.items():
            if did in ('version', 'app_version') or d.column.lower() in ('appversion', 'clientversion'):
                _version_dim = d
                break
        if _version_dim:
            queries['profile_version'] = ProfileQuery(
                table_name='profile_version',
                kql=(
                    f"{org_time_filter}\n"
                    f"| where isnotempty({_version_dim.column})\n"
                    f"| summarize events={metrics['events'].kql}"
                    + (f", active_orgs={metrics['active_orgs'].kql}" if 'active_orgs' in _metric_ids else "")
                    + f" by {_version_dim.kql_group}\n"
                    f"| order by events desc\n"
                    f"| take 20"
                ),
                description=f"App version adoption ({_version_dim.column})",
                answers=["which version", "version adoption", "rollout", "app version"],
                tier=2,
                scope_days=90,
            )
            logger.info(f"Enhanced: profile_version (column: {_version_dim.column})")

        logger.info(f"Total profile queries: {len(queries)} "
                     f"({len(queries) - len([q for q in queries if q.startswith('profile_')])} cross-tabs)")

        return queries

    @staticmethod
    def _parse_days(scope: str) -> int:
        """Parse '365d' -> 365, '90d' -> 90."""
        try:
            return int(scope.replace('d', '').replace('D', ''))
        except ValueError:
            return 365

    # ═══════════════════════════════════════════════════════════════════════════
    # Schema Validation
    # ═══════════════════════════════════════════════════════════════════════════

    def validate_against_schema(self, discovered_columns: Dict[str, str]) -> Dict:
        """
        Validate that metrics/dimensions reference columns that exist in the cluster.

        Args:
            discovered_columns: {column_name: type} from .show table schema

        Returns:
            {'valid': bool, 'warnings': [...], 'confirmed': [...]}
        """
        if not self.model:
            return {'valid': False, 'warnings': ['No semantic model loaded']}

        col_names = {c.lower() for c in discovered_columns.keys()}
        warnings = []
        confirmed = []

        # Check dimension columns exist
        for dim in self.model.dimensions.values():
            if dim.column.lower() in col_names:
                confirmed.append(f"✓ Dimension '{dim.display_name}' → {dim.column}")
            else:
                warnings.append(
                    f"⚠ Dimension '{dim.display_name}' references column "
                    f"'{dim.column}' which was NOT found in schema"
                )

        # Check metric columns (extract column references from KQL expressions)
        import re
        for m in self.model.metrics.values():
            # Extract column names from expressions like dcount(OrgId), count()
            cols_in_expr = re.findall(r'dcount\((\w+)\)|sum\((\w+)\)', m.kql)
            for col_tuple in cols_in_expr:
                col = next(c for c in col_tuple if c)
                if col.lower() in col_names:
                    confirmed.append(f"✓ Metric '{m.display_name}' → {col}")
                else:
                    warnings.append(
                        f"⚠ Metric '{m.display_name}' references column "
                        f"'{col}' which was NOT found in schema"
                    )

        return {
            'valid': len(warnings) == 0,
            'warnings': warnings,
            'confirmed': confirmed,
        }

    def prune_queries_by_schema(self, discovered_columns: Dict[str, str]):
        """Remove profile queries that reference missing columns."""
        if not self.model:
            return

        col_names = {c.lower() for c in discovered_columns.keys()}
        to_remove = []

        for qname, pq in self.model.profile_queries.items():
            # Check if all dimension columns in the KQL exist
            kql_lower = pq.kql.lower()
            for dim in self.model.dimensions.values():
                if dim.column.lower() in kql_lower and dim.column.lower() not in col_names:
                    logger.warning(
                        f"Pruning profile query '{qname}' — "
                        f"column '{dim.column}' not in schema"
                    )
                    to_remove.append(qname)
                    break

        for qname in to_remove:
            del self.model.profile_queries[qname]

        if to_remove:
            logger.info(f"Pruned {len(to_remove)} profile queries due to missing columns")

    # ═══════════════════════════════════════════════════════════════════════════
    # Query Resolution
    # ═══════════════════════════════════════════════════════════════════════════

    @property
    def is_loaded(self) -> bool:
        return self.model is not None

    def get_profile_queries(self) -> Dict[str, ProfileQuery]:
        if not self.model:
            return {}
        return self.model.profile_queries

    def get_queries_by_tier(self, max_tier: int = 3) -> Dict[str, ProfileQuery]:
        """Get profile queries up to a given tier level."""
        if not self.model:
            return {}
        return {
            name: pq for name, pq in self.model.profile_queries.items()
            if pq.tier <= max_tier
        }

    def build_resolve_prompt(self, profile_summaries: Dict[str, str] = None) -> str:
        """Build the prompt the LLM uses to route questions."""
        if not self.model:
            return "No semantic model loaded."

        # Profile tables
        table_lines = []
        for name, pq in self.model.profile_queries.items():
            answers_str = ", ".join(pq.answers[:4]) if pq.answers else ""
            table_lines.append(
                f"  {name}: {pq.description}"
                + (f"\n    Answers: {answers_str}" if answers_str else "")
            )
            if profile_summaries and name in profile_summaries:
                table_lines.append(f"    {profile_summaries[name]}")

        # Metrics
        metric_lines = []
        for m in self.model.metrics.values():
            metric_lines.append(f"  {m.id}: {m.display_name} — {m.description}")

        # Dimensions
        dim_lines = []
        for d in self.model.dimensions.values():
            dim_lines.append(
                f"  {d.id}: {d.display_name} (column: {d.sql_column}) — {d.description}"
            )

        return SEMANTIC_RESOLVE_PROMPT.format(
            profile_tables="\n".join(table_lines),
            metrics="\n".join(metric_lines),
            dimensions="\n".join(dim_lines),
        )

    def resolve_question(
        self,
        question: str,
        llm_client,
        llm_model: str,
        profile_summaries: Dict[str, str] = None,
        conversation_context: str = "",
    ) -> Dict[str, Any]:
        """
        Ask the LLM to resolve a question using the semantic model.

        Returns:
            {
                'source': 'profile' | 'live_kusto',
                'table': str | None,
                'sql': str | None,
                'explanation': str,
            }
        """
        system = self.build_resolve_prompt(profile_summaries)

        user_parts = []
        if conversation_context:
            user_parts.append(f"CONTEXT:\n{conversation_context}\n")
        user_parts.append(f"QUESTION: {question}")
        user_parts.append("\nResolve this (JSON only):")

        response = llm_client.chat.completions.create(
            model=llm_model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": "\n".join(user_parts)},
            ],
            max_tokens=600,
            temperature=0.1,
        )

        raw = response.choices[0].message.content.strip()
        raw = raw.replace("```json", "").replace("```", "").strip()

        try:
            result = json.loads(raw)
            logger.info(
                f"Semantic resolve: source={result.get('source')}, "
                f"table={result.get('table')}, sql={result.get('sql', '')[:80]}"
            )
            return result
        except json.JSONDecodeError as e:
            logger.error(f"Semantic resolve JSON failed: {e}\nRaw: {raw[:300]}")
            return {
                'source': 'live_kusto',
                'reason': 'Failed to parse LLM response',
            }

    def get_kql_schema_context(self) -> str:
        """Rich schema context for when we DO need live KQL fallback."""
        if not self.model:
            return ""

        lines = [
            "EACH ROW = one telemetry event.",
            f"TIME SCOPE: Last {self.model.time_scope} (auto-filtered).",
            "",
            "KEY COLUMNS:",
        ]
        for d in self.model.dimensions.values():
            lines.append(f"  {d.column} — {d.description}")

        lines.append("")
        lines.append("METRICS (counting patterns):")
        for m in self.model.metrics.values():
            lines.append(f"  {m.display_name}: {m.kql}")

        return "\n".join(lines)
