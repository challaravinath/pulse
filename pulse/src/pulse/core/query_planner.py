"""
Query Planner v1.0 — Multi-Step Query Decomposition Engine
============================================================

The killer feature. Handles questions that can't be answered in a
single KQL query by breaking them into steps:

  Step 1: KQL → fetch raw data from Kusto → cache in DuckDB
  Step 2: SQL → slice/pivot/aggregate locally
  Step 3: SQL → combine/compare results
  ...

Architecture:
  User question → LLM produces a Plan (list of Steps)
  → Executor runs each Step, passing intermediate results via DuckDB
  → Final step's result is returned to the user

Error recovery: if a step fails, the LLM gets one retry with the
error message as context.

Author: PULSE Team
"""

import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Any, List, Optional
import pandas as pd

logger = logging.getLogger(__name__)


# ── Data Structures ──────────────────────────────────────────────────────────

class StepType(Enum):
    KQL = "kql"       # Run on Kusto (fetches new data)
    SQL = "sql"       # Run on DuckDB (operates on cached data)


@dataclass
class PlanStep:
    """Single step in a query plan."""
    step_number: int
    description: str          # Human-readable "what this step does"
    step_type: StepType       # kql or sql
    query: str                # The KQL clause or SQL statement
    output_table: str         # DuckDB table name for this step's result
    depends_on: List[str] = field(default_factory=list)  # Tables this step reads


@dataclass
class PlanResult:
    """Result of executing a complete plan."""
    success: bool
    final_df: Optional[pd.DataFrame]
    steps_executed: List[Dict[str, Any]]  # Log of each step
    total_time_ms: float
    error: Optional[str] = None


# ── Planning Prompt ──────────────────────────────────────────────────────────

PLANNER_PROMPT = """You are a query planner for a telemetry analysis system. Your job is to
break complex data questions into a sequence of simple steps.

AVAILABLE TOOLS:
1. KQL step: Runs a Kusto query. Use for fetching raw data from the database.
   - Do NOT include table names (we prepend them automatically)
   - Do NOT include a leading pipe
   - Must start with a valid operator: where, summarize, project, extend, order, take

2. SQL step: Runs DuckDB SQL on previously fetched data.
   - References tables from prior steps by their output_table name
   - Standard SQL syntax
   - Great for: filtering, pivoting, joins, window functions, percentages

SCHEMA:
{schema}

RULES:
1. The FIRST step should ALWAYS be a KQL step that fetches broad data from Kusto.
   Fetch MORE data than needed — it's cheaper to filter locally than make multiple Kusto calls.
2. Subsequent steps should be SQL steps that refine the fetched data.
3. Use at most 4 steps. Simpler is better.
4. Each step's output_table must be unique (e.g., "step_1", "step_2").
5. SQL steps should reference prior step tables: SELECT * FROM step_1 WHERE ...
6. The LAST step produces the final result the user sees.

OUTPUT FORMAT (respond with ONLY this JSON, no markdown):
{{
  "needs_planning": true,
  "reasoning": "Brief explanation of why multiple steps are needed",
  "steps": [
    {{
      "step_number": 1,
      "description": "Fetch all org activity with entity breakdown",
      "step_type": "kql",
      "query": "summarize Events=count(), Entities=dcount(EntityName) by OrgId, EntityName",
      "output_table": "step_1",
      "depends_on": []
    }},
    {{
      "step_number": 2,
      "description": "Find orgs active in multiple regions",
      "step_type": "sql",
      "query": "SELECT OrgId, COUNT(DISTINCT EntityName) as EntityCount FROM step_1 GROUP BY OrgId HAVING COUNT(DISTINCT EntityName) > 3 ORDER BY EntityCount DESC",
      "output_table": "step_2",
      "depends_on": ["step_1"]
    }}
  ]
}}

If the question CAN be answered in a single KQL query, respond with:
{{
  "needs_planning": false,
  "reasoning": "Simple single-query question",
  "single_kql": "summarize Events=count() by OrgId | order by Events desc | take 10"
}}

IMPORTANT: Keep KQL steps broad (fetch lots of data). Do the slicing/comparing in SQL steps.

EXAMPLES OF QUESTIONS THAT NEED PLANNING:
- "Which orgs are active in multiple regions?" → KQL: get org+region data → SQL: count distinct regions per org → SQL: filter >1
- "What's the breakdown of the top org?" → KQL: get all data → SQL: find top org → SQL: get its entity/region/day breakdown
- "Compare entity usage between top 5 and bottom 5 orgs" → KQL: get org+entity data → SQL: rank orgs → SQL: compare groups
- "Entities used in EU but not in US" → KQL: get entity+region data → SQL: EXCEPT query
- "Crosstab of region vs entity" → KQL: get counts by region+entity → SQL: pivot
- "Which entity type is growing fastest?" → KQL: get daily entity counts → SQL: compute slope per entity → SQL: rank

EXAMPLES OF QUESTIONS THAT DO NOT NEED PLANNING:
- "Top 10 orgs by events" → single KQL
- "Events per region" → single KQL
- "Daily event trend" → single KQL
"""


# ── Complexity Detector ──────────────────────────────────────────────────────

# Keywords that suggest a question needs multi-step planning
COMPLEXITY_SIGNALS = [
    # Multi-entity analysis
    "breakdown of the top", "breakdown of top", "detail on the top",
    "deep dive into", "drill into the top",
    # Set operations
    "but not in", "used in .* but not", "only in .* not in",
    "exclusive to", "unique to",
    # Cross-dimensional
    "crosstab", "cross-tab", "pivot", "matrix of",
    "heatmap of .* vs", "by .* and .*",
    # Comparative groups
    "compare top .* vs bottom", "compare .* with .*",
    "top .* versus bottom", "power users vs",
    # Multi-region/temporal
    "active in multiple", "appeared in more than one",
    "used all", "used every",
    # Growth/ranking analysis
    "growing fastest", "declining fastest", "most improved",
    "biggest change", "rate of change",
    # Conditional / having
    "orgs that have more than .* entity",
    "orgs that use .* and .*",
    "only appeared on one day",
]

# Simple patterns that definitely DON'T need planning
SIMPLE_SIGNALS = [
    "how many", "total", "count of",
    "top \\d+", "bottom \\d+",
    "events per day", "events per region",
    "sessions per", "orgs per",
    "most used", "least used",
    "busiest day", "quietest day",
]


class QueryPlanner:
    """Decomposes complex questions into multi-step execution plans."""

    def __init__(self, llm_service, kusto_client, duckdb_engine):
        self.llm_service = llm_service
        self.kusto_client = kusto_client
        self.duckdb_engine = duckdb_engine

    # ── Public API ───────────────────────────────────────────────────────────

    def needs_planning(self, question: str) -> bool:
        """Quick heuristic check: does this question need multi-step planning?"""
        q = question.lower()

        import re

        # ★ Check complexity signals FIRST (they override simple patterns)
        for pattern in COMPLEXITY_SIGNALS:
            if re.search(pattern, q):
                return True

        # Then check simple signals (fast exit for obviously simple queries)
        for pattern in SIMPLE_SIGNALS:
            if re.search(pattern, q):
                return False

        # Count analytical dimensions requested
        dimensions = 0
        dim_keywords = [
            ("org", ["org", "organization"]),
            ("region", ["region", "geo", "country", "emea", "apac"]),
            ("entity", ["entity", "feature", "module"]),
            ("time", ["day", "week", "trend", "over time", "daily"]),
            ("session", ["session"]),
        ]
        for dim_name, keywords in dim_keywords:
            if any(k in q for k in keywords):
                dimensions += 1

        # If question touches 3+ dimensions, likely needs planning
        if dimensions >= 3:
            return True

        # Questions with "and" connecting multiple analyses
        if q.count(" and ") >= 2:
            return True

        return False

    def create_plan(
        self,
        question: str,
        schema_context: str,
        conversation_context: str = ""
    ) -> List[PlanStep]:
        """Ask LLM to create an execution plan."""

        system = PLANNER_PROMPT.format(schema=schema_context)

        user_parts = []
        if conversation_context:
            user_parts.append(f"CONTEXT:\n{conversation_context}\n")
        user_parts.append(f"QUESTION: {question}")
        user_parts.append("\nCreate the plan (JSON only):")

        response = self.llm_service.client.chat.completions.create(
            model=self.llm_service.model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": "\n".join(user_parts)}
            ],
            max_tokens=1200,
            temperature=0.15,
        )

        raw = response.choices[0].message.content.strip()
        raw = raw.replace("```json", "").replace("```", "").strip()

        try:
            plan_data = json.loads(raw)
        except json.JSONDecodeError as e:
            logger.error(f"Plan JSON parse failed: {e}\nRaw: {raw[:500]}")
            raise ValueError(
                "I had trouble breaking this question into steps. "
                "Try rephrasing — e.g., be more specific about what you want to compare."
            )

        # ── If LLM says no planning needed, return single-step plan ──
        if not plan_data.get("needs_planning", True):
            kql = plan_data.get("single_kql", "")
            if kql:
                return [PlanStep(
                    step_number=1,
                    description="Direct query",
                    step_type=StepType.KQL,
                    query=kql,
                    output_table="step_1",
                    depends_on=[],
                )]
            raise ValueError("Planner returned no query.")

        # ── Parse multi-step plan ────────────────────────────────────
        steps = []
        for s in plan_data.get("steps", []):
            step = PlanStep(
                step_number=s["step_number"],
                description=s["description"],
                step_type=StepType(s["step_type"]),
                query=s["query"],
                output_table=s["output_table"],
                depends_on=s.get("depends_on", []),
            )
            steps.append(step)

        if not steps:
            raise ValueError("Planner produced empty plan.")

        # Validate: first step must be KQL
        if steps[0].step_type != StepType.KQL:
            logger.warning("First step is not KQL — reordering.")
            kql_steps = [s for s in steps if s.step_type == StepType.KQL]
            sql_steps = [s for s in steps if s.step_type == StepType.SQL]
            steps = kql_steps + sql_steps
            for i, s in enumerate(steps):
                s.step_number = i + 1

        # Cap at 5 steps
        if len(steps) > 5:
            logger.warning(f"Plan has {len(steps)} steps — capping at 5")
            steps = steps[:5]

        logger.info(f"Plan: {len(steps)} steps — "
                     + " → ".join(f"[{s.step_type.value}] {s.description}" for s in steps))

        return steps

    def execute_plan(
        self,
        steps: List[PlanStep],
        schema_validator=None,
    ) -> PlanResult:
        """Execute a multi-step plan and return the final result."""

        start = time.time()
        step_log: List[Dict[str, Any]] = []

        # ── Ensure DuckDB has a connection for intermediate tables ───
        if not self.duckdb_engine.connection:
            # Create a fresh connection for planning
            import duckdb
            self.duckdb_engine.connection = duckdb.connect(':memory:')
            self.duckdb_engine.loaded = True

        final_df = None

        for step in steps:
            step_start = time.time()
            logger.info(f"Step {step.step_number}: [{step.step_type.value}] {step.description}")

            try:
                if step.step_type == StepType.KQL:
                    df = self._execute_kql_step(step, schema_validator)
                else:
                    df = self._execute_sql_step(step)

                # Store result as a named table in DuckDB
                self._store_intermediate(step.output_table, df)
                final_df = df

                elapsed = (time.time() - step_start) * 1000
                step_log.append({
                    'step': step.step_number,
                    'type': step.step_type.value,
                    'description': step.description,
                    'query': step.query,
                    'rows': len(df),
                    'columns': list(df.columns),
                    'time_ms': round(elapsed, 1),
                    'status': 'success',
                })
                logger.info(f"  ✓ Step {step.step_number}: {len(df):,} rows in {elapsed:.0f}ms")

            except Exception as e:
                logger.warning(f"  ✗ Step {step.step_number} failed: {e}")

                # ── Retry with error context ─────────────────────────
                retry_df = self._retry_step(step, str(e))
                if retry_df is not None:
                    self._store_intermediate(step.output_table, retry_df)
                    final_df = retry_df
                    elapsed = (time.time() - step_start) * 1000
                    step_log.append({
                        'step': step.step_number,
                        'type': step.step_type.value,
                        'description': step.description,
                        'query': step.query + " (retried)",
                        'rows': len(retry_df),
                        'columns': list(retry_df.columns),
                        'time_ms': round(elapsed, 1),
                        'status': 'retried',
                    })
                    logger.info(f"  ✓ Step {step.step_number} succeeded on retry: {len(retry_df):,} rows")
                else:
                    # Step failed even after retry
                    elapsed = (time.time() - step_start) * 1000
                    step_log.append({
                        'step': step.step_number,
                        'type': step.step_type.value,
                        'description': step.description,
                        'query': step.query,
                        'rows': 0,
                        'time_ms': round(elapsed, 1),
                        'status': 'failed',
                        'error': str(e)[:200],
                    })

                    # If the KQL step (data fetch) fails, abort the plan
                    if step.step_type == StepType.KQL:
                        total_time = (time.time() - start) * 1000
                        return PlanResult(
                            success=False,
                            final_df=None,
                            steps_executed=step_log,
                            total_time_ms=round(total_time, 1),
                            error=f"Step {step.step_number} failed: {e}",
                        )

                    # SQL step failed — use the last successful result
                    logger.info(f"  ⚠ Continuing with result from previous step")

        total_time = (time.time() - start) * 1000
        return PlanResult(
            success=final_df is not None and not final_df.empty,
            final_df=final_df,
            steps_executed=step_log,
            total_time_ms=round(total_time, 1),
        )

    # ── Step Executors ───────────────────────────────────────────────────────

    def _execute_kql_step(self, step: PlanStep, schema_validator=None) -> pd.DataFrame:
        """Execute a KQL step on Kusto."""
        kql = step.query.strip()

        # Clean up common LLM mistakes
        kql = kql.replace("```kql", "").replace("```", "").strip()
        if kql.startswith("|"):
            kql = kql[1:].strip()

        # Validate
        if schema_validator:
            is_valid, error_msg, suggestions = schema_validator.validate_kql(kql)
            if not is_valid:
                corrected = schema_validator.suggest_corrections(kql)
                if corrected:
                    kql = corrected
                else:
                    raise ValueError(f"Invalid KQL: {error_msg}")

        # Execute on Kusto
        return self.kusto_client.execute_query(kql)

    def _execute_sql_step(self, step: PlanStep) -> pd.DataFrame:
        """Execute a SQL step on DuckDB."""
        sql = step.query.strip()
        sql = sql.replace("```sql", "").replace("```", "").strip()
        return self.duckdb_engine.query(sql)

    def _store_intermediate(self, table_name: str, df: pd.DataFrame):
        """Store a step's result as a named DuckDB table."""
        conn = self.duckdb_engine.connection
        # Drop if exists (for retries)
        try:
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            pass

        conn.register('_tmp_plan', df)
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM _tmp_plan")
        conn.unregister('_tmp_plan')
        logger.info(f"  Stored {len(df):,} rows as '{table_name}'")

        # Also keep as main "telemetry" table for compatibility
        try:
            conn.execute("DROP TABLE IF EXISTS telemetry")
        except Exception:
            pass
        conn.register('_tmp_telem', df)
        conn.execute("CREATE TABLE telemetry AS SELECT * FROM _tmp_telem")
        conn.unregister('_tmp_telem')

        self.duckdb_engine._columns = list(df.columns)
        self.duckdb_engine._row_count = len(df)

    # ── Error Recovery ───────────────────────────────────────────────────────

    def _retry_step(self, step: PlanStep, error_msg: str) -> Optional[pd.DataFrame]:
        """Retry a failed step with the error as context."""
        try:
            prompt = (
                f"The following {step.step_type.value.upper()} query failed:\n"
                f"Query: {step.query}\n"
                f"Error: {error_msg}\n\n"
                f"Fix the query. Return ONLY the corrected query, nothing else."
            )

            if step.step_type == StepType.SQL:
                # Tell it about available tables
                tables = self._list_duckdb_tables()
                prompt += f"\n\nAvailable tables: {', '.join(tables)}"
                for t in tables:
                    cols = self._get_table_columns(t)
                    if cols:
                        prompt += f"\n  {t} columns: {', '.join(cols[:10])}"

            response = self.llm_service.client.chat.completions.create(
                model=self.llm_service.model,
                messages=[
                    {"role": "system", "content": "Fix the query. Return ONLY the corrected query."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=500,
                temperature=0.1,
            )

            fixed = response.choices[0].message.content.strip()
            fixed = fixed.replace("```sql", "").replace("```kql", "").replace("```", "").strip()
            if fixed.startswith("|"):
                fixed = fixed[1:].strip()

            logger.info(f"  Retry query: {fixed[:100]}…")

            if step.step_type == StepType.KQL:
                return self.kusto_client.execute_query(fixed)
            else:
                return self.duckdb_engine.query(fixed)

        except Exception as e:
            logger.error(f"  Retry also failed: {e}")
            return None

    def _list_duckdb_tables(self) -> List[str]:
        """List all tables currently in DuckDB."""
        try:
            result = self.duckdb_engine.connection.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchdf()
            return result['table_name'].tolist()
        except Exception:
            return []

    def _get_table_columns(self, table: str) -> List[str]:
        """Get column names for a DuckDB table."""
        try:
            result = self.duckdb_engine.connection.execute(
                f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}'"
            ).fetchdf()
            return result['column_name'].tolist()
        except Exception:
            return []

    # ── Plan Formatting ──────────────────────────────────────────────────────

    @staticmethod
    def format_plan_log(step_log: List[Dict]) -> str:
        """Format step log for display in the UI."""
        lines = []
        for s in step_log:
            icon = "✓" if s['status'] == 'success' else ("🔄" if s['status'] == 'retried' else "✗")
            lines.append(
                f"{icon} **Step {s['step']}** ({s['type'].upper()}): "
                f"{s['description']} → {s.get('rows', 0):,} rows "
                f"({s['time_ms']:.0f}ms)"
            )
        return "\n".join(lines)
