"""
LLM Service v3.0 — With Streaming Support
============================================

Changes from v2.2:
  1. Added stream=True option to generate_analysis() and generate_conversational_response()
  2. New stream_analysis() and stream_conversational() yield tokens for progressive UI
  3. generate_kql_filter() stays non-streaming (we need the full KQL before executing)

Author: PULSE Team
"""
from dotenv import load_dotenv
load_dotenv()

import os
import re
import json
import logging
from openai import AzureOpenAI

logger = logging.getLogger(__name__)


# ── Prompt library (unchanged) ───────────────────────────────────────────────

KQL_SYSTEM_PROMPT = """You are an expert KQL generator for PULSE, a telemetry analysis tool.

═══════════════════════════════════════════════════════════════
CRITICAL RULES
═══════════════════════════════════════════════════════════════
1. Output ONLY the KQL clause. NO markdown, NO explanation, NO backticks.
2. Do NOT include table names — we prepend them automatically.
3. Do NOT include a leading pipe |.
4. Start with: where, summarize, project, extend, order, take, limit, distinct, top.
5. Use ONLY columns from the SCHEMA section.
6. A 30-day time filter is auto-applied. You do NOT need to add ago(30d).
   Only add time filters if the user asks for a SPECIFIC period (e.g. "last 7 days").

═══════════════════════════════════════════════════════════════
UNDERSTANDING THE DATA
═══════════════════════════════════════════════════════════════
Each ROW = one telemetry event (one user action in the app).
To count different things:
  - Events (how much activity)  → count()
  - Organizations               → dcount(OrgId)
  - Users                       → dcount(UserId)
  - Sessions                    → dcount(SessionId)
  - Entity types used           → dcount(EntityName)

═══════════════════════════════════════════════════════════════
KQL EXAMPLES — learn these patterns
═══════════════════════════════════════════════════════════════

--- Simple Counts ---
Q: How many events?
A: summarize TotalEvents=count()

Q: How many active orgs?
A: summarize ActiveOrgs=dcount(OrgId)

Q: How many unique users?
A: summarize Users=dcount(UserId)

--- Rankings ---
Q: Top 10 orgs by events
A: summarize Events=count() by OrgId | order by Events desc | take 10

Q: Top 5 orgs by sessions
A: summarize Sessions=dcount(SessionId) by OrgId | order by Sessions desc | take 5

Q: Bottom 10 orgs by events
A: summarize Events=count() by OrgId | order by Events asc | take 10

--- Per-Org Metrics ---
Q: Average number of sessions per org
A: summarize Sessions=dcount(SessionId) by OrgId | summarize AvgSessions=avg(Sessions), MedianSessions=percentile(Sessions, 50), TotalOrgs=count()

Q: Average events per org
A: summarize Events=count() by OrgId | summarize AvgEvents=avg(Events), MedianEvents=percentile(Events, 50)

Q: Events per session ratio by org
A: summarize Events=count(), Sessions=dcount(SessionId) by OrgId | extend Ratio=todouble(Events)/todouble(Sessions) | order by Ratio desc | take 15

--- By Region ---
Q: Events per region
A: summarize Events=count() by GeoName | order by Events desc

Q: Active orgs per region
A: summarize Orgs=dcount(OrgId) by GeoName | order by Orgs desc

Q: Sessions per region
A: summarize Sessions=dcount(SessionId) by GeoName | order by Sessions desc

--- By Entity/Feature ---
Q: Most used entity types
A: summarize Events=count() by EntityName | order by Events desc | take 15

Q: How many orgs use each entity?
A: summarize Orgs=dcount(OrgId) by EntityName | order by Orgs desc

Q: Entity diversity per org
A: summarize EntityTypes=dcount(EntityName), Events=count() by OrgId | order by EntityTypes desc | take 15

--- Time Series ---
Q: Events per day
A: summarize Events=count() by Day=startofday(EventInfo_Time) | order by Day asc

Q: Daily active orgs
A: summarize ActiveOrgs=dcount(OrgId) by Day=startofday(EventInfo_Time) | order by Day asc

Q: Sessions per day
A: summarize Sessions=dcount(SessionId) by Day=startofday(EventInfo_Time) | order by Day asc

Q: Events per day by region
A: summarize Events=count() by Day=startofday(EventInfo_Time), GeoName | order by Day asc

Q: Last 7 days only
A: where EventInfo_Time > ago(7d) | summarize Events=count() by Day=startofday(EventInfo_Time) | order by Day asc

--- Comparisons ---
Q: Weekend vs weekday activity
A: extend IsWeekend=iff(dayofweek(EventInfo_Time) >= 5d, "Weekend", "Weekday") | summarize Events=count(), Sessions=dcount(SessionId), Orgs=dcount(OrgId) by IsWeekend

Q: Busiest day
A: summarize Events=count() by Day=startofday(EventInfo_Time) | order by Events desc | take 1

Q: Which orgs disappeared recently?
A: summarize LastSeen=max(EventInfo_Time) by OrgId | extend DaysAgo=datetime_diff('day', now(), LastSeen) | where DaysAgo > 7 | order by DaysAgo desc | take 20

--- Advanced ---
Q: Orgs that only appeared on one day
A: summarize DaysActive=dcount(startofday(EventInfo_Time)), Events=count() by OrgId | where DaysActive == 1 | order by Events desc

Q: Orgs with more than 100 events
A: summarize Events=count() by OrgId | where Events > 100 | order by Events desc

═══════════════════════════════════════════════════════════════
PATTERN REFERENCE
═══════════════════════════════════════════════════════════════
- "How many X"           → summarize dcount(X) or count()
- "Top N X by Y"         → summarize Y by X | order by Y desc | take N
- "X per day"            → summarize X by Day=startofday(EventInfo_Time) | order by Day asc
- "X by region"          → summarize X by GeoName | order by X desc
- "Average X per Y"      → summarize X by Y | summarize avg(X)  [TWO-STEP!]
- "Growing/declining"    → time series by Day
- "Percentage/share"     → compute totals and divide

Return ONLY the KQL clause. Nothing else."""


DUCKDB_SQL_PROMPT = """You are a SQL expert. Generate a DuckDB SQL query to answer the user's follow-up question.

RULES:
1. The data is in a table called "telemetry".
2. Output ONLY the SQL statement. No markdown, no explanation.
3. Use standard SQL syntax compatible with DuckDB.
4. Use only the columns listed in the schema.
5. For date operations, use DuckDB date functions: date_trunc, date_part, etc.

AVAILABLE COLUMNS:
{columns}

CURRENT DATA SUMMARY:
{data_summary}

SQL EXAMPLES:
Q: "Show just EMEA"
A: SELECT * FROM telemetry WHERE GeoName = 'EMEA'

Q: "Only the top 3"
A: SELECT * FROM telemetry ORDER BY Events DESC LIMIT 3

Q: "Exclude test orgs"
A: SELECT * FROM telemetry WHERE OrgId NOT LIKE '%test%'

Q: "Break this down by region"
A: SELECT GeoName, COUNT(*) as Events FROM telemetry GROUP BY GeoName ORDER BY Events DESC

Q: "What percentage is each?"
A: SELECT *, ROUND(100.0 * Events / SUM(Events) OVER(), 2) as Pct FROM telemetry

Q: "Average per org"
A: SELECT AVG(Events) as AvgEvents FROM telemetry

Return ONLY the SQL. Nothing else."""


CONVERSATIONAL_PROMPT = """You are PULSE, a telemetry analysis assistant. You help people understand their product data through natural conversation.

Guidelines:
- Be concise — 2-4 sentences max for simple questions, bullet points for complex ones.
- Use SPECIFIC numbers from the data context, not vague language.
- Don't apologise. Don't be generic. Don't say "the data shows" without actual numbers.
- If asked about capabilities, list concrete examples the user can try.
- If the question is off-topic, redirect warmly to data analysis.

{context}"""


ANALYSIS_PROMPT = """You are a senior data analyst reviewing telemetry data. Give sharp, executive-ready insights.

RULES:
1. Answer the EXACT question — don't give generic analysis.
2. Lead with the most important finding.
3. Use SPECIFIC numbers from the data below (not approximations).
4. Flag anything surprising, concerning, or noteworthy.
5. Keep it to 3-5 bullet points max.
6. If asked for a summary, make it ready to paste into Slack/email.

DATA:
{data_stats}

CONVERSATION CONTEXT:
{context}

USER QUESTION: {question}

Respond with this structure EXACTLY (use these headers):
SUMMARY: [2-3 sentence executive answer]
INSIGHTS:
- [specific finding with numbers]
- [specific finding with numbers]
- [etc.]
RECOMMENDATIONS:
- [actionable next step]"""


class LLMService:
    def __init__(self):
        self.client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-12-01-preview"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
        )
        self.model = os.getenv("AZURE_OPENAI_MODEL", "gpt-4o-mini")

    # ── KQL Generation (non-streaming — we need full KQL before executing) ──

    def generate_kql_filter(
        self,
        question: str,
        schema_context: str,
        conversation_context: str = ""
    ) -> str:
        """Generate KQL filter/aggregation clause from natural language."""

        user_parts = [
            "SCHEMA:", schema_context, ""
        ]

        if conversation_context:
            user_parts.extend(["CONVERSATION CONTEXT:", conversation_context, ""])

        user_parts.extend([
            f"QUESTION: {question}",
            "",
            "Generate the KQL clause now (must start with a valid operator):"
        ])

        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": KQL_SYSTEM_PROMPT},
                {"role": "user", "content": "\n".join(user_parts)}
            ],
            max_tokens=500,
            temperature=0.15
        )

        kql = response.choices[0].message.content.strip()
        kql = kql.replace("```kql", "").replace("```sql", "").replace("```", "").strip()

        # Remove accidental leading pipe
        if kql.startswith("|"):
            kql = kql[1:].strip()

        # Validate start
        valid_starts = [
            'where', 'summarize', 'project', 'extend', 'order',
            'take', 'limit', 'distinct', 'sort', 'top', 'let'
        ]
        kql_lower = kql.lower().strip()
        is_valid = any(kql_lower.startswith(op) for op in valid_starts)

        if not is_valid:
            logger.warning(f"LLM produced invalid KQL start: {kql[:50]}...")
            kql = self._attempt_kql_fix(kql, question)

        logger.info(f"Generated KQL: {kql}")
        return kql

    def _attempt_kql_fix(self, kql: str, question: str) -> str:
        """Try to fix common LLM KQL mistakes."""
        kql_lower = kql.lower()

        match = re.match(r'top\s+(\d+)\s+(\w+)', kql_lower)
        if match:
            n = match.group(1)
            return f"summarize Count=count() by OrgId | order by Count desc | take {n}"

        if '|' in kql:
            parts = kql.split('|', 1)
            if len(parts) == 2:
                return parts[1].strip()

        raise ValueError(
            f"Could not generate valid KQL for: \"{question}\"\n"
            f"The AI returned: {kql[:100]}\n"
            f"Try rephrasing — e.g., \"top 10 orgs by events\" or \"events per day\"."
        )

    # ── DuckDB SQL Generation (non-streaming) ───────────────────────────────

    def generate_duckdb_sql(
        self,
        question: str,
        columns: list,
        data_summary: str,
        conversation_context: str = ""
    ) -> str:
        """Generate DuckDB SQL for follow-up queries on cached data."""

        system = DUCKDB_SQL_PROMPT.format(
            columns=", ".join(columns),
            data_summary=data_summary
        )

        user_parts = []
        if conversation_context:
            user_parts.append(f"Context:\n{conversation_context}\n")
        user_parts.append(f"Question: {question}")
        user_parts.append("\nGenerate DuckDB SQL:")

        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": "\n".join(user_parts)}
            ],
            max_tokens=400,
            temperature=0.15
        )

        sql = response.choices[0].message.content.strip()
        sql = sql.replace("```sql", "").replace("```", "").strip()

        logger.info(f"Generated DuckDB SQL: {sql}")
        return sql

    # ══════════════════════════════════════════════════════════════════════════
    # CONVERSATIONAL — non-streaming (unchanged) + streaming (NEW)
    # ══════════════════════════════════════════════════════════════════════════

    def generate_conversational_response(
        self,
        user_message: str,
        context: str,
        data_summary: dict
    ) -> str:
        """Generate natural conversational response (non-streaming)."""
        messages = self._build_conversational_messages(user_message, context, data_summary)

        response = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            max_tokens=400,
            temperature=0.3
        )
        return response.choices[0].message.content.strip()

    def stream_conversational_response(
        self,
        user_message: str,
        context: str,
        data_summary: dict
    ):
        """
        ★ NEW: Stream conversational response token-by-token.
        Yields text chunks as they arrive from the LLM.
        """
        messages = self._build_conversational_messages(user_message, context, data_summary)

        response = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            max_tokens=400,
            temperature=0.3,
            stream=True,  # ← the key change
        )

        for chunk in response:
            if chunk.choices and chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content

    def _build_conversational_messages(self, user_message, context, data_summary):
        """Build messages array for conversational LLM calls."""
        ctx_parts = []
        if data_summary.get('has_data'):
            ctx_parts.append(f"Currently loaded: {data_summary['rows']:,} rows")
            ctx_parts.append(f"Columns: {', '.join(data_summary.get('column_names', [])[:8])}")
        else:
            ctx_parts.append("No data loaded yet.")
        if context:
            ctx_parts.append(f"\nRecent conversation:\n{context}")

        system = CONVERSATIONAL_PROMPT.format(context="\n".join(ctx_parts))
        return [
            {"role": "system", "content": system},
            {"role": "user", "content": user_message}
        ]

    # ══════════════════════════════════════════════════════════════════════════
    # ANALYSIS — non-streaming (unchanged) + streaming (NEW)
    # ══════════════════════════════════════════════════════════════════════════

    def generate_analysis(
        self,
        question: str,
        data_stats: str,
        context: str = ""
    ) -> str:
        """Generate structured analysis from data stats (non-streaming)."""
        messages = self._build_analysis_messages(question, data_stats, context)

        response = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            max_tokens=800,
            temperature=0.25
        )
        return response.choices[0].message.content.strip()

    def stream_analysis(
        self,
        question: str,
        data_stats: str,
        context: str = ""
    ):
        """
        ★ NEW: Stream analysis response token-by-token.
        Yields text chunks as they arrive from the LLM.

        Usage:
            for token in llm.stream_analysis(q, stats, ctx):
                placeholder.markdown(accumulated_text + token)
        """
        messages = self._build_analysis_messages(question, data_stats, context)

        response = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            max_tokens=800,
            temperature=0.25,
            stream=True,  # ← the key change
        )

        for chunk in response:
            if chunk.choices and chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content

    def _build_analysis_messages(self, question, data_stats, context):
        """Build messages array for analysis LLM calls."""
        prompt = ANALYSIS_PROMPT.format(
            data_stats=data_stats,
            context=context,
            question=question
        )
        return [
            {"role": "system", "content": "You are a senior data analyst."},
            {"role": "user", "content": prompt}
        ]
