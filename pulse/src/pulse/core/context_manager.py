"""
Context Manager v2.2 — Conversation State + Rich LLM Context
==============================================================

Improvements:
- Tracks prior KQL queries so follow-ups can reference them
- Stores column names of returned data for SQL follow-ups
- Builds a dense, token-efficient LLM context string
- Supports "modify the last query" patterns

Author: PULSE Team
"""

import pandas as pd
from typing import Optional, Dict, List, Any
from dataclasses import dataclass, field
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


@dataclass
class ConversationTurn:
    """Single turn in conversation."""
    timestamp: str
    user_message: str
    intent: str
    kql_query: Optional[str] = None
    sql_query: Optional[str] = None
    result_rows: int = 0
    result_columns: List[str] = field(default_factory=list)
    visualization_type: Optional[str] = None


class ConversationContext:
    """Manages conversation state and provides rich context for LLM."""

    MAX_HISTORY = 10  # Keep last N turns for context

    def __init__(self):
        self.loaded_data: Optional[pd.DataFrame] = None
        self.last_query: Optional[str] = None
        self.last_kql: Optional[str] = None
        self.conversation_history: List[ConversationTurn] = []
        self.schema: Dict[str, str] = {}
        self.data_source: Optional[str] = None
        self.load_timestamp: Optional[datetime] = None
        self._result_columns: List[str] = []

    # ── Data State ───────────────────────────────────────────────────────────

    def has_data(self) -> bool:
        return self.loaded_data is not None and not self.loaded_data.empty

    def load_data(
        self, df: pd.DataFrame, query: str, data_source: str,
        kql: Optional[str] = None
    ):
        """Load new data into context."""
        self.loaded_data = df
        self.last_query = query
        self.last_kql = kql
        self.data_source = data_source
        self.load_timestamp = datetime.now()
        self._result_columns = list(df.columns) if df is not None else []

    def clear_data(self):
        self.loaded_data = None
        self.last_query = None
        self.last_kql = None
        self.data_source = None
        self.load_timestamp = None
        self._result_columns = []

    # ── Conversation History ─────────────────────────────────────────────────

    def add_turn(
        self,
        user_message: str,
        intent: str,
        kql_query: Optional[str] = None,
        sql_query: Optional[str] = None,
        result_rows: int = 0,
        result_columns: Optional[List[str]] = None,
        visualization_type: Optional[str] = None,
    ):
        turn = ConversationTurn(
            timestamp=datetime.now().isoformat(),
            user_message=user_message,
            intent=intent,
            kql_query=kql_query,
            sql_query=sql_query,
            result_rows=result_rows,
            result_columns=result_columns or [],
            visualization_type=visualization_type,
        )
        self.conversation_history.append(turn)

        # Trim to max
        if len(self.conversation_history) > self.MAX_HISTORY:
            self.conversation_history = self.conversation_history[-self.MAX_HISTORY:]

    def get_recent_turns(self, n: int = 5) -> List[ConversationTurn]:
        return self.conversation_history[-n:]

    # ── Schema ───────────────────────────────────────────────────────────────

    def set_schema(self, schema: Dict[str, str]):
        self.schema = schema

    def get_available_columns(self) -> List[str]:
        return list(self.schema.keys())

    def get_result_columns(self) -> List[str]:
        """Columns in the most recent result set (for SQL follow-ups)."""
        return self._result_columns

    # ── Data Summary ─────────────────────────────────────────────────────────

    def get_data_summary(self) -> Dict[str, Any]:
        if not self.has_data():
            return {'has_data': False, 'message': 'No data loaded'}

        df = self.loaded_data
        return {
            'has_data': True,
            'rows': len(df),
            'columns': len(df.columns),
            'column_names': list(df.columns),
            'source': self.data_source,
            'loaded_at': self.load_timestamp.isoformat() if self.load_timestamp else None,
            'last_query': self.last_query,
            'last_kql': self.last_kql,
        }

    # ── LLM Context Formatting ───────────────────────────────────────────────

    def format_context_for_llm(self) -> str:
        """
        Build a dense, token-efficient context string for the LLM.

        This is THE critical piece that enables follow-up queries like
        "now break that down by region" to work.
        """
        sections = []

        # ── 1. Current data state ───────────────────────────────────
        if self.has_data():
            df = self.loaded_data
            cols = ', '.join(list(df.columns)[:10])
            extra = f"… +{len(df.columns)-10} more" if len(df.columns) > 10 else ""
            sections.append(
                f"📊 LOADED DATA: {len(df):,} rows\n"
                f"  Columns: {cols}{extra}\n"
                f"  Source: {self.data_source}"
            )
            if self.last_kql:
                sections.append(f"  Last KQL: {self.last_kql}")
        else:
            sections.append("📊 NO DATA LOADED")

        # ── 2. Conversation history (most recent 5) ─────────────────
        recent = self.get_recent_turns(5)
        if recent:
            history_lines = ["💬 RECENT CONVERSATION:"]
            for t in recent:
                line = f"  User: \"{t.user_message}\""
                if t.kql_query:
                    line += f"\n    → KQL: {t.kql_query}"
                if t.sql_query:
                    line += f"\n    → SQL: {t.sql_query}"
                if t.result_rows:
                    line += f" → {t.result_rows} rows"
                history_lines.append(line)
            sections.append("\n".join(history_lines))

        # ── 3. Available schema (abbreviated) ───────────────────────
        if self.schema:
            col_list = []
            for name, ctype in list(self.schema.items())[:15]:
                col_list.append(f"{name} ({ctype})")
            extra = f"… +{len(self.schema)-15}" if len(self.schema) > 15 else ""
            sections.append(
                f"📋 SCHEMA ({len(self.schema)} cols): {', '.join(col_list)}{extra}"
            )

        return "\n\n".join(sections)

    def format_context_for_sql(self) -> str:
        """Build context for DuckDB SQL generation — focuses on result columns."""
        lines = []
        if self._result_columns:
            lines.append(f"Current result columns: {', '.join(self._result_columns)}")
        if self.last_query:
            lines.append(f"User originally asked: \"{self.last_query}\"")
        if self.last_kql:
            lines.append(f"KQL that produced this data: {self.last_kql}")

        recent = self.get_recent_turns(3)
        if recent:
            lines.append("Recent questions:")
            for t in recent:
                lines.append(f"  - \"{t.user_message}\"")

        return "\n".join(lines)
