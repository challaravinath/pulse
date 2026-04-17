"""
DuckDB Query Engine v2.2 — Local Analytical Cache
===================================================

Improvements:
- Proper connection lifecycle (close before reopen)
- SQL follow-up queries on cached data
- Data summary for LLM context
- Column introspection

Author: PULSE Team
"""
import duckdb
import pandas as pd
from pathlib import Path
import logging
from typing import Optional, List, Dict

logger = logging.getLogger(__name__)


class DuckDBQueryEngine:
    """Fast local analytical cache backed by DuckDB."""

    TABLE = "telemetry"

    def __init__(self, cache_dir: str = "./cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.connection: Optional[duckdb.DuckDBPyConnection] = None
        self.loaded = False
        self._columns: List[str] = []
        self._row_count: int = 0

    # ── Load / Clear ─────────────────────────────────────────────────────────

    def load_data(self, df: pd.DataFrame, table_name: str = "telemetry"):
        """Load a DataFrame into DuckDB. Preserves existing tables."""
        # Create connection if needed, but NEVER destroy existing one
        # (profile tables live in this connection!)
        if not self.connection:
            self.connection = duckdb.connect(':memory:')

        logger.info(f"Loading {len(df):,} rows into DuckDB '{table_name}'…")

        # Drop only THIS table, not the whole connection
        try:
            self.connection.execute(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            pass

        self.connection.register('_tmp_load', df)
        self.connection.execute(f"CREATE TABLE {table_name} AS SELECT * FROM _tmp_load")
        self.connection.unregister('_tmp_load')

        self.loaded = True
        self._columns = list(df.columns)
        self._row_count = len(df)
        logger.info(f"Loaded into DuckDB table '{table_name}' ({self._row_count:,} rows)")

    def clear(self):
        """Clear cached data and close connection."""
        if self.connection:
            try:
                self.connection.close()
            except Exception:
                pass
        self.connection = None
        self.loaded = False
        self._columns = []
        self._row_count = 0
        logger.info("DuckDB cache cleared")

    # ── Query ────────────────────────────────────────────────────────────────

    def query(self, sql: str) -> pd.DataFrame:
        """Execute a SQL query on cached data."""
        self._ensure_loaded()
        logger.info(f"DuckDB SQL: {sql[:120]}…")
        try:
            result = self.connection.execute(sql).fetchdf()
            logger.info(f"✓ DuckDB returned {len(result):,} rows")
            return result
        except Exception as e:
            logger.error(f"DuckDB query failed: {e}")
            raise ValueError(f"SQL query failed: {e}\nQuery: {sql}")

    def query_safe(self, sql: str) -> Optional[pd.DataFrame]:
        """Execute SQL and return None on failure instead of raising."""
        try:
            return self.query(sql)
        except Exception as e:
            logger.warning(f"Safe query failed: {e}")
            return None

    # ── Introspection ────────────────────────────────────────────────────────

    @property
    def columns(self) -> List[str]:
        return self._columns

    @property
    def row_count(self) -> int:
        return self._row_count

    def get_data_summary(self) -> str:
        """Get a concise summary suitable for LLM context."""
        if not self.loaded:
            return "No data loaded."

        lines = [f"Cached table '{self.TABLE}': {self._row_count:,} rows, {len(self._columns)} columns."]
        lines.append(f"Columns: {', '.join(self._columns[:15])}")
        if len(self._columns) > 15:
            lines.append(f"  …and {len(self._columns) - 15} more")

        # Quick value ranges for numeric columns
        try:
            info = self.connection.execute(f"""
                SELECT column_name, column_type
                FROM information_schema.columns
                WHERE table_name = '{self.TABLE}'
            """).fetchdf()

            numeric_cols = info[info['column_type'].str.contains(
                'INTEGER|BIGINT|DOUBLE|FLOAT|DECIMAL|NUMERIC', case=False, na=False
            )]['column_name'].tolist()

            for col in numeric_cols[:5]:
                row = self.connection.execute(
                    f'SELECT MIN("{col}") as mn, MAX("{col}") as mx, '
                    f'AVG("{col}") as av FROM {self.TABLE}'
                ).fetchone()
                if row:
                    lines.append(f"  {col}: min={row[0]}, max={row[1]}, avg={row[2]:.1f}")
        except Exception:
            pass

        return "\n".join(lines)

    def get_current_data(self) -> Optional[pd.DataFrame]:
        """Return the full cached DataFrame (use sparingly — can be large)."""
        if not self.loaded:
            return None
        try:
            return self.connection.execute(f"SELECT * FROM {self.TABLE}").fetchdf()
        except Exception:
            return None

    # ── Private ──────────────────────────────────────────────────────────────

    def _ensure_loaded(self):
        if not self.loaded or not self.connection:
            raise ValueError("No data loaded. Fetch data from Kusto first.")
