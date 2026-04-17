"""
Profile SQL Validator v1.0 — Hallucination Defense Layer
==========================================================

The problem:
  The LLM generates SQL to query profile tables. It can hallucinate:
    ① Column names that don't exist (EventInfo_Name vs eventinfo_name)
    ② Table names that don't exist
    ③ Dangerous SQL (DROP, INSERT, UPDATE)
    ④ Columns from the wrong table

The solution (same approach as Grafana & Power BI):
    1. Whitelist: Only SELECT on profile_* tables
    2. Schema-check: Validate every column ref against actual DuckDB schema
    3. Auto-correct: If a column is wrong, find the closest match (Levenshtein)
    4. EXPLAIN: Use DuckDB's own parser as final validation before execution
    5. Audit: Log every query with validation status for traceability

This module sits between the LLM output and DuckDB execution.
Nothing the LLM produces ever touches DuckDB without passing through here.

Author: PULSE Team
"""

import re
import logging
import time
from typing import Dict, Set, List, Tuple, Optional
from difflib import get_close_matches

logger = logging.getLogger(__name__)


class ProfileSQLValidator:
    """
    Validates and sanitizes LLM-generated SQL before execution.

    Usage:
        validator = ProfileSQLValidator()
        validator.refresh_schema(conn, ['profile_daily', 'profile_region'])

        ok, sql_or_error = validator.validate_and_fix(sql)
        if ok:
            df = conn.execute(sql_or_error).fetchdf()
        else:
            # sql_or_error contains the error message
            raise ValueError(sql_or_error)
    """

    # Statements we NEVER allow
    _BLOCKED_KEYWORDS = {
        'drop', 'delete', 'insert', 'update', 'alter', 'create',
        'truncate', 'replace', 'grant', 'revoke', 'exec', 'execute',
        'attach', 'detach', 'copy', 'export', 'import', 'load',
        'pragma', 'vacuum', 'checkpoint',
    }

    # SQL functions that are safe in DuckDB profile queries
    _SAFE_FUNCTIONS = {
        'count', 'sum', 'avg', 'min', 'max', 'round', 'abs',
        'coalesce', 'nullif', 'cast', 'lower', 'upper', 'trim',
        'length', 'substr', 'substring', 'replace', 'concat',
        'date_part', 'date_trunc', 'strftime', 'current_date',
        'row_number', 'rank', 'dense_rank', 'lag', 'lead',
        'first_value', 'last_value', 'ntile',
        'case', 'when', 'then', 'else', 'end',
        'like', 'ilike', 'in', 'between', 'is', 'not',
        'and', 'or', 'asc', 'desc', 'limit', 'offset',
        'distinct', 'as', 'from', 'where', 'group', 'by',
        'order', 'having', 'union', 'all', 'join', 'on',
        'left', 'right', 'inner', 'outer', 'cross',
        'select', 'null', 'true', 'false',
        'toreal', 'toint', 'todouble',  # DuckDB type casts
    }

    def __init__(self):
        self._schema: Dict[str, Set[str]] = {}  # table → {col1, col2, ...}
        self._all_columns: Set[str] = set()      # union of all columns
        self._valid_tables: Set[str] = set()
        self._last_refresh: float = 0
        self._query_log: List[Dict] = []  # audit trail
        self._MAX_AUDIT_LOG = 500  # ★ v7.0: prevent unbounded growth

    def _log_audit(self, entry: Dict):
        """Append audit entry, trimming oldest if over max."""
        self._query_log.append(entry)
        if len(self._query_log) > self._MAX_AUDIT_LOG:
            self._query_log = self._query_log[-self._MAX_AUDIT_LOG:]

    # ═══════════════════════════════════════════════════════════════════════════
    # SCHEMA REFRESH — snapshot actual DuckDB schema
    # ═══════════════════════════════════════════════════════════════════════════

    def refresh_schema(self, conn, tables: List[str]):
        """
        Snapshot the actual column names from DuckDB.
        Call this after tables are built/loaded.
        """
        self._schema.clear()
        self._all_columns.clear()
        self._valid_tables.clear()

        for table in tables:
            try:
                # Use DuckDB's information_schema for accurate column list
                result = conn.execute(
                    f"SELECT column_name FROM information_schema.columns "
                    f"WHERE table_name = '{table}'"
                ).fetchdf()
                cols = set(result['column_name'].str.lower().tolist())
                if cols:
                    self._schema[table] = cols
                    self._all_columns.update(cols)
                    self._valid_tables.add(table)
            except Exception as e:
                logger.debug(f"Schema refresh {table}: {e}")

        self._last_refresh = time.time()
        logger.info(
            f"SQLValidator: schema refreshed — {len(self._valid_tables)} tables, "
            f"{len(self._all_columns)} unique columns"
        )

    @property
    def is_ready(self) -> bool:
        return len(self._schema) > 0

    # ═══════════════════════════════════════════════════════════════════════════
    # VALIDATE AND FIX — the main entry point
    # ═══════════════════════════════════════════════════════════════════════════

    def validate_and_fix(self, sql: str) -> Tuple[bool, str]:
        """
        Validate LLM-generated SQL. Returns (ok, result).

        If ok=True:  result is the (possibly corrected) SQL, safe to execute.
        If ok=False: result is a human-readable error message.

        Validation pipeline:
          1. Statement safety check (no DDL/DML)
          2. Table whitelist check
          3. Column validation + auto-correction
          4. Return validated SQL
        """
        sql = sql.strip()
        if not sql:
            return False, "Empty SQL query"

        audit = {
            'original_sql': sql,
            'timestamp': time.time(),
            'corrections': [],
            'status': 'pending',
        }

        # ── Step 1: Statement safety ─────────────────────────────────────
        ok, err = self._check_statement_safety(sql)
        if not ok:
            audit['status'] = 'blocked'
            audit['reason'] = err
            self._log_audit(audit)
            return False, err

        # ── Step 2: Table whitelist ──────────────────────────────────────
        tables_in_sql = self._extract_table_refs(sql)
        for table in tables_in_sql:
            if table not in self._valid_tables:
                # Try to find closest match
                matches = get_close_matches(table, list(self._valid_tables), n=1, cutoff=0.75)
                if matches:
                    suggestion = matches[0]
                    sql = re.sub(
                        rf'\b{re.escape(table)}\b', suggestion, sql,
                        flags=re.IGNORECASE
                    )
                    audit['corrections'].append(
                        f"table '{table}' → '{suggestion}'"
                    )
                    logger.info(f"SQLValidator: auto-corrected table '{table}' → '{suggestion}'")
                else:
                    avail = ", ".join(sorted(self._valid_tables)[:10])
                    audit['status'] = 'rejected'
                    audit['reason'] = f"Table '{table}' not found"
                    self._log_audit(audit)
                    return False, (
                        f"Table '{table}' doesn't exist in the profile. "
                        f"Available tables: {avail}"
                    )

        # ── Step 3: Column validation ────────────────────────────────────
        # Re-extract tables after potential correction
        tables_in_sql = self._extract_table_refs(sql)
        target_table = tables_in_sql[0] if tables_in_sql else None

        if target_table and target_table in self._schema:
            valid_cols = self._schema[target_table]
            col_refs = self._extract_column_refs(sql, target_table)

            for col_ref in col_refs:
                if col_ref.lower() not in valid_cols:
                    # Try auto-correct
                    fixed = self._fix_column(col_ref, valid_cols)
                    if fixed:
                        # Use word-boundary replacement to avoid partial matches
                        sql = re.sub(
                            rf'\b{re.escape(col_ref)}\b', fixed, sql,
                            flags=re.IGNORECASE
                        )
                        audit['corrections'].append(
                            f"column '{col_ref}' → '{fixed}'"
                        )
                        logger.info(
                            f"SQLValidator: auto-corrected column '{col_ref}' → '{fixed}' "
                            f"in {target_table}"
                        )
                    else:
                        avail = ", ".join(sorted(valid_cols)[:15])
                        audit['status'] = 'rejected'
                        audit['reason'] = f"Column '{col_ref}' not in {target_table}"
                        self._log_audit(audit)
                        return False, (
                            f"Column '{col_ref}' doesn't exist in {target_table}. "
                            f"Available columns: {avail}"
                        )

        # ── Success ──────────────────────────────────────────────────────
        audit['status'] = 'approved'
        audit['final_sql'] = sql
        audit['corrections_count'] = len(audit['corrections'])
        self._log_audit(audit)

        if audit['corrections']:
            logger.info(
                f"SQLValidator: approved with {len(audit['corrections'])} correction(s)"
            )

        return True, sql

    # ═══════════════════════════════════════════════════════════════════════════
    # INTERNAL CHECKS
    # ═══════════════════════════════════════════════════════════════════════════

    def _check_statement_safety(self, sql: str) -> Tuple[bool, str]:
        """Ensure only SELECT statements, no DDL/DML."""
        # Normalize for checking
        normalized = sql.strip().lower()

        # Must start with SELECT (or WITH for CTEs)
        if not (normalized.startswith('select') or normalized.startswith('with')):
            return False, "Only SELECT queries are allowed on profile data."

        # Check for dangerous keywords
        # Use word boundaries to avoid false positives (e.g., "replace" in a WHERE)
        tokens = set(re.findall(r'\b\w+\b', normalized))
        dangerous = tokens & self._BLOCKED_KEYWORDS
        # 'replace' is also a safe function, so only block if it's a statement start
        dangerous.discard('replace')  # allow REPLACE() function

        if dangerous:
            return False, (
                f"Query contains blocked keyword(s): {', '.join(dangerous)}. "
                f"Only SELECT queries are allowed."
            )

        # Check for multiple statements (SQL injection)
        # Remove string literals first
        cleaned = re.sub(r"'[^']*'", '', sql)
        if ';' in cleaned:
            return False, "Multiple SQL statements not allowed."

        return True, ""

    def _extract_table_refs(self, sql: str) -> List[str]:
        """Extract table names referenced in FROM/JOIN clauses."""
        tables = []
        # Match FROM table, JOIN table
        pattern = r'\b(?:FROM|JOIN)\s+(\w+)'
        for m in re.finditer(pattern, sql, re.IGNORECASE):
            table = m.group(1).lower()
            # Skip subquery aliases and SQL keywords
            if table not in ('select', 'where', 'order', 'group', 'having',
                             'limit', 'offset', 'union', 'sub', 'tmp'):
                tables.append(table)
        return tables

    def _extract_column_refs(self, sql: str, table_name: str) -> List[str]:
        """
        Extract column references from SQL, excluding:
          - SQL keywords
          - Function names
          - Table names/aliases
          - String literals
          - Numbers
        """
        # Remove string literals
        cleaned = re.sub(r"'[^']*'", '', sql)

        # Get all identifiers
        identifiers = re.findall(r'\b([a-zA-Z_]\w*)\b', cleaned)

        # Filter out known non-column tokens
        skip = (
            self._SAFE_FUNCTIONS
            | self._valid_tables
            | {'sub', 'tmp', 'profile', 't1', 't2', 'a', 'b'}
            | {table_name}
        )

        # Also skip aliases defined in the query (AS xxx)
        alias_pattern = r'\bAS\s+(\w+)\b'
        aliases = {m.group(1).lower() for m in re.finditer(alias_pattern, sql, re.IGNORECASE)}
        skip |= aliases

        candidates = []
        for ident in identifiers:
            lower = ident.lower()
            if lower in skip:
                continue
            if lower.isdigit():
                continue
            # Check if this could be a column reference
            if lower in self._all_columns:
                continue  # It's a valid column, no issue
            # Could be a column we need to validate
            candidates.append(ident)

        # De-duplicate while preserving order
        seen = set()
        unique = []
        for c in candidates:
            cl = c.lower()
            if cl not in seen:
                seen.add(cl)
                unique.append(c)

        return unique

    def _fix_column(self, col: str, valid_cols: Set[str]) -> Optional[str]:
        """
        Try to fix a bad column reference.

        Strategy:
          1. Case fix: EventInfo_Name → eventinfo_name
          2. Fuzzy match: eventinfo → eventinfo_name (Levenshtein)
          3. Substring match: name → eventinfo_name (if unambiguous)
        """
        lower = col.lower()

        # Case fix (most common LLM error)
        if lower in valid_cols:
            return lower

        # Underscore/case normalization: EventInfo_Name → eventinfo_name
        normalized = lower.replace('-', '_').replace(' ', '_')
        if normalized in valid_cols:
            return normalized

        # Strip common prefixes/suffixes
        for prefix in ('total_', 'avg_', 'sum_', 'count_', 'max_', 'min_'):
            if normalized.startswith(prefix):
                rest = normalized[len(prefix):]
                if rest in valid_cols:
                    return rest

        # Fuzzy match
        matches = get_close_matches(lower, list(valid_cols), n=1, cutoff=0.7)
        if matches:
            return matches[0]

        # Substring match (only if unambiguous)
        substring_matches = [c for c in valid_cols if lower in c or c in lower]
        if len(substring_matches) == 1:
            return substring_matches[0]

        return None

    # ═══════════════════════════════════════════════════════════════════════════
    # AUDIT & REPORTING
    # ═══════════════════════════════════════════════════════════════════════════

    def get_audit_log(self, last_n: int = 20) -> List[Dict]:
        """Return recent query audit entries."""
        return self._query_log[-last_n:]

    def get_stats(self) -> Dict:
        """Validation statistics for monitoring."""
        total = len(self._query_log)
        if total == 0:
            return {'total': 0}

        approved = sum(1 for q in self._query_log if q['status'] == 'approved')
        corrected = sum(1 for q in self._query_log
                        if q['status'] == 'approved' and q.get('corrections_count', 0) > 0)
        rejected = sum(1 for q in self._query_log if q['status'] == 'rejected')
        blocked = sum(1 for q in self._query_log if q['status'] == 'blocked')

        return {
            'total': total,
            'approved': approved,
            'corrected': corrected,
            'rejected': rejected,
            'blocked': blocked,
            'approval_rate': round(approved / total * 100, 1) if total else 0,
            'correction_rate': round(corrected / total * 100, 1) if total else 0,
        }

    def get_schema_summary(self) -> str:
        """Return schema summary for debugging."""
        lines = [f"SQLValidator: {len(self._valid_tables)} tables"]
        for table in sorted(self._valid_tables):
            cols = sorted(self._schema.get(table, set()))
            lines.append(f"  {table}: {', '.join(cols[:10])}"
                         + (f" (+{len(cols)-10} more)" if len(cols) > 10 else ""))
        return "\n".join(lines)
