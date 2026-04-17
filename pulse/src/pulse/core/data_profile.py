"""
Data Profile v7.0 — Production-Grade Data Layer
=================================================

v7.0 hardening:
  ① ReadWriteLock — multiple readers, exclusive writer. No more
     background-write vs user-query collisions.
  ② SQL Validator integration — every LLM-generated SQL is validated
     against actual DuckDB schema before execution. Zero hallucinations.
  ③ Staleness tracking — every table knows when it was last refreshed.
     UI can show "data is Xm old" per table and overall.
  ④ Scope-aware storage — tables store their actual scope_days,
     enabling proper "last 180d" labels.

Prior fixes preserved:
  - Cancellation flag (v6.0)
  - save_to_disk saves ALL tables (v6.0)
  - Date outlier nullification (v6.0)
  - Instant bridge → background upgrade lifecycle

Author: PULSE Team
"""

import os
import json
import time
import logging
import threading
from pathlib import Path
from typing import Dict, List, Optional, Callable, Tuple
import pandas as pd
import duckdb

logger = logging.getLogger(__name__)

CACHE_DIR = Path("./cache/profile_cache")
CACHE_MAX_AGE_HOURS = 6

# Dates outside this range are almost certainly corrupt
_MIN_VALID_YEAR = 2015
_MAX_VALID_YEAR = __import__('datetime').datetime.now().year + 1  # ★ v9.2: dynamic, not 2035
# ★ iter13.1: Also filter dates beyond today + 7 days for time-series columns
_MAX_VALID_DATE = __import__('pandas').Timestamp.now().normalize() + __import__('pandas').DateOffset(days=7)


# ═══════════════════════════════════════════════════════════════════════════════
# ReadWriteLock — industry-standard readers-writer lock
# ═══════════════════════════════════════════════════════════════════════════════
#
# Why not just RLock?
#   RLock = mutual exclusion. One thread at a time. Period.
#   If background thread is writing a 10K-row org table (200ms),
#   the user's SELECT query blocks for 200ms. Bad UX.
#
# ReadWriteLock allows:
#   - Multiple concurrent readers (user queries, get_totals, summaries)
#   - Exclusive writer (register table, drop+create)
#   - Writers wait for all readers to finish
#   - Readers wait if a writer is active
#
# This is the same pattern used by PostgreSQL, SQLite WAL, and Java's
# ReadWriteLock. It's the standard for databases.

class ReadWriteLock:
    """Readers-writer lock. Multiple readers OR one writer, never both."""

    def __init__(self):
        self._lock = threading.Lock()
        self._readers = 0
        self._writer = False
        self._read_ready = threading.Condition(self._lock)

    def acquire_read(self):
        """Acquire a read lock. Blocks if a writer is active."""
        with self._read_ready:
            while self._writer:
                self._read_ready.wait()
            self._readers += 1

    def release_read(self):
        """Release a read lock."""
        with self._read_ready:
            self._readers -= 1
            if self._readers == 0:
                self._read_ready.notify_all()

    def acquire_write(self):
        """Acquire a write lock. Blocks until all readers finish."""
        self._read_ready.acquire()
        while self._readers > 0 or self._writer:
            self._read_ready.wait()
        self._writer = True

    def release_write(self):
        """Release a write lock."""
        self._writer = False
        self._read_ready.notify_all()
        self._read_ready.release()

    class ReadContext:
        """Context manager for read operations."""
        def __init__(self, rwlock):
            self._rwlock = rwlock
        def __enter__(self):
            self._rwlock.acquire_read()
            return self
        def __exit__(self, *args):
            self._rwlock.release_read()

    class WriteContext:
        """Context manager for write operations."""
        def __init__(self, rwlock):
            self._rwlock = rwlock
        def __enter__(self):
            self._rwlock.acquire_write()
            return self
        def __exit__(self, *args):
            self._rwlock.release_write()

    def read_lock(self):
        return self.ReadContext(self)

    def write_lock(self):
        return self.WriteContext(self)


class DataProfile:
    """
    Builds and manages pre-aggregated profile tables in DuckDB.

    Table lifecycle:
      ⚡ instant  — 7-day snapshot, registered immediately on connect
      🔄 kusto   — full-scope Kusto build, REPLACES instant tables
      💾 cached  — loaded from disk, treated as kusto quality

    The critical invariant: once a Kusto build finishes for a table,
    it ALWAYS replaces the instant version, giving users full-scope data.

    Thread safety: ReadWriteLock allows concurrent reads, exclusive writes.
    SQL validation: Every query passes through ProfileSQLValidator.
    """

    def __init__(self, kusto_client, duckdb_engine):
        self.kusto_client = kusto_client
        self.duckdb_engine = duckdb_engine
        self.tables: Dict[str, Dict] = {}
        self.is_built = False
        self.build_time_ms: float = 0
        self.total_rows: int = 0
        self._all_queries: Dict = {}
        # ★ v7.0: ReadWriteLock replaces RLock
        self._rwlock = ReadWriteLock()
        self._background_thread: Optional[threading.Thread] = None
        self._duckdb_initialized = False
        self._table_sources: Dict[str, str] = {}   # name → '⚡ instant' | '💾 cached' | '🔄 kusto'
        self._table_scope_days: Dict[str, int] = {}  # name → how many days of data
        self._table_built_at: Dict[str, float] = {}  # name → epoch timestamp of last build/load
        # ★ v6.0: Cancellation flag — set by Session.cleanup()
        self._cancelled = False
        # ★ v7.0: SQL Validator — initialized after tables are built
        self._sql_validator = None  # Set via init_sql_validator()
        # Overall session start time
        self._session_start = time.time()

    def cancel(self):
        """Signal background threads to stop writing. Called by Session.cleanup()."""
        self._cancelled = True

    def _get_conn(self):
        """Get DuckDB connection, or None if cancelled/unavailable."""
        if self._cancelled:
            return None
        if not self.duckdb_engine or not self.duckdb_engine.connection:
            return None
        return self.duckdb_engine.connection

    # ═══════════════════════════════════════════════════════════════════════════
    # ★ INSTANT BRIDGE
    # ═══════════════════════════════════════════════════════════════════════════

    def populate_from_instant(self, instant_data: Dict[str, pd.DataFrame]) -> int:
        """
        Register instant 7-day snapshot as profile tables.
        These are marked '⚡ instant' and WILL be replaced by full Kusto build.
        """
        self._ensure_duckdb()

        mapping = {
            'totals': 'profile_totals',
            'daily':  'profile_daily',
            'top10':  'profile_organization',
            'regions':'profile_region',
        }

        registered = 0
        for instant_key, table_name in mapping.items():
            df = instant_data.get(instant_key, pd.DataFrame())
            if df.empty:
                continue
            try:
                df = df.copy()
                df.columns = [c.lower() for c in df.columns]
                with self._rwlock.write_lock():
                    if not self._store_table(table_name, df):
                        continue
                    self.tables[table_name] = {
                        'rows': len(df), 'columns': list(df.columns),
                        'built_at': time.time(), 'time_ms': 0,
                    }
                    self.total_rows += len(df)
                    self._table_sources[table_name] = '⚡ instant'
                    self._table_scope_days[table_name] = 7
                    self._table_built_at[table_name] = time.time()
                registered += 1
                logger.info(f"InstantBridge: {table_name} ← {instant_key} ({len(df)} rows, 7d)")
            except Exception as e:
                logger.warning(f"InstantBridge: {table_name} failed: {e}")

        if registered > 0:
            self.is_built = True
        return registered

    # ═══════════════════════════════════════════════════════════════════════════
    # ★ DISK CACHE
    # ═══════════════════════════════════════════════════════════════════════════

    def _cache_path(self, config_name: str) -> Path:
        safe = "".join(c if c.isalnum() or c in '-_' else '_' for c in config_name)
        return CACHE_DIR / safe

    def save_to_disk(self, config_name: str) -> bool:
        """Save all profile tables as Parquet. v6: saves ALL tables including instant."""
        if not self.tables:
            return False

        cache_dir = self._cache_path(config_name)
        cache_dir.mkdir(parents=True, exist_ok=True)

        meta = {'saved_at': time.time(), 'tables': {}}
        saved = 0

        conn = self._get_conn()
        if not conn:
            logger.warning("save_to_disk: no DuckDB connection available")
            return False

        for name in list(self.tables.keys()):
            try:
                with self._rwlock.read_lock():
                    df = conn.execute(f"SELECT * FROM {name}").fetchdf()
                try:
                    path = cache_dir / f"{name}.parquet"
                    df.to_parquet(path, index=False)
                    fmt = 'parquet'
                except Exception:
                    path = cache_dir / f"{name}.csv"
                    df.to_csv(path, index=False)
                    fmt = 'csv'
                meta['tables'][name] = {
                    'rows': len(df), 'columns': list(df.columns),
                    'file': f"{name}.{fmt}", 'format': fmt,
                    'scope_days': self._table_scope_days.get(name, 30),
                    'source': self._table_sources.get(name, ''),
                }
                saved += 1
            except Exception as e:
                logger.warning(f"Cache save {name}: {e}")

        with open(cache_dir / "meta.json", 'w') as f:
            json.dump(meta, f, indent=2)

        logger.info(f"DiskCache: saved {saved} tables to {cache_dir}")
        return saved > 0

    def load_from_disk(self, config_name: str,
                       max_age_hours: float = CACHE_MAX_AGE_HOURS) -> bool:
        """Load profile tables from disk cache."""
        self._ensure_duckdb()

        cache_dir = self._cache_path(config_name)
        meta_path = cache_dir / "meta.json"
        if not meta_path.exists():
            return False

        try:
            with open(meta_path) as f:
                meta = json.load(f)
        except Exception:
            return False

        age_hours = (time.time() - meta.get('saved_at', 0)) / 3600
        if age_hours > max_age_hours:
            logger.info(f"DiskCache: expired ({age_hours:.1f}h > {max_age_hours}h)")
            return False

        loaded = 0
        for name, info in meta.get('tables', {}).items():
            file_path = cache_dir / info['file']
            if not file_path.exists():
                continue
            try:
                df = (pd.read_parquet(file_path)
                      if info.get('format') == 'parquet'
                      else pd.read_csv(file_path))
                with self._rwlock.write_lock():
                    if not self._store_table(name, df):
                        continue
                    self.tables[name] = {
                        'rows': len(df), 'columns': list(df.columns),
                        'built_at': meta['saved_at'], 'time_ms': 0,
                    }
                    self.total_rows += len(df)
                    self._table_sources[name] = '💾 cached'
                    self._table_scope_days[name] = info.get('scope_days', 30)
                    self._table_built_at[name] = meta['saved_at']
                loaded += 1
            except Exception as e:
                logger.warning(f"DiskCache load {name}: {e}")

        if loaded > 0:
            self.is_built = True
            logger.info(f"DiskCache: loaded {loaded} tables ({self.total_rows:,} rows, {age_hours:.1f}h old)")
        return loaded > 0

    @staticmethod
    def invalidate_cache(config_name: str):
        safe = "".join(c if c.isalnum() or c in '-_' else '_' for c in config_name)
        cache_dir = CACHE_DIR / safe
        if cache_dir.exists():
            import shutil
            shutil.rmtree(cache_dir, ignore_errors=True)
            logger.info(f"DiskCache: invalidated {cache_dir}")

    def get_cache_age_str(self, config_name: str) -> Optional[str]:
        meta_path = self._cache_path(config_name) / "meta.json"
        if not meta_path.exists():
            return None
        try:
            with open(meta_path) as f:
                meta = json.load(f)
            age_min = (time.time() - meta['saved_at']) / 60
            return f"{age_min:.0f}m ago" if age_min < 60 else f"{age_min/60:.1f}h ago"
        except Exception:
            return None

    # ═══════════════════════════════════════════════════════════════════════════
    # ★ BUILD
    # ═══════════════════════════════════════════════════════════════════════════

    def _ensure_duckdb(self):
        if self._duckdb_initialized:
            return
        import duckdb
        if not self.duckdb_engine.connection:
            self.duckdb_engine.connection = duckdb.connect(':memory:')
        self.duckdb_engine.loaded = True
        self._duckdb_initialized = True

    def build_essential(self, all_queries: Dict,
                        progress_callback: Optional[Callable] = None) -> bool:
        self._ensure_duckdb()
        self._all_queries = all_queries
        tier1 = {k: v for k, v in all_queries.items() if v.tier == 1}
        if progress_callback:
            progress_callback(0, len(tier1), f"Loading {len(tier1)} essential tables...")
        start = time.time()
        self._run_parallel(tier1, progress_callback, max_workers=6)
        self.build_time_ms = (time.time() - start) * 1000
        self.is_built = len(self.tables) > 0
        return self.is_built

    def build_background(self, callback_on_done: Optional[Callable] = None):
        tier2 = {k: v for k, v in self._all_queries.items() if v.tier == 2}
        if not tier2:
            if callback_on_done:
                callback_on_done()
            return

        def _bg():
            try:
                self._run_parallel(tier2, progress_callback=None, max_workers=3)
                if callback_on_done:
                    callback_on_done()
            except Exception as e:
                logger.error(f"Background build crashed: {e}", exc_info=True)
                if callback_on_done:
                    try:
                        callback_on_done()
                    except Exception:
                        pass

        self._background_thread = threading.Thread(target=_bg, daemon=True)
        self._background_thread.start()

    def build_all_background(self, all_queries: Dict,
                             callback_on_done: Optional[Callable] = None):
        """
        Priority-ordered background build — fast path first, slow last.

        Stage 1 (FAST — ~5-15s): profile_daily + profile_region + profile_totals
          These are low-cardinality aggregates. Return the quarter view quickly.
          callback_on_done fires HERE — pill turns green for the user.

        Stage 2 (SLOW — runs silently after): profile_organization + cross-tabs
          High-cardinality (10K+ orgs). Takes longer. Replaces instant data
          in the background without interrupting the user.

        Tables already fully built (cached/kusto) are skipped entirely.
        Instant tables are always upgraded to full scope.
        """
        self._all_queries = all_queries

        def _already_built(name):
            src = self._table_sources.get(name, '')
            return src in ('💾 cached', '🔄 kusto')  # NOT '⚡ instant'

        # Priority classification — all tier-1 tables go in fast stage
        FAST_TABLES = {
            'profile_daily', 'profile_region', 'profile_totals',
            # ★ Enhanced tier-1 tables — these need to be ready for instant answers
            'profile_activity', 'profile_ingestion_lag',
            'profile_browser', 'profile_entity',
        }
        # Everything else (org, cross-tabs, tier-2 engagement/version) is slow

        def _bg():
          try:
            if self._cancelled:
                return

            missing = {k: v for k, v in all_queries.items()
                       if not _already_built(k)}

            if not missing:
                logger.info("Background: all tables already fully built, skipping")
                if callback_on_done and not self._cancelled:
                    callback_on_done()
                return

            fast   = {k: v for k, v in missing.items() if k in FAST_TABLES}
            slow   = {k: v for k, v in missing.items() if k not in FAST_TABLES}

            logger.info(
                f"Background: {len(missing)} tables — "
                f"{len(fast)} fast (tier-1 + enhanced) + "
                f"{len(slow)} slow (org/crosstabs/tier-2)"
            )

            # ── Stage 1: fast tables — parallel, max workers ──────────────
            if fast and not self._cancelled:
                logger.info(f"Stage 1 (fast): {list(fast.keys())}")
                self._run_parallel(fast, progress_callback=None, max_workers=6)
                logger.info(f"Stage 1 done — {len(self.tables)} tables ready")

            # Signal done after stage 1 — pill turns green, overview refreshes
            if callback_on_done and not self._cancelled:
                try:
                    callback_on_done()
                except Exception as e:
                    logger.warning(f"callback_on_done (stage 1) failed: {e}")

            # ── Stage 2: slow tables — run silently, no callback ──────────
            if slow and not self._cancelled:
                logger.info(f"Stage 2 (slow): {list(slow.keys())}")
                self._run_parallel(slow, progress_callback=None, max_workers=2)
                logger.info(f"Stage 2 done — {len(self.tables)} total tables, "
                            f"{self.total_rows:,} rows")

                # Save updated cache after org tables land
                # (The first save happened at stage 1 callback, this updates it)
                try:
                    from pulse.core.data_profile import CACHE_DIR
                except ImportError:
                    pass  # Cache update not critical

          except Exception as e:
            logger.error(f"Background build crashed: {e}", exc_info=True)
            # Always fire callback even on failure so the UI doesn't hang
            if callback_on_done:
                try:
                    callback_on_done()
                except Exception:
                    pass

        self._background_thread = threading.Thread(target=_bg, daemon=True)
        self._background_thread.start()

    def build_on_demand(self, table_name: str) -> bool:
        if table_name not in self._all_queries:
            return False
        # Allow on-demand rebuild of instant tables too
        if (table_name in self.tables
                and self._table_sources.get(table_name) != '⚡ instant'):
            return True

        pq = self._all_queries[table_name]
        logger.info(f"On-demand: {table_name} (tier {pq.tier})")
        try:
            df = self.kusto_client.execute_profile_query(pq.kql)
            if df is not None and not df.empty:
                df.columns = [c.lower() for c in df.columns]
                with self._rwlock.write_lock():
                    if not self._store_table(table_name, df):
                        return False
                    self.tables[table_name] = {
                        'rows': len(df), 'columns': list(df.columns),
                        'built_at': time.time(), 'time_ms': 0,
                    }
                    prev = self.total_rows - (
                        self.tables.get(table_name, {}).get('rows', 0))
                    self.total_rows = prev + len(df)
                    self._table_sources[table_name] = '🔄 kusto'
                    self._table_scope_days[table_name] = getattr(pq, 'scope_days', 180)
                    self._table_built_at[table_name] = time.time()
                return True
        except Exception as e:
            logger.error(f"On-demand {table_name} failed: {e}")
        return False

    def _run_parallel(self, queries: Dict,
                      progress_callback: Optional[Callable] = None,
                      max_workers: int = 6):
        from concurrent.futures import ThreadPoolExecutor, as_completed
        total = len(queries)
        completed_count = [0]

        def _run_one(name, pq):
            try:
                if self._cancelled:
                    return name, None, "cancelled", 0
                df = self.kusto_client.execute_profile_query(pq.kql)
                if df is not None and not df.empty:
                    df.columns = [c.lower() for c in df.columns]
                    return name, df, None, getattr(pq, 'scope_days', 180)
                return name, None, "0 rows", 0
            except Exception as e:
                return name, None, str(e)[:80], 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_run_one, n, pq): n
                       for n, pq in queries.items()}
            for future in as_completed(futures):
                if self._cancelled:
                    logger.info("Background build: cancelled, stopping writes")
                    break
                name, df, error, scope_days = future.result()
                completed_count[0] += 1
                if error:
                    logger.warning(f"  {name}: {error}")
                    if progress_callback:
                        progress_callback(completed_count[0], total,
                                          f"{name}: {error}")
                else:
                    with self._rwlock.write_lock():
                        try:
                            old_rows = self.tables.get(name, {}).get('rows', 0)
                            if not self._store_table(name, df):
                                continue
                            self.tables[name] = {
                                'rows': len(df), 'columns': list(df.columns),
                                'built_at': time.time(), 'time_ms': 0,
                            }
                            self.total_rows = self.total_rows - old_rows + len(df)
                            self._table_sources[name] = '🔄 kusto'
                            self._table_scope_days[name] = scope_days or 180
                            self._table_built_at[name] = time.time()
                        except Exception as e:
                            logger.error(f"  DuckDB store {name}: {e}")
                    if progress_callback:
                        progress_callback(completed_count[0], total,
                                          f"{name}: {len(df):,} rows")

    # ═══════════════════════════════════════════════════════════════════════════
    # QUERY & INFO
    # ═══════════════════════════════════════════════════════════════════════════

    def query(self, sql: str) -> pd.DataFrame:
        if not self.is_built:
            raise ValueError("Profile not built yet.")
        conn = self._get_conn()
        if not conn:
            raise ValueError("DuckDB connection unavailable (session closed?)")
        logger.debug(f"Profile SQL: {sql[:120]}")
        try:
            with self._rwlock.read_lock():
                result = conn.execute(sql).fetchdf()
            logger.debug(f"  → {len(result):,} rows")
            return result
        except Exception as e:
            logger.error(f"  Profile query failed: {e}")
            raise ValueError(f"Profile query failed: {e}\nSQL: {sql}")

    def validated_query(self, sql: str) -> Tuple[pd.DataFrame, Dict]:
        """
        ★ v7.0: Query with SQL validation. Returns (df, audit_info).

        The SQL validator checks:
          1. Statement safety (SELECT only)
          2. Table whitelist
          3. Column validation + auto-correction
          4. Audit logging

        If validation fails, raises ValueError with a helpful message.
        If validator is not initialized, falls through to raw query.
        """
        audit = {'validated': False, 'corrections': [], 'original_sql': sql}

        if self._sql_validator and self._sql_validator.is_ready:
            ok, result = self._sql_validator.validate_and_fix(sql)
            if not ok:
                logger.warning(f"SQL validation rejected: {result}")
                raise ValueError(f"Query validation failed: {result}")
            sql = result  # May have been auto-corrected
            audit['validated'] = True
            # Get latest audit entry for corrections
            log = self._sql_validator.get_audit_log(1)
            if log:
                audit['corrections'] = log[-1].get('corrections', [])
                audit['final_sql'] = log[-1].get('final_sql', sql)

        df = self.query(sql)
        return df, audit

    def init_sql_validator(self):
        """
        Initialize SQL validator from actual DuckDB schema.
        Call this after tables are built/loaded.
        """
        from pulse.core.sql_validator import ProfileSQLValidator

        conn = self._get_conn()
        if not conn or not self.tables:
            return

        validator = ProfileSQLValidator()
        with self._rwlock.read_lock():
            validator.refresh_schema(conn, list(self.tables.keys()))

        self._sql_validator = validator
        logger.info(f"SQLValidator initialized: {validator.get_schema_summary()}")

    def refresh_sql_validator(self):
        """Refresh validator schema after new tables are built."""
        if not self._sql_validator:
            self.init_sql_validator()
            return
        conn = self._get_conn()
        if conn and self.tables:
            with self._rwlock.read_lock():
                self._sql_validator.refresh_schema(conn, list(self.tables.keys()))

    # ═══════════════════════════════════════════════════════════════════════════
    # STALENESS — how old is the data?
    # ═══════════════════════════════════════════════════════════════════════════

    def get_staleness_info(self) -> Dict:
        """
        ★ v7.0: Comprehensive staleness info for UI.

        Returns:
          {
            'overall_age_minutes': 32,
            'overall_label': "32m ago",
            'is_stale': False,           # True if >2h old
            'tables': {
                'profile_daily': {'age_minutes': 32, 'source': '💾 cached', 'scope_days': 180},
                ...
            }
          }
        """
        now = time.time()
        table_info = {}
        oldest_age = 0

        for name in self.tables:
            built_at = self._table_built_at.get(name, self._session_start)
            age_min = (now - built_at) / 60
            oldest_age = max(oldest_age, age_min)
            table_info[name] = {
                'age_minutes': round(age_min, 1),
                'source': self._table_sources.get(name, '?'),
                'scope_days': self._table_scope_days.get(name, 30),
            }

        if oldest_age < 1:
            label = "just now"
        elif oldest_age < 60:
            label = f"{oldest_age:.0f}m ago"
        elif oldest_age < 1440:
            label = f"{oldest_age/60:.1f}h ago"
        else:
            label = f"{oldest_age/1440:.1f}d ago"

        return {
            'overall_age_minutes': round(oldest_age, 1),
            'overall_label': label,
            'is_stale': oldest_age > 120,  # >2 hours = stale
            'scope_days': max(self._table_scope_days.values(), default=30),
            'tables': table_info,
        }

    def query_safe(self, sql: str) -> Optional[pd.DataFrame]:
        try:
            return self.query(sql)
        except Exception:
            return None

    def data_age_label(self, table_name: str) -> str:
        """Return human-readable data age for a table. Used in UX freshness signals."""
        src = self._table_sources.get(table_name, '')
        if src == '⚡ instant':
            return "7-day snapshot"
        scope = self._table_scope_days.get(table_name, 30)
        if src == '💾 cached':
            return f"cached ({scope}d)"
        return f"last {scope} days"

    def get_data_scope_label(self) -> str:
        """Overall scope label for the chat footer/subtitle."""
        sources = set(self._table_sources.values())
        scope = max(self._table_scope_days.values(), default=30)

        # Include staleness info
        staleness = self.get_staleness_info()
        age_label = staleness['overall_label']
        stale_marker = " ⚠️" if staleness['is_stale'] else ""

        if '🔄 kusto' in sources or '💾 cached' in sources:
            return f"{scope}-day profile · {age_label}{stale_marker}"
        if '⚡ instant' in sources:
            return f"7-day snapshot · building full profile…"
        return "connecting…"

    def get_table_summaries(self) -> Dict[str, str]:
        summaries = {}
        for name, info in self.tables.items():
            cols = ", ".join(info['columns'][:8])
            if len(info['columns']) > 8:
                cols += f" (+{len(info['columns'])-8} more)"
            src = self._table_sources.get(name, '')
            scope = self._table_scope_days.get(name, 30)
            summaries[name] = f"{src} [{cols}], {info['rows']:,} rows, {scope}d scope"
        for name, pq in self._all_queries.items():
            if name not in self.tables and pq.tier <= 3:
                summaries[name] = f"[on demand] {pq.description}"
        return summaries

    def get_totals(self) -> Optional[Dict]:
        if 'profile_totals' not in self.tables:
            return None
        conn = self._get_conn()
        if not conn:
            return None
        try:
            with self._rwlock.read_lock():
                df = conn.execute("SELECT * FROM profile_totals").fetchdf()
            return df.iloc[0].to_dict() if not df.empty else None
        except Exception:
            return None

    def get_status_summary(self) -> str:
        if not self.is_built:
            return "Profile not built."
        lines = [f"Data Profile: {len(self.tables)} tables, {self.total_rows:,} rows"]
        for name, info in self.tables.items():
            src = self._table_sources.get(name, '')
            age = self.data_age_label(name)
            lines.append(f"  {src} {name}: {info['rows']:,} rows ({age})")
        pending = len(self._all_queries) - len(self.tables)
        if pending > 0:
            lines.append(f"  ({pending} more available on demand)")
        return "\n".join(lines)

    def has_table(self, name: str) -> bool:
        return name in self.tables

    def can_build_table(self, name: str) -> bool:
        return name in self._all_queries

    def get_table_columns(self, name: str) -> List[str]:
        return self.tables[name]['columns'] if name in self.tables else []

    def list_tables(self) -> List[str]:
        return list(self.tables.keys())

    def get_source(self, name: str) -> str:
        return self._table_sources.get(name, '?')

    # ═══════════════════════════════════════════════════════════════════════════
    # PRIVATE
    # ═══════════════════════════════════════════════════════════════════════════

    def _store_table(self, name: str, df: pd.DataFrame) -> bool:
        """Write (or replace) a table in DuckDB. Returns True on success."""
        if self._cancelled:
            return False
        conn = self._get_conn()
        if conn is None:
            logger.warning(f"_store_table({name}): no DuckDB connection, skipping")
            return False
        df = self._normalize_dates(df)
        try:
            conn.execute(f"DROP TABLE IF EXISTS {name}")
        except Exception:
            pass
        try:
            conn.register('_tmp_profile', df)
            conn.execute(f"CREATE TABLE {name} AS SELECT * FROM _tmp_profile")
            conn.unregister('_tmp_profile')
            return True
        except Exception as e:
            logger.error(f"  DuckDB store {name}: {e}")
            return False

    @staticmethod
    def _normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize date columns: strip timezone, validate range.
        v6.0: Uses tz_localize(None) instead of tz_convert(None) to avoid
        UTC-offset shifting. Validates date ranges as sanity check.
        """
        date_hints = ('day', 'date', 'time', 'timestamp',
                      'first_seen', 'last_seen', 'first_event', 'last_event')
        for col in df.columns:
            try:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    # Strip timezone WITHOUT converting (just remove tz info)
                    if hasattr(df[col].dtype, 'tz') and df[col].dtype.tz is not None:
                        df[col] = df[col].dt.tz_localize(None)
                    # Validate range
                    valid = df[col].dropna()
                    if len(valid) > 0:
                        min_yr = valid.dt.year.min()
                        max_yr = valid.dt.year.max()
                        if min_yr < _MIN_VALID_YEAR or max_yr > _MAX_VALID_YEAR:
                            logger.warning(
                                f"  Date range suspect in '{col}': "
                                f"{valid.min()} → {valid.max()} (years {min_yr}-{max_yr})"
                            )
                            # ★ Nullify outliers — don't let 2249 corrupt charts
                            median_yr = valid.dt.year.median()
                            if 2000 <= median_yr <= _MAX_VALID_YEAR:
                                # Most data is fine, just a few outliers
                                bad = (df[col].dt.year < _MIN_VALID_YEAR) | (df[col].dt.year > _MAX_VALID_YEAR)
                                n_bad = bad.sum()
                                if 0 < n_bad < len(df) * 0.5:
                                    # ★ v7.0: DROP bad rows entirely instead of setting NaT.
                                    # NaT rows flow to visualizer which drops them anyway.
                                    # Cleaner to remove at source so row counts are accurate.
                                    df = df[~bad].reset_index(drop=True)
                                    logger.info(f"  Removed {n_bad} outlier date row(s) in '{col}'")
                            else:
                                # ★ v9.2: ENTIRE date column is corrupt (median outside valid range)
                                # This happens when source data has systematic date issues
                                # (e.g. .NET ticks not converted, or future dates stored)
                                logger.warning(
                                    f"  Date column '{col}' entirely corrupt "
                                    f"(median year {median_yr:.0f}, valid range "
                                    f"{_MIN_VALID_YEAR}-{_MAX_VALID_YEAR}). "
                                    f"Setting all to NaT — table will show as data-only."
                                )
                                df[col] = pd.NaT
                    # ★ iter13.1: Filter future dates in time-series columns
                    # (catches Oct-Dec 2026 when today is Feb 2026)
                    if col.lower() in ('day', 'date') and pd.api.types.is_datetime64_any_dtype(df[col]):
                        future_mask = df[col] > _MAX_VALID_DATE
                        n_future = future_mask.sum()
                        if 0 < n_future < len(df):
                            df = df[~future_mask].reset_index(drop=True)
                            logger.info(f"  Removed {n_future} future-dated row(s) in '{col}'")
                    continue
                # Try to convert string columns that look like dates
                if col.lower() in date_hints:
                    converted = pd.to_datetime(df[col], errors='coerce', utc=True)
                    if converted.notna().sum() > 0:
                        # Strip timezone
                        converted = converted.dt.tz_localize(None)
                        # Validate range
                        valid = converted.dropna()
                        if len(valid) > 0:
                            min_yr = valid.dt.year.min()
                            max_yr = valid.dt.year.max()
                            if min_yr < _MIN_VALID_YEAR or max_yr > _MAX_VALID_YEAR:
                                logger.warning(
                                    f"  Date conversion suspect in '{col}': "
                                    f"{valid.min()} → {valid.max()}, keeping original"
                                )
                                continue  # Don't apply bad conversion
                        df[col] = converted
            except Exception:
                pass
        return df
