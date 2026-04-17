"""
Kusto Client v3.1 - Cached Connections, Fast Profile Queries
=============================================================

Key changes from v3.0:
  1. Client CACHING - authenticate ONCE per cluster, reuse forever
  2. Timeout protection - 60s default, configurable per query
  3. Profile-optimized execute - skip DuckDB load, return raw DataFrame
  4. Single-cluster profile queries - no need to union for aggregates

Author: PULSE Team
"""

import os
import logging
import pandas as pd
from typing import Dict
from azure.kusto.data import KustoClient, ClientRequestProperties
from azure.kusto.data.helpers import dataframe_from_result_table
from .config_loader import DataSourceConfig, StrategyType
from .auth_manager import KustoAuthManager, AuthMethod
from .duckdb_engine import DuckDBQueryEngine

logger = logging.getLogger(__name__)


class ConfigDrivenKustoClient:
    """Multi-cluster Kusto client with connection caching."""

    def __init__(self, config: DataSourceConfig, duckdb_engine: DuckDBQueryEngine):
        self.config = config
        self.duckdb_engine = duckdb_engine
        self._clients = {}          # cluster_name -> KustoClient
        self._client_created: dict = {}  # cluster_name -> created timestamp
        # Circuit breaker: consecutive timeout counter + skip-until timestamp per cluster
        self._cluster_failures: dict = {}
        self._cluster_skip_until: dict = {}

    # -- Connection Management --

    def _get_client(self, cluster) -> KustoClient:
        """Get or create a client. Recreates after 45min to prevent token expiry disconnect."""
        import time as _time
        age = _time.time() - self._client_created.get(cluster.name, 0)
        # Azure CLI tokens last ~60min. Proactively recreate client after 45min.
        if cluster.name not in self._clients or age > 2700:
            if cluster.name in self._clients:
                logger.info(f"Refreshing client for {cluster.name} (age {age/60:.0f}min)")
            client = KustoAuthManager.create_client(
                cluster_url=cluster.url,
                auth_method=self.config.auth_method,
                client_id=self.config.auth_client_id,
                client_secret=os.getenv("AZURE_CLIENT_SECRET"),
                tenant_id=self.config.auth_tenant_id,
            )
            if not client:
                raise ValueError(f"Failed to create client for {cluster.name}")
            self._clients[cluster.name] = client
            self._client_created[cluster.name] = _time.time()
            logger.info(f"Client ready for {cluster.name}")
        return self._clients[cluster.name]

    def connect_all_clusters(self):
        """Connect to ALL clusters and cache clients."""
        logger.info(f"Connecting to {len(self.config.clusters)} cluster(s)...")

        errors = []
        for cluster in self.config.clusters:
            try:
                logger.info(f"Connecting to {cluster.name}...")
                client = self._get_client(cluster)

                # Quick test query
                props = self._make_props(timeout_sec=30)
                result = client.execute(cluster.database, f"{cluster.table} | take 1", props)
                logger.info(f"Connected to {cluster.name}")

            except Exception as e:
                error_msg = f"Failed to connect to {cluster.name}: {e}"
                logger.error(error_msg)
                errors.append(error_msg)
                # Remove failed client from cache
                self._clients.pop(cluster.name, None)

        if errors:
            if len(errors) == len(self.config.clusters):
                raise ConnectionError(f"All clusters failed:\n" + "\n".join(errors))
            else:
                logger.warning(f"{len(errors)} cluster(s) failed")
        else:
            logger.info(f"All {len(self.config.clusters)} cluster(s) connected")

    # -- Query Execution --

    def execute_query(self, user_kql: str, timeout_sec: int = 120) -> pd.DataFrame:
        """Execute query across all clusters, load into DuckDB."""

        table = self.config.clusters[0].table
        complete_kql = self._build_kql(table, user_kql)
        logger.info(f"KQL: {complete_kql[:150]}...")

        results = []
        errors = []
        props = self._make_props(timeout_sec)

        for cluster in self.config.clusters:
            try:
                client = self._get_client(cluster)
                result = client.execute(cluster.database, complete_kql, props)
                df = dataframe_from_result_table(result.primary_results[0])

                if self.config.get_effective_strategy() == StrategyType.LABELED_UNION:
                    df['_source_cluster'] = cluster.name

                results.append(df)
                logger.info(f"{cluster.name}: {len(df):,} rows")

            except Exception as e:
                logger.error(f"{cluster.name} failed: {str(e)[:100]}")
                errors.append(str(e))
                # Only reset client on connection errors
                err_str = str(e).lower()
                if 'timeout' in err_str or 'connection' in err_str or 'socket' in err_str:
                    self._clients.pop(cluster.name, None)

        if not results:
            raise ValueError(f"All clusters failed:\n" + "\n".join(errors))

        combined = results[0] if len(results) == 1 else pd.concat(results, ignore_index=True)
        logger.info(f"Total: {len(combined):,} rows from {len(results)} cluster(s)")

        self.duckdb_engine.load_data(combined)
        return combined

    def execute_profile_query(self, kql: str, timeout_sec: int = 90) -> pd.DataFrame:
        """
        Execute a PROFILE query. Optimized:
          - Uses FIRST available cluster only (aggregates don't need union)
          - Skips DuckDB load (profile manager handles that)
          - Has its own timeout
          - Retries on second cluster if first fails
        """
        table = self.config.clusters[0].table

        # KQL from semantic layer starts with "where ..." — need "| where ..."
        kql_clean = kql.strip()
        if not kql_clean.startswith("|"):
            kql_clean = f"| {kql_clean}"
        # ★ Performance hints for large-scale profile queries:
        #   query_now     → skip result cache staleness check, start immediately
        #   notruncation  → don't truncate results at default 64MB limit
        #   shufflekey    → hints are embedded in individual queries where needed
        kusto_hints = (
            "set notruncation;\n"
            "set query_results_cache_max_age = time(4h);\n"
        )
        complete_kql = f"{kusto_hints}{table}\n{kql_clean}"

        props = self._make_props(timeout_sec)

        import time as _time
        _now = _time.time()

        for cluster in self.config.clusters:
            # ── Circuit breaker: skip degraded clusters ──────────────────────
            skip_until = self._cluster_skip_until.get(cluster.name, 0)
            if _now < skip_until:
                remaining = int(skip_until - _now)
                logger.info(f"Profile [{cluster.name}]: skipping (circuit open, {remaining}s remaining)")
                continue

            try:
                client = self._get_client(cluster)
                result = client.execute(cluster.database, complete_kql, props)
                df = dataframe_from_result_table(result.primary_results[0])
                logger.info(f"Profile [{cluster.name}]: {len(df):,} rows")
                # Reset failure count on success
                self._cluster_failures[cluster.name] = 0
                return df

            except Exception as e:
                err_str = str(e).lower()
                err_msg = str(e)[:120]

                # ★ 0x80131500 = resource limit — skip to next, don't retry same query
                if '0x80131500' in err_str or 'partial query failure' in err_str:
                    logger.warning(f"Profile [{cluster.name}] resource limit: {err_msg}")
                    raise ValueError(
                        f"Profile query hit Kusto resource limit (0x80131500). "
                        f"Try reducing the time scope. Error: {err_msg}"
                    )

                logger.warning(f"Profile [{cluster.name}] failed: {err_msg}")

                # ── Update circuit breaker on timeout ─────────────────────────
                if 'timeout' in err_str or 'timed out' in err_str:
                    self._clients.pop(cluster.name, None)
                    failures = self._cluster_failures.get(cluster.name, 0) + 1
                    self._cluster_failures[cluster.name] = failures
                    if failures >= 2:
                        # Open circuit for 90s after 2 consecutive timeouts
                        self._cluster_skip_until[cluster.name] = _now + 90
                        logger.warning(
                            f"Profile [{cluster.name}]: circuit OPEN for 90s "
                            f"after {failures} consecutive timeouts"
                        )
                elif 'connection' in err_str or 'socket' in err_str:
                    self._clients.pop(cluster.name, None)

                continue  # Try next cluster

        raise ValueError(f"Profile query failed on all clusters")

    # -- Schema Discovery --

    def discover_schema(self) -> dict:
        """Discover table schema from first available cluster."""
        type_map = {
            'System.DateTime': 'datetime', 'System.String': 'string',
            'System.Int32': 'int', 'System.Int64': 'long',
            'System.Double': 'decimal', 'System.Boolean': 'bool',
            'System.Guid': 'guid', 'System.Dynamic': 'dynamic',
            'System.TimeSpan': 'timespan',
        }

        for cluster in self.config.clusters:
            try:
                logger.info(f"Discovering schema from {cluster.name}...")
                client = self._get_client(cluster)
                props = self._make_props(timeout_sec=30)
                query = f"{cluster.table} | getschema"
                result = client.execute(cluster.database, query, props)
                df = dataframe_from_result_table(result.primary_results[0])

                schema = {}
                for _, row in df.iterrows():
                    col_name = row.get('ColumnName', '')
                    col_type = row.get('DataType', 'System.String')
                    schema[col_name] = type_map.get(col_type, 'string')

                logger.info(f"Discovered {len(schema)} columns from {cluster.name}")
                return schema

            except Exception as e:
                logger.error(f"Schema discovery failed on {cluster.name}: {e}")
                continue

        return {}

    def fire_instant_dashboard(self, time_column: str = 'EventInfo_Time',
                               org_column: str = 'OrgId',
                               geo_column: str = 'GeoName',
                               timeout_sec: int = 60,
                               scope: str = '30d') -> Dict[str, pd.DataFrame]:
        """
        ★ INSTANT DASHBOARD: Parallel lightweight queries.

        v7.0: Runs all 3 queries concurrently via ThreadPoolExecutor.
        ~3x faster than sequential — from ~40s down to ~15s on typical clusters.

        Default scope is 30d. Accepts '7d', '30d', '90d', '180d' etc.
        Uses config-driven column names (not hardcoded) so it works for any team.
        Each query succeeds or fails independently.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        table = self.config.clusters[0].table
        cluster = self.config.clusters[0]
        client = self._get_client(cluster)
        results = {k: pd.DataFrame() for k in ('totals', 'daily', 'top10', 'regions')}

        queries = [
            ('daily',
             f"{table}\n| where {time_column} > ago({scope})\n"
             f"| summarize events=count(), active_orgs=dcount({org_column})"
             f" by Day=bin({time_column}, 1d)\n"
             f"| order by Day asc",
             30),
            ('top10',
             f"{table}\n| where {time_column} > ago({scope})\n"
             f"| summarize events=count() by {org_column}\n"
             f"| top 10 by events desc",
             30),
            ('regions',
             f"{table}\n| where {time_column} > ago({scope})\n"
             f"| summarize events=count(), active_orgs=dcount({org_column})"
             f" by {geo_column}\n"
             f"| order by active_orgs desc",
             30),
        ]

        def _run_query(name, kql, timeout):
            try:
                props = self._make_props(timeout)
                result = client.execute(cluster.database, kql, props)
                df = dataframe_from_result_table(result.primary_results[0])
                df.columns = [c.lower() for c in df.columns]
                logger.info(f"InstantDash [{name}]: {len(df)} rows")
                return name, df
            except Exception as e:
                logger.warning(f"InstantDash [{name}] skipped: {str(e)[:80]}")
                return name, pd.DataFrame()

        # ★ Run all 3 queries in parallel
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(_run_query, n, k, t) for n, k, t in queries]
            for future in as_completed(futures):
                name, df = future.result()
                if not df.empty:
                    results[name] = df

        # Derive totals from daily data (free, no extra query)
        daily = results.get('daily', pd.DataFrame())
        if not daily.empty:
            for c in daily.columns:
                if c.lower() in ('events', 'event_count'):
                    total = daily[c].sum()
                    results['totals'] = pd.DataFrame({'events': [total]})
                    logger.info(f"InstantDash [totals]: derived {total:,.0f} from daily sum")
                    break

        loaded = {k: len(v) for k, v in results.items() if not v.empty}
        logger.info(f"InstantDash complete: {loaded}")
        return results

    # -- Helpers --

    def _build_kql(self, table: str, user_kql: str) -> str:
        """Build complete KQL with table, filters, and user query."""
        parts = [table]

        for f in self.config.mandatory_filters:
            parts.append(f"| where {f}")

        user_kql = user_kql.strip()
        time_keywords = ['ago(', 'eventinfo_time', 'between(', 'startofday', 'startofweek', 'startofmonth']
        if not any(kw in user_kql.lower() for kw in time_keywords):
            parts.append("| where EventInfo_Time > ago(90d)")  # PULSE quarter scope

        if user_kql:
            if user_kql.startswith("|"):
                user_kql = user_kql[1:].strip()
            parts.append(f"| {user_kql}")

        return "\n".join(parts)

    @staticmethod
    def _make_props(timeout_sec: int = 60) -> ClientRequestProperties:
        """Create request properties with timeout."""
        from datetime import timedelta
        props = ClientRequestProperties()
        props.set_option("servertimeout", timedelta(seconds=timeout_sec))
        return props

    def get_schema_context(self):
        """Schema context for LLM prompt."""
        config = self.config
        table = config.clusters[0].table

        lines = [
            f"TABLE: {table}",
            f"DESCRIPTION: {config.description}",
            f"EACH ROW = one telemetry event.",
            f"DATA SCOPE: Last 30 days. {len(config.clusters)} cluster(s): "
            + ", ".join(c.name for c in config.clusters) + ".",
            "",
            "KEY COLUMNS:",
            "  EventInfo_Time (datetime) - Primary timestamp",
            "  OrgId (string) - Organization ID, use dcount(OrgId) for active orgs",
            "  UserId (string) - User ID",
            "  SessionId (string) - Session ID",
            "  EntityName (string) - Feature/entity used",
            "  OperationType (string) - Operation type",
            "  GeoName (string) - Region (EMEA, NAM, APAC)",
            "  PipelineInfo_ClientCountry (string) - Country",
            "  AppModule (string) - App module",
            "  DeviceInfo_BrowserName (string) - Browser",
            "",
            "COUNTING PATTERNS:",
            "  Events -> count()",
            "  Active orgs -> dcount(OrgId)",
            "  Users -> dcount(UserId)",
            "  Sessions -> dcount(SessionId)",
        ]

        return "\n".join(lines)
