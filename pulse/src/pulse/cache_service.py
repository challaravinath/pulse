"""
PULSE Cache Pre-Warm Service
==============================

Runs independently of user sessions.  Queries Kusto on a schedule and saves
pre-aggregated data to disk (Parquet).  When users connect via the FastAPI
server, they load from disk in < 1 second — no Kusto auth required.

Usage:
  # One-shot warm (CI, cron, Task Scheduler)
  python -m pulse.cache_service

  # Continuous daemon (keeps running, refreshes on schedule)
  python -m pulse.cache_service --daemon

  # Custom schedule
  python -m pulse.cache_service --daemon --fast-interval 1800 --slow-interval 7200

Schedule (daemon mode):
  Fast loop  (default 30 min) : 7d + 30d  daily / region / org / totals
  Slow loop  (default 2 hrs)  : 90d  daily / region / totals
  Deep loop  (default 12 hrs) : 180d daily / region / totals

Author: PULSE Team
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import threading
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

# ── Path setup ──────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent.parent   # pulse_final/
sys.path.insert(0, str(ROOT / "src"))

from pulse.core.config_loader import ConfigLoader, AuthMethod
from pulse.core.kusto_client import ConfigDrivenKustoClient
from pulse.core.duckdb_engine import DuckDBQueryEngine
from pulse.core.semantic_layer import SemanticLayer
from pulse.utils.config import AppConfig

logger = logging.getLogger("pulse.cache_service")

# ── Cache constants (must match data_profile.py) ────────────────────────────
CACHE_DIR = Path("./cache/profile_cache")

def _safe_name(config_name: str) -> str:
    return "".join(c if c.isalnum() or c in '-_' else '_' for c in config_name)


# ═══════════════════════════════════════════════════════════════════════════════
# Core: run all profile queries for a given scope and save to disk
# ═══════════════════════════════════════════════════════════════════════════════

class CacheWarmer:
    """Connects to Kusto, runs profile queries, writes Parquet cache."""

    def __init__(self, config_dir: str = None):
        self.config_dir = config_dir
        self._loader: Optional[ConfigLoader] = None
        self._connections: Dict[str, tuple] = {}   # config_name → (kc, sem)
        self._last_warm: Dict[str, float] = {}     # "config:scope" → timestamp

    def discover_configs(self) -> list:
        """Load all valid configs from disk."""
        self._loader = ConfigLoader(
            config_dir=self.config_dir or str(ROOT / "configs"),
            system_defaults_path=str(ROOT / "system_defaults.yaml"),
        )
        configs = self._loader.get_all_configs()
        logger.info(f"Discovered {len(configs)} config(s): {[c.name for c in configs]}")
        return configs

    def connect(self, cfg, auth_method: str = "azure_cli") -> bool:
        """Authenticate and connect to Kusto clusters for a config."""
        if cfg.name in self._connections:
            return True

        auth_map = {
            "azure_cli": AuthMethod.AZURE_CLI,
            "managed_identity": AuthMethod.MANAGED_IDENTITY,
            "service_principal": AuthMethod.SERVICE_PRINCIPAL,
        }
        cfg.auth_method = auth_map.get(auth_method, AuthMethod.AZURE_CLI)

        try:
            app_config = AppConfig.load()
            duckdb_engine = DuckDBQueryEngine(app_config.cache_dir)

            kc = ConfigDrivenKustoClient(cfg, duckdb_engine)
            kc.connect_all_clusters()

            # Discover schema for semantic layer
            schema = kc.discover_schema()

            # Load semantic layer
            sem = SemanticLayer(config_path=cfg.source_path) if cfg.source_path else SemanticLayer()

            if sem.is_loaded and schema:
                validation = sem.validate_against_schema(schema)
                if not validation.get('valid', True):
                    sem.prune_queries_by_schema(schema)

            self._connections[cfg.name] = (kc, sem, duckdb_engine)
            logger.info(f"Connected: {cfg.name} ({len(kc._clients)} clusters)")
            return True

        except Exception as e:
            logger.error(f"Connect failed for {cfg.name}: {e}")
            return False

    def warm(self, cfg, scopes: list[str] = None) -> Dict[str, int]:
        """
        Run profile queries and save results to disk cache.

        Args:
            cfg: Config object
            scopes: List of scopes to warm, e.g. ['7d', '30d', '90d', '180d']
                    Default: ['7d', '30d']

        Returns:
            Dict of scope → rows cached
        """
        if cfg.name not in self._connections:
            if not self.connect(cfg):
                return {}

        kc, sem, duckdb_engine = self._connections[cfg.name]
        if not sem.is_loaded:
            logger.warning(f"No semantic model for {cfg.name} — skipping")
            return {}

        scopes = scopes or ['7d', '30d']
        results = {}

        for scope in scopes:
            cache_key = f"{cfg.name}:{scope}"

            try:
                t0 = time.time()
                logger.info(f"Warming {cfg.name} @ {scope}...")

                rows = self._run_scope(cfg, kc, sem, duckdb_engine, scope)
                elapsed = time.time() - t0

                self._last_warm[cache_key] = time.time()
                results[scope] = rows
                logger.info(f"  {scope}: {rows:,} rows in {elapsed:.1f}s")

            except Exception as e:
                logger.error(f"  {scope} failed: {e}")
                results[scope] = 0

        return results

    def _run_scope(self, cfg, kc, sem, duckdb_engine, scope: str) -> int:
        """Run instant dashboard + profile queries for a specific scope, save to disk."""

        time_col = sem.model.time_column if sem.model else "EventInfo_Time"
        org_col, geo_col = "OrgId", "GeoName"
        if sem.model and sem.model.dimensions:
            for did, dim in sem.model.dimensions.items():
                if did in ("organization", "org", "tenant"):
                    org_col = dim.column
                elif did in ("region", "geo", "geography"):
                    geo_col = dim.column

        # ── Run instant dashboard queries at this scope ──
        instant = kc.fire_instant_dashboard(
            time_column=time_col,
            org_column=org_col,
            geo_column=geo_col,
            scope=scope,
        )

        # ── Also run the semantic profile queries if scope > 7d ──
        # For wider scopes, run the daily/region/totals queries directly
        # since instant dashboard only gives basic aggregates
        profile_data = {}

        # Map instant dashboard results to profile table names
        mapping = {
            'totals': 'profile_totals',
            'daily':  'profile_daily',
            'top10':  'profile_organization',
            'regions': 'profile_region',
        }

        for instant_key, table_name in mapping.items():
            df = instant.get(instant_key, pd.DataFrame())
            if not df.empty:
                df.columns = [c.lower() for c in df.columns]
                profile_data[table_name] = df

        # For wider scopes, try semantic profile queries too (they have richer metrics)
        if scope not in ('7d',):
            profile_queries = sem.get_profile_queries()
            for qname, pq in profile_queries.items():
                # Only run tier 1 queries (fast ones)
                if pq.tier > 1:
                    continue
                # Skip if we already have this table from instant dash
                if qname in profile_data and len(profile_data[qname]) > 10:
                    continue
                try:
                    df = kc.execute_profile_query(pq.kql)
                    if df is not None and not df.empty:
                        df.columns = [c.lower() for c in df.columns]
                        profile_data[qname] = df
                        logger.info(f"  Profile [{qname}]: {len(df)} rows")
                except Exception as e:
                    logger.warning(f"  Profile [{qname}] failed: {str(e)[:80]}")

        # ── Save to disk in DataProfile-compatible format ──
        total_rows = self._save_cache(cfg.name, scope, profile_data)
        return total_rows

    def _save_cache(self, config_name: str, scope: str, tables: Dict[str, pd.DataFrame]) -> int:
        """Save tables to Parquet in DataProfile-compatible format."""
        if not tables:
            return 0

        safe = _safe_name(config_name)
        cache_dir = CACHE_DIR / safe
        cache_dir.mkdir(parents=True, exist_ok=True)

        # ── Load existing meta to merge scopes ──
        meta_path = cache_dir / "meta.json"
        try:
            if meta_path.exists():
                with open(meta_path) as f:
                    meta = json.load(f)
            else:
                meta = {'saved_at': time.time(), 'tables': {}}
        except Exception:
            meta = {'saved_at': time.time(), 'tables': {}}

        saved = 0
        total_rows = 0

        # Parse scope to days
        scope_days = int(scope.replace('d', '').replace('D', ''))

        for name, df in tables.items():
            try:
                # Normalize dates before saving
                df = _normalize_dates(df)

                path = cache_dir / f"{name}.parquet"
                df.to_parquet(path, index=False)

                # Only update meta if this scope is wider than existing
                existing_scope = meta.get('tables', {}).get(name, {}).get('scope_days', 0)
                if scope_days >= existing_scope:
                    meta['tables'][name] = {
                        'rows': len(df),
                        'columns': list(df.columns),
                        'file': f"{name}.parquet",
                        'format': 'parquet',
                        'scope_days': scope_days,
                    }

                saved += 1
                total_rows += len(df)
            except Exception as e:
                logger.warning(f"  Save {name}: {e}")

        meta['saved_at'] = time.time()
        meta['warmed_scope'] = scope
        meta['warmed_by'] = 'cache_service'

        with open(meta_path, 'w') as f:
            json.dump(meta, f, indent=2)

        logger.info(f"  Saved {saved} tables ({total_rows:,} rows) to {cache_dir}")
        return total_rows


def _normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Strip timezone info from datetime columns for DuckDB compatibility."""
    date_hints = ('day', 'date', 'time', 'timestamp', 'first_seen', 'last_seen',
                  'first_event', 'last_event')
    for col in df.columns:
        try:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                if hasattr(df[col].dtype, 'tz') and df[col].dtype.tz is not None:
                    df[col] = df[col].dt.tz_convert(None)
                continue
            if col.lower() in date_hints:
                converted = pd.to_datetime(df[col], errors='coerce')
                if converted.notna().sum() > 0:
                    if hasattr(converted.dtype, 'tz') and converted.dtype.tz is not None:
                        converted = converted.dt.tz_convert(None)
                    df[col] = converted
        except Exception:
            pass
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# Daemon mode: scheduled loop
# ═══════════════════════════════════════════════════════════════════════════════

class CacheDaemon:
    """Runs CacheWarmer on a schedule."""

    def __init__(self, warmer: CacheWarmer,
                 fast_interval: int = 1800,   # 30 min
                 slow_interval: int = 7200,   # 2 hours
                 deep_interval: int = 43200): # 12 hours
        self.warmer = warmer
        self.fast_interval = fast_interval
        self.slow_interval = slow_interval
        self.deep_interval = deep_interval
        self._stop = threading.Event()

    def run(self, configs: list):
        """Main daemon loop."""
        logger.info(
            f"Cache daemon starting — "
            f"fast: {self.fast_interval}s, slow: {self.slow_interval}s, deep: {self.deep_interval}s"
        )

        last_fast = 0
        last_slow = 0
        last_deep = 0

        while not self._stop.is_set():
            now = time.time()

            for cfg in configs:
                try:
                    # ── Fast loop: 7d + 30d ──
                    if now - last_fast >= self.fast_interval:
                        self.warmer.warm(cfg, scopes=['7d', '30d'])

                    # ── Slow loop: 90d ──
                    if now - last_slow >= self.slow_interval:
                        self.warmer.warm(cfg, scopes=['90d'])

                    # ── Deep loop: 180d ──
                    if now - last_deep >= self.deep_interval:
                        self.warmer.warm(cfg, scopes=['180d'])

                except Exception as e:
                    logger.error(f"Daemon error for {cfg.name}: {e}")

            # Update timestamps after processing all configs
            if now - last_fast >= self.fast_interval:
                last_fast = now
            if now - last_slow >= self.slow_interval:
                last_slow = now
            if now - last_deep >= self.deep_interval:
                last_deep = now

            # Sleep in small increments so we can stop quickly
            for _ in range(60):  # check every 1s, sleep up to 60s
                if self._stop.is_set():
                    break
                time.sleep(1)

        logger.info("Cache daemon stopped")

    def stop(self):
        self._stop.set()


# ═══════════════════════════════════════════════════════════════════════════════
# Integration: start as background thread inside FastAPI server
# ═══════════════════════════════════════════════════════════════════════════════

_daemon_instance: Optional[CacheDaemon] = None
_daemon_thread: Optional[threading.Thread] = None


def start_background_warmer(config_dir: str = None,
                            fast_interval: int = 1800,
                            slow_interval: int = 7200,
                            deep_interval: int = 43200):
    """Start cache warmer as a background thread (call from FastAPI lifespan)."""
    global _daemon_instance, _daemon_thread

    if _daemon_thread and _daemon_thread.is_alive():
        logger.info("Cache warmer already running")
        return

    warmer = CacheWarmer(config_dir=config_dir)
    configs = warmer.discover_configs()
    if not configs:
        logger.warning("No configs found — cache warmer not started")
        return

    _daemon_instance = CacheDaemon(
        warmer,
        fast_interval=fast_interval,
        slow_interval=slow_interval,
        deep_interval=deep_interval,
    )

    def _run():
        try:
            # Initial warm: connect + warm all configs immediately
            for cfg in configs:
                try:
                    warmer.connect(cfg)
                    warmer.warm(cfg, scopes=['7d', '30d'])
                except Exception as e:
                    logger.error(f"Initial warm failed for {cfg.name}: {e}")

            # Then run the scheduled daemon
            _daemon_instance.run(configs)
        except Exception as e:
            logger.error(f"Cache warmer crashed: {e}")

    _daemon_thread = threading.Thread(target=_run, daemon=True, name="cache-warmer")
    _daemon_thread.start()
    logger.info("Cache warmer started as background thread")


def stop_background_warmer():
    """Stop the background cache warmer."""
    global _daemon_instance
    if _daemon_instance:
        _daemon_instance.stop()
        logger.info("Cache warmer stop requested")


# ═══════════════════════════════════════════════════════════════════════════════
# CLI entry point
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    from dotenv import load_dotenv
    load_dotenv()

    parser = argparse.ArgumentParser(description="PULSE Cache Pre-Warm Service")
    parser.add_argument("--daemon", action="store_true", help="Run as continuous daemon")
    parser.add_argument("--config-dir", default=None, help="Config directory path")
    parser.add_argument("--auth", default="azure_cli", choices=["azure_cli", "managed_identity", "service_principal"])
    parser.add_argument("--scopes", default="7d,30d", help="Comma-separated scopes (e.g. 7d,30d,90d,180d)")
    parser.add_argument("--fast-interval", type=int, default=1800, help="Fast refresh interval (seconds)")
    parser.add_argument("--slow-interval", type=int, default=7200, help="Slow refresh interval (seconds)")
    parser.add_argument("--deep-interval", type=int, default=43200, help="Deep refresh interval (seconds)")
    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    warmer = CacheWarmer(config_dir=args.config_dir)
    configs = warmer.discover_configs()

    if not configs:
        logger.error("No configs found. Check your configs/ directory.")
        sys.exit(1)

    scopes = [s.strip() for s in args.scopes.split(',')]

    if args.daemon:
        # Daemon mode: initial warm then scheduled loop
        daemon = CacheDaemon(
            warmer,
            fast_interval=args.fast_interval,
            slow_interval=args.slow_interval,
            deep_interval=args.deep_interval,
        )
        for cfg in configs:
            warmer.connect(cfg, auth_method=args.auth)
            warmer.warm(cfg, scopes=scopes)
        try:
            daemon.run(configs)
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            daemon.stop()
    else:
        # One-shot mode: warm all configs and exit
        for cfg in configs:
            logger.info(f"\n{'='*60}")
            logger.info(f"Warming: {cfg.name}")
            logger.info(f"{'='*60}")
            if warmer.connect(cfg, auth_method=args.auth):
                results = warmer.warm(cfg, scopes=scopes)
                for scope, rows in results.items():
                    status = "✓" if rows > 0 else "✗"
                    logger.info(f"  {status} {scope}: {rows:,} rows")

    logger.info("Done.")


if __name__ == "__main__":
    main()
