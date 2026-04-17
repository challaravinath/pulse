"""
PULSE FastAPI Server — Iteration 2
====================================
Real connect flow over WebSocket. Mirrors app.py lines 465-720.

Run from pulse_final/:
  uvicorn src.pulse.api.app:app --host 0.0.0.0 --port 8000 --reload

Test connect:
  Open http://localhost:8000/docs → WebSocket tab
  Or use the test page at http://localhost:8000/test
"""

import asyncio
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

# ── Path setup ────────────────────────────────────────────────────────────────
_project_root = Path(__file__).resolve().parent.parent.parent.parent  # → pulse_final/
_src_dir = _project_root / "src"
if str(_src_dir) not in sys.path:
    sys.path.insert(0, str(_src_dir))
if _project_root / ".env":
    load_dotenv(_project_root / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s  %(message)s")
logger = logging.getLogger("pulse.api")

# Thread pool for running sync pulse code
_pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="pulse")


async def run_sync(fn, *args, **kwargs):
    """Run a blocking function in the thread pool."""
    loop = asyncio.get_event_loop()
    if kwargs:
        fn = partial(fn, **kwargs)
    return await loop.run_in_executor(_pool, fn, *args)


# ═════════════════════════════════════════════════════════════════════════════
# Session — one per WebSocket connection
# ═════════════════════════════════════════════════════════════════════════════

@dataclass
class Session:
    sid: str = ""
    connected: bool = False
    config_name: str = ""
    current_config: Any = None
    kusto_client: Any = None
    duckdb_engine: Any = None
    llm_service: Any = None
    semantic_layer: Any = None
    data_profile: Any = None
    ai_orchestrator: Any = None
    visualizer: Any = None
    instant_dash: dict = field(default_factory=dict)
    analyst_opening: str = ""
    messages: list = field(default_factory=list)
    profile_status: str = "idle"

    def cleanup(self):
        # ★ v6.0: Cancel profile FIRST so background threads stop writing
        try:
            if self.data_profile and hasattr(self.data_profile, "cancel"):
                self.data_profile.cancel()
        except Exception:
            pass
        # Then clear DuckDB
        try:
            if self.duckdb_engine and hasattr(self.duckdb_engine, "clear"):
                self.duckdb_engine.clear()
        except Exception:
            pass
        self.connected = False


# Active sessions: sid → (WebSocket, Session)
_sessions: dict[str, tuple[WebSocket, Session]] = {}


async def send(ws: WebSocket, data: dict):
    """Send JSON to a WebSocket. Handles NaN/Infinity from Plotly."""
    try:
        import json as _json
        import math

        def _default(obj):
            """Handle numpy types and other non-serializable objects."""
            import numpy as np
            if isinstance(obj, (np.integer,)):
                return int(obj)
            if isinstance(obj, (np.floating,)):
                v = float(obj)
                if math.isnan(v) or math.isinf(v):
                    return None
                return v
            if isinstance(obj, np.ndarray):
                return obj.tolist()
            if isinstance(obj, (np.bool_,)):
                return bool(obj)
            if hasattr(obj, 'isoformat'):
                return obj.isoformat()
            return str(obj)

        text = _json.dumps(data, default=_default, allow_nan=False)
        await ws.send_text(text)
    except ValueError:
        try:
            text = _json.dumps(_sanitize(data), default=str)
            await ws.send_text(text)
        except Exception as e:
            logger.error(f"WS send failed after sanitize: {e}")
    except Exception as e:
        logger.error(f"WS send failed: {e}")


def _sanitize(obj):
    """Recursively replace NaN/Infinity with None for JSON safety."""
    import math
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize(v) for v in obj]
    return obj


def _plotly_safe(obj):
    """★ iter16: Recursively convert numpy types to Python native in Plotly dicts."""
    import numpy as np
    import math
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        v = float(obj)
        return None if (math.isnan(v) or math.isinf(v)) else v
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    if isinstance(obj, np.ndarray):
        return [_plotly_safe(x) for x in obj.tolist()]
    if isinstance(obj, dict):
        return {k: _plotly_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return type(obj)(_plotly_safe(v) for v in obj)
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
    return obj


CHIP_LABELS = {
    "__chip_trending__": "📈 How are we trending?",
    "__chip_top_customers__": "🏆 Top customers",
    "__chip_bottom_customers__": "⚠️ Bottom 10",
    "__chip_manager_summary__": "📋 Manager summary",
}


def _fmt_suggestions(items):
    """Format suggestion list for the frontend."""
    out = []
    for s in (items or []):
        if isinstance(s, str):
            out.append({"label": CHIP_LABELS.get(s, f"💡 {s}"), "value": s})
        elif isinstance(s, dict):
            out.append(s)
    return out


def _opening_to_str(opening) -> str:
    """Convert analyst_opening dict {summary, insights} to markdown string."""
    if not opening:
        return ""
    if isinstance(opening, str):
        return opening
    if isinstance(opening, dict):
        parts = []
        if opening.get("summary"):
            parts.append(str(opening["summary"]))
        for insight in (opening.get("insights") or []):
            if isinstance(insight, dict):
                parts.append(f"• {insight.get('text', '')}")
            else:
                parts.append(f"• {insight}")
        return "\n\n".join(parts) if parts else ""
    return str(opening)


# ═════════════════════════════════════════════════════════════════════════════
# Lifespan
# ═════════════════════════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("PULSE API starting …")
    app.state.start_time = time.time()
    app.state.configs = []
    app.state.config_loader = None

    try:
        from pulse.core.config_loader import ConfigLoader
        configs_dir = str(_project_root / "configs")
        defaults_file = str(_project_root / "system_defaults.yaml")
        loader = ConfigLoader(config_dir=configs_dir, defaults_file=defaults_file)
        loader.discover_and_load()
        app.state.config_loader = loader
        all_cfgs = loader.get_all_configs()
        app.state.configs = [
            {"name": c.name, "owner": getattr(c, "owner", ""), "clusters": len(c.clusters)}
            for c in all_cfgs
        ]
        logger.info(f"✓ Loaded {len(app.state.configs)} config(s)")
    except Exception as e:
        logger.error(f"✗ Config load failed: {e}", exc_info=True)

    yield

    for sid, (ws, sess) in list(_sessions.items()):
        sess.cleanup()
    _sessions.clear()
    logger.info("PULSE API stopped")


app = FastAPI(title="PULSE API", version="5.0.0-iter9", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


# ═════════════════════════════════════════════════════════════════════════════
# REST
# ═════════════════════════════════════════════════════════════════════════════

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "version": "5.0.0-iter9",
        "uptime_seconds": round(time.time() - app.state.start_time, 1),
        "configs_loaded": len(app.state.configs),
        "active_connections": len(_sessions),
    }

@app.get("/api/configs")
async def list_configs():
    return {"configs": app.state.configs}


@app.get("/app", response_class=HTMLResponse)
async def react_app():
    """Serve the React frontend."""
    static_dir = Path(__file__).parent / "static"
    index_path = static_dir / "index.html"
    if index_path.exists():
        return index_path.read_text(encoding="utf-8")
    return HTMLResponse("<h1>React frontend not found</h1><p>Expected at: src/pulse/api/static/index.html</p>", status_code=404)


# ═════════════════════════════════════════════════════════════════════════════
# Connect flow — mirrors app.py lines 465-720
# ═════════════════════════════════════════════════════════════════════════════

async def handle_connect(ws: WebSocket, session: Session, msg: dict):
    """
    Full connect flow with FAST PATH.

    Fast path (cache fresh):  Load from disk → AI engine → ready in ~2-3s
    Slow path (cache stale):  Auth → schema → semantic → data → AI → ready in ~30s

    Kusto auth is LAZY on fast path — only connects when user asks ad-hoc KQL.
    """

    cfg_name = msg.get("config_name", "")
    auth_method = msg.get("auth_method", "azure_cli")

    if not cfg_name:
        return await send(ws, {"type": "error", "message": "config_name is required"})

    t0 = time.time()

    try:
        from pulse.core.config_loader import ConfigLoader, AuthMethod
        from pulse.core.kusto_client import ConfigDrivenKustoClient
        from pulse.core.duckdb_engine import DuckDBQueryEngine
        from pulse.core.llm_service import LLMService
        from pulse.core.semantic_layer import SemanticLayer
        from pulse.core.data_profile import DataProfile
        from pulse.core.visualizer import SmartVisualizer
        from pulse.core.ai_orchestrator import AIOrchestrator
        from pulse.utils.config import AppConfig

        # ── Step 0: Find config ──────────────────────────────────────────
        loader = app.state.config_loader
        if not loader:
            return await send(ws, {"type": "error", "message": "No configs loaded on server"})

        cfg_map = {c.name: c for c in loader.get_all_configs()}
        if cfg_name not in cfg_map:
            return await send(ws, {
                "type": "error",
                "message": f"Unknown config: {cfg_name}. Available: {list(cfg_map.keys())}",
            })

        sel_cfg = cfg_map[cfg_name]
        session.config_name = cfg_name
        session.current_config = sel_cfg

        auth_map = {
            "azure_cli": AuthMethod.AZURE_CLI,
            "managed_identity": AuthMethod.MANAGED_IDENTITY,
            "service_principal": AuthMethod.SERVICE_PRINCIPAL,
        }
        sel_cfg.auth_method = auth_map.get(auth_method, AuthMethod.AZURE_CLI)

        app_config = AppConfig.load()
        session.duckdb_engine = DuckDBQueryEngine(app_config.cache_dir)
        config_path = sel_cfg.source_path

        # ── FAST PATH: Try disk cache first (no Kusto auth needed) ────
        await send(ws, {"type": "status", "phase": "cache", "message": "Checking cache…", "progress": 0.10})

        # Build semantic layer (local only, no network)
        sem = await run_sync(lambda: SemanticLayer(config_path=config_path) if config_path else SemanticLayer())
        session.semantic_layer = sem

        fast_path = False
        profile = DataProfile(None, session.duckdb_engine)  # No kusto_client yet

        if sem.is_loaded:
            cache_loaded = await run_sync(profile.load_from_disk, sel_cfg.name)
            if cache_loaded and len(profile.tables) >= 3:
                fast_path = True
                age = profile.get_cache_age_str(sel_cfg.name)
                scope_days = max(
                    (profile._table_scope_days.get(t, 0) for t in profile.tables),
                    default=7
                )
                await send(ws, {
                    "type": "status", "phase": "cache",
                    "message": f"✓ Cache hit: {len(profile.tables)} tables, {scope_days}d scope ({age})",
                    "progress": 0.50,
                })
                logger.info(
                    f"FAST PATH: {len(profile.tables)} tables, "
                    f"{profile.total_rows:,} rows, {scope_days}d scope, {age}"
                )

        if fast_path:
            # ── FAST PATH: Skip Kusto, go straight to AI engine ──────
            session.data_profile = profile
            profile._all_queries = sem.get_profile_queries() if sem.is_loaded else {}

            # ★ v7.0: Initialize SQL validator from cached schema
            try:
                profile.init_sql_validator()
            except Exception as e:
                logger.warning(f"SQL validator init (cache path): {e}")

            await send(ws, {"type": "status", "phase": "ai", "message": "Starting AI engine…", "progress": 0.70})

            session.llm_service = LLMService()
            session.visualizer = SmartVisualizer()

            # Use schema from config (no discovery needed)
            schema = {c.name: c.type for c in sel_cfg.get_all_columns()}

            orch = AIOrchestrator(
                session.llm_service, None, session.duckdb_engine, session.visualizer,
                semantic_layer=session.semantic_layer,
                data_profile=session.data_profile,
                enrichment_config=sel_cfg.enrichment,
                config_path=config_path,
            )
            orch.set_schema(schema)
            orch.init_predictive_cache(visualizer=session.visualizer)
            session.ai_orchestrator = orch
            session.connected = True

            # Lazy Kusto: connect in background for ad-hoc queries
            asyncio.create_task(
                _lazy_kusto_connect(session, sel_cfg, app_config, profile, sem)
            )

            # Generate opening from cached data
            try:
                from pulse.core.analyst_opening import generate_opening_statement
                # Build synthetic instant_dash from cached profile tables
                _instant = _instant_from_cache(profile, session.duckdb_engine)
                if _instant:
                    opening = await run_sync(
                        generate_opening_statement,
                        instant_data=_instant,
                        llm_client=orch.llm_service.client,
                        llm_model=orch.llm_service.model,
                        enricher=getattr(orch, "enricher", None),
                        product_name=sel_cfg.name,
                    )
                    session.analyst_opening = _opening_to_str(opening)
            except Exception as e:
                logger.warning(f"Opening (fast path): {e}")
                session.analyst_opening = ""
            elapsed = time.time() - t0
            nm = len(sem.model.metrics) if sem.is_loaded and sem.model else 0
            nd = len(sem.model.dimensions) if sem.is_loaded and sem.model else 0

            await send(ws, {
                "type": "status", "phase": "complete",
                "message": f"Ready in {elapsed:.1f}s (cached)",
                "progress": 1.0,
            })
            await send(ws, {
                "type": "connected",
                "config_name": cfg_name,
                "elapsed_seconds": round(elapsed, 1),
                "tables": len(profile.tables),
                "rows": profile.total_rows,
                "data_source": "cache",
                "clusters": 0,  # not connected yet
                "columns": len(schema),
                "metrics": nm,
                "dimensions": nd,
                "analyst_opening": session.analyst_opening,
                "fast_path": True,
                "scope_days": max((profile._table_scope_days.get(t, 7) for t in profile.tables), default=7) if profile.tables else 7,
            })
            return  # ← fast path complete

        # ══════════════════════════════════════════════════════════════════
        # SLOW PATH: No cache — full Kusto connect (original flow)
        # ══════════════════════════════════════════════════════════════════
        logger.info("SLOW PATH: Cache miss — full Kusto connect")

        # ── Step 1: Authenticate clusters ─────────────────────────────────
        await send(ws, {"type": "status", "phase": "auth", "message": "Authenticating clusters…", "progress": 0.10})

        kc = await run_sync(ConfigDrivenKustoClient, sel_cfg, session.duckdb_engine)
        await run_sync(kc.connect_all_clusters)
        session.kusto_client = kc

        # Update profile with kusto client
        profile = DataProfile(kc, session.duckdb_engine)

        connected_clusters = [cl.name for cl in sel_cfg.clusters if cl.name in kc._clients]
        await send(ws, {
            "type": "status", "phase": "auth",
            "message": f"Authenticated {len(connected_clusters)} cluster(s)",
            "detail": ", ".join(connected_clusters),
            "progress": 0.25,
        })

        # ── Step 2: Discover schema ───────────────────────────────────────
        await send(ws, {"type": "status", "phase": "schema", "message": "Discovering schema…", "progress": 0.30})

        discovered_schema = await run_sync(kc.discover_schema)
        col_count = len(discovered_schema) if discovered_schema else 0
        await send(ws, {
            "type": "status", "phase": "schema",
            "message": f"Mapped {col_count} columns" if discovered_schema else "Schema discovery failed — using config",
            "progress": 0.40,
        })

        # ── Step 3: Semantic model ────────────────────────────────────────
        await send(ws, {"type": "status", "phase": "semantic", "message": "Building semantic model…", "progress": 0.45})

        # Auto-discover if semantic model is empty
        if (not sem.is_loaded or not sem.model.metrics) and discovered_schema:
            await send(ws, {"type": "status", "phase": "semantic", "message": "Auto-discovering metrics…", "progress": 0.50})
            try:
                from pulse.core.config_generator import ConfigGenerator, ColumnInfo
                def _auto_discover():
                    gen = ConfigGenerator()
                    col_infos = {name: ColumnInfo(name=name, data_type=dtype) for name, dtype in discovered_schema.items()}
                    cluster = sel_cfg.clusters[0]
                    client = kc._get_client(cluster)
                    col_infos = gen._analyze_cardinality(client, cluster.database, cluster.table, col_infos)
                    col_infos = gen._fetch_samples(client, cluster.database, cluster.table, col_infos)
                    col_infos = gen._classify_columns(col_infos)
                    result = gen._generate_config(col_infos, cluster.url, cluster.database, cluster.table, 'azure_cli', sel_cfg.name, sel_cfg.owner or '')
                    if config_path:
                        with open(config_path, 'w', encoding='utf-8') as f:
                            f.write(result.yaml_text)
                    return SemanticLayer(config_path=config_path) if config_path else SemanticLayer()
                sem = await run_sync(_auto_discover)
            except Exception as e:
                logger.warning(f"Auto-discover failed: {e}")

        session.semantic_layer = sem

        nm = len(sem.model.metrics) if sem.is_loaded and sem.model else 0
        nd = len(sem.model.dimensions) if sem.is_loaded and sem.model else 0
        await send(ws, {
            "type": "status", "phase": "semantic",
            "message": f"{nm} metrics, {nd} dimensions loaded",
            "progress": 0.55,
        })

        # Validate semantic model against discovered schema
        if sem.is_loaded and discovered_schema:
            validation = sem.validate_against_schema(discovered_schema)
            if not validation['valid']:
                sem.prune_queries_by_schema(discovered_schema)

        # ── Step 4: Data loading ──────────────────────────────────────────
        await send(ws, {"type": "status", "phase": "data", "message": "Loading data…", "progress": 0.60})

        all_queries = sem.get_profile_queries() if sem.is_loaded else {}
        profile._all_queries = all_queries
        bg_queries = {k: v for k, v in all_queries.items() if v.tier <= 1}
        data_source = None

        if sem.is_loaded:
            # Try disk cache first
            cache_loaded = await run_sync(profile.load_from_disk, sel_cfg.name)
            if cache_loaded and len(profile.tables) >= 3:
                data_source = "cache"
                age = profile.get_cache_age_str(sel_cfg.name)
                await send(ws, {
                    "type": "status", "phase": "data",
                    "message": f"Restored {len(profile.tables)} tables from cache ({age})",
                    "progress": 0.75,
                })
            else:
                # Fire instant dashboard (7-day snapshot)
                await send(ws, {"type": "status", "phase": "data", "message": "Running 7-day snapshot (90-day builds in background)…", "progress": 0.65})
                try:
                    time_col = sem.model.time_column if sem.model else "EventInfo_Time"
                    org_col, geo_col = "OrgId", "GeoName"
                    if sem.model and sem.model.dimensions:
                        for did, dim in sem.model.dimensions.items():
                            if did in ("organization", "org", "tenant"):
                                org_col = dim.column
                            elif did in ("region", "geo", "geography"):
                                geo_col = dim.column

                    instant = await run_sync(
                        kc.fire_instant_dashboard,
                        time_column=time_col,
                        org_column=org_col,
                        geo_column=geo_col,
                        scope='7d',  # 7d instant (fast), background builds to 90d
                    )
                    session.instant_dash = instant

                    registered = profile.populate_from_instant(instant)
                    if registered > 0:
                        data_source = "instant"
                        await send(ws, {
                            "type": "status", "phase": "data",
                            "message": f"{registered} tables from instant snapshot",
                            "progress": 0.75,
                        })
                except Exception as e:
                    logger.warning(f"Instant dashboard failed: {e}")
                    session.instant_dash = {}

        session.data_profile = profile

        # Background build (async — doesn't block the connect response)
        if sem.is_loaded and data_source != "cache":
            session.profile_status = "building"
            asyncio.create_task(_background_build(session, profile, bg_queries, sel_cfg.name))

        # ── Step 5: AI engine ─────────────────────────────────────────────
        await send(ws, {"type": "status", "phase": "ai", "message": "Starting AI engine…", "progress": 0.85})

        session.llm_service = LLMService()
        session.visualizer = SmartVisualizer()

        schema = discovered_schema or {c.name: c.type for c in sel_cfg.get_all_columns()}
        orch = AIOrchestrator(
            session.llm_service, kc, session.duckdb_engine, session.visualizer,
            semantic_layer=session.semantic_layer,
            data_profile=session.data_profile,
            enrichment_config=sel_cfg.enrichment,
            config_path=config_path,
        )
        orch.set_schema(schema)

        if sel_cfg.enrichment and sel_cfg.enrichment.get("source", "none") != "none":
            await run_sync(orch.load_enrichment)

        orch.init_predictive_cache(visualizer=session.visualizer)
        session.ai_orchestrator = orch
        session.connected = True

        # ── Analyst opening statement ─────────────────────────────────────
        try:
            from pulse.core.analyst_opening import generate_opening_statement
            if session.instant_dash:
                opening = await run_sync(
                    generate_opening_statement,
                    instant_data=session.instant_dash,
                    llm_client=orch.llm_service.client,
                    llm_model=orch.llm_service.model,
                    enricher=getattr(orch, "enricher", None),
                    product_name=sel_cfg.name,
                )
                session.analyst_opening = _opening_to_str(opening)
        except Exception as e:
            logger.warning(f"Opening statement skipped: {e}")
            session.analyst_opening = ""

        # ── Done ──────────────────────────────────────────────────────────
        elapsed = time.time() - t0
        table_count = len(profile.tables) if profile else 0
        row_count = profile.total_rows if profile else 0

        await send(ws, {
            "type": "status", "phase": "complete",
            "message": f"Ready in {elapsed:.1f}s",
            "progress": 1.0,
        })

        await send(ws, {
            "type": "connected",
            "config_name": cfg_name,
            "elapsed_seconds": round(elapsed, 1),
            "tables": table_count,
            "rows": row_count,
            "data_source": data_source or "live",
            "clusters": len(connected_clusters),
            "columns": col_count,
            "metrics": nm,
            "dimensions": nd,
            "analyst_opening": session.analyst_opening,
            "scope_days": max((profile._table_scope_days.get(t, 7) for t in profile.tables), default=7) if profile.tables else 7,
        })

    except Exception as e:
        logger.error(f"Connect failed: {e}", exc_info=True)
        await send(ws, {"type": "error", "message": f"Connection failed: {e}", "code": "connect_error"})


# ── Lazy Kusto connect: runs in background after fast-path ────────────────
async def _lazy_kusto_connect(session: Session, sel_cfg, app_config, profile, sem):
    """Connect to Kusto in background so ad-hoc KQL works later."""
    try:
        from pulse.core.kusto_client import ConfigDrivenKustoClient

        logger.info("Lazy Kusto: connecting in background…")
        kc = await run_sync(ConfigDrivenKustoClient, sel_cfg, session.duckdb_engine)
        await run_sync(kc.connect_all_clusters)

        session.kusto_client = kc
        # Update orchestrator and profile with live Kusto client
        if session.ai_orchestrator:
            session.ai_orchestrator.kusto_client = kc
        if session.data_profile:
            session.data_profile.kusto_client = kc

        # Discover real schema
        schema = await run_sync(kc.discover_schema)
        if schema and session.ai_orchestrator:
            session.ai_orchestrator.set_schema(schema)

        # Load enrichment
        if sel_cfg.enrichment and sel_cfg.enrichment.get("source", "none") != "none":
            if session.ai_orchestrator:
                await run_sync(session.ai_orchestrator.load_enrichment)

                # ★ iter15: Re-send scorecard with resolved org names
                if getattr(session, '_scorecard_needs_refresh', False) and profile:
                    try:
                        from pulse.core import compound_analyst as _ca
                        _enricher = getattr(session.ai_orchestrator, 'enricher', None)

                        # ★ iter16: Pre-load org names so scorecard can resolve GUIDs
                        if _enricher and profile.has_table('profile_organization'):
                            try:
                                _org_df = profile.query_safe(
                                    "SELECT DISTINCT OrgId FROM profile_organization LIMIT 100")
                                if _org_df is not None and not _org_df.empty:
                                    _org_ids = _org_df.iloc[:, 0].dropna().astype(str).tolist()
                                    _enricher.load_for_orgs(_org_ids)
                                    logger.info(f"Pre-loaded {len(_org_ids)} org names for scorecard")
                            except Exception as _pre_err:
                                logger.warning(f"Org pre-load failed: {_pre_err}")

                        _scope = max(profile._table_scope_days.values(), default=180) if profile._table_scope_days else 180
                        compound = _ca.analyze(profile, mode='scorecard', enricher=_enricher,
                                               scope_days=_scope)
                        _sc_payload = _ca.build_scorecard_payload(compound)
                        entry2 = _sessions.get(session.sid)
                        if entry2 and _sc_payload:
                            ws2, _ = entry2
                            await send(ws2, _sc_payload)
                            logger.info("Scorecard re-sent with enriched org names")
                        session._scorecard_needs_refresh = False
                    except Exception as sc_err:
                        logger.warning(f"Scorecard re-send failed: {sc_err}")

        clusters = len([cl for cl in sel_cfg.clusters if cl.name in kc._clients])
        logger.info(f"Lazy Kusto: {clusters} cluster(s) connected")

        # Notify client
        entry = _sessions.get(session.sid)
        if entry:
            ws, _ = entry
            await send(ws, {
                "type": "data_upgraded",
                "scope": "live",
                "message": f"🔗 Kusto connected ({clusters} clusters) — ad-hoc queries ready",
            })

    except Exception as e:
        logger.warning(f"Lazy Kusto connect failed: {e}")
        # Not critical — cached data still works for profile queries


# ── Helper: build instant_dash-like dict from cached profile tables ───────
def _instant_from_cache(profile, duckdb_engine) -> dict:
    """Extract summary data from cached DuckDB tables for analyst opening."""
    result = {}
    try:
        conn = duckdb_engine.connection
        if 'profile_daily' in profile.tables:
            result['daily'] = conn.execute("SELECT * FROM profile_daily").fetchdf()
        if 'profile_totals' in profile.tables:
            result['totals'] = conn.execute("SELECT * FROM profile_totals").fetchdf()
        if 'profile_organization' in profile.tables:
            result['top10'] = conn.execute(
                "SELECT * FROM profile_organization ORDER BY events DESC LIMIT 10"
            ).fetchdf()
        if 'profile_region' in profile.tables:
            result['regions'] = conn.execute("SELECT * FROM profile_region").fetchdf()
    except Exception as e:
        logger.warning(f"_instant_from_cache: {e}")
    return result


async def _background_build(session: Session, profile, queries: dict, config_name: str):
    """Background profile build — notifies client when wider data is ready."""
    try:
        loop = asyncio.get_event_loop()
        done_event = asyncio.Event()

        def _on_done():
            try:
                profile.save_to_disk(config_name)
                session.profile_status = "done"
                tables = len(profile.tables)
                rows = profile.total_rows

                # ★ v7.0: Initialize SQL validator after tables are built
                try:
                    profile.init_sql_validator()
                except Exception as e:
                    logger.warning(f"SQL validator init failed: {e}")

                scope = max(profile._table_scope_days.values(), default=180)
                logger.info(f"Background build done: {tables} tables, {rows:,} rows, {scope}d scope")

                # ★ v10.0: Compute scorecard for landing dashboard
                scorecard_payload = {}
                try:
                    from pulse.core import compound_analyst as _ca
                    _enricher = getattr(session.ai_orchestrator, 'enricher', None) if session.ai_orchestrator else None
                    compound = _ca.analyze(profile, mode='scorecard', enricher=_enricher,
                                           scope_days=scope)
                    scorecard_payload = _ca.build_scorecard_payload(compound)
                    # ★ iter15: Store for re-send after enrichment loads
                    session._scorecard_needs_refresh = True
                    logger.info(
                        f"Scorecard: {scorecard_payload.get('anomaly_count', 0)} anomalies, "
                        f"{compound.compute_ms:.0f}ms"
                    )
                except Exception as sc_err:
                    logger.warning(f"Scorecard computation failed: {sc_err}")

                # Notify client via WebSocket (thread-safe)
                entry = _sessions.get(session.sid)
                if entry:
                    ws, _ = entry
                    asyncio.run_coroutine_threadsafe(
                        send(ws, {
                            "type": "data_upgraded",
                            "scope": f"{scope}d",
                            "scope_days": scope,
                            "tables": tables,
                            "rows": rows,
                            "message": f"📊 {scope}-day profile ready — {tables} tables, {rows:,} rows",
                        }),
                        loop,
                    )
                    # Send scorecard as separate message
                    if scorecard_payload:
                        asyncio.run_coroutine_threadsafe(
                            send(ws, scorecard_payload),
                            loop,
                        )
            except Exception as e:
                logger.error(f"Cache save failed: {e}")
                session.profile_status = "failed"

        # This already spawns its own background thread
        profile.build_all_background(queries, callback_on_done=_on_done)

    except Exception as e:
        logger.error(f"Background build failed: {e}")
        session.profile_status = "failed"


# ═════════════════════════════════════════════════════════════════════════════
# Question flow — placeholder for iter3
# ═════════════════════════════════════════════════════════════════════════════

async def handle_question(ws: WebSocket, session: Session, question: str):
    """
    ★ iter16: Classifier-based routing — one decision point.

    Bucket → Handler:
      GREETING    → conversational text
      META        → session info (no SQL)
      COMPOUND    → compound_analyst (multi-table briefing)
      INVESTIGATE → health_analysis (cross-table)
      SINGLE      → process_message → fast_router → profile
      KUSTO       → process_message → kusto_handler
    """
    if not session.connected or not session.ai_orchestrator:
        return await send(ws, {"type": "error", "message": "Not connected — click Connect first."})

    orch = session.ai_orchestrator
    t0 = time.time()

    # ══════════════════════════════════════════════════════════════════
    # STEP 1: Classify the question
    # ══════════════════════════════════════════════════════════════════
    from pulse.core.question_classifier import classify
    bucket = classify(question)
    logger.info(f"Question classified: [{bucket}] ← \"{question[:60]}\"")

    try:
        # ── GREETING ─────────────────────────────────────────────────
        if bucket == 'GREETING':
            q = question.lower().strip().rstrip('?.!')
            if any(w in q for w in ['what can you do', 'help', 'what are you']):
                msg = ("I'm PULSE — your telemetry intelligence assistant. "
                       "I can show you trends, rankings, health checks, anomalies, "
                       "and executive summaries from your data. Try asking:\n\n"
                       "• **\"How are we doing?\"** for a full overview\n"
                       "• **\"Any issues?\"** for anomaly detection\n"
                       "• **\"Top 10 orgs\"** for rankings\n"
                       "• **\"Show the trend\"** for daily activity")
            elif any(w in q for w in ['thanks', 'thank', 'thx', 'cheers']):
                msg = "You're welcome! Let me know if you need anything else."
            else:
                msg = "Hey! Ask me anything about your telemetry data. Try **\"How are we doing?\"** to start."
            await send(ws, {
                "type": "answer", "intent": "greeting", "response_type": "conversational",
                "content": msg, "elapsed_seconds": round(time.time() - t0, 2),
            })
            await send(ws, {"type": "suggestions", "items": _fmt_suggestions([
                "How are we doing?", "Any issues?", "Top 10 orgs", "Show the trend"
            ])})
            return

        # ── META ─────────────────────────────────────────────────────
        if bucket == 'META':
            meta = _build_meta_response(session, question)
            await send(ws, {
                "type": "answer", "intent": "meta", "response_type": "conversational",
                "content": meta, "elapsed_seconds": round(time.time() - t0, 2),
                "source": "session",
            })
            await send(ws, {"type": "suggestions", "items": _fmt_suggestions([
                "How are we doing?", "Show the trend", "Top 10 orgs"
            ])})
            return

        # ── COMPOUND (multi-table briefing) ──────────────────────────
        if bucket == 'COMPOUND':
            stream_prep = None
            try:
                stream_prep = await asyncio.wait_for(
                    run_sync(orch.prepare_analysis_stream, question, 'narrative'),
                    timeout=15.0,
                )
            except (asyncio.TimeoutError, Exception) as e:
                logger.warning(f"Compound prep failed: {e}")

            if stream_prep:
                return await _send_stream_response(ws, stream_prep, question, orch, t0)

            # Fallback: run as standard query
            logger.warning("COMPOUND fell through — running as standard query")
            # Fall through to SINGLE path below

        # ── INVESTIGATE (health/anomaly analysis) ────────────────────
        if bucket == 'INVESTIGATE':
            stream_prep = None
            try:
                stream_prep = await asyncio.wait_for(
                    run_sync(orch.prepare_analysis_stream, question, 'health'),
                    timeout=15.0,
                )
            except (asyncio.TimeoutError, Exception) as e:
                logger.warning(f"Investigate prep failed: {e}")

            if stream_prep:
                return await _send_stream_response(ws, stream_prep, question, orch, t0)

            # Fallback: run as standard query
            logger.warning("INVESTIGATE fell through — running as standard query")
            # Fall through to SINGLE path below

        # ══════════════════════════════════════════════════════════════
        # SINGLE / KUSTO / fallback — standard process_message path
        # ══════════════════════════════════════════════════════════════

        # ★ iter16: Guard against vague/incomplete questions producing garbage
        _words = question.strip().split()
        if bucket == 'SINGLE' and len(_words) <= 1 and not any(
            w in question.lower() for w in ['trend', 'total', 'org', 'region', 'entity']
        ):
            await send(ws, {
                "type": "answer", "intent": "help", "response_type": "conversational",
                "content": ("I need a bit more detail to help you. Try asking:\n\n"
                            "• **\"Top 10 orgs\"** for organization rankings\n"
                            "• **\"Show the trend\"** for daily activity\n"
                            "• **\"How are we doing?\"** for a full overview\n"
                            "• **\"Any issues?\"** to check for anomalies"),
                "elapsed_seconds": round(time.time() - t0, 2),
            })
            await send(ws, {"type": "suggestions", "items": _fmt_suggestions([
                "How are we doing?", "Top 10 orgs", "Show the trend", "Any issues?"
            ])})
            return

        await send(ws, {"type": "processing", "message": "Analyzing…"})

        response = await run_sync(orch.process_message, question)
        elapsed = time.time() - t0

        await _send_standard_response(ws, session, orch, question, response, elapsed)

    except Exception as e:
        logger.error(f"handle_question error: {e}", exc_info=True)
        await send(ws, {"type": "error", "message": f"Something went wrong: {str(e)[:200]}"})


def _build_meta_response(session: Session, question: str) -> str:
    """★ iter16: Answer meta questions from session state — no SQL needed."""
    parts = []

    # Config info
    if session.current_config:
        cfg = session.current_config
        parts.append(f"**Data Source:** {getattr(cfg, 'display_name', session.config_name) or session.config_name}")
        cluster_names = [c.name for c in cfg.clusters] if hasattr(cfg, 'clusters') and cfg.clusters else []
        if cluster_names:
            parts.append(f"**Clusters:** {', '.join(cluster_names)}")
    elif session.config_name:
        parts.append(f"**Data Source:** {session.config_name}")

    # Connection status
    parts.append(f"**Connected:** {'Yes ✅' if session.connected else 'No ❌'}")

    # Profile info
    profile = session.data_profile
    if profile and hasattr(profile, 'is_built') and profile.is_built:
        tables = list(profile.tables.keys()) if hasattr(profile, 'tables') else []
        total_rows = getattr(profile, 'total_rows', 0)
        parts.append(f"**Tables:** {len(tables)} ({', '.join(tables)})")
        parts.append(f"**Total Rows:** {total_rows:,}")

        # Scope
        if hasattr(profile, '_table_scope_days') and profile._table_scope_days:
            scope = max(profile._table_scope_days.values(), default=0)
            parts.append(f"**Data Scope:** {scope} days")

        # Freshness
        try:
            staleness = profile.get_staleness_info()
            parts.append(f"**Data Age:** {staleness.get('overall_label', 'unknown')}")
        except Exception:
            pass
    else:
        parts.append("**Profile:** Not built yet — still loading.")

    # Kusto status
    if session.kusto_client:
        parts.append("**Live Kusto:** Connected ✅ (ad-hoc queries available)")
    else:
        parts.append("**Live Kusto:** Not connected (using cached profiles)")

    return '\n\n'.join(parts)


async def _send_stream_response(ws, stream_prep, question, orch, t0):
    """★ iter16: Send health/narrative/exec stream response to client."""
    elapsed = time.time() - t0
    mode = stream_prep.get("mode")
    intent = stream_prep.get("intent", mode or "analysis")

    # ── HEALTH ANALYSIS ──────────────────────────────────────────────
    if mode == "health":
        msg_text = stream_prep.get("content") or stream_prep.get("message", "")
        source = stream_prep.get("source", "profile")

        if msg_text:
            await send(ws, {
                "type": "stream_start", "intent": "health_analysis", "source": source,
            })
            await send(ws, {
                "type": "stream_end", "full_text": msg_text,
                "elapsed_seconds": round(elapsed, 2), "source": source,
            })

        chart = stream_prep.get("chart")
        viz = stream_prep.get("viz")
        if chart is not None:
            chart_dict = _plotly_safe(chart.to_dict()) if hasattr(chart, 'to_dict') else chart
            await send(ws, {
                "type": "chart", "plotly_json": chart_dict,
                "chart_type": "health_analysis",
            })
        elif viz and viz.get("chart") is not None:
            await send(ws, {
                "type": "chart", "plotly_json": _plotly_safe(viz["chart"].to_dict()),
                "chart_type": "health_analysis",
            })

        df = stream_prep.get("df")
        if df is not None and hasattr(df, "empty") and not df.empty and len(df) > 1:
            await send(ws, {
                "type": "data_table", "columns": list(df.columns),
                "rows": df.head(100).to_dict("records"), "total_rows": len(df),
            })

        await send(ws, {
            "type": "suggestions",
            "items": _fmt_suggestions(["Show the trend", "Top 10 orgs", "Events by region"]),
        })
        return

    # ── EXECUTIVE BRIEFING ───────────────────────────────────────────
    if intent == "executive_briefing":
        await send(ws, {
            "type": "overview",
            "html": stream_prep.get("executive_briefing_html", ""),
            "content": stream_prep.get("content", ""),
            "scope": "14d",
            "elapsed_seconds": round(elapsed, 2),
        })
        await send(ws, {
            "type": "suggestions",
            "items": _fmt_suggestions(["Show the trend", "Top 10 orgs", "Events by region"]),
        })
        return

    # ── STREAMING NARRATIVE ──────────────────────────────────────────
    if mode == "narrative":
        await send(ws, {
            "type": "stream_start", "intent": "analyze_cache", "source": "streaming",
        })

        accumulated = ""
        for chunk in stream_prep["stream"]:
            if chunk["type"] == "token":
                accumulated += chunk["text"]
                await send(ws, {"type": "stream_token", "text": chunk["text"]})
            elif chunk["type"] == "done":
                accumulated = chunk.get("full_text", accumulated)

        elapsed = time.time() - t0
        await send(ws, {
            "type": "stream_end", "full_text": accumulated,
            "elapsed_seconds": round(elapsed, 2), "source": "streaming",
        })

        viz = stream_prep.get("viz")
        if viz and viz.get("chart") is not None:
            await send(ws, {
                "type": "chart", "plotly_json": _plotly_safe(viz["chart"].to_dict()),
                "chart_type": viz.get("type", ""),
            })

        df = stream_prep.get("df")
        if df is not None and hasattr(df, "empty") and not df.empty and len(df) > 1:
            await send(ws, {
                "type": "data_table", "columns": list(df.columns),
                "rows": df.head(100).to_dict("records"), "total_rows": len(df),
            })

        try:
            follow_ups = orch._build_suggestions(question, {"intent": "analyze_cache"})
        except Exception:
            follow_ups = ["Show the trend", "Top 10 orgs", "Events by region"]
        await send(ws, {"type": "suggestions", "items": _fmt_suggestions(follow_ups)})
        return


async def _send_standard_response(ws, session, orch, question, response, elapsed):
    """★ iter16: Send standard profile/kusto response to client."""
    rtype = response.get("response_type", "")
    intent = response.get("intent", "")

    # ── Error ────────────────────────────────────────────────────
    if rtype == "error":
        await send(ws, {
            "type": "error", "message": response.get("message", "Unknown error"),
            "kql": response.get("kql"),
            "elapsed_seconds": round(elapsed, 2),
        })
        return

    # ── Build response payload ───────────────────────────────────
    result = {
        "type": "answer",
        "intent": intent,
        "response_type": rtype,
        "elapsed_seconds": round(elapsed, 2),
        "content": "",
        "source": None,
        "kql": response.get("kql") or response.get("kql_snippet"),
        "scope_label": response.get("scope_label", ""),
        "data_scope": "",
    }

    # Scope label + staleness
    profile = session.data_profile
    if profile:
        try:
            result["data_scope"] = profile.get_data_scope_label()
        except Exception:
            pass
        try:
            staleness = profile.get_staleness_info()
            result["data_freshness"] = staleness.get('overall_label', '')
            result["is_stale"] = staleness.get('is_stale', False)
            result["scope_days"] = staleness.get('scope_days', 30)
        except Exception:
            pass

    # Chip responses (intent starts with chip_)
    if intent.startswith("chip_"):
        result["content"] = response.get("content", "")
        result["source"] = response.get("source", "profile")
        if response.get("chart_data"):
            result["chart_data"] = response["chart_data"]
        if response.get("table_rows"):
            result["table_rows"] = response["table_rows"]
        if response.get("data_scope"):
            result["data_scope"] = response["data_scope"]

    elif rtype == "conversational":
        result["content"] = response.get("message", "")
        result["source"] = "cache"

    elif rtype in ("data", "analysis"):
        result["content"] = response.get("message", "")
        if intent == "profile":
            result["source"] = "profile"
        elif intent == "complex_query":
            result["source"] = "planner"
        elif rtype == "data":
            result["source"] = "kusto"
        else:
            result["source"] = "cache"

    # Send text
    await send(ws, result)

    # ── Chart ────────────────────────────────────────────────────
    viz = response.get("visualization")
    if viz and viz.get("chart") is not None:
        try:
            chart_dict = _plotly_safe(viz["chart"].to_dict())
            logger.info(f"Sending chart: {viz.get('type', '?')}, {len(chart_dict.get('data', []))} traces")
            await send(ws, {
                "type": "chart",
                "plotly_json": chart_dict,
                "chart_type": viz.get("type", ""),
            })
        except Exception as e:
            logger.warning(f"Chart serialize failed: {e}")
    else:
        logger.info(f"No chart: viz={viz.get('type') if viz else None}")

    # ── Data table ───────────────────────────────────────────────
    df = response.get("data")
    if df is not None and hasattr(df, "empty") and not df.empty and len(df) > 1:
        await send(ws, {
            "type": "data_table",
            "columns": list(df.columns),
            "rows": df.head(100).to_dict("records"),
            "total_rows": len(df),
        })

    # ── KQL ──────────────────────────────────────────────────────
    kql = response.get("kql") or response.get("kql_snippet")
    if kql:
        await send(ws, {"type": "kql", "code": kql})

    # ── Suggestions ──────────────────────────────────────────────
    follow_ups = response.get("follow_up_suggestions")
    if not follow_ups:
        try:
            follow_ups = orch._build_suggestions(question, response)
        except Exception:
            follow_ups = []
    if follow_ups:
        await send(ws, {"type": "suggestions", "items": _fmt_suggestions(follow_ups)})


# ═════════════════════════════════════════════════════════════════════════════
# WebSocket handler
# ═════════════════════════════════════════════════════════════════════════════

@app.websocket("/ws/chat")
async def websocket_chat(websocket: WebSocket):
    await websocket.accept()
    session = Session(sid=str(id(websocket)))
    _sessions[session.sid] = (websocket, session)
    logger.info(f"WS+ {session.sid[-6:]}  ({len(_sessions)} total)")

    try:
        await send(websocket, {
            "type": "welcome",
            "message": "Connected to PULSE API",
            "configs": app.state.configs,
        })

        while True:
            raw = await websocket.receive_text()
            # ★ v7.0: Input safety — reject oversized payloads
            if len(raw) > 10_000:
                await send(websocket, {"type": "error", "message": "Message too large (max 10KB)"})
                continue
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await send(websocket, {"type": "error", "message": "Invalid JSON"})
                continue

            t = msg.get("type", "")

            if t == "connect":
                await handle_connect(websocket, session, msg)

            elif t == "question":
                text = msg.get("text", "").strip()[:2000]  # ★ v7.0: cap question length
                if text:
                    await handle_question(websocket, session, text)
                else:
                    await send(websocket, {"type": "error", "message": "Empty question"})

            elif t == "chip":
                chip_id = msg.get("chip_id", "").strip()
                if chip_id:
                    await handle_question(websocket, session, chip_id)
                else:
                    await send(websocket, {"type": "error", "message": "Empty chip_id"})

            elif t == "disconnect":
                break

            else:
                await send(websocket, {"type": "error", "message": f"Unknown type: {t}"})

    except WebSocketDisconnect:
        logger.info(f"WS- {session.sid[-6:]}")
    except Exception as e:
        logger.error(f"WS error: {e}", exc_info=True)
    finally:
        session.cleanup()
        _sessions.pop(session.sid, None)


# ═════════════════════════════════════════════════════════════════════════════
# Test page — quick way to test the WebSocket from your browser
# ═════════════════════════════════════════════════════════════════════════════


@app.get("/test", response_class=HTMLResponse)
async def test_page():
    configs_json = json.dumps(app.state.configs)
    return f"""<!DOCTYPE html>
<html><head>
<title>PULSE</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; background: #f8fafc; height: 100vh; display: flex; }}
  .sidebar {{ width: 280px; background: #1e293b; color: #e2e8f0; padding: 20px; display: flex; flex-direction: column; gap: 16px; flex-shrink: 0; }}
  .sidebar h2 {{ font-size: 18px; color: white; display: flex; align-items: center; gap: 8px; }}
  .sidebar label {{ font-size: 12px; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.5px; }}
  .sidebar select, .sidebar button {{ width: 100%; padding: 10px; border-radius: 8px; border: none; font-size: 14px; }}
  .sidebar select {{ background: #334155; color: white; }}
  .sidebar select option {{ background: #334155; color: white; }}
  .btn-connect {{ background: #3b82f6; color: white; font-weight: 600; cursor: pointer; }}
  .btn-connect:hover {{ background: #2563eb; }}
  .btn-disconnect {{ background: #ef4444; color: white; font-weight: 600; cursor: pointer; margin-top: 4px; }}
  .status-bar {{ padding: 8px 12px; border-radius: 6px; font-size: 12px; }}
  .status-idle {{ background: #334155; color: #94a3b8; }}
  .status-connecting {{ background: #1e3a5f; color: #60a5fa; }}
  .status-connected {{ background: #064e3b; color: #6ee7b7; }}
  .progress-bar {{ height: 4px; background: #334155; border-radius: 2px; margin-top: 8px; overflow: hidden; }}
  .progress-fill {{ height: 100%; background: #3b82f6; border-radius: 2px; transition: width 0.3s; }}
  .details {{ font-size: 11px; color: #64748b; margin-top: 12px; white-space: pre-wrap; max-height: 200px; overflow-y: auto; }}
  .main {{ flex: 1; display: flex; flex-direction: column; overflow: hidden; }}
  .chat {{ flex: 1; overflow-y: auto; padding: 24px; display: flex; flex-direction: column; gap: 16px; }}
  .msg {{ max-width: 85%; padding: 14px 18px; border-radius: 16px; font-size: 14px; line-height: 1.6; }}
  .msg-user {{ background: #3b82f6; color: white; align-self: flex-end; border-bottom-right-radius: 4px; }}
  .msg-bot {{ background: white; color: #1e293b; align-self: flex-start; border-bottom-left-radius: 4px;
              box-shadow: 0 1px 3px rgba(0,0,0,0.08); border: 1px solid #e2e8f0; }}
  .msg-bot strong {{ color: #0f172a; }}
  .msg-error {{ background: #fef2f2; color: #991b1b; border: 1px solid #fecaca; }}
  .msg-meta {{ font-size: 11px; color: #94a3b8; margin-top: 6px; display: flex; gap: 8px; flex-wrap: wrap; }}
  .msg-meta span {{ background: #f1f5f9; padding: 2px 8px; border-radius: 4px; }}
  .chart-container {{ margin: 8px 0; border-radius: 8px; overflow: hidden; }}
  .kql-toggle {{ font-size: 11px; color: #64748b; cursor: pointer; margin-top: 6px; }}
  .kql-block {{ background: #0f172a; color: #a5b4fc; padding: 12px; border-radius: 8px; font-size: 12px;
                font-family: 'Cascadia Code', 'Fira Code', monospace; white-space: pre-wrap; margin-top: 4px; display: none; }}
  .stream-cursor {{ display: inline-block; width: 2px; height: 14px; background: #3b82f6; animation: blink 1s infinite; }}
  @keyframes blink {{ 0%,50% {{ opacity: 1; }} 51%,100% {{ opacity: 0; }} }}
  .chips {{ display: flex; gap: 6px; flex-wrap: wrap; padding: 0 24px 8px; }}
  .chip {{ padding: 6px 14px; border-radius: 20px; border: 1px solid #d1d5db; background: white; cursor: pointer;
           font-size: 13px; color: #374151; transition: all 0.15s; }}
  .chip:hover {{ background: #eff6ff; border-color: #93c5fd; color: #1d4ed8; }}
  .input-bar {{ padding: 16px 24px; background: white; border-top: 1px solid #e2e8f0; display: flex; gap: 8px; }}
  .input-bar input {{ flex: 1; padding: 12px 16px; border: 1px solid #d1d5db; border-radius: 12px; font-size: 14px; outline: none; }}
  .input-bar input:focus {{ border-color: #3b82f6; box-shadow: 0 0 0 3px rgba(59,130,246,0.1); }}
  .input-bar button {{ padding: 12px 24px; background: #3b82f6; color: white; border: none; border-radius: 12px;
                        font-weight: 600; cursor: pointer; font-size: 14px; }}
  .input-bar button:hover {{ background: #2563eb; }}
  .input-bar button:disabled {{ background: #94a3b8; cursor: not-allowed; }}
  .welcome {{ flex: 1; display: flex; align-items: center; justify-content: center; text-align: center; color: #64748b; }}
  .welcome h1 {{ font-size: 28px; color: #1e293b; margin-bottom: 8px; }}
  .data-table {{ width: 100%; border-collapse: collapse; font-size: 12px; margin: 8px 0; }}
  .data-table th {{ background: #f1f5f9; padding: 6px 10px; text-align: left; font-weight: 600; color: #475569; border-bottom: 2px solid #e2e8f0; }}
  .data-table td {{ padding: 5px 10px; border-bottom: 1px solid #f1f5f9; color: #334155; }}
</style>
</head>
<body>
<div class="sidebar">
  <h2>⚡ PULSE</h2>
  <div><label>Data Source</label><select id="configSelect"></select></div>
  <div><label>Auth</label><select id="authSelect"><option value="azure_cli">Azure CLI</option><option value="managed_identity">Managed Identity</option></select></div>
  <button class="btn-connect" id="btnConnect" onclick="doConnect()">Connect</button>
  <button class="btn-disconnect" id="btnDisconnect" onclick="doDisconnect()" style="display:none">Disconnect</button>
  <div class="status-bar status-idle" id="statusBar">Not connected</div>
  <div class="progress-bar" id="progressWrap" style="display:none"><div class="progress-fill" id="progressFill" style="width:0%"></div></div>
  <div class="details" id="details"></div>
</div>
<div class="main">
  <div class="chat" id="chat"><div class="welcome" id="welcomeScreen"><div><h1>⚡ PULSE</h1><p>Connect to a data source to start asking questions</p></div></div></div>
  <div class="chips" id="chips"></div>
  <div class="input-bar">
    <input id="questionInput" placeholder="Ask a question…" onkeydown="if(event.key==='Enter')doQuestion()" disabled>
    <button id="btnAsk" onclick="doQuestion()" disabled>Ask</button>
  </div>
</div>
<script>
const configs = {configs_json};
let ws, isConnected = false, streamEl = null, chartCount = 0;
const sel = document.getElementById('configSelect');
configs.forEach(c => {{ const o = document.createElement('option'); o.value = c.name; o.textContent = c.name; sel.appendChild(o); }});

function initWS() {{
  ws = new WebSocket('ws://' + location.host + '/ws/chat');
  ws.onopen = () => setStatus('idle', 'Ready to connect');
  ws.onclose = () => {{ setStatus('idle', 'Disconnected'); setConnected(false); }};
  ws.onmessage = (e) => handleMessage(JSON.parse(e.data));
}}

function handleMessage(data) {{
  const t = data.type;
  if (t === 'welcome') return;
  if (t === 'status') {{ setStatus('connecting', data.message); if (data.progress) setProgress(data.progress); addDetail(data.message + (data.detail ? ' · ' + data.detail : '')); return; }}
  if (t === 'connected') {{ setStatus('connected', '✓ ' + data.tables + ' tables · ' + (data.rows||0).toLocaleString() + ' rows'); setProgress(1); setConnected(true); if (data.analyst_opening) addBot(data.analyst_opening); return; }}
  if (t === 'processing') {{ streamEl = addBot('<span class="stream-cursor"></span>', true); return; }}
  if (t === 'stream_start') {{ streamEl = addBot('<span class="stream-cursor"></span>', true); streamEl._text = ''; return; }}
  if (t === 'stream_token') {{ if (streamEl) {{ streamEl._text = (streamEl._text || '') + data.text; streamEl.querySelector('.msg-body').innerHTML = md(streamEl._text) + ' <span class="stream-cursor"></span>'; scroll(); }} return; }}
  if (t === 'stream_end') {{ if (streamEl) {{ streamEl.querySelector('.msg-body').innerHTML = md(data.full_text); meta(streamEl, data); streamEl = null; }} else {{ const el = addBot(data.full_text); meta(el, data); }} return; }}
  if (t === 'answer') {{ if (streamEl) {{ streamEl.querySelector('.msg-body').innerHTML = md(data.content||''); meta(streamEl, data); streamEl = null; }} else if (data.content) {{ const el = addBot(data.content); meta(el, data); }} return; }}
  if (t === 'chart') {{
    const id = 'ch_' + (chartCount++);
    const div = document.createElement('div'); div.className = 'chart-container'; div.id = id;
    const msgs = document.querySelectorAll('.msg-bot'); const last = msgs[msgs.length - 1];
    if (last) last.appendChild(div); else document.getElementById('chat').appendChild(div);
    try {{ const fig = data.plotly_json; Plotly.newPlot(id, fig.data, fig.layout || {{}}, {{responsive:true,displayModeBar:false}}); }} catch(e) {{ div.textContent = 'Chart error: '+e; }}
    scroll(); return;
  }}
  if (t === 'data_table') {{
    const last = document.querySelectorAll('.msg-bot'); const el = last[last.length-1];
    if (el && data.columns && data.rows) {{
      let h = '<table class="data-table"><thead><tr>' + data.columns.map(c=>'<th>'+c+'</th>').join('') + '</tr></thead><tbody>';
      data.rows.slice(0,20).forEach(r => {{ h += '<tr>' + data.columns.map(c=>'<td>'+(r[c]!=null?r[c]:'')+'</td>').join('') + '</tr>'; }});
      h += '</tbody></table>';
      if (data.total_rows > 20) h += '<div style="font-size:11px;color:#94a3b8;margin-top:4px;">Showing 20 of '+data.total_rows+' rows</div>';
      el.insertAdjacentHTML('beforeend', h); scroll();
    }} return;
  }}
  if (t === 'kql') {{
    const last = document.querySelectorAll('.msg-bot'); const el = last[last.length-1];
    if (el && data.code) {{ const kid = 'kql_'+Date.now(); el.insertAdjacentHTML('beforeend', '<div class="kql-toggle" onclick="toggleKql(\\''+kid+'\\')">⟨/⟩ Show KQL</div><pre class="kql-block" id="'+kid+'">'+esc(data.code)+'</pre>'); }}
    return;
  }}
  if (t === 'suggestions') {{
    const chips = document.getElementById('chips'); chips.innerHTML = '';
    (data.items||[]).forEach(item => {{ const b = document.createElement('button'); b.className='chip'; b.textContent=item.label||item.value; b.onclick=()=>{{ addUser(item.label||item.value); ws.send(JSON.stringify({{type:'chip',chip_id:item.value}})); }}; chips.appendChild(b); }});
    return;
  }}
  if (t === 'overview') {{ const el = document.createElement('div'); el.className='msg msg-bot'; el.innerHTML=data.html||data.content||''; document.getElementById('chat').appendChild(el); scroll(); return; }}
  if (t === 'error') {{ addError(data.message); streamEl = null; return; }}
  if (t === 'build_complete') {{ addDetail('Build done: '+data.tables+' tables'); return; }}
  if (t === 'data_upgraded') {{ addDetail(data.message); setStatus('connected', '✓ '+data.tables+' tables · '+(data.rows||0).toLocaleString()+' rows · '+data.scope); return; }}
}}

function addUser(text) {{ document.getElementById('welcomeScreen').style.display='none'; const el=document.createElement('div'); el.className='msg msg-user'; el.textContent=text; document.getElementById('chat').appendChild(el); scroll(); }}
function addBot(content, raw) {{ document.getElementById('welcomeScreen').style.display='none'; const el=document.createElement('div'); el.className='msg msg-bot'; el.innerHTML='<div class="msg-body">'+(raw?content:md(content))+'</div>'; document.getElementById('chat').appendChild(el); scroll(); return el; }}
function addError(text) {{ const el=document.createElement('div'); el.className='msg msg-bot msg-error'; el.innerHTML=md(text); document.getElementById('chat').appendChild(el); scroll(); }}
function meta(el,data) {{ if(!el)return; let p=[]; if(data.elapsed_seconds)p.push(data.elapsed_seconds+'s'); if(data.source)p.push(data.source); if(data.scope_label)p.push(data.scope_label); if(data.data_scope)p.push(data.data_scope); if(data.data_freshness)p.push('⏱ '+data.data_freshness); if(data.is_stale)p.push('⚠️ stale'); if(p.length) el.insertAdjacentHTML('beforeend','<div class="msg-meta">'+p.map(x=>'<span>'+x+'</span>').join('')+'</div>'); }}
function scroll() {{ const c=document.getElementById('chat'); c.scrollTop=c.scrollHeight; }}
function md(t) {{ if(!t)return''; return t.replace(/\\*\\*(.+?)\\*\\*/g,'<strong>$1</strong>').replace(/\\*(.+?)\\*/g,'<em>$1</em>').replace(/\\n/g,'<br>'); }}
function esc(t) {{ return t.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }}
function toggleKql(id) {{ const el=document.getElementById(id); el.style.display=el.style.display==='none'?'block':'none'; }}
function setStatus(s,t) {{ const b=document.getElementById('statusBar'); b.textContent=t; b.className='status-bar status-'+s; }}
function setProgress(p) {{ document.getElementById('progressWrap').style.display=p<1?'block':'none'; document.getElementById('progressFill').style.width=(p*100)+'%'; }}
function setConnected(v) {{ isConnected=v; document.getElementById('btnConnect').style.display=v?'none':'block'; document.getElementById('btnDisconnect').style.display=v?'block':'none'; document.getElementById('questionInput').disabled=!v; document.getElementById('btnAsk').disabled=!v; if(v)document.getElementById('questionInput').focus(); }}
function addDetail(t) {{ const d=document.getElementById('details'); d.textContent+=new Date().toLocaleTimeString()+' '+t+'\\n'; d.scrollTop=d.scrollHeight; }}
function doConnect() {{ const n=document.getElementById('configSelect').value; const a=document.getElementById('authSelect').value; document.getElementById('details').textContent=''; document.getElementById('progressWrap').style.display='block'; setStatus('connecting','Connecting…'); ws.send(JSON.stringify({{type:'connect',config_name:n,auth_method:a}})); }}
function doQuestion() {{ const i=document.getElementById('questionInput'); const t=i.value.trim(); if(!t)return; addUser(t); document.getElementById('chips').innerHTML=''; ws.send(JSON.stringify({{type:'question',text:t}})); i.value=''; }}
function doDisconnect() {{ ws.send(JSON.stringify({{type:'disconnect'}})); ws.close(); setConnected(false); setStatus('idle','Disconnected'); }}
initWS();
</script>
</body></html>"""
