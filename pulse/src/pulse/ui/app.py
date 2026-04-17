"""
PULSE v3.1 — Production UI
==============================

What changed from v2.2:
  1. STREAMING: LLM analysis responses stream token-by-token (~200ms first token)
  2. PROGRESSIVE RENDER: Chart renders first, text fills in after
  3. FOLLOW-UP BUTTONS: Clickable suggestions after every response
  4. SOURCE BADGES + TIMING: Know if it came from profile/kusto/cache and how fast
  5. st.fragment: Chat area isolated from sidebar — no more double-rerun tax

Author: PULSE Team
"""
from dotenv import load_dotenv
load_dotenv()

import streamlit as st
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pulse.core.config_loader import ConfigLoader, AuthMethod
from pulse.core.auth_manager import KustoAuthManager, AuthenticationError
from pulse.core.kusto_client import ConfigDrivenKustoClient
from pulse.core.duckdb_engine import DuckDBQueryEngine
from pulse.core.llm_service import LLMService
from pulse.core.rate_limiter import RateLimiter, RateLimitExceeded
from pulse.core.visualizer import SmartVisualizer
from pulse.core.ai_orchestrator import AIOrchestrator
from pulse.core.semantic_layer import SemanticLayer
from pulse.core.data_profile import DataProfile
from pulse.utils.logger import setup_logging
from pulse.utils.config import AppConfig

import plotly.graph_objects as go
import pandas as pd
import logging

# Date parsing utility (handles .NET ticks, Unix ms, tz-aware)
def _safe_parse_dates(series):
    """Safe date parser: handles .NET ticks, Unix timestamps, tz-aware datetimes.
    Always clamps output to 2000–2035 — any date outside that range is coerced to NaT.
    """
    import pandas as _pd
    import numpy as _np
    s = series.copy()

    def _clamp(s):
        """Force year into 2000–2035 range. Coerce outliers to NaT."""
        try:
            if _pd.api.types.is_datetime64_any_dtype(s):
                bad = (s.dt.year < 2000) | (s.dt.year > 2035)
                if bad.any():
                    s = s.copy()
                    s[bad] = _pd.NaT
        except Exception:
            pass
        return s

    if _pd.api.types.is_datetime64_any_dtype(s):
        try:
            if hasattr(s.dt, 'tz') and s.dt.tz is not None:
                s = s.dt.tz_convert(None)
        except Exception:
            pass
        try:
            years = s.dt.year
            if years.max() > 2035 or years.min() < 2000:
                numeric = s.astype('int64')
                NET_EPOCH_TICKS = 621355968000000000
                if float(_np.nanmedian(numeric)) > 6e17:
                    unix_ns = (numeric - NET_EPOCH_TICKS) * 100
                    s = _pd.to_datetime(unix_ns, unit='ns', errors='coerce')
        except Exception:
            pass
        return _clamp(s)
    try:
        numeric = _pd.to_numeric(s, errors='coerce')
        if numeric.notna().all():
            med = float(numeric.median())
            NET_EPOCH_TICKS = 621355968000000000
            if med > 6e17:
                unix_ns = (numeric - NET_EPOCH_TICKS) * 100
                return _pd.to_datetime(unix_ns, unit='ns', errors='coerce')
            elif med > 1e12:
                return _pd.to_datetime(numeric, unit='ms', errors='coerce')
            elif med > 1e9:
                return _pd.to_datetime(numeric, unit='s', errors='coerce')
    except Exception:
        pass
    try:
        result = _pd.to_datetime(s, errors='coerce', utc=True)
        if hasattr(result.dt, 'tz') and result.dt.tz is not None:
            result = result.dt.tz_convert(None)
        return result
    except Exception:
        pass
    return s

logger = logging.getLogger(__name__)

setup_logging()
app_config = AppConfig.load()

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="PULSE - Kusto Intelligence",
    page_icon="📡",
    layout="wide"
)

_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,400;0,9..40,500;0,9..40,600;1,9..40,400&family=DM+Mono:wght@400;500&display=swap');

/* ── Reset ───────────────────────────────────────────────────────── */
*, *::before, *::after { box-sizing: border-box; }

html, body, [data-testid="stAppViewContainer"] {
  font-family: 'DM Sans', system-ui, sans-serif;
  background: #F7F8FA;
  color: #0F172A;
}

/* ── Sidebar ─────────────────────────────────────────────────────── */
[data-testid="stSidebar"] {
  background: #0F172A !important;
  border-right: none !important;
}
[data-testid="stSidebar"] * { color: #CBD5E1 !important; }
[data-testid="stSidebar"] h1,
[data-testid="stSidebar"] .stMarkdown strong {
  color: #F8FAFC !important;
  font-size: 18px;
  font-weight: 600;
  letter-spacing: -0.3px;
}
[data-testid="stSidebar"] .stSelectbox label,
[data-testid="stSidebar"] .stTextInput label { color: #94A3B8 !important; font-size: 11px; text-transform: uppercase; letter-spacing: 0.8px; }
[data-testid="stSidebar"] .stSelectbox > div > div,
[data-testid="stSidebar"] .stTextInput input {
  background: #1E293B !important;
  border: 1px solid #334155 !important;
  color: #F1F5F9 !important;
  border-radius: 8px !important;
}
[data-testid="stSidebar"] hr { border-color: #1E293B !important; }
[data-testid="stSidebar"] .stButton button {
  background: #1E293B !important;
  border: 1px solid #334155 !important;
  color: #CBD5E1 !important;
  border-radius: 8px !important;
  font-size: 13px !important;
  transition: all 0.15s ease;
}
[data-testid="stSidebar"] .stButton button:hover {
  background: #2D3F55 !important;
  border-color: #475569 !important;
  color: #F1F5F9 !important;
}

/* ── Main content area ────────────────────────────────────────────── */
.main .block-container {
  max-width: 1100px;
  padding: 2rem 2rem 4rem;
}

/* ── Metric cards ─────────────────────────────────────────────────── */
.metric-card {
  background: #FFFFFF;
  border: 1px solid #E2E8F0;
  border-radius: 12px;
  padding: 20px;
  height: 110px;
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  transition: box-shadow 0.15s ease;
}
.metric-card:hover { box-shadow: 0 4px 16px rgba(15,23,42,0.08); }
.metric-card .label {
  font-size: 11px;
  font-weight: 500;
  text-transform: uppercase;
  letter-spacing: 0.8px;
  color: #64748B;
}
.metric-card .value {
  font-family: 'DM Mono', monospace;
  font-size: 28px;
  font-weight: 500;
  color: #0F172A;
  letter-spacing: -1px;
  line-height: 1;
}
.metric-card .delta { font-size: 12px; color: #64748B; }
.delta-up   { color: #10B981 !important; font-weight: 600; }
.delta-down { color: #EF4444 !important; font-weight: 600; }

/* ── Section titles ───────────────────────────────────────────────── */
.section-title {
  font-size: 11px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 1px;
  color: #94A3B8;
  margin-bottom: 12px;
}

/* ── Chips ────────────────────────────────────────────────────────── */
.stButton button {
  background: #0F172A !important;
  color: #F8FAFC !important;
  border: none !important;
  border-radius: 100px !important;
  padding: 10px 20px !important;
  font-family: 'DM Sans', sans-serif !important;
  font-size: 13px !important;
  font-weight: 500 !important;
  letter-spacing: -0.1px !important;
  transition: all 0.15s ease !important;
  box-shadow: 0 1px 3px rgba(15,23,42,0.15) !important;
}
.stButton button:hover {
  background: #1E293B !important;
  transform: translateY(-1px) !important;
  box-shadow: 0 4px 12px rgba(15,23,42,0.2) !important;
}

/* ── Chat bubbles ─────────────────────────────────────────────────── */
.user-bubble {
  background: #0F172A;
  color: #F8FAFC;
  border-radius: 16px 16px 4px 16px;
  padding: 12px 18px;
  margin: 8px 0 8px auto;
  max-width: 72%;
  width: fit-content;
  float: right;
  font-size: 14px;
  line-height: 1.5;
}
.clearfix { clear: both; }

.bot-message {
  background: #FFFFFF;
  border: 1px solid #E2E8F0;
  border-radius: 4px 16px 16px 16px;
  padding: 14px 18px;
  margin: 8px 0;
  max-width: 88%;
  font-size: 14px;
  line-height: 1.6;
  color: #1E293B;
}
.bot-message strong { color: #0F172A; font-weight: 600; }
.bot-message p { margin: 0 0 8px 0; }
.bot-message p:last-child { margin-bottom: 0; }

/* ── Scope pill ───────────────────────────────────────────────────── */
.scope-pill {
  display: inline-block;
  background: #F1F5F9;
  color: #64748B;
  border-radius: 20px;
  padding: 2px 10px;
  font-size: 11px;
  font-weight: 500;
  margin-left: 8px;
  vertical-align: middle;
  border: 1px solid #E2E8F0;
}

/* ── KQL code block ───────────────────────────────────────────────── */
.stCode { border-radius: 8px !important; font-family: 'DM Mono', monospace !important; }

/* ── Timing ───────────────────────────────────────────────────────── */
.timing-subtle { color: #94A3B8; font-size: 11px; }

/* ── Build pill ───────────────────────────────────────────────────── */
.build-pill {
  border-radius: 10px;
  padding: 12px 14px;
  display: flex;
  align-items: center;
  gap: 10px;
  margin: 8px 0;
  font-size: 13px;
}
.build-pill.building { background: #FFF7ED; border: 1px solid #FED7AA; color: #92400E; }
.build-pill.done     { background: #F0FDF4; border: 1px solid #BBF7D0; color: #14532D; }
.build-pill.failed   { background: #FEF2F2; border: 1px solid #FECACA; color: #7F1D1D; }
.build-dot {
  width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0;
}
.build-dot.amber { background: #F59E0B; animation: pulse-dot 1.4s ease-in-out infinite; }
.build-dot.green { background: #10B981; }
.build-dot.red   { background: #EF4444; }
@keyframes pulse-dot {
  0%, 100% { opacity: 1; transform: scale(1); }
  50%       { opacity: 0.4; transform: scale(0.75); }
}

/* ── Cluster pills ────────────────────────────────────────────────── */
.cluster-pill {
  display: inline-block; border-radius: 20px;
  padding: 2px 10px; font-size: 11px; font-weight: 600; margin-top: 4px;
}
.cluster-pill.ok { background: #DCFCE7; color: #166534; }

/* ── Analyst dashboard header ─────────────────────────────────────── */
.dash-title {
  font-size: 22px;
  font-weight: 600;
  color: #0F172A;
  letter-spacing: -0.5px;
  margin: 0;
}
.dash-subtitle {
  font-size: 13px;
  color: #64748B;
  margin-top: 4px;
}
.dash-divider {
  border: none;
  border-top: 1px solid #E2E8F0;
  margin: 16px 0 20px;
}

/* ── Chat input ───────────────────────────────────────────────────── */
.stChatInput textarea, .stTextInput input {
  border-radius: 100px !important;
  border: 1.5px solid #E2E8F0 !important;
  background: #FFFFFF !important;
  font-family: 'DM Sans', sans-serif !important;
  font-size: 14px !important;
  padding: 12px 20px !important;
  transition: border-color 0.15s ease !important;
}
.stChatInput textarea:focus, .stTextInput input:focus {
  border-color: #0F172A !important;
  box-shadow: none !important;
}

/* ── Expander clean ───────────────────────────────────────────────── */
.streamlit-expanderHeader {
  font-size: 13px !important;
  font-weight: 500 !important;
  color: #64748B !important;
  background: #F8FAFC !important;
  border-radius: 8px !important;
}

/* ── Connect button ───────────────────────────────────────────────── */
[data-testid="stSidebar"] .stButton:first-of-type button {
  background: #2563EB !important;
  color: white !important;
  border: none !important;
  width: 100% !important;
  font-weight: 600 !important;
}
[data-testid="stSidebar"] .stButton:first-of-type button:hover {
  background: #1D4ED8 !important;
}

/* ── Step detail (connect flow) ───────────────────────────────────── */
.step-detail { font-size: 12px; padding: 2px 0; color: #64748B; }
.step-detail.success { color: #059669; }
.step-detail.warn    { color: #D97706; }
.step-detail.error   { color: #DC2626; }

/* ── Manager summary bullets ──────────────────────────────────────── */
.manager-bullet {
  display: flex;
  align-items: flex-start;
  gap: 10px;
  padding: 12px 0;
  border-bottom: 1px solid #F1F5F9;
  font-size: 14px;
  line-height: 1.5;
  color: #1E293B;
}
.manager-bullet:last-child { border-bottom: none; }
.bullet-dot {
  width: 6px; height: 6px; border-radius: 50%;
  background: #0F172A; margin-top: 8px; flex-shrink: 0;
}
</style>
"""
# st.html() works in Streamlit >= 1.31; fall back to st.markdown for older versions
try:
    st.html(_CSS)
except AttributeError:
    st.markdown(_CSS, unsafe_allow_html=True)


def init(key, val):
    if key not in st.session_state:
        st.session_state[key] = val

init('config_loader', None)
init('duckdb_engine', DuckDBQueryEngine(app_config.cache_dir))
init('llm_service', LLMService())
init('rate_limiter', RateLimiter())
init('visualizer', SmartVisualizer())
init('ai_orchestrator', None)
init('kusto_client', None)
init('semantic_layer', None)
init('data_profile', None)
init('connected', False)

# ── Thread-safe build completion signal ───────────────────────────────────────
# _on_build_done() runs in a background thread and MUST NOT write st.session_state
# directly — Streamlit is not thread-safe. We use a threading.Event instead.
# The status pill fragment reads this event each poll cycle.
import threading as _threading
_build_done_event = _threading.Event()
_build_done_event.clear()
init('current_config', None)
init('messages', [])
init('pending_suggestion', None)
init('dash_data', None)
init('instant_dash', {})
init('profile_build_status', 'idle')   # idle | building | done | failed
init('analyst_opening', '')
init('profile_build_pct', 0)
init('overview_needs_refresh', False)

if st.session_state.config_loader is None:
    loader = ConfigLoader()
    loader.discover_and_load()
    st.session_state.config_loader = loader


# ── Sidebar ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## PULSE")
    st.caption("Telemetry Intelligence")
    st.markdown("---")

    configs = st.session_state.config_loader.get_all_configs()
    if not configs:
        st.error("No data sources in configs/")
        st.stop()

    config_map = {c.name: c for c in configs}
    sel_name = st.selectbox("Data Source", list(config_map.keys()))
    sel_cfg = config_map[sel_name]
    st.caption(f"Owner: {sel_cfg.owner}")
    for cl in sel_cfg.clusters:
        st.caption(f"  {cl.name}")

    st.markdown("---")
    st.markdown("##### Auth")
    auth_map = {
        "Azure CLI": AuthMethod.AZURE_CLI,
        "Managed Identity": AuthMethod.MANAGED_IDENTITY,
        "Service Principal": AuthMethod.SERVICE_PRINCIPAL,
    }
    sel_auth = auth_map[st.selectbox("Method", list(auth_map.keys()))]

    if st.button("Connect", use_container_width=True, type="primary"):
        try:
            with st.status("⚡ Initializing PULSE...", expanded=True) as status:
                connect_start = time.time()

                # ── Branded Header ──
                st.markdown(f"""
                <div class="connect-header">
                    <h3>📡 PULSE Connecting</h3>
                    <div class="subtitle">{sel_cfg.name} · {len(sel_cfg.clusters)} cluster{'s' if len(sel_cfg.clusters) > 1 else ''}</div>
                </div>""", unsafe_allow_html=True)

                # ── Step 1: Authenticate ──
                st.markdown("""
                <div class="connect-step">
                    <div class="step-line"></div>
                    <div class="step-icon" style="background:#DBEAFE;">🔗</div>
                    <div class="step-body">
                        <div class="step-title">Authenticating clusters</div>
                    </div>
                </div>""", unsafe_allow_html=True)

                sel_cfg.auth_method = sel_auth
                kc = ConfigDrivenKustoClient(sel_cfg, st.session_state.duckdb_engine)
                kc.connect_all_clusters()
                st.session_state.kusto_client = kc
                st.session_state.current_config = sel_cfg

                cluster_details = ""
                for cl in sel_cfg.clusters:
                    ok = cl.name in kc._clients
                    icon = "✅" if ok else "❌"
                    cls = "success" if ok else "warn"
                    cluster_details += f'<div class="step-detail {cls}" style="margin-left:50px;">{icon} {cl.name} — {cl.database}</div>'
                st.markdown(cluster_details, unsafe_allow_html=True)

                # ── Step 2: Schema ──
                st.markdown("""
                <div class="connect-step">
                    <div class="step-line"></div>
                    <div class="step-icon" style="background:#FEF3C7;">🔍</div>
                    <div class="step-body">
                        <div class="step-title">Discovering table schema</div>
                    </div>
                </div>""", unsafe_allow_html=True)

                discovered_schema = kc.discover_schema()
                if discovered_schema:
                    st.markdown(f'<div class="step-detail success" style="margin-left:50px;">✅ {len(discovered_schema)} columns mapped across all tables</div>', unsafe_allow_html=True)
                else:
                    st.markdown('<div class="step-detail warn" style="margin-left:50px;">⚠️ Schema discovery failed — continuing with config</div>', unsafe_allow_html=True)

                # ── Step 3: Semantic model ──
                st.markdown("""
                <div class="connect-step">
                    <div class="step-line"></div>
                    <div class="step-icon" style="background:#E0E7FF;">📐</div>
                    <div class="step-body">
                        <div class="step-title">Building semantic model</div>
                    </div>
                </div>""", unsafe_allow_html=True)

                config_path = sel_cfg.source_path
                sem = SemanticLayer(config_path=config_path) if config_path else SemanticLayer()

                if not sem.is_loaded or not sem.model.metrics:
                    if discovered_schema:
                        from pulse.core.config_generator import ConfigGenerator, ColumnInfo
                        gen = ConfigGenerator()
                        col_infos = {
                            name: ColumnInfo(name=name, data_type=dtype)
                            for name, dtype in discovered_schema.items()
                        }
                        cluster = sel_cfg.clusters[0]
                        client = kc._get_client(cluster)
                        col_infos = gen._analyze_cardinality(client, cluster.database, cluster.table, col_infos)
                        col_infos = gen._fetch_samples(client, cluster.database, cluster.table, col_infos)
                        col_infos = gen._classify_columns(col_infos)
                        result = gen._generate_config(
                            col_infos, cluster.url, cluster.database, cluster.table,
                            'azure_cli', sel_cfg.name, sel_cfg.owner or '',
                        )
                        if config_path:
                            with open(config_path, 'w', encoding='utf-8') as f:
                                f.write(result.yaml_text)
                        sem = SemanticLayer(config_path=config_path) if config_path else SemanticLayer()
                        st.markdown(f'<div class="step-detail success" style="margin-left:50px;">✅ Auto-discovered {len(result.metrics)} metrics, {len(result.dimensions)} dimensions</div>', unsafe_allow_html=True)
                    else:
                        st.markdown('<div class="step-detail warn" style="margin-left:50px;">⚠️ No schema — live Kusto only</div>', unsafe_allow_html=True)
                else:
                    nm = len(sem.model.metrics) if sem.model else 0
                    nd = len(sem.model.dimensions) if sem.model else 0
                    st.markdown(f'<div class="step-detail success" style="margin-left:50px;">✅ {nm} metrics, {nd} dimensions loaded</div>', unsafe_allow_html=True)

                st.session_state.semantic_layer = sem

                if sem.is_loaded and discovered_schema:
                    validation = sem.validate_against_schema(discovered_schema)
                    if not validation['valid']:
                        sem.prune_queries_by_schema(discovered_schema)

                # ── Step 4: Data Loading (cache → instant → bridge) ──
                profile = DataProfile(kc, st.session_state.duckdb_engine)
                all_queries = sem.get_profile_queries() if sem.is_loaded else {}
                profile._all_queries = all_queries
                # Only background-build tier 1 tables (totals, daily, region, org).
                # Tier 3 cross-tabs (org×region, org×entity) are on-demand only —
                # they are expensive joins that would make the build take 5+ minutes.
                _bg_queries = {k: v for k, v in all_queries.items() if v.tier <= 1}
                logger.info(f"Background will build {len(_bg_queries)} tier-1 tables "
                            f"(skipping {len(all_queries)-len(_bg_queries)} tier-3 cross-tabs)")
                data_source = None

                if sem.is_loaded:
                    # 4a. Try disk cache
                    st.markdown("""
                    <div class="connect-step">
                        <div class="step-line"></div>
                        <div class="step-icon" style="background:#D1FAE5;">💾</div>
                        <div class="step-body">
                            <div class="step-title">Loading data</div>
                        </div>
                    </div>""", unsafe_allow_html=True)

                    cache_loaded = profile.load_from_disk(sel_cfg.name)
                    if cache_loaded:
                        age = profile.get_cache_age_str(sel_cfg.name)
                        st.markdown(f'<div class="step-detail success" style="margin-left:50px;">💾 Restored {len(profile.tables)} tables from cache ({age})</div>', unsafe_allow_html=True)
                        data_source = 'cache'
                    else:
                        st.markdown('<div class="step-detail" style="margin-left:50px;color:#64748B;">No fresh cache — running 7-day snapshot...</div>', unsafe_allow_html=True)

                        try:
                            time_col = sem.model.time_column if sem.model else 'EventInfo_Time'
                            # Use config-driven column names — not hardcoded OrgId/GeoName
                            org_col = 'OrgId'
                            geo_col = 'GeoName'
                            if sem.model and sem.model.dimensions:
                                for did, dim in sem.model.dimensions.items():
                                    if did in ('organization', 'org', 'tenant'):
                                        org_col = dim.column
                                    elif did in ('region', 'geo', 'geography'):
                                        geo_col = dim.column
                            instant = kc.fire_instant_dashboard(
                                time_column=time_col,
                                org_column=org_col,
                                geo_column=geo_col,
                            )
                            st.session_state.instant_dash = instant

                            loaded = {k: len(v) for k, v in instant.items() if not v.empty}
                            if loaded:
                                parts = []
                                for k, v in loaded.items():
                                    label = {'daily': 'trend', 'top10': 'orgs', 'regions': 'geo', 'totals': 'totals'}.get(k, k)
                                    parts.append(f"{label}({v})")
                                st.markdown(f'<div class="step-detail success" style="margin-left:50px;">⚡ {" · ".join(parts)}</div>', unsafe_allow_html=True)

                            # Instant Bridge
                            registered = profile.populate_from_instant(instant)
                            if registered > 0:
                                st.markdown(f'<div class="step-detail success" style="margin-left:50px;">✅ {registered} tables bridged → instant queries ready</div>', unsafe_allow_html=True)
                                data_source = 'instant'


                        except Exception as e:
                            logger.warning(f"Instant dashboard failed: {e}")
                            st.markdown('<div class="step-detail warn" style="margin-left:50px;">⚠️ Snapshot skipped</div>', unsafe_allow_html=True)
                            st.session_state.instant_dash = {}

                    st.session_state.data_profile = profile

                    # ★ Background: build missing tables then save cache
                    # build_all_background() automatically skips tables already
                    # loaded from instant bridge or disk cache.
                    def _on_build_done():
                        # IMPORTANT: this runs in a background thread.
                        # Never write st.session_state here — Streamlit is not
                        # thread-safe and doing so causes WebSocket disconnects.
                        try:
                            profile.save_to_disk(sel_cfg.name)
                            logger.info("Background: saved to disk cache")
                        except Exception as e:
                            logger.error(f"Cache save failed: {e}")
                        # Signal completion via thread-safe Event — the fragment
                        # polls this and updates session_state from the main thread.
                        _build_done_event.set()
                        logger.info("Background build complete — event set")

                    _build_done_event.clear()  # reset from any prior session
                    st.session_state.profile_build_status = 'building'
                    st.session_state.profile_build_pct = 0
                    profile.build_all_background(_bg_queries, callback_on_done=_on_build_done)
                else:
                    st.session_state.data_profile = None
                    st.session_state.instant_dash = {}

                # ── Step 5: AI Engine ──
                st.markdown("""
                <div class="connect-step">
                    <div class="step-icon" style="background:#FCE7F3;">🤖</div>
                    <div class="step-body">
                        <div class="step-title">Starting AI engine</div>
                    </div>
                </div>""", unsafe_allow_html=True)

                schema = discovered_schema or {c.name: c.type for c in sel_cfg.get_all_columns()}
                orch = AIOrchestrator(
                    st.session_state.llm_service, kc,
                    st.session_state.duckdb_engine,
                    st.session_state.visualizer,
                    semantic_layer=st.session_state.semantic_layer,
                    data_profile=st.session_state.data_profile,
                    enrichment_config=sel_cfg.enrichment,
                    config_path=config_path,
                )
                orch.set_schema(schema)

                if sel_cfg.enrichment and sel_cfg.enrichment.get('source', 'none') != 'none':
                    orch.load_enrichment()
                    if orch.enricher.is_loaded:
                        st.markdown(f'<div class="step-detail success" style="margin-left:50px;">✅ {orch.enricher.get_status_line()}</div>', unsafe_allow_html=True)

                # Enable predictive cache — pre-computes follow-ups in background
                orch.init_predictive_cache(visualizer=st.session_state.visualizer)

                st.session_state.ai_orchestrator = orch
                st.session_state.connected = True

                # ── Generate analyst opening statement (orchestrator now available) ──
                try:
                    from pulse.core.analyst_opening import generate_opening_statement
                    _instant_data = st.session_state.get('instant_dash', {})
                    if _instant_data:
                        _opening = generate_opening_statement(
                            instant_data=_instant_data,
                            llm_client=orch.llm_service.client,
                            llm_model=orch.llm_service.model,
                            enricher=getattr(orch, 'enricher', None),
                            product_name=sel_cfg.name if sel_cfg else 'your product',
                        )
                        st.session_state.analyst_opening = _opening
                except Exception as _oe:
                    logger.warning(f"Opening statement skipped: {_oe}")
                    st.session_state.analyst_opening = ""

                # ── Hero Summary Card ──
                elapsed = time.time() - connect_start
                table_count = len(profile.tables) if profile else 0
                row_count = profile.total_rows if profile else 0

                source_icon = {'cache': '💾', 'instant': '⚡', None: '🔄'}[data_source]
                source_text = {'cache': 'Cached profile', 'instant': '7-day snapshot', None: 'Live mode'}[data_source]

                cluster_count = sum(1 for cl in sel_cfg.clusters if cl.name in kc._clients)
                col_count = len(discovered_schema) if discovered_schema else 0

                data_ready_msg = (
                    "Full 90-day profile ready — instant answers, zero lag."
                    if source_text == "Cached profile"
                    else "7-day data loaded. Full 90-day profile building in background — answers available immediately."
                )
                st.markdown(f"""
                <div class="connect-summary">
                    <div style="display:flex;justify-content:space-between;align-items:flex-start;">
                        <div>
                            <div class="hero-number">{elapsed:.1f}s</div>
                            <div class="hero-label">Connected in</div>
                        </div>
                        <div style="text-align:right;">
                            <div style="font-size:24px;font-weight:700;color:#1E40AF;">{row_count:,}</div>
                            <div class="hero-label" style="color:#2563EB;">Rows ready</div>
                        </div>
                    </div>
                    <div class="hero-detail">
                        {source_icon} {data_ready_msg}
                    </div>
                </div>""", unsafe_allow_html=True)

                status.update(label=f"✅ Ready in {elapsed:.1f}s — {source_text}", state="complete", expanded=False)

                # ── Business Overview Card — auto-appears on connect ────────────
                try:
                    import time as _t
                    _daily_df  = st.session_state.data_profile.query_safe(
                        "SELECT * FROM profile_daily ORDER BY day ASC"
                    ) if st.session_state.data_profile.has_table('profile_daily') else None
                    _org_df    = st.session_state.data_profile.query_safe(
                        "SELECT * FROM profile_organization ORDER BY events DESC"
                    ) if st.session_state.data_profile.has_table('profile_organization') else None
                    _region_df = st.session_state.data_profile.query_safe(
                        "SELECT * FROM profile_region ORDER BY events DESC"
                    ) if st.session_state.data_profile.has_table('profile_region') else None
                    _totals_df = st.session_state.data_profile.query_safe(
                        "SELECT * FROM profile_totals"
                    ) if st.session_state.data_profile.has_table('profile_totals') else None

                    _scope = '7d' if data_source == 'instant' else '90d'
                    # Extract column names for KQL snippets
                    _time_col = 'EventInfo_Time'
                    _org_col  = 'OrgId'
                    _geo_col  = 'GeoName'
                    _table    = sel_cfg.clusters[0].table if sel_cfg.clusters else 'YOUR_TABLE'
                    if hasattr(sem, 'model') and sem.model:
                        if sem.model.time_column:
                            _time_col = sem.model.time_column
                        if sem.model.dimensions:
                            for _did, _dim in sem.model.dimensions.items():
                                if _did in ('organization', 'org', 'tenant'):
                                    _org_col = _dim.column
                                elif _did in ('region', 'geo', 'geography'):
                                    _geo_col = _dim.column

                    _overview = build_business_overview(
                        profile_daily=_daily_df,
                        profile_org=_org_df,
                        profile_region=_region_df,
                        profile_totals=_totals_df,
                        config={
                            'table': _table,
                            'time_col': _time_col,
                            'org_col': _org_col,
                            'geo_col': _geo_col,
                        },
                        data_scope=_scope,
                        product_name=sel_cfg.name,
                    )
                    if _overview and _overview.get('html'):
                        _overview_msg = {
                            "role": "assistant",
                            "ts": _t.time(),
                            "intent": "business_overview",
                            "content": "",
                            "executive_briefing_html": _overview['html'],
                            "chart": None,
                            "dataframe": None,
                            "kql": None,
                            "source": "profile",
                            "elapsed": None,
                            "error": None,
                            "suggestions": None,
                            "follow_up_suggestions": [],
                            "user_question": "",
                        }
                        if 'messages' not in st.session_state:
                            st.session_state.messages = []
                        st.session_state.messages.insert(0, _overview_msg)
                        st.session_state['business_overview_scope'] = _scope
                except Exception as _oe:
                    logger.warning(f"Business overview failed: {_oe}")

                # ── Proactive digest — "since you last looked" ────────────────────
                try:
                    digest = orch.get_proactive_digest()
                    if digest:
                        import time as _t2
                        digest_msg = {
                            "role": "assistant",
                            "ts": _t.time() - 0.001,
                            "intent": "proactive_digest",
                            "content": digest,
                            "chart": None,
                            "dataframe": None,
                            "kql": None,
                            "source": "memory",
                            "elapsed": None,
                            "error": None,
                            "suggestions": None,
                            "follow_up_suggestions": ["Show the trend", "Top 10 orgs", "Regional adoption"],
                            "user_question": "",
                        }
                        if 'messages' not in st.session_state:
                            st.session_state.messages = []
                        st.session_state.messages.insert(0, digest_msg)
                except Exception as _de:
                    pass  # digest is optional, never break connect

            st.rerun()
        except AuthenticationError as e:
            st.error(f"Auth failed: {e}")
            st.session_state.connected = False
        except Exception as e:
            st.error(f"Failed: {e}")
            st.session_state.connected = False

    st.markdown("---")

    # ── Status pill — auto-polls every 2s while build is in progress ──────────
    # Uses st.fragment(run_every=4) so ONLY this tiny section reruns,
    # not the entire page. Zero cost. Flips to green the moment build finishes.
    def _render_status_pill():
        """Render the build status pill. Called directly or as a fragment."""
        if not st.session_state.connected:
            st.markdown("**Not connected**")
            return

        # Check thread-safe event — update session_state from main thread (safe)
        if _build_done_event.is_set():
            _build_done_event.clear()
            st.session_state.profile_build_status = 'done'
            st.session_state.overview_needs_refresh = True
            logger.info("Status pill: build done event received, session updated")

        st.markdown("**Connected**")
        st.caption(st.session_state.get('current_config').name
                   if st.session_state.get('current_config') else "")

        profile      = st.session_state.data_profile
        instant      = st.session_state.get('instant_dash', {})
        build_status = st.session_state.get('profile_build_status', 'idle')

        # Also check if the background thread has actually finished by inspecting
        # the profile tables directly — catches cases where the flag was set but
        # rerun hasn't happened yet.
        if build_status == 'building' and profile:
            try:
                expected = len(st.session_state.get('semantic_layer').get_profile_queries() or {})
                built    = len(profile.tables)
                if expected > 0 and built >= expected:
                    st.session_state.profile_build_status = 'done'
                    st.session_state.overview_needs_refresh = True
                    build_status = 'done'
            except Exception:
                pass

        if profile and profile.tables:
            scope   = profile.get_data_scope_label()
            age     = profile.get_cache_age_str(st.session_state.current_config.name)
            age_str = f" · {age}" if age else ""

            if build_status == 'building':
                # Count progress: how many tables built vs total expected
                progress_note = ""
                try:
                    expected = len(st.session_state.get('semantic_layer').get_profile_queries() or {})
                    built    = len(profile.tables)
                    if expected > 0:
                        pct = int(built / expected * 100)
                        progress_note = f"{built}/{expected} tables · {pct}%"
                except Exception:
                    progress_note = "loading…"

                st.markdown(f"""
<div class="build-pill building">
  <div class="build-dot amber"></div>
  <div>
    <div style="font-weight:600;">Building 90-day profile…</div>
    <div style="font-size:11px;margin-top:1px;opacity:0.8;">
      Answers available now · {progress_note}
    </div>
  </div>
</div>""", unsafe_allow_html=True)

            elif build_status == 'done':
                st.markdown(f"""
<div class="build-pill done">
  <div class="build-dot green"></div>
  <div>
    <div style="font-weight:600;">90-day profile ready</div>
    <div style="font-size:11px;margin-top:1px;opacity:0.8;">{scope}{age_str}</div>
  </div>
</div>""", unsafe_allow_html=True)

            elif build_status == 'failed':
                st.markdown("""
<div class="build-pill failed">
  <div class="build-dot red"></div>
  <div style="font-weight:600;">Profile build failed · try Refresh</div>
</div>""", unsafe_allow_html=True)
            else:
                # Idle / already cached — just show scope
                st.markdown(f"""
<div class="build-pill done">
  <div class="build-dot green"></div>
  <div>
    <div style="font-weight:600;">{scope}</div>
    <div style="font-size:11px;margin-top:1px;opacity:0.8;">{age_str.strip(' ·') or 'ready'}</div>
  </div>
</div>""", unsafe_allow_html=True)

        elif instant:
            st.markdown("""
<div class="build-pill building">
  <div class="build-dot amber"></div>
  <div>
    <div style="font-weight:600;">7-day snapshot active</div>
    <div style="font-size:11px;margin-top:1px;opacity:0.8;">90-day profile loading…</div>
  </div>
</div>""", unsafe_allow_html=True)
        else:
            st.caption("Live mode — no profile")

    # Use fragment with run_every=4 while building so it auto-updates.
    # When done, stop polling — no cost at rest.
    _build_status_now = st.session_state.get('profile_build_status', 'idle')
    _fragment_supported = hasattr(st, 'fragment')

    if _fragment_supported and _build_status_now == 'building':
        # Auto-poll every 2 seconds ONLY while build is in progress
        @st.fragment(run_every=4)  # 4s — enough for status; 2s caused excess WS traffic
        def _status_fragment():
            _render_status_pill()
        _status_fragment()
    else:
        # Build done or not started — render once, no polling overhead
        _render_status_pill()

    # ── Action buttons ────────────────────────────────────────────────────────
    if st.session_state.connected:
        col_disc, col_ref = st.columns(2)
        with col_disc:
            if st.button("Disconnect", use_container_width=True):
                _build_done_event.clear()
                st.session_state.connected = False
                st.session_state.kusto_client = None
                st.session_state.ai_orchestrator = None
                st.session_state.semantic_layer = None
                st.session_state.data_profile = None
                st.session_state.dash_data = None
                st.session_state.instant_dash = {}
                st.session_state.profile_build_status = 'idle'
                st.session_state.duckdb_engine.clear()
                st.session_state.messages = []
                st.rerun()
        with col_ref:
            if st.button("♻️ Refresh", use_container_width=True, help="Invalidate cache & reconnect"):
                cfg_name = st.session_state.current_config.name
                DataProfile.invalidate_cache(cfg_name)
                st.session_state.connected = False
                st.session_state.kusto_client = None
                st.session_state.ai_orchestrator = None
                st.session_state.semantic_layer = None
                st.session_state.data_profile = None
                st.session_state.dash_data = None
                st.session_state.instant_dash = {}
                st.session_state.profile_build_status = 'idle'
                st.session_state.duckdb_engine.clear()
                st.session_state.messages = []
                st.toast("Cache cleared — reconnect for fresh data")
                st.rerun()

    st.markdown("---")
    if st.button("Clear Chat", use_container_width=True):
        st.session_state.messages = []
        st.rerun()


# ── Main: Welcome ────────────────────────────────────────────────────────────

if not st.session_state.connected:
    st.title("PULSE")
    # ── Auto-refresh overview when 90-day profile completes ─────────────────
    if (st.session_state.get('overview_needs_refresh')
            and st.session_state.get('connected')
            and st.session_state.get('data_profile')):
        try:
            st.session_state.overview_needs_refresh = False
            _profile = st.session_state.data_profile
            _sel = st.session_state.current_config
            _sem  = st.session_state.get('semantic_layer')

            _d  = _profile.query_safe("SELECT * FROM profile_daily ORDER BY day ASC") if _profile.has_table('profile_daily') else None
            _o  = _profile.query_safe("SELECT * FROM profile_organization ORDER BY events DESC") if _profile.has_table('profile_organization') else None
            _r  = _profile.query_safe("SELECT * FROM profile_region ORDER BY events DESC") if _profile.has_table('profile_region') else None
            _t  = _profile.query_safe("SELECT * FROM profile_totals") if _profile.has_table('profile_totals') else None

            _tc = 'EventInfo_Time'; _oc = 'OrgId'; _gc = 'GeoName'
            _tb = _sel.clusters[0].table if _sel.clusters else 'YOUR_TABLE'
            if _sem and _sem.model:
                if _sem.model.time_column: _tc = _sem.model.time_column
                for _did, _dim in (_sem.model.dimensions or {}).items():
                    if _did in ('organization','org','tenant'): _oc = _dim.column
                    elif _did in ('region','geo','geography'):  _gc = _dim.column

            _ov = build_business_overview(
                profile_daily=_d, profile_org=_o,
                profile_region=_r, profile_totals=_t,
                config={'table':_tb,'time_col':_tc,'org_col':_oc,'geo_col':_gc},
                data_scope='90d',
                product_name=_sel.name,
            )
            if _ov and _ov.get('html') and st.session_state.messages:
                # Replace the overview card (first message) with 90-day version
                msgs = st.session_state.messages
                for i, msg in enumerate(msgs):
                    if msg.get('intent') == 'business_overview':
                        import time as _rt
                        msgs[i] = {**msg,
                            'executive_briefing_html': _ov['html'],
                            'ts': _rt.time(),
                        }
                        break
                logger.info("Overview auto-refreshed with 90-day data")
        except Exception as _re:
            logger.warning(f"Overview refresh failed: {_re}")

    st.markdown("Natural-language telemetry intelligence. Connect a data source in the sidebar to begin.")
    st.markdown("")
    st.markdown("**Try asking:**")
    for q in [
        "🏆 Top 10 orgs by events",
        "📈 Is usage growing or declining?",
        "🌍 How does EMEA compare to other regions?",
        "⚠️ Any anomalies or issues this week?",
        "📋 Give me a summary for my manager",
    ]:
        st.markdown(f"- {q}")
    st.stop()


# ══════════════════════════════════════════════════════════════════════════════
# EXECUTIVE DASHBOARD — proactive insights on first load
# ══════════════════════════════════════════════════════════════════════════════

def _format_number(n):
    """Format large numbers: 1234567 → 1.2B, 12345 → 12.3K"""
    if n is None or pd.isna(n):
        return "—"
    n = float(n)
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.1f}B"
    elif n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    elif n >= 1_000:
        return f"{n / 1_000:.1f}K"
    else:
        return f"{n:,.0f}"


def _compute_dashboard_data(profile, config):
    """Compute dashboard metrics. Uses instant dashboard data first, profile tables as upgrade."""

    instant = st.session_state.get('instant_dash', {})

    data = {
        'total_events': None,
        'total_orgs': None,
        'total_regions': None,
        'top_orgs_df': pd.DataFrame(),
        'daily_df': pd.DataFrame(),
        'region_df': pd.DataFrame(),
        'profile_tables': len(profile.tables) if profile else 0,
        'profile_rows': profile.total_rows if profile else 0,
        'profile_build_ms': profile.build_time_ms if profile else 0,
        'clusters': [(c.name, c.database, c.table) for c in (config.clusters if config else [])],
        'config_name': config.name if config else 'Unknown',
        'is_instant': bool(instant),  # Badge: "30-DAY ANALYSIS"
    }

    # ── Source 1: Instant dashboard data (available immediately after connect) ──

    # Daily first — this is our most reliable data source
    try:
        daily_df = instant.get('daily', pd.DataFrame())
        if not daily_df.empty:
            daily_df = daily_df.copy()
            # Robust date parsing
            for c in daily_df.columns:
                if c.lower() in ('day', 'date'):
                    try:
                        dt = pd.to_datetime(daily_df[c], errors='coerce')
                        # ★ Always strip tz correctly:
                        # tz_localize(None) fails on tz-aware; use tz_convert(None)
                        if hasattr(dt.dt, 'tz') and dt.dt.tz is not None:
                            dt = dt.dt.tz_convert(None)
                        elif hasattr(dt.dtype, 'tz') and dt.dtype.tz is not None:
                            dt = dt.dt.tz_convert(None)
                        daily_df[c] = dt
                        daily_df = daily_df.sort_values(c)
                    except Exception as _e:
                        logger.warning(f"Dashboard date parse failed for {c}: {_e}")
                    break
            data['daily_df'] = daily_df

            # Derive total events from daily sum (always available)
            for c in daily_df.columns:
                if c.lower() in ('events', 'event_count', 'totalevents'):
                    data['total_events'] = daily_df[c].sum()
                    break
    except Exception:
        pass

    # Totals (may have been derived from daily in kusto_client)
    try:
        totals_df = instant.get('totals', pd.DataFrame())
        if not totals_df.empty and data['total_events'] is None:
            row = totals_df.iloc[0]
            for c in totals_df.columns:
                if 'event' in c.lower():
                    data['total_events'] = row[c]
                    break
    except Exception:
        pass

    # Top 10
    try:
        top10_df = instant.get('top10', pd.DataFrame())
        if not top10_df.empty:
            data['top_orgs_df'] = top10_df.head(5)
            data['total_orgs'] = len(top10_df)
    except Exception:
        pass

    # Regions
    try:
        region_df = instant.get('regions', pd.DataFrame())
        if not region_df.empty:
            data['region_df'] = region_df
            data['total_regions'] = len(region_df)
    except Exception:
        pass

    # ── Source 2: Profile tables (upgrade when background build finishes) ──
    if profile and profile.tables:
        available = set(profile.list_tables())
        data['profile_tables'] = len(available)
        data['profile_rows'] = profile.total_rows

        try:
            if 'profile_totals' in available and data['total_events'] is None:
                tot = profile.query("SELECT * FROM profile_totals")
                if not tot.empty:
                    row = tot.iloc[0]
                    for c in tot.columns:
                        cl = c.lower()
                        if 'event' in cl and data['total_events'] is None:
                            data['total_events'] = row[c]
                        elif 'org' in cl and data['total_orgs'] is None:
                            data['total_orgs'] = row[c]
        except Exception:
            pass

        try:
            if 'profile_organization' in available and data['top_orgs_df'].empty:
                data['top_orgs_df'] = profile.query(
                    "SELECT * FROM profile_organization ORDER BY events DESC LIMIT 5")
        except Exception:
            pass

        try:
            if 'profile_region' in available and data['region_df'].empty:
                data['region_df'] = profile.query(
                    "SELECT * FROM profile_region ORDER BY events DESC")
                if not data['region_df'].empty:
                    data['total_regions'] = len(data['region_df'])
        except Exception:
            pass

        # Upgrade from instant data — profile is no longer "instant" mode
        if len(available) >= 4:
            data['is_instant'] = False

    return data


def _render_analyst_dashboard():
    """
    Redesigned dashboard — premium light-mode cards.
    Dark narrative header + insight pills → metric cards → trend chart
    → regions + org watch → chips.
    """
    profile      = st.session_state.data_profile
    config       = st.session_state.current_config
    instant      = st.session_state.get('instant_dash', {})
    opening      = st.session_state.get('analyst_opening', '')
    build_status = st.session_state.get('profile_build_status', 'idle')

    has_data = bool(instant) or (profile and profile.is_built)
    if not has_data:
        return

    def _q(sql):
        try:
            r = profile.query_safe(sql) if (profile and profile.is_built) else None
            return r if r is not None else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def _fmt(n):
        try:
            n = float(n)
            if abs(n) >= 1e9:  return f"{n/1e9:.1f}B"
            if abs(n) >= 1e6:  return f"{n/1e6:.1f}M"
            if abs(n) >= 1e3:  return f"{n/1e3:.0f}K"
            return f"{int(n):,}"
        except Exception:
            return "—"

    daily_df  = _q("SELECT * FROM profile_daily ORDER BY day ASC")                 if (profile and profile.has_table('profile_daily'))                 else instant.get('daily', pd.DataFrame())
    region_df = _q("SELECT * FROM profile_region ORDER BY events DESC")                 if (profile and profile.has_table('profile_region'))                 else instant.get('regions', pd.DataFrame())
    org_df    = _q("SELECT * FROM profile_organization ORDER BY events ASC LIMIT 10")                 if (profile and profile.has_table('profile_organization'))                 else pd.DataFrame()

    # ─── 1. NARRATIVE HEADER ────────────────────────────────────────────────
    _summary  = ""
    _insights = []
    if isinstance(opening, dict) and opening:
        _summary  = opening.get("summary", "")
        _insights = opening.get("insights", [])
    elif isinstance(opening, str) and opening:
        _summary = opening

    if _summary or _insights:
        _dir_cfg = {
            "up":      ("↑", "#10B981", "#022C22", "#10B98120"),
            "down":    ("↓", "#F87171", "#1C0A0A", "#F8717120"),
            "neutral": ("→", "#94A3B8", "#1E293B", "#94A3B815"),
        }
        pills_html = ""
        for ins in _insights[:3]:
            d   = ins.get("direction", "neutral")
            txt = ins.get("text", "")
            val = ins.get("value", "")
            icon, tcol, bgcol, bdr = _dir_cfg.get(d, _dir_cfg["neutral"])
            pills_html += (
                f'<div style="display:flex;align-items:center;gap:10px;'
                f'padding:11px 14px;background:{bgcol};border:1px solid {bdr};'
                f'border-radius:10px;margin-bottom:8px;">'
                f'<span style="font-size:16px;font-weight:800;color:{tcol};width:18px;text-align:center;">{icon}</span>'
                f'<span style="font-size:13px;color:#CBD5E1;flex:1;">{txt}</span>'
                f'<span style="font-size:13px;font-weight:700;color:{tcol};font-variant-numeric:tabular-nums;">{val}</span>'
                f'</div>'
            )

        status_dot = "🟢" if build_status == "done" else "🔵"
        status_txt = "14-day profile ready" if build_status == "done" else "14-day profile building…"
        col_l, col_r = st.columns([1.1, 1])
        with col_l:
            st.markdown(
                f'<div style="background:#0F172A;border-radius:14px;padding:24px 26px;'
                f'min-height:168px;display:flex;flex-direction:column;justify-content:space-between;">'
                f'<div style="font-size:16px;line-height:1.8;color:#F1F5F9;font-weight:400;">{_summary}</div>'
                f'<div style="margin-top:16px;font-size:11px;color:#475569;">'
                f'{status_dot} Last 7 days · {status_txt}</div>'
                f'</div>',
                unsafe_allow_html=True
            )
        with col_r:
            if pills_html:
                st.markdown(pills_html, unsafe_allow_html=True)
    else:
        name = config.name if config else "Dashboard"
        st.markdown(
            f'<div style="font-size:22px;font-weight:700;color:#0F172A;margin-bottom:4px;">{name}</div>',
            unsafe_allow_html=True
        )

    st.markdown("<div style='margin:18px 0 4px;'></div>", unsafe_allow_html=True)

    # ─── 2. METRIC CARDS ────────────────────────────────────────────────────
    _active_orgs   = 0
    _total_events  = 0
    _ev_wow_pct    = 0.0
    _org_wow_pct   = 0.0
    _top_region    = "—"
    _top_region_ev = 0
    _at_risk_n     = 0

    if not daily_df.empty:
        ao_col = next((c for c in daily_df.columns if c.lower() in
                      ('active_orgs','active_organizations','dcount_orgid')), None)
        ev_col = next((c for c in daily_df.columns if c.lower() in ('events','event_count')), None)
        if ao_col:
            vals = pd.to_numeric(daily_df[ao_col], errors='coerce').fillna(0).tolist()
            _active_orgs = int(round(float(pd.Series(vals[-4:]).median()))) if len(vals)>=4 else (int(vals[-1]) if vals else 0)
            prior_orgs   = float(pd.Series(vals[:-4]).median()) if len(vals)>=7 else float(_active_orgs)
            _org_wow_pct = ((_active_orgs - prior_orgs) / prior_orgs * 100) if prior_orgs > 0 else 0.0
            _org_wow_pct = max(-200.0, min(200.0, _org_wow_pct))
        if ev_col:
            ev_vals = pd.to_numeric(daily_df[ev_col], errors='coerce').fillna(0).tolist()
            _total_events  = sum(ev_vals[-7:])
            prior_ev       = sum(ev_vals[-14:-7]) if len(ev_vals) >= 14 else 0
            _ev_wow_pct    = ((_total_events - prior_ev) / prior_ev * 100) if prior_ev > 0 else 0.0
            _ev_wow_pct    = max(-200.0, min(200.0, _ev_wow_pct))

    if not region_df.empty:
        g_col    = next((c for c in region_df.columns if c.lower() in ('geoname','geo','region','geography')), None)
        ao_col2  = next((c for c in region_df.columns if c.lower() in ('active_orgs','active_organizations','dcount_orgid')), None)
        ev_col2  = next((c for c in region_df.columns if c.lower() in ('events','event_count')), None)
        val_col2 = ao_col2 or ev_col2
        if g_col and val_col2:
            _rdf = region_df.copy()
            _rdf[val_col2] = pd.to_numeric(_rdf[val_col2], errors='coerce').fillna(0)
            if not _rdf.empty:
                top_row = _rdf.nlargest(1, val_col2).iloc[0]
                _raw_region = str(top_row[g_col])
                # Clean raw DB values like _NA_, N/A, null, None
                # Raw DB values like _NA_, N/A, null → show dash, not "Unknown"
                _clean_map  = {'_na_':'—','na':'—','n/a':'—',
                               'null':'—','none':'—','':'—',
                               '_none_':'—','_null_':'—'}
                _top_region = _clean_map.get(_raw_region.lower().strip(), _raw_region)
                _top_region_ev = int(top_row[val_col2])
                if _active_orgs == 0 and ao_col2:
                    _active_orgs = int(_rdf[ao_col2].sum())

    if not org_df.empty:
        ev_col3 = next((c for c in org_df.columns if c.lower() in ('events','event_count')), None)
        if ev_col3:
            _at_risk_n = int((pd.to_numeric(org_df[ev_col3], errors='coerce').fillna(0) < 100).sum())

    def _wow_parts(pct):
        """Return (icon, label, color) for a WoW percentage."""
        if pct > 1:
            return "↑", f"+{pct:.1f}% vs prev week", "#10B981"
        elif pct < -1:
            return "↓", f"{pct:.1f}% vs prev week", "#EF4444"
        else:
            return "→", "stable week-on-week", "#94A3B8"

    def _mcard(col, label, value, sub1, sub1_color, sub2=None, sub2_color="#94A3B8", accent=None):
        """
        Clean metric card — no sparklines, just numbers and signals.
        accent: left border color (green/red/blue/none)
        """
        border_left = f"4px solid {accent}" if accent else "1px solid #E2E8F0"
        with col:
            st.markdown(
                f'<div style="background:#FFFFFF;border:1px solid #E2E8F0;'
                f'border-left:{border_left};border-radius:12px;'
                f'padding:18px 20px 16px;box-shadow:0 1px 4px rgba(0,0,0,0.04);min-height:110px;">'
                # Label
                f'<div style="font-size:10px;font-weight:700;text-transform:uppercase;'
                f'letter-spacing:0.9px;color:#94A3B8;margin-bottom:10px;">{label}</div>'
                # Big value
                f'<div style="font-size:30px;font-weight:800;color:#0F172A;'
                f'letter-spacing:-0.5px;line-height:1;margin-bottom:10px;">{value}</div>'
                # Primary sub-label (WoW or context)
                f'<div style="font-size:12px;font-weight:600;color:{sub1_color};'
                f'margin-bottom:{"4px" if sub2 else "0"};">{sub1}</div>'
                # Optional secondary sub-label
                + (f'<div style="font-size:11px;color:{sub2_color};">{sub2}</div>' if sub2 else '')
                + f'</div>',
                unsafe_allow_html=True
            )

    org_icon, org_label, org_color = _wow_parts(_org_wow_pct)
    ev_icon,  ev_label,  ev_color  = _wow_parts(_ev_wow_pct)

    # Orgs to Watch accent: red if any at risk, green if all clear
    watch_accent = "#EF4444" if _at_risk_n > 0 else "#10B981"
    watch_val    = str(_at_risk_n) if _at_risk_n else "None"
    watch_sub    = f"{_at_risk_n} orgs below 100 events" if _at_risk_n else "All orgs healthy"
    watch_color  = "#EF4444" if _at_risk_n else "#10B981"

    mc1, mc2, mc3, mc4 = st.columns(4)

    _mcard(mc1,
           label="Active Orgs",
           value=_fmt(_active_orgs),
           sub1=f"{org_icon} {org_label}",
           sub1_color=org_color,
           accent=org_color if abs(_org_wow_pct) > 1 else None)

    _mcard(mc2,
           label="Events · Last 7 Days",
           value=_fmt(_total_events),
           sub1=f"{ev_icon} {ev_label}",
           sub1_color=ev_color,
           accent=ev_color if abs(_ev_wow_pct) > 1 else None)

    _mcard(mc3,
           label="Most Active Region",
           value=_top_region,
           sub1=f"{_fmt(_top_region_ev)} events" if _top_region_ev else "no region data",
           sub1_color="#2563EB",
           sub2="14-day total",
           sub2_color="#94A3B8")

    _mcard(mc4,
           label="Orgs to Watch",
           value=watch_val,
           sub1=watch_sub,
           sub1_color=watch_color,
           accent=watch_accent)

    st.markdown("<div style='margin:14px 0 4px;'></div>", unsafe_allow_html=True)

    # ─── 3. TREND CHART (full width, white card) ────────────────────────────
    t_col   = next((col for col in daily_df.columns if col.lower() in ('day','date','eventinfo_time')), None) if not daily_df.empty else None
    ao_col  = next((col for col in daily_df.columns if col.lower() in ('active_orgs','active_organizations','dcount_orgid')), None) if not daily_df.empty else None
    ev_col  = next((col for col in daily_df.columns if col.lower() in ('events','event_count')), None) if not daily_df.empty else None
    val_col = ao_col or ev_col
    tlabel  = "Active Orgs" if ao_col else "Events"

    if t_col and val_col and not daily_df.empty:
        df_p = daily_df.copy()
        df_p[val_col] = pd.to_numeric(df_p[val_col], errors='coerce').fillna(0)
        df_p[t_col]   = _safe_parse_dates(df_p[t_col])
        df_p = df_p.dropna(subset=[t_col])
        # Never show future dates — they are zero-filled placeholders
        _today = pd.Timestamp.now().normalize()
        df_p = df_p[(df_p[t_col] >= (_today - pd.DateOffset(days=30))) &
                    (df_p[t_col] <= _today)].sort_values(t_col)

        if not df_p.empty and (df_p[t_col].max() - df_p[t_col].min()).days <= 30:
            roll = df_p[val_col].rolling(3, min_periods=1).mean()
            fig_t = go.Figure()
            fig_t.add_trace(go.Scatter(
                x=df_p[t_col], y=df_p[val_col], mode='lines',
                line=dict(color='#BFDBFE', width=1),
                fill='tozeroy', fillcolor='rgba(37,99,235,0.07)',
                showlegend=False,
                hovertemplate='%{x|%b %d}: %{y:,.0f}<extra></extra>',
            ))
            fig_t.add_trace(go.Scatter(
                x=df_p[t_col], y=roll, mode='lines',
                line=dict(color='#2563EB', width=2.5),
                showlegend=False,
                hovertemplate='%{x|%b %d} avg: %{y:,.0f}<extra></extra>',
            ))
            fig_t.update_layout(
                height=190, margin=dict(l=0,r=0,t=4,b=0),
                paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                xaxis=dict(showgrid=False, tickformat='%b %d', type='date', nticks=7,
                           tickfont=dict(size=11, color='#94A3B8'), showline=False),
                yaxis=dict(showgrid=True, gridcolor='#F8FAFC', gridwidth=1,
                           tickfont=dict(size=11, color='#94A3B8'),
                           zeroline=False, rangemode='tozero'),
            )
            st.markdown(
                '<div style="background:#FFFFFF;border:1px solid #E2E8F0;border-radius:12px;'
                'padding:16px 18px 6px;box-shadow:0 1px 4px rgba(0,0,0,0.05);margin-bottom:12px;">'
                '<div style="font-size:10px;font-weight:700;text-transform:uppercase;'
                'letter-spacing:0.8px;color:#94A3B8;margin-bottom:10px;">Activity Trend</div>',
                unsafe_allow_html=True
            )
            st.plotly_chart(fig_t, use_container_width=True, key="dash_trend",
                            config={'displayModeBar': False})
            st.markdown(
                f'<div style="font-size:11px;color:#94A3B8;padding:0 0 12px 2px;">'
                f'{tlabel} · 3-day rolling avg</div></div>',
                unsafe_allow_html=True
            )

    # ─── 4. REGIONS + ORG WATCH ─────────────────────────────────────────────
    col_reg, col_watch = st.columns([1, 1])

    with col_reg:
        st.markdown(
            '<div style="background:#FFFFFF;border:1px solid #E2E8F0;border-radius:12px;'
            'padding:16px 18px;box-shadow:0 1px 4px rgba(0,0,0,0.05);">'
            '<div style="font-size:10px;font-weight:700;text-transform:uppercase;'
            'letter-spacing:0.8px;color:#94A3B8;margin-bottom:12px;">Regions</div>',
            unsafe_allow_html=True
        )
        if not region_df.empty:
            g_col   = next((col for col in region_df.columns if col.lower() in ('geoname','geo','region','geography')), None)
            ao_col2 = next((col for col in region_df.columns if col.lower() in ('active_orgs','active_organizations','dcount_orgid')), None)
            ev_col2 = next((col for col in region_df.columns if col.lower() in ('events','event_count')), None)
            vc = ao_col2 or ev_col2
            if g_col and vc:
                df_r  = region_df.copy()
                df_r[vc] = pd.to_numeric(df_r[vc], errors='coerce').fillna(0)
                df_r  = df_r[df_r[g_col].notna()].sort_values(vc, ascending=False).head(7)
                total = df_r[vc].sum()
                clrs  = ['#1D4ED8','#2563EB','#3B82F6','#60A5FA','#93C5FD','#BFDBFE','#DBEAFE']
                html  = ""
                for i, (_, row) in enumerate(df_r.iterrows()):
                    nm  = str(row[g_col])
                    if nm.lower() in ('nan','none',''): continue
                    v   = float(row[vc])
                    pct = v / total * 100 if total > 0 else 0
                    bar = max(4, int(pct * 0.9))
                    html += (
                        f'<div style="display:flex;align-items:center;gap:10px;margin-bottom:10px;">'
                        f'<div style="width:44px;font-size:12px;font-weight:600;color:#334155;white-space:nowrap;">{nm[:6]}</div>'
                        f'<div style="flex:1;background:#F1F5F9;border-radius:4px;height:7px;">'
                        f'<div style="background:{clrs[i%len(clrs)]};border-radius:4px;height:7px;width:{bar}%;"></div></div>'
                        f'<div style="font-size:12px;color:#64748B;font-weight:600;width:32px;text-align:right;">{pct:.0f}%</div>'
                        f'</div>'
                    )
                st.markdown(html + "</div>", unsafe_allow_html=True)
        else:
            st.markdown('<div style="color:#94A3B8;font-size:13px;">Loading…</div></div>', unsafe_allow_html=True)

    with col_watch:
        st.markdown(
            '<div style="background:#FFFFFF;border:1px solid #FEE2E2;border-radius:12px;'
            'padding:16px 18px;box-shadow:0 1px 4px rgba(0,0,0,0.05);">'
            '<div style="font-size:10px;font-weight:700;text-transform:uppercase;'
            'letter-spacing:0.8px;color:#EF4444;margin-bottom:12px;">⚠ Orgs to Watch</div>',
            unsafe_allow_html=True
        )
        if not org_df.empty:
            id_col  = next((col for col in org_df.columns if col.lower() in ('orgid','org_id','organizationid')), None)
            ev_col3 = next((col for col in org_df.columns if col.lower() in ('events','event_count')), None)
            if id_col and ev_col3:
                df_o = org_df.copy()
                df_o[ev_col3] = pd.to_numeric(df_o[ev_col3], errors='coerce').fillna(0)
                enricher = (st.session_state.ai_orchestrator.enricher
                            if st.session_state.ai_orchestrator else None)
                html = ""
                for i, (_, row) in enumerate(df_o.head(6).iterrows()):
                    oid  = str(row[id_col])
                    name = oid
                    if enricher and enricher.is_loaded:
                        try:
                            r = enricher.resolve(oid)
                            if r and r.org_name: name = r.display_name
                        except Exception: pass
                    if name == oid and len(oid) > 18: name = oid[:15] + "…"
                    ev  = float(row[ev_col3])
                    sev = "#EF4444" if i <= 1 else ("#F97316" if i <= 3 else "#F59E0B")
                    html += (
                        f'<div style="display:flex;align-items:center;gap:10px;'
                        f'padding:8px 10px;background:#FFF7F7;border-radius:8px;margin-bottom:6px;">'
                        f'<div style="width:6px;height:6px;border-radius:50%;background:{sev};flex-shrink:0;"></div>'
                        f'<div style="font-size:13px;color:#1E293B;font-weight:500;flex:1;'
                        f'white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">{name}</div>'
                        f'<div style="font-size:12px;color:{sev};font-weight:700;font-variant-numeric:tabular-nums;">{_fmt(ev)}</div>'
                        f'</div>'
                    )
                st.markdown(
                    html + '<div style="font-size:10px;color:#94A3B8;margin-top:8px;">Lowest activity · bottom 10</div></div>',
                    unsafe_allow_html=True
                )
        else:
            st.markdown('<div style="color:#94A3B8;font-size:13px;">Building profile…</div></div>', unsafe_allow_html=True)

    st.markdown("<div style='margin:18px 0 8px;'></div>", unsafe_allow_html=True)

    # ─── 5. CHIPS ────────────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        if st.button("📈  How are we trending?", use_container_width=True, key="chip_trend"):
            st.session_state.pending_suggestion = "__chip_trending__"
            st.rerun()
    with c2:
        if st.button("🏆  Top customers", use_container_width=True, key="chip_top"):
            st.session_state.pending_suggestion = "__chip_top_customers__"
            st.rerun()
    with c3:
        if st.button("⚠️  Bottom 10", use_container_width=True, key="chip_bottom"):
            st.session_state.pending_suggestion = "__chip_bottom_customers__"
            st.rerun()
    with c4:
        if st.button("📋  Manager summary", use_container_width=True, key="chip_summary"):
            st.session_state.pending_suggestion = "__chip_manager_summary__"
            st.rerun()

def _render(msg, msg_idx=0, lightweight=False):
    """Render a single chat message."""
    role = msg.get('role', '')
    ts   = msg.get('ts', 0)

    if role == 'user':
        st.markdown(
            f'<div class="user-bubble">{msg["content"]}</div>'
            f'<div class="clearfix"></div>',
            unsafe_allow_html=True)
        return

    # ── Text answer ───────────────────────────────────────────────────────────
    content = msg.get('content', '')
    if content:
        elapsed    = msg.get('elapsed')
        scope      = msg.get('data_scope', 'Last 14 days')
        timing_html = (f'<span class="timing-subtle"> · {elapsed:.1f}s</span>'
                       if elapsed and elapsed > 1 else '')
        scope_html  = (f'<span class="scope-pill">📅 {scope}</span>' if scope else '')

        # Convert basic markdown to HTML
        import re as _re
        _c = content
        _c = _re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', _c)
        _c = _re.sub(r'(?<!\*)\*([^*]+?)\*(?!\*)', r'<em>\1</em>', _c)
        _c = _c.replace('\n', '<br>').replace("\n", "<br>")
        content_html = f'<p style="margin:0;">{_c}</p>'

        st.markdown(
            f'<div class="bot-message">{content_html}{timing_html}{scope_html}</div>',
            unsafe_allow_html=True)

    # ── Error ─────────────────────────────────────────────────────────────────
    err = msg.get('error')
    if err:
        st.error(err)
        for s in (msg.get('suggestions') or []):
            inv, sugg = s.get('invalid', ''), s.get('suggestions', [])
            if sugg:
                st.info(f"'{inv}' → Try: {', '.join(sugg)}")
        return

    # ── Chip: trending line chart ─────────────────────────────────────────────
    chart_data = msg.get('chart_data')
    intent     = msg.get('intent', '')

    if chart_data and intent == 'chip_trending':
        try:
            xs   = chart_data.get('x', [])
            ys   = [float(v) for v in chart_data.get('y', [])]
            lbl  = chart_data.get('label', 'Active Orgs')
            roll = pd.Series(ys).rolling(3, min_periods=1).mean().tolist()
            fig  = go.Figure()
            fig.add_trace(go.Scatter(x=xs, y=ys, mode='lines',
                line=dict(color='#E2E8F0', width=1),
                fill='tozeroy', fillcolor='rgba(37,99,235,0.06)',
                showlegend=False))
            fig.add_trace(go.Scatter(x=xs, y=roll, mode='lines',
                line=dict(color='#2563EB', width=2.5), showlegend=False))
            fig.update_layout(height=220, margin=dict(l=0,r=0,t=8,b=0),
                paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                xaxis=dict(showgrid=False, tickfont=dict(size=11, color='#94A3B8')),
                yaxis=dict(showgrid=True, gridcolor='#F1F5F9',
                           tickfont=dict(size=11, color='#94A3B8'), zeroline=False))
            st.plotly_chart(fig, use_container_width=True,
                            key=f'chip_trend_{ts}_{msg_idx}')
        except Exception:
            pass

    # ── Chip: top customers bars ──────────────────────────────────────────────
    if chart_data and intent == 'chip_top_customers':
        try:
            names  = chart_data.get('names', [])
            values = [float(v) for v in chart_data.get('values', [])]
            pcts   = chart_data.get('pcts', [])
            labels = chart_data.get('events', [str(v) for v in values])
            medals = {0:'🥇',1:'🥈',2:'🥉'}
            html   = ''
            for i, (name, pct) in enumerate(zip(names, pcts)):
                bar_w = max(2, int(pct * 0.85))
                html += (
                    f'<div style="display:flex;align-items:center;gap:12px;margin-bottom:10px;">'
                    f'<div style="width:22px;font-size:14px;">{medals.get(i,"")}</div>'
                    f'<div style="width:140px;font-size:13px;font-weight:500;color:#0F172A;'
                    f'white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">{name}</div>'
                    f'<div style="flex:1;background:#F1F5F9;border-radius:4px;height:8px;">'
                    f'<div style="background:#2563EB;border-radius:4px;height:8px;width:{bar_w}%;"></div></div>'
                    f'<div style="font-family:monospace;font-size:12px;color:#64748B;width:60px;'
                    f'text-align:right;">{labels[i] if i<len(labels) else ""}</div>'
                    f'<div style="font-size:11px;color:#94A3B8;width:36px;text-align:right;">'
                    f'{pct:.0f}%</div></div>'
                )
            st.markdown(html, unsafe_allow_html=True)
        except Exception:
            pass

    # ── Chip: bottom customers table ──────────────────────────────────────────
    table_rows = msg.get('table_rows')
    if table_rows and intent == 'chip_bottom_customers':
        try:
            def _fn(n):
                n=float(n)
                if n>=1e9: return f"{n/1e9:.1f}B"
                if n>=1e6: return f"{n/1e6:.1f}M"
                if n>=1e3: return f"{n/1e3:.0f}K"
                return str(int(n))
            html = ('<div style="border:1px solid #E2E8F0;border-radius:10px;overflow:hidden;">'
                    '<div style="display:grid;grid-template-columns:1fr 80px 80px;'
                    'background:#F8FAFC;padding:8px 14px;font-size:11px;font-weight:600;'
                    'color:#64748B;text-transform:uppercase;letter-spacing:0.6px;">'
                    '<div>Organisation</div><div style="text-align:right;">Events</div>'
                    '<div style="text-align:right;">Last Active</div></div>')
            for i, row in enumerate(table_rows):
                bg = '#FFFFFF' if i%2==0 else '#F8FAFC'
                html += (
                    f'<div style="display:grid;grid-template-columns:1fr 80px 80px;'
                    f'background:{bg};padding:10px 14px;border-top:1px solid #F1F5F9;'
                    f'font-size:13px;align-items:center;">'
                    f'<div style="font-weight:500;color:#0F172A;">{row.get("name","")}</div>'
                    f'<div style="text-align:right;font-family:monospace;font-size:12px;'
                    f'color:#64748B;">{_fn(row.get("events",0))}</div>'
                    f'<div style="text-align:right;font-size:12px;color:#94A3B8;">'
                    f'{row.get("last_seen","—")}</div></div>'
                )
            html += '</div>'
            st.markdown(html, unsafe_allow_html=True)
        except Exception:
            pass

    # ── Chip: manager summary bullets ─────────────────────────────────────────
    if intent == 'chip_manager_summary' and content:
        try:
            lines = [l.strip().lstrip('•●-').strip()
                     for l in content.split("\n") if l.strip()]
            html  = '<div style="margin-top:4px;">'
            for line in lines:
                if line:
                    html += (
                        f'<div class="manager-bullet">'
                        f'<div class="bullet-dot"></div>'
                        f'<div>{line}</div></div>'
                    )
            html += '</div>'
            st.markdown(html, unsafe_allow_html=True)
        except Exception:
            pass

    # ── Legacy chart ──────────────────────────────────────────────────────────
    chart_dict = msg.get('chart')
    if chart_dict is not None:
        try:
            fig = go.Figure(chart_dict)
            fig.update_layout(margin=dict(l=0,r=0,t=20,b=0), height=300,
                paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
            if lightweight:
                with st.expander('📈 Chart', expanded=False):
                    st.plotly_chart(fig, use_container_width=True,
                                    key=f'ch_{ts}_{msg_idx}')
            else:
                st.plotly_chart(fig, use_container_width=True,
                                key=f'ch_{ts}_{msg_idx}')
        except Exception:
            pass

    # ── Data table ────────────────────────────────────────────────────────────
    df_dict = msg.get('dataframe')
    if df_dict is not None:
        try:
            df = pd.DataFrame(df_dict)
            if not df.empty and len(df) > 1:
                with st.expander(f'📊 Data — {len(df):,} rows', expanded=False):
                    st.dataframe(df, use_container_width=True,
                                 height=min(400, 40 + len(df)*35))
        except Exception:
            pass

    # ── KQL ───────────────────────────────────────────────────────────────────
    kql = msg.get('kql')
    if kql and not lightweight:
        with st.expander('📋 KQL — copy & run in Azure Data Explorer', expanded=False):
            st.code(kql, language='sql')

    # ── Follow-up chips ───────────────────────────────────────────────────────
    if not lightweight:
        follow_ups = msg.get('follow_up_suggestions', [])
        if follow_ups:
            _chip_labels = {
                '__chip_trending__':         '📈 How are we trending?',
                '__chip_top_customers__':    '🏆 Top customers',
                '__chip_bottom_customers__': '⚠️ Bottom 10',
                '__chip_manager_summary__':  '📋 Manager summary',
            }
            cols = st.columns(len(follow_ups) + 1)
            for i, sug in enumerate(follow_ups):
                with cols[i]:
                    label = _chip_labels.get(sug, f'💡 {sug}')
                    if st.button(label, key=f'sug_{msg_idx}_{i}_{ts}',
                                 use_container_width=True):
                        st.session_state.pending_suggestion = sug
                        st.rerun()


def _process_question(question: str):
    """Process a question with streaming and progressive rendering."""
    if not st.session_state.connected or not st.session_state.ai_orchestrator:
        st.error("Not connected — please connect first.")
        return

    orch = st.session_state.ai_orchestrator

    # Save & render user message
    _chip_labels = {
        '__chip_trending__':         '📈 How are we trending?',
        '__chip_top_customers__':    '🏆 Top customers',
        '__chip_bottom_customers__': '⚠️ Bottom 10',
        '__chip_manager_summary__':  '📋 Manager summary',
    }
    user_msg = {"role": "user", "content": _chip_labels.get(question, question), "ts": time.time()}
    st.session_state.messages.append(user_msg)
    _render(user_msg)

    try:
        st.session_state.rate_limiter.check_question_limit("default_user")
        t0 = time.time()

        # ══════════════════════════════════════════════════════════════
        # PATH 1A: HEALTH ANALYSIS (instant Plotly chart, zero LLM)
        # PATH 1B: STREAMING NARRATIVE (LLM token-by-token)
        # ══════════════════════════════════════════════════════════════
        stream_prep = orch.prepare_analysis_stream(question)

        if stream_prep and stream_prep.get('mode') == 'health':
            # ── HEALTH MODE: findings text first, chart below ──
            elapsed = time.time() - t0

            # Text findings first
            msg_text = stream_prep.get('message', '')
            if msg_text:
                timing = f'<span class="timing-subtle"> · {elapsed:.1f}s</span>' if elapsed and elapsed > 1 else ""
                st.markdown(f'<div class="bot-message">{_md_to_html(msg_text)}{timing}</div>', unsafe_allow_html=True)

            # Chart below
            viz = stream_prep.get('viz')
            chart_dict = None
            if viz and viz.get('chart') is not None:
                chart_dict = viz['chart'].to_dict()
                fig = go.Figure(chart_dict)
                fig.update_layout(
                    margin=dict(l=0, r=0, t=20, b=0),
                    height=300,
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(fig, use_container_width=True, key=f"ch_health_{t0}")

            follow_ups = ["Show the trend", "Top 10 orgs", "Events by region"]
            df = stream_prep.get('df')
            a = {
                "role": "assistant", "ts": time.time(),
                "intent": "analyze_cache", "content": msg_text,
                "chart": chart_dict,
                "dataframe": df.to_dict(orient="list") if df is not None and not df.empty and len(df) > 1 else None,
                "kql": None, "source": "cache", "elapsed": elapsed,
                "error": None, "suggestions": None,
                "follow_up_suggestions": follow_ups,
                "user_question": question,
            }
            st.session_state.messages.append(a)

            msg_idx = len(st.session_state.messages) - 1
            cols = st.columns(len(follow_ups) + 1)
            for i, suggestion in enumerate(follow_ups):
                with cols[i]:
                    _chip_labels_1 = {'__chip_trending__':'📈 How are we trending?','__chip_top_customers__':'🏆 Top customers','__chip_bottom_customers__':'⚠️ Bottom 10','__chip_manager_summary__':'📋 Manager summary'}
                    _lbl1 = _chip_labels_1.get(suggestion, f'💡 {suggestion}')
                    if st.button(_lbl1, key=f"sug_{msg_idx}_{i}_{t0}",
                                 use_container_width=True):
                        st.session_state.pending_suggestion = suggestion
                        st.rerun()
            return

        # ── EXECUTIVE BRIEFING MODE ─────────────────────────────────────────
        if stream_prep and stream_prep.get('intent') == 'executive_briefing':
            elapsed = time.time() - t0
            exec_html = stream_prep.get('executive_briefing_html', '')
            summary_text = stream_prep.get('content', '')

            # Render the cards immediately
            if exec_html:
                try:
                    st.html(exec_html)
                except AttributeError:
                    st.markdown(exec_html, unsafe_allow_html=True)

            follow_ups = ["Show the trend", "Top 10 orgs", "Events by region"]
            a = {
                "role": "assistant", "ts": time.time(),
                "intent": "executive_briefing",
                "content": summary_text,
                "executive_briefing_html": exec_html,
                "chart": None, "dataframe": None,
                "kql": None, "source": "profile",
                "elapsed": elapsed, "error": None, "suggestions": None,
                "follow_up_suggestions": follow_ups,
                "user_question": question,
            }
            st.session_state.messages.append(a)
            msg_idx = len(st.session_state.messages) - 1
            cols = st.columns(len(follow_ups) + 1)
            for i, suggestion in enumerate(follow_ups):
                with cols[i]:
                    if st.button(suggestion, key=f"sug_exec_{msg_idx}_{i}_{t0}",
                                 use_container_width=True):
                        st.session_state.pending_suggestion = suggestion
                        st.rerun()
            return

        if stream_prep and stream_prep.get('mode') == 'narrative':
            # ── NARRATIVE MODE: text streams first, chart appears after ──

            # Stream text FIRST — user reads the answer as it comes in
            text_placeholder = st.empty()
            accumulated_text = ""
            for chunk in stream_prep['stream']:
                if chunk['type'] == 'token':
                    accumulated_text += chunk['text']
                    text_placeholder.markdown(
                        f'<div class="bot-message">{accumulated_text}</div>',
                        unsafe_allow_html=True
                    )
                elif chunk['type'] == 'done':
                    accumulated_text = chunk.get('full_text', accumulated_text)

            elapsed = time.time() - t0

            # Finalize text
            text_placeholder.markdown(
                f'<div class="bot-message">{accumulated_text}</div>',
                unsafe_allow_html=True
            )

            # Chart AFTER text — supporting evidence, not the lead
            viz = stream_prep.get('viz')
            chart_dict = None
            if viz and viz.get('chart') is not None:
                chart_dict = viz['chart'].to_dict()
                fig = go.Figure(chart_dict)
                fig.update_layout(
                    margin=dict(l=0, r=0, t=20, b=0),
                    height=300,
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(fig, use_container_width=True, key=f"ch_stream_{t0}")

            # Build suggestions
            follow_ups = (
                orch._build_suggestions(question, {'intent': 'analyze_cache'})
                if hasattr(orch, '_build_suggestions')
                else _get_suggestions(question)
            )

            # Save to history
            df = stream_prep.get('df')
            a = {
                "role": "assistant",
                "ts": time.time(),
                "intent": "analyze_cache",
                "content": accumulated_text,
                "chart": chart_dict,
                "dataframe": df.to_dict(orient="list") if df is not None and not df.empty and len(df) > 1 else None,
                "kql": None,
                "source": "streaming",
                "elapsed": elapsed,
                "error": None,
                "suggestions": None,
                "follow_up_suggestions": follow_ups,
                "user_question": question,
            }
            st.session_state.messages.append(a)

            # Render follow-up buttons
            msg_idx = len(st.session_state.messages) - 1
            if follow_ups:
                cols = st.columns(len(follow_ups) + 1)
                for i, suggestion in enumerate(follow_ups):
                    with cols[i]:
                        if st.button(
                            {'__chip_trending__':'📈 How are we trending?','__chip_top_customers__':'🏆 Top customers','__chip_bottom_customers__':'⚠️ Bottom 10','__chip_manager_summary__':'📋 Manager summary'}.get(suggestion, f'💡 {suggestion}'),
                            key=f"sug_{msg_idx}_{i}_{t0}",
                            use_container_width=True,
                        ):
                            st.session_state.pending_suggestion = suggestion
                            st.rerun()

            return

        # ══════════════════════════════════════════════════════════════
        # PATH 2: STANDARD (profile / kusto / fast router)
        # ══════════════════════════════════════════════════════════════

        with st.status("Working on it…", expanded=True) as status:
            response = orch.process_message(question)
            elapsed = time.time() - t0

            intent = response.get("intent", "")
            status.update(
                label=f"Done ({elapsed:.1f}s)",
                state="complete", expanded=False
            )

        # Build serialisable assistant message
        # Get data scope label from profile (e.g. "7-day snapshot")
        _profile = st.session_state.get("data_profile")
        _data_scope = _profile.get_data_scope_label() if _profile else ""

        a = {
            "role": "assistant",
            "ts": time.time(),
            "intent": intent,
            "content": "",
            "chart": None,
            "dataframe": None,
            "kql": response.get("kql"),
            "source": None,
            "elapsed": elapsed,
            "error": None,
            "suggestions": None,
            "follow_up_suggestions": [],
            "user_question": question,
            "data_scope": _data_scope,
        }

        rtype = response.get("response_type", "")

        # ── Chip responses: no response_type, identified by intent starting chip_ ──
        # Must be handled BEFORE rtype checks — they bypass the rtype system entirely.
        if response.get("intent", "").startswith("chip_"):
            a["content"]              = response.get("content", "")
            a["intent"]               = response["intent"]
            a["source"]               = response.get("source", "profile")
            a["kql"]                  = response.get("kql", "")
            if response.get("chart_data"):
                a["chart_data"]       = response["chart_data"]
            if response.get("table_rows"):
                a["table_rows"]       = response["table_rows"]
            if response.get("data_scope"):
                a["data_scope"]       = response["data_scope"]
            if response.get("follow_up_suggestions"):
                a["follow_up_suggestions"] = response["follow_up_suggestions"]

        elif rtype == "error":
            a["error"] = response.get("message", "Unknown error")
            a["suggestions"] = response.get("suggestions")

        elif rtype == "conversational":
            a["content"] = response.get("message", "")
            a["source"] = "cache"

        elif rtype in ("data", "analysis"):
            a["content"] = response.get("message", "")

            # Determine source
            if intent == "profile":
                a["source"] = "profile"
            elif intent == "complex_query":
                a["source"] = "planner"
            elif rtype == "data":
                a["source"] = "kusto"
            else:
                a["source"] = "cache"

            # Chart
            viz = response.get("visualization")
            if viz and viz.get("chart") is not None:
                a["chart"] = viz["chart"].to_dict()

            # DataFrame
            df = response.get("data")
            if df is not None:
                try:
                    if not df.empty:
                        a["dataframe"] = df.to_dict(orient="list")
                except Exception:
                    pass

            # KQL transparency — pass through from orchestrator if present
            kql_from_response = response.get("kql") or response.get("kql_snippet")
            if kql_from_response:
                a["kql"] = kql_from_response

            # ── Chip response fields — chart_data, table_rows, data_scope ──────
            # Chip handlers return these directly; PATH 2 must preserve them
            if response.get("chart_data"):
                a["chart_data"] = response["chart_data"]
            if response.get("table_rows"):
                a["table_rows"] = response["table_rows"]
            if response.get("data_scope"):
                a["data_scope"] = response["data_scope"]
            if response.get("follow_up_suggestions"):
                a["follow_up_suggestions"] = response["follow_up_suggestions"]
            # Chip intents — set directly from response intent
            if response.get("intent", "").startswith("chip_"):
                a["intent"] = response["intent"]

            # Scope label
            scope_from_response = response.get("scope_label", "")
            if scope_from_response:
                a["scope_label"] = scope_from_response

        # Conversational responses may also carry scope info
        if not a.get("scope_label"):
            bstatus = st.session_state.get("profile_build_status", "idle")
            src     = a.get("source", "")
            if src == "profile":
                a["scope_label"] = "Last 90 days" if bstatus == "done" else "7-day snapshot · full profile loading"
            elif src == "live_kusto":
                a["scope_label"] = "Live query"

        # Follow-up suggestions — chip tokens already set above, don't overwrite
        if not a.get("follow_up_suggestions"):
            a["follow_up_suggestions"] = (
                response.get("follow_up_suggestions")
                or _get_suggestions(question)
            )

        # Save & render
        msg_idx = len(st.session_state.messages)
        st.session_state.messages.append(a)
        _render(a, msg_idx=msg_idx)

    except RateLimitExceeded as e:
        st.error(f"⚠️ {e}")
        st.info(f"⏳ Wait {int(e.retry_after)}s")

    except Exception as e:
        import traceback
        err = {
            "role": "assistant", "ts": time.time(),
            "intent": "", "content": "", "chart": None,
            "dataframe": None, "kql": None, "source": None,
            "elapsed": None, "error": f"Error: {e}",
            "suggestions": None, "follow_up_suggestions": [],
            "user_question": question,
        }
        st.session_state.messages.append(err)
        _render(err)
        with st.expander("Traceback"):
            st.code(traceback.format_exc())


# ══════════════════════════════════════════════════════════════════════════════
# CHAT UI — isolated with st.fragment to kill double-rerun tax

def _md_to_html(text: str) -> str:
    """Convert markdown bold/italic to HTML so it renders inside unsafe_allow_html divs."""
    import re as _re
    if not text: return text
    text = _re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', text)
    text = _re.sub(r'\*(.+?)\*',       r'<em>\1</em>',         text)
    text = text.replace('\n\n', '<br><br>').replace('\n', '<br>')
    return text
# ══════════════════════════════════════════════════════════════════════════════

# Analyst dashboard — collapses to minimal header once user starts chatting
if st.session_state.messages:
    with st.expander("📊 Dashboard", expanded=False):
        _render_analyst_dashboard()
else:
    _render_analyst_dashboard()

_has_fragment = hasattr(st, 'fragment')

def _chat_ui():
    """The main chat UI — render history + handle input."""
    messages = st.session_state.messages
    total = len(messages)

    # Only render full charts/buttons for the last 4 messages (2 Q&A pairs)
    # Older messages get lightweight rendering (collapsed charts, no buttons)
    lightweight_cutoff = max(0, total - 4)

    for idx, m in enumerate(messages):
        _render(m, msg_idx=idx, lightweight=(idx < lightweight_cutoff))

    # Handle suggestion button clicks
    if st.session_state.pending_suggestion:
        suggestion = st.session_state.pending_suggestion
        st.session_state.pending_suggestion = None
        _process_question(suggestion)

    # Handle chat input
    question = st.chat_input("Ask a question or paste KQL…")
    if question:
        _process_question(question)


# Use st.fragment if available (Streamlit >= 1.33) — saves 200-400ms per interaction
if _has_fragment:
    @st.fragment
    def _chat_fragment():
        _chat_ui()
    _chat_fragment()
else:
    _chat_ui()
