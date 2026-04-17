# PULSE — Architecture

**PULSE** (Platform for Unified Live Signal Exploration) is a config-driven, AI-powered natural-language interface for Azure Data Explorer (Kusto). It connects to one or more Kusto clusters, builds local pre-aggregated caches in DuckDB, and answers natural-language questions with charts, insights, and the underlying KQL.

Two key design principles keep hallucination risk low:

1. **Config is the source of truth.** Cluster URLs, database and table names, metrics, dimensions, filters, and auth strategy all live in YAML. The LLM never invents them.
2. **LLM scope is narrow.** The model writes simple `where` / `summarize` clauses and natural-language narratives. It does not choose clusters, compose schemas, or decide aggregation strategies — the engine builds full queries from config plus the model's constrained output.

---

## 1. System Overview

```
┌───────────────────────────────────────────────────────────────┐
│                          CLIENT                               │
│                                                               │
│   Streamlit UI              OR          FastAPI + WebSocket   │
│   (ui/app.py)                           (api/app.py)          │
│   In-process state                      Bidirectional events  │
└──────────────────────────────┬────────────────────────────────┘
                               │
┌──────────────────────────────▼────────────────────────────────┐
│                       CORE ENGINE                             │
│                    (src/pulse/core/*)                         │
│                                                               │
│   AI Orchestrator ─ coordinates routing, planning, answer     │
│   Fast Router    ─ zero-LLM pattern matching for common Qs    │
│   Intent Router  ─ LLM-based classification for the rest      │
│   Query Planner  ─ KQL generation (constrained by config)     │
│   Semantic Layer ─ metrics / dimensions / profile definitions │
│   Data Profile   ─ DuckDB cache + background builds           │
│   Narrative /    ─ streaming natural-language answers         │
│     Insight Cards  color-coded findings, executive briefings  │
│   Visualizer     ─ Plotly chart selection + styling           │
│   Org Enrichment ─ GUID-to-friendly-name resolution           │
└──────────────────────────────┬────────────────────────────────┘
                               │
┌──────────────────────────────▼────────────────────────────────┐
│                    EXTERNAL SERVICES                          │
│                                                               │
│   Azure OpenAI (gpt-4o-mini by default)                       │
│     - Intent classification, KQL generation, narratives       │
│                                                               │
│   Kusto cluster(s) — read-only                                │
│     - Primary telemetry source                                │
│     - Multi-cluster union supported                           │
│                                                               │
│   Kusto cluster (org metadata) — optional                     │
│     - Enriches GUIDs with human-readable names                │
└───────────────────────────────────────────────────────────────┘
```

---

## 2. Request Flow

A user asks **"Show me errors from yesterday."**

```
1. Fast Router inspects the question
      ├── match on known pattern? ──► return cached/templated answer in ~3-50ms
      └── no match ──► continue

2. Intent Router classifies intent via LLM
      (e.g. "time-series query on errors, last 1 day")

3. Query Planner assembles context from config
      - Which clusters to hit (EU-West, US-Central, ...)
      - Which table, time column, mandatory filters
      - Which metric ("errors" maps to a config-defined KQL fragment)

4. LLM fills the narrow gap: the WHERE clause or SUMMARIZE group-by

5. Engine composes the full KQL and executes it:
      - If data is already in the DuckDB cache → query DuckDB (3-50 ms)
      - Otherwise query Kusto, load result into DuckDB, answer

6. Narrative Engine streams a natural-language response

7. Visualizer picks chart type (line / bar / hbar / pie / grouped)
      and returns Plotly JSON

8. Insight Cards surface anomalies, top movers, health status
```

On follow-up questions ("…and break that down by region?"), the Predictive Cache and DuckDB often answer without ever re-contacting Kusto.

---

## 3. Data & Caching

```
    ┌──────────────────┐
    │   KUSTO CLUSTER  │
    │                  │
    └────────┬─────────┘
             │  Narrow, pre-aggregated queries
             ▼
    ┌──────────────────┐
    │ ConfigDrivenKusto│  Connect, auth, multi-cluster union
    │      Client      │  Schema discovery, token refresh
    └────────┬─────────┘
             │  Pandas DataFrames
             ▼
    ┌──────────────────┐
    │    Data Profile  │  "Instant dashboard" = 7-day snapshot
    │                  │  Background build = 30 / 90 / 180 day scopes
    │                  │  Disk cache = Parquet per scope
    └────────┬─────────┘
             │  Load into in-memory tables
             ▼
    ┌──────────────────┐
    │  DuckDB Engine   │  Tables: profile_daily, profile_region,
    │                  │          profile_organization, profile_totals,
    │                  │          profile_entity, profile_browser, ...
    │                  │  SQL follow-up queries: 3-50 ms
    └──────────────────┘
```

The split keeps Kusto costs bounded (few narrow queries per connect) while follow-up interactivity stays sub-100 ms against the local cache.

---

## 4. Config-Driven Core

Everything structural lives in `configs/example.yaml`. A minimal config looks like:

```yaml
metadata:
  id: "example-app-telemetry"
  name: "Example App-Level Monitoring"
  owner: "team@example.com"
  version: "1.0.0"

clusters:
  - name: "EU-West"
    url: "https://example-eu.kusto.windows.net"
    database: "ExampleTelemetryDB"
    table: "app_telemetry"
    region: "EU"
  - name: "US-Central"
    url: "https://example-us.kusto.windows.net"
    database: "ExampleTelemetryDB"
    table: "app_telemetry"
    region: "US"

strategy: "union"

authentication:
  method: "azure_cli"   # or managed_identity, service_principal

metrics:
  events:
    display_name: "Events"
    kql: "count()"
  active_orgs:
    display_name: "Active Organizations"
    kql: "dcount(OrgId)"

dimensions:
  # ...
```

Adding a second team / data source is copy-and-edit; no code changes. A separate `example_context.yaml` sibling defines business context (baselines, KPI targets, seasonal adjustments) that the LLM reads when producing narratives.

---

## 5. Module Map

```
src/pulse/
├── api/
│   └── app.py                     FastAPI server + WebSocket
├── ui/
│   └── app.py                     Streamlit app (alternative client)
├── core/
│   ├── ai_orchestrator.py         Coordinator — the brain
│   ├── fast_router.py             Pattern matching (zero LLM)
│   ├── fast_router_v2.py          Successor router with better coverage
│   ├── intent_router.py           LLM-based intent classification
│   ├── query_planner.py           KQL generation from config + intent
│   ├── kusto_client.py            Multi-cluster Kusto execution
│   ├── kusto_handlers.py          Kusto result → DataFrame glue
│   ├── kql_detector.py            Detects when user pasted raw KQL
│   ├── duckdb_engine.py           Local DuckDB cache wrapper
│   ├── data_profile.py            Profile table manager (cache lifecycle)
│   ├── semantic_layer.py          Metric & dimension definitions
│   ├── schema_manager.py          Schema discovery from Kusto
│   ├── schema_validator.py        YAML config validation
│   ├── config_loader.py           YAML → typed Config objects
│   ├── config_generator.py        Scaffolding helper for new configs
│   ├── business_context.py        Baselines / KPIs / seasonal adjustments
│   ├── business_overview.py       High-level summary generation
│   ├── context_manager.py         Conversation context
│   ├── memory_store.py            Conversation history
│   ├── predictive_cache.py        Pre-compute likely follow-ups
│   ├── analysis_engine.py         Health + anomaly analysis
│   ├── anomaly_drill.py           Anomaly deep-dive
│   ├── smart_insights.py          Automated insight generation
│   ├── narrative_engine.py        Streaming text generation
│   ├── analyst_opening.py         Welcome / greeting message
│   ├── analyst_voice.py           Tone & personality
│   ├── compound_analyst.py        Multi-step analysis
│   ├── executive_briefing.py      Executive-level summaries
│   ├── insight_cards.py           Color-coded finding cards
│   ├── visualizer.py              Plotly chart selection & styling
│   ├── chip_handlers.py           Follow-up chip-button generation
│   ├── org_enrichment.py          GUID → friendly-name resolution
│   ├── profile_handler.py         Profile-scoped question routing
│   ├── scope_handler.py           Time-scope handling
│   ├── question_classifier.py     Kusto-vs-profile classifier
│   ├── auth_manager.py            Azure auth (CLI / MI / SPN)
│   ├── llm_service.py             Azure OpenAI wrapper
│   └── rate_limiter.py            Per-user rate limits
├── cache_service.py               Standalone pre-warm cache runner
└── utils/
    ├── config.py                  App-level AppConfig
    └── logger.py                  Logging setup

configs/
├── example.yaml                   Data source + metrics + dimensions
└── example_context.yaml           Business context for narratives

system_defaults.yaml               Default columns available to all configs
```

---

## 6. Two Server Options

PULSE ships with two front-end server options sharing the same core engine:

| Aspect                      | Streamlit (`ui/app.py`)             | FastAPI (`api/app.py`)           |
|-----------------------------|-------------------------------------|----------------------------------|
| Client delivery             | Server-rendered, in-process state   | WebSocket + static HTML / SPA    |
| Server → client push        | Polling only                        | Native (data_upgraded events)    |
| Multi-client                | Session per browser tab             | Shared sessions                  |
| Reusable from other clients | No (UI-coupled)                     | Yes (any WS or HTTP client)      |
| Best for                    | Quick demos, internal tools         | Production, multi-user, SPAs     |

Both consume the same `src/pulse/core/*` engine. Adding a third client (CLI, Slack bot, MCP server) is a matter of translating WebSocket events to the new transport — no engine changes needed.

---

## 7. Extending PULSE

**Add a new data source (same Kusto dialect):** copy `configs/example.yaml` to `configs/your-team.yaml`, swap cluster URL / database / table / metrics. No code changes.

**Add a new metric or dimension:** edit the `metrics:` or `dimensions:` block in your config. The semantic layer picks it up automatically and the LLM is told about it through generated context.

**Add a new backend (e.g., another SQL engine):** the architecture supports it but the current code assumes Kusto. The path of least resistance is to implement a `ConfigDrivenXClient` sibling to `ConfigDrivenKustoClient` with the same public interface (`connect_all_clusters`, `execute`, `discover_schema`), and route to it based on a config field. The LLM prompt also needs a dialect-appropriate variant.

**Add a new client (CLI, Slack, MCP):** consume the FastAPI WebSocket protocol. Message types are listed in `api/app.py` (connect, question, chip, answer, chart, data_table, kql, suggestions, stream_start/token/end, data_upgraded, error).

---

## 8. Security Notes for Self-Hosters

- The repo ships `.env.example` only. Put your real Azure OpenAI key in a local `.env` (gitignored).
- Kusto auth defaults to `azure_cli` for local dev. For production, switch `authentication.method` in your config to `managed_identity` or `service_principal` and provide credentials via environment variables.
- Per-user rate limits (10/min, 100/hr, 1000/day) are enforced in `rate_limiter.py`; tune for your deployment.
- All Kusto connections are read-only; no write operations are issued by any component.

---

## 9. Status

This repository is a **showcase / reference implementation**. It is not a hosted service and running it requires your own Azure Data Explorer cluster plus Azure OpenAI credentials. The core engine is production-grade; the surrounding deployment story (pre-warm cache service, polished single-page frontend, MCP server) is in progress.
