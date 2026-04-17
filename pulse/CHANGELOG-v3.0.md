# PULSE v3.0 — Profile-First Architecture

## What Changed

### Architecture (fundamental shift)
- **Before:** LLM writes raw KQL → Kusto every time → hope it works
- **After:** Config defines metrics+dimensions → auto-build profile on connect → LLM picks from menu → instant answers from DuckDB

### One Config Per Team
- **Before:** 2 files (`example.yaml` + `semantic_model.yaml`)
- **After:** 1 file (`example.yaml`) — cluster info + metrics + dimensions all in one place
- New teams onboard by writing ONE YAML file

### Three-Phase Connect
```
Phase 1: Connect to clusters + discover schema (.show table schema)
Phase 2: Load semantic model from config + validate against discovered schema
Phase 3: Auto-generate profile queries from metrics × dimensions → execute → DuckDB
```

### Auto-Generated Profile Queries
- **Before:** 7 hand-written KQL queries in YAML
- **After:** System auto-generates from config: 5 metrics × 7 dimensions = 14 optimized queries
  - 1 totals query
  - 1 daily time series
  - 6 per-dimension queries (org, region, entity, browser, country, app_module)
  - 5 org × dimension cross-tabs
  - 1 region × entity cross-tab
- Add a new dimension to config → profile queries auto-update

### Query Flow
```
Question → Semantic Layer resolves → SQL on profile → instant (<500ms)
                                   ↘ if profile can't answer → KQL on Kusto (fallback)
```

### Schema Discovery
- On connect: runs `.show table schema` against the cluster
- Validates that config metrics/dimensions reference real columns
- Prunes profile queries that reference missing columns

## Files Changed

| File | Change |
|------|--------|
| `configs/example.yaml` | Merged config (cluster + metrics + dimensions) |
| `configs/semantic_model.yaml` | **REMOVED** (merged into example.yaml) |
| `src/pulse/core/semantic_layer.py` | Rewritten: loads from merged config, auto-generates profile queries |
| `src/pulse/core/config_generator.py` | **NEW**: Auto-discovers schema and generates complete config |
| `src/pulse/core/kusto_client.py` | Added `discover_schema()` method |
| `src/pulse/core/config_loader.py` | Added `source_path` tracking |
| `src/pulse/ui/app.py` | Three-phase connect + Setup Wizard in sidebar |

## Setup Wizard (New Team Onboarding)

New sidebar section: **"➕ Setup New Data Source"**

Team provides 3 things:
- Cluster URL
- Database name
- Table name

PULSE auto-discovers everything else:
1. Connects to cluster
2. Runs `.show table schema` → discovers all columns + types
3. Runs `dcount()` on string/guid columns → measures cardinality
4. Fetches sample values for low-cardinality columns
5. Classifies each column:
   - DateTime → time dimension
   - High-cardinality strings (OrgId, UserId) → `dcount()` metrics
   - Low-cardinality strings (Region, Browser) → group-by dimensions
   - Internal columns (CorrelationId, IngestionTime) → skipped
6. Generates complete YAML config with metrics, dimensions, comments
7. Saves to `configs/` folder → ready to Connect

**What the generator produces:**
```
Input:  Cluster URL + Database + Table (10 seconds of input)
Output: Complete config with:
  - 4 auto-detected metrics (events + dcount of ID columns)
  - 8 auto-detected dimensions (grouped by cardinality)
  - Comments showing ALL discovered columns with classification
  - Ready for semantic layer → auto-generates 14-18 profile queries
```

## Files Unchanged
- `ai_orchestrator.py` — already had profile-first routing (from scaffolding)
- `data_profile.py` — already had build + query (from scaffolding)
- `visualizer.py` — unchanged (with previous bug fixes)
- `intent_router.py`, `query_planner.py`, `analysis_engine.py` — unchanged
- `auth_manager.py`, `rate_limiter.py` — unchanged

## For New Team Onboarding

**Option A: Setup Wizard (recommended)**
1. Open PULSE sidebar → "➕ Setup New Data Source"
2. Enter Cluster URL, Database, Table
3. Click "Discover & Generate Config"
4. Review generated config, tweak if needed
5. Reload page → new data source appears in dropdown
6. Click Connect → done

**Option B: Manual config**
1. Copy `example.yaml` to `configs/new-team.yaml`
2. Update cluster URL, database, table
3. Define your metrics (what you count)
4. Define your dimensions (what you group by)
5. Click Connect → PULSE does the rest
