# PULSE Refactoring Plan — SOLID Principles
## Pre-Frontend Code Segregation

**Problem**: 3 god-files that will only get worse when we add frontend code.

| File | Lines | Violation | Responsibilities Crammed In |
|------|-------|-----------|-----------------------------|
| `ai_orchestrator.py` | **2,511** | Single Responsibility | Dispatch + Chips + Profile routing + KQL building + Enrichment + At-risk + Beyond-scope + Analysis streaming + Memory + Kusto handlers |
| `app.py` | **1,199** | Single Responsibility | FastAPI routes + WebSocket + Session mgmt + Connect flow (371 lines!) + Question flow (250 lines!) + Test page HTML (153 lines) |
| `visualizer.py` | **988** | Open/Closed | Every chart type hardcoded in one class, can't add new types without modifying |

---

## Proposed Extractions

### 1. `ai_orchestrator.py` (2,511 → ~800 core + 4 modules)

**Keep in orchestrator** (~800 lines): `__init__`, `process_message`, `_process_core`, `_has_profile`, `_build_suggestions`, `_error`, `_conversational`, `_friendly_error`. This becomes the **thin dispatcher** — its only job is routing.

**Extract → `chip_handlers.py`** (~400 lines)
```
handle_chip_trending()        79 lines
handle_chip_top_customers()   87 lines
handle_chip_bottom_customers() 94 lines
handle_chip_manager_summary() 140 lines
_chip_kql()                    53 lines
```
One class: `ChipHandler(data_profile, kusto_client, org_enrichment, context)`

**Extract → `profile_handler.py`** (~500 lines)
```
_try_profile_exhaustively()   55 lines
_execute_fast_result()       168 lines
_handle_fast_profile()        12 lines
_handle_profile()            150 lines
_build_live_kql_for_profile() 87 lines
_build_insight()              10 lines
_chart_title_for()            40 lines
```
One class: `ProfileHandler(data_profile, semantic_layer, fast_router, sql_validator, visualizer)`

**Extract → `kusto_handlers.py`** (~450 lines)
```
_handle_fetch()               20 lines
_handle_direct_kql()           8 lines
_handle_complex()             64 lines
_handle_analyze()             52 lines
_handle_refine()              51 lines
_validate_execute_visualize() 51 lines
_result_looks_wrong()         18 lines
_retry_kql()                  18 lines
prepare_analysis_stream()    174 lines
```
One class: `KustoHandler(kusto_client, llm_service, visualizer, query_planner, context)`

**Extract → `scope_handler.py`** (~200 lines)
```
_detect_beyond_scope()        37 lines
build_beyond_scope_response() 116 lines
_handle_at_risk_query()      184 lines  (or stays — it's domain-specific)
_build_at_risk_kql()          56 lines
_handle_health_analysis()     55 lines
_drop_investigator_kql()      22 lines
```
One class: `ScopeHandler(kusto_client, data_profile, config)`

**Result**: Orchestrator delegates to handlers. Each handler is testable in isolation.

```python
# ai_orchestrator.py — AFTER refactoring (~800 lines)
class AIOrchestrator:
    def __init__(self, ...):
        self.chip_handler = ChipHandler(...)
        self.profile_handler = ProfileHandler(...)
        self.kusto_handler = KustoHandler(...)
        self.scope_handler = ScopeHandler(...)

    def _process_core(self, message):
        # 1. Beyond scope?
        if days := self.scope_handler.detect_beyond_scope(message):
            return self.scope_handler.build_response(message, days)
        # 2. Profile?
        if result := self.profile_handler.try_exhaustively(message):
            return result
        # 3. Kusto fallback
        return self.kusto_handler.handle(message, intent)
```

---

### 2. `app.py` (1,199 → ~400 core + 3 modules)

**Extract → `ws_connect.py`** (~400 lines)
```
handle_connect()           371 lines
_lazy_kusto_connect()       44 lines
_background_build()         52 lines
_instant_from_cache()       20 lines
```

**Extract → `ws_question.py`** (~250 lines)
```
handle_question()          250 lines
```

**Extract → `test_page.py`** (~153 lines)
```
test_page()                153 lines  (HTML string → separate .html file)
```

**Keep in app.py** (~400 lines): FastAPI init, lifespan, routes, WebSocket loop, Session class, utilities.

---

### 3. `visualizer.py` (988 → ~300 core + chart plugins)

Not urgent but follows Open/Closed principle:

**Extract chart methods into individual files:**
```
charts/bar_chart.py       (~100 lines: _bar, _hbar, _grouped_bar, _stacked_bar)
charts/line_chart.py      (~250 lines: _line, _multi_line)
charts/pie_chart.py       (~60 lines: _pie)
charts/executive.py       (~50 lines: _executive, _auto_fallback)
charts/base.py            (~100 lines: _premium_layout, _has_time_col, _time_col, etc.)
```

**Keep in visualizer.py** (~300 lines): `analyze_and_visualize`, `_detect`, `_type_from_intent`, chart registration. New chart types → just add a file, don't touch visualizer core.

---

### 4. `data_profile.py` (871 lines) — Extract ReadWriteLock

**Extract → `rwlock.py`** (~60 lines)
```
class ReadWriteLock    (currently lines 63-122)
```
Reusable primitive, shouldn't live inside a domain class.

---

## File Tree: Before vs After

```
BEFORE (7 files, 20.8K lines):
  ai_orchestrator.py    2,511  ← god class
  app.py                1,199  ← god file
  visualizer.py           988
  data_profile.py         871
  semantic_layer.py       758
  fast_router.py          639
  ...

AFTER (~15 files, same total lines but nothing >800):
  core/
    ai_orchestrator.py      ~800  (thin dispatcher only)
    chip_handlers.py        ~400  (4 chip methods + KQL builder)
    profile_handler.py      ~500  (profile routing + execution)
    kusto_handlers.py       ~450  (Kusto query + analysis streaming)
    scope_handler.py        ~200  (beyond-scope + at-risk)
    rwlock.py                ~60  (ReadWriteLock primitive)
    data_profile.py         ~810  (minus rwlock)
    sql_validator.py         414
    semantic_layer.py        758
    fast_router.py           639
    fast_router_v2.py        411
    visualizer.py           ~300  (dispatcher only)
    charts/
      bar.py               ~100
      line.py              ~250
      pie.py                ~60
      executive.py          ~50
      base.py              ~100
    ...existing files unchanged...

  api/
    app.py                 ~400  (routes + WS loop only)
    ws_connect.py          ~400  (connect flow)
    ws_question.py         ~250  (question flow)

  templates/
    test_page.html          153  (extracted from app.py)
```

---

## Priority Order

| # | Extraction | Impact | Risk | Do Before FE? |
|---|-----------|--------|------|---------------|
| 1 | `chip_handlers.py` from orchestrator | High (cleanest cut, zero coupling) | Low | ✅ Yes |
| 2 | `profile_handler.py` from orchestrator | High (most active code path) | Medium | ✅ Yes |
| 3 | `ws_connect.py` + `ws_question.py` from app.py | High (FE will touch app.py heavily) | Low | ✅ Yes |
| 4 | `test_page.html` from app.py | Easy win | None | ✅ Yes |
| 5 | `kusto_handlers.py` from orchestrator | Medium | Medium | ✅ Yes |
| 6 | `scope_handler.py` from orchestrator | Medium | Low | ✅ Yes |
| 7 | `rwlock.py` from data_profile | Low (small) | None | Optional |
| 8 | `charts/` from visualizer | Medium | Low | After FE |

Items 1-6 should happen before FE. Gets orchestrator from 2,511 → ~800, app.py from 1,199 → ~400.

---

## SOLID Alignment

| Principle | Current Violation | Fix |
|-----------|------------------|-----|
| **S**ingle Responsibility | Orchestrator does dispatch + chips + profiles + kusto + scope + enrichment | Each handler = one responsibility |
| **O**pen/Closed | Adding a chart type = editing visualizer.py | Chart plugins: add file, register |
| **L**iskov Substitution | N/A (no inheritance hierarchy) | — |
| **I**nterface Segregation | app.py exposes connect + question + test in one blob | Separate modules per concern |
| **D**ependency Inversion | Handlers reach into orchestrator's attributes directly | Inject dependencies via constructor |

---

*Estimated effort: 2-3 hours of mechanical extraction (no logic changes, just moving methods + updating imports).*
*Risk: Low — pure refactoring, all existing tests still pass.*
