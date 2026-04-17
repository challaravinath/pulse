# PULSE v2.1 → v2.2 — Complete Review & Changelog

---

## Files Changed (7 of 8 core files rewritten)

| File | Status | What Changed |
|------|--------|-------------|
| `ui/app.py` | **Rewritten** | Persistent chat, no refresh data loss, polished UI |
| `core/visualizer.py` | **Rewritten** | 7 chart types, aggressive detection, auto-fallback |
| `core/llm_service.py` | **Rewritten** | 25+ KQL examples, DuckDB SQL generation, better prompts |
| `core/analysis_engine.py` | **Rewritten** | Real stats (z-score, trends, Pareto), LLM narrates |
| `core/duckdb_engine.py` | **Rewritten** | Proper lifecycle, SQL queries, data summaries |
| `core/context_manager.py` | **Rewritten** | Rich LLM context, SQL context, KQL history tracking |
| `core/ai_orchestrator.py` | **Rewritten** | DuckDB follow-ups, friendly errors, full pipeline |
| `core/intent_router.py` | **Rewritten** | Follow-up detection, smarter cache-vs-fetch, safer fallback |

Unchanged: `kql_detector.py`, `schema_validator.py`, `kusto_client.py`, `auth_manager.py`, `config_loader.py`, `rate_limiter.py`

---

## Problem 1: UI Refresh Loses All Data ✅ FIXED

**Root cause:** Streamlit reruns the full script on every interaction. Charts/data were rendered inside `if question:` — on next rerun `question` is `None`, so everything vanished.

**Fix:** Full message store in `st.session_state.messages`. Every assistant response (chart, data, insights) is serialised as a plain dict and replayed on every rerun.

```python
# Charts: stored as dict, reconstructed on render
assistant_msg["chart"] = fig.to_dict()
# Later: fig = go.Figure(msg["chart"])

# DataFrames: stored as dict
assistant_msg["dataframe"] = df.to_dict(orient="list")
# Later: df = pd.DataFrame(msg["dataframe"])
```

---

## Problem 2: Charts Rarely Shown ✅ FIXED

**Root cause:** Old visualiser required exact keyword + KQL pattern combos. Most questions fell through to table-only.

**Fix:** Layered heuristics with aggressive fallback:

| Priority | Trigger | Chart Type |
|----------|---------|-----------|
| 1 | Executive keywords | Summary card |
| 2 | Time column + time keywords | Line |
| 3 | Time col + group col + distribution keywords | Multi-series line |
| 4 | Pie keywords + ≤8 rows | Donut |
| 5 | Comparison keywords + ≥2 numeric cols | Grouped bar |
| 6 | Ranking keywords (top, bottom, most) | Horizontal bar |
| 7 | Distribution keywords (by region, across) | Bar or Pie |
| 8 | KQL has `summarize…by` + `order…desc` | Horizontal bar |
| 9 | 2-column result (label + number) | Bar or Pie |
| **10** | **Fallback: any label + value cols found** | **Bar** |

New chart types: grouped bar, stacked bar, multi-series line, donut pie.

---

## Problem 3: Fragile LLM Prompts ✅ FIXED

**Old:** 2 KQL examples, minimal instructions. Complex questions generated bad KQL.

**New:** `KQL_SYSTEM_PROMPT` with:
- 25+ domain-specific examples covering all question bank categories
- Pattern templates: "How many X" → `dcount()`, "Top N by" → `summarize | order | take`
- Explicit column references (EventInfo_Time, GeoName, OrgId, EntityName)
- Clear anti-patterns (no table names, no pipes, no markdown)

Also added `_attempt_kql_fix()` for common LLM mistakes (e.g., "top 10 orgs" → proper summarize pattern).

---

## Problem 4: Thin Analysis Engine ✅ FIXED

**Old:** Sent raw stats to GPT and hoped for structured output.

**New:** Real statistical computation BEFORE the LLM:

| Computation | Method |
|------------|--------|
| **Anomaly detection** | Z-score (>2σ from mean) |
| **Trend analysis** | Linear regression slope + percentage change |
| **Concentration** | Pareto / cumulative sum (N items for 80% of value) |
| **Period comparison** | First-half vs second-half sum |
| **Dominance detection** | Categorical value > 50% of rows |

The LLM *narrates* these pre-computed findings. If the LLM fails, the auto-computed insights are shown directly.

---

## Problem 5: DuckDB Follow-ups Broken ✅ FIXED

**Old:** `_handle_refine_cache` had `# TODO: Implement actual filtering logic using DuckDB`.

**New:** Complete pipeline:

1. `LLMService.generate_duckdb_sql()` — new method with dedicated SQL prompt
2. `DuckDBQueryEngine.query()` — executes SQL on cached data
3. Result replaces the context and gets visualised

Now these work:
- "Show just EMEA" → `SELECT * FROM telemetry WHERE GeoName = 'EMEA'`
- "Only the top 5" → `SELECT * FROM telemetry ORDER BY Events DESC LIMIT 5`
- "Break that down by region" → `SELECT GeoName, COUNT(*) ... GROUP BY GeoName`
- "Exclude test orgs" → `SELECT * FROM telemetry WHERE OrgId NOT LIKE '%test%'`

---

## Problem 6: No Conversation Memory ✅ FIXED

**Old:** Context manager tracked history but didn't feed it back effectively.

**New:**
- `ConversationContext.format_context_for_llm()` sends recent questions + KQL queries + result columns to the KQL generator
- `ConversationContext.format_context_for_sql()` sends result columns + prior context to the SQL generator
- `last_kql` tracked so "modify the last query" patterns work
- History capped at 10 turns to stay within token limits

---

## Problem 7: Developer-Facing Errors ✅ FIXED

**Old:** Raw stack traces, Kusto error strings.

**New:** `AIOrchestrator._friendly_error()` maps technical errors to actionable messages:

| Error Type | User Sees |
|-----------|----------|
| Connection timeout | "Connection timed out — try reconnecting" |
| Auth expired (401) | "Authentication expired — reconnect" |
| KQL syntax error | "Query had a syntax issue — try rephrasing" |
| Rate limit (429) | "Kusto is rate-limiting — wait and retry" |
| DuckDB SQL error | "Follow-up didn't work — try rephrasing" |

Stack traces still available in expandable sections for debugging.

---

## Smaller Fixes

- **DuckDB connection leak:** Old engine never closed connections on re-load. Now calls `.close()` before creating new connection.
- **Debug `st.write` removed:** No more `DEBUG: Inside spinner` in production.
- **Intent router fallback:** If LLM fails AND data is cached, defaults to `ANALYZE_CACHE` (cheap) instead of `FETCH_DATA` (expensive Kusto query).
- **Unused `openai` import** removed from `intent_router.py`.
- **Visualizer auto-fallback:** If detection fails but data has a label + value column, charts it anyway.

---

## Test Coverage

28 automated tests pass covering:

- DuckDB: load, re-load lifecycle, query, clear, summary
- Context: load, history, LLM context, SQL context
- Visualizer: bar, hbar, line, pie, grouped bar, multi-line, auto-fallback
- Analysis: basic insights, trend detection, concentration detection
- Intent Router: 12 classification cases (fetch, analyze, refine, meta, redirect)

---

## How to Verify

**Refresh bug:** Ask a question → chart appears → ask another → first chart still visible above.

**Charts for question bank:**
| Question | Expected Chart |
|----------|---------------|
| "Top 10 orgs by events" | Horizontal bar |
| "Events per region" | Bar |
| "Daily event trend" | Line |
| "Events per day by region" | Multi-series line |
| "Compare first half vs second half" | Grouped bar |
| "What percentage of events come from EMEA?" | Donut pie |
| "Summary for my manager" | Executive summary |

**DuckDB follow-ups:** Fetch data first, then:
- "Show just EMEA" → filtered result
- "Break that down by region" → grouped result
- "Only the top 5" → limited result

**Analysis:** After fetching data:
- "What should I be worried about?" → real insights with numbers
- "Any anomalies?" → z-score based detection
- "Is usage growing?" → trend computation
