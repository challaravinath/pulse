# 🎉 PULSE v2.0 - New Features!

## What's New

### 1. 📊 Smart Visualizations
**Automatically generates executive-ready charts based on your query!**

**Before:**
```
Query: "Top 10 orgs by events"
Result: Plain table with 10 rows
```

**Now:**
```
Query: "Top 10 orgs by events"
Result: 
  📊 Interactive Bar Chart
  💡 Key Insights:
    • Total: 105M events
    • Top org accounts for 54% of volume
    • Top 3 drive 82% of activity
  📋 Data Table (collapsible)
```

**Auto-Detects Patterns:**
- **Rankings** ("top 10", "highest") → Horizontal Bar Chart
- **Trends** ("per day", "over time") → Line Chart with growth indicators
- **Distributions** ("by region", "breakdown") → Pie or Bar Chart
- **Summaries** ("summary for manager") → Executive Brief

**Interactive Features:**
- Hover for details
- Zoom and pan
- Export as PNG
- Responsive layout

---

### 2. 🔍 Schema Auto-Discovery
**No more manually typing 44 columns!**

**Before:**
```yaml
# You had to manually type each column:
llm_context:
  additional_columns:
    - name: "EventInfo_Time"
      type: "datetime"
      description: "..."
    # ... 43 more to go! 😱
```

**Now:**
```bash
# One command:
python discover_schema.py configs/example.yaml

# Output:
✓ Discovered 44 columns from 2 clusters
✓ Schemas match across all clusters
✓ Saved to config
```

**Config Gets Auto-Populated:**
```yaml
# AUTO-DISCOVERED (don't edit)
schema:
  discovered_at: "2026-02-15T12:00:00Z"
  columns:
    EventInfo_Time: datetime
    EventInfo_Name: string
    OrgId: string
    # ... all 44 columns!

# YOU ONLY EDIT HINTS (optional)
column_hints:
  EventInfo_Time: "Use for date filters"
  OrgId: "Org ID for grouping"
```

**Benefits:**
- ✅ Always up-to-date (re-run to refresh)
- ✅ Works for ANY table
- ✅ Validates schema across clusters
- ✅ Onboard new projects in minutes

---

### 3. ✨ Chat-Style UX
**Query experience now flows like a conversation!**

**Improvements:**
- Query history shows last 3 queries
- Charts appear first (tables in expandable section)
- Clean, executive-ready layout
- Previous queries are re-runnable

**Layout:**
```
┌──────────────────────────┐
│ 📝 Query History         │
│ [Q1: Top orgs]          │
│ [Q2: Events per day]    │
├──────────────────────────┤
│ 💬 Ask a Question        │
│ [Your question here]     │
│ [🚀 Ask PULSE]          │
├──────────────────────────┤
│ 📊 Chart                 │
│ (Interactive visual)     │
├──────────────────────────┤
│ 💡 Key Insights          │
│ • Insight 1              │
│ • Insight 2              │
├──────────────────────────┤
│ 📋 Data Table ▼          │
│ (Click to expand)        │
└──────────────────────────┘
```

---

## Quick Start

### 1. Set Up Schema Discovery (One Time)

```bash
# Navigate to PULSE directory
cd PULSE-FINAL

# Make sure Azure CLI is logged in
az login --scope https://kusto.kusto.windows.net/.default

# Discover schema for your config
python discover_schema.py configs/example.yaml
```

**Output:**
```
============================================================
PULSE Schema Discovery
============================================================

Config: configs/example.yaml

✓ Discovered 44 columns from 2 cluster(s)
✓ Schemas match across all clusters
✓ Saved to configs/example.yaml

============================================================
✅ Next Steps:
============================================================
1. Open configs/example.yaml
2. Review the auto-discovered schema
3. Add hints for important columns
4. Restart PULSE
============================================================
```

### 2. Add Column Hints (Optional but Recommended)

Edit your config to add hints for key columns:

```yaml
column_hints:
  EventInfo_Time: "⚠️ Use this for ALL date/time filters (not 'Date')"
  OrgId: "Organization ID - use for grouping by org"
  EventInfo_Name: "Event type - filter by specific activities"
  UserId: "User identifier - track individual usage"
```

### 3. Run PULSE

```bash
python -m streamlit run src/pulse/ui/app.py
```

### 4. Try These Queries

**For Bar Charts:**
- "Top 10 orgs by number of events"
- "Events by region"
- "Most used event types"

**For Line Charts:**
- "Events per day over last week"
- "Daily active orgs trend"
- "Growth in new orgs"

**For Executive Summaries:**
- "Give me a summary for my manager"
- "What's the health of adoption?"
- "Summarize last week's activity"

---

## Examples from Real Usage

### Query: "Top 10 orgs by events"

**You Get:**
```
📊 Top 10 Organizations by Event Volume

█████████████████████ Org 6a25... 57.1M
███████████ Org c974... 27.6M  
█████████ Org eb81... 23.2M
...

💡 Key Insights
• Total Events: 105.2M
• Top org: 6a25... (57.1M, 54.3%)
• Top 3 account for 82% of total

🔵 Kusto (2 clusters)

📋 View Data Table ▼
[20 rows × 2 columns]
[📥 Download CSV]
```

### Query: "Events per day last 7 days"

**You Get:**
```
📈 Events Per Day Trend

[Interactive line chart showing daily trend]

💡 Key Insights
📈 Overall change: +12.3%
• Peak: 15.2M on 2026-02-14
• Lowest: 11.8M on 2026-02-10
• Average: 13.5M

🔵 Kusto (2 clusters)

📋 View Data Table ▼
```

---

## Technical Details

### Visualizer Architecture

```python
class SmartVisualizer:
    """
    Analyzes query + data patterns
    Returns: {chart, insights, type}
    """
    
    def analyze_and_visualize(df, query, kql):
        # 1. Detect pattern
        pattern = detect_pattern(df, query, kql)
        
        # 2. Generate chart
        if pattern == "ranking":
            return create_bar_chart(df)
        elif pattern == "time_series":
            return create_line_chart(df)
        
        # 3. Add insights
        insights = generate_insights(df, pattern)
        
        return {chart, insights, type}
```

### Schema Discovery Flow

```
1. Load config → Get clusters
2. For each cluster:
   - Connect with Azure CLI
   - Run: {table} | getschema
   - Parse columns + types
3. Validate schemas match
4. Save to config YAML
5. Users add hints for key columns
```

---

## Migration Guide

### Existing Configs

**Before (manual columns):**
```yaml
llm_context:
  additional_columns:
    - name: "EventInfo_Time"
      type: "datetime"
    # ... rest manually typed
```

**After (auto-discovered):**
```yaml
schema:
  discovered_at: "2026-02-15T12:00:00Z"
  columns:
    EventInfo_Time: datetime
    # ... auto-populated

column_hints:
  EventInfo_Time: "Use for date filters"
```

**Migration Steps:**
1. Run `python discover_schema.py configs/your-config.yaml`
2. Review auto-discovered schema
3. Move important descriptions to `column_hints`
4. Test queries

---

## FAQ

**Q: Do I need to run schema discovery every time?**
A: No! Only run when:
- Setting up a new config
- Table schema changes
- Adding new columns

**Q: Will old configs still work?**
A: Yes! PULSE supports both:
- New: `schema` + `column_hints`
- Old: `llm_context.additional_columns`

**Q: Can I customize chart colors/styles?**
A: Charts use Plotly defaults (professional blue theme). To customize, edit `src/pulse/core/visualizer.py`.

**Q: Do visualizations work with DuckDB cache?**
A: Yes! Follow-up questions use cached data and regenerate charts instantly.

**Q: Can I disable charts and show only tables?**
A: Yes. In config, add:
```yaml
visualization:
  enabled: false
```

---

## Performance

**Visualization Overhead:**
- Chart generation: ~50-100ms
- Negligible compared to query time (1-5 seconds)

**Schema Discovery:**
- One-time: ~2-3 seconds per cluster
- Cached in config (no runtime overhead)

---

## Support

**Issues?**
- Charts not showing: Check `requirements/base.txt` includes `plotly==5.24.1`
- Schema discovery fails: Verify Azure CLI is logged in
- Wrong chart type: Check query keywords match patterns

**Feedback:**
Share what's working and what could be better!

---

**Built with ❤️ by the PULSE Team**
