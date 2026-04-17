# PULSE Frontend Requirements — React `/app` Page
## Final Leg: Backend → Frontend Polish

**Status**: Backend hardened (iter 9.2). All data layer, AI orchestrator, and WebSocket APIs production-ready.
**Goal**: Replace `/test` page with polished React SPA at `/app`.

---

## 1. Core Chat Experience

### 1.1 Connect Flow
- [ ] Config selector dropdown (team configs from `/api/configs`)
- [ ] Auth method selector (Azure CLI / Device Code / Managed Identity)
- [ ] Connect button → WebSocket `connect` message
- [ ] Progress bar during connection phases: `auth → schema → semantic → data → ai → ready`
- [ ] Show phase labels + percentage from WebSocket `status` messages
- [ ] "Connected ✓" state with config name displayed

### 1.2 Chat Interface
- [ ] Message input with send button + Enter key
- [ ] User message bubbles (right-aligned)
- [ ] Assistant message bubbles (left-aligned) with streaming token support
- [ ] Markdown rendering in assistant messages (bold, bullets, code blocks)
- [ ] Auto-scroll to latest message
- [ ] Loading indicator while waiting for response

### 1.3 Suggested Chips
- [ ] Show clickable chips from WebSocket `chips` payload
- [ ] Chips update after each answer (contextual follow-ups)
- [ ] Chip click → sends as question

---

## 2. Data Visualization

### 2.1 Charts (Plotly)
- [ ] Render `visualization.plotly_json` from WebSocket `answer` payload
- [ ] Responsive sizing (fill chat bubble width)
- [ ] Plotly config: no mode bar clutter, zoom/pan enabled
- [ ] Dark/light theme support for chart backgrounds

### 2.2 Data Tables
- [ ] Render tabular data when no chart (table-only responses)
- [ ] **Pagination** — show 20 rows at a time with page controls (265-row results need this)
- [ ] Sortable columns (click header to sort)
- [ ] Sticky header row
- [ ] Number formatting (commas for thousands)

### 2.3 KQL Display
- [ ] Expandable "View KQL" section below each answer
- [ ] Syntax-highlighted KQL code block
- [ ] Copy-to-clipboard button
- [ ] Every profile answer now includes KQL — always show the toggle

---

## 3. Staleness & Data Freshness (★ NEW — from iter 9)

### 3.1 Header/Footer Bar
- [ ] Show `data_scope` label: "180-day profile · 32m ago"
- [ ] Show `⚠️ stale` warning when `is_stale: true` (data > 2 hours old)
- [ ] Auto-update age label every 60 seconds (client-side timer)
- [ ] Color coding: green (<30m), yellow (30m-2h), red (>2h stale)

### 3.2 Per-Answer Metadata
- [ ] Show response time (`elapsed_seconds`)
- [ ] Show source strategy (`source`: FastRouter / Semantic LLM / Kusto)
- [ ] Show scope label (`scope_label`: "last 180 days" / "last 90 days")
- [ ] Show freshness (`data_freshness`: "32m ago")
- [ ] Compact meta row below each answer (grey text, small font)

### 3.3 Reconnect Prompt
- [ ] When stale, show subtle "Data is X hours old — Reconnect?" button
- [ ] Reconnect clears cache and re-runs full connect flow

---

## 4. Connection Status

### 4.1 Status Indicators
- [ ] WebSocket connection state: connecting / connected / disconnected / error
- [ ] Profile build progress: "Building profile... 7/20 tables"
- [ ] Background build notification: "Full profile ready ✓" when stage 2 completes
- [ ] Circuit breaker status: if timeouts occurred, show "Some data sources unavailable"

### 4.2 Error Handling
- [ ] Graceful WebSocket disconnect → auto-reconnect with backoff
- [ ] Server error messages displayed in chat (red banner, not crash)
- [ ] Timeout messages: "Query took too long — try a simpler question"

---

## 5. Layout & Design

### 5.1 Structure
- [ ] Left sidebar: config selector, connection status, data freshness
- [ ] Main area: chat messages + charts
- [ ] Bottom: message input + chips
- [ ] Collapsible sidebar for mobile/narrow screens

### 5.2 Responsive
- [ ] Desktop: sidebar + main chat (1200px+)
- [ ] Tablet: collapsible sidebar (768-1200px)
- [ ] Mobile: full-width chat, hamburger menu for config (< 768px)

### 5.3 Theme
- [ ] Clean, professional color palette
- [ ] Clean, professional look — not flashy
- [ ] Accessible contrast ratios

---

## 6. WebSocket Protocol Reference

The backend already sends all of these — frontend just needs to handle them:

```
← welcome          { configs: [...] }
→ connect           { type: "connect", config: "...", auth_method: "..." }
← status            { phase, message, progress (0-1) }
← data_upgraded     { data_scope, scope_days, data_freshness, is_stale }
← chips             { chips: [{id, label, icon}] }
← ready             { message }

→ question          { type: "question", text: "..." }
← thinking          { message }
← stream_token      { text }  (token-by-token narrative)
← stream_end        {}
← answer            { message, visualization, kql, data_scope, 
                      data_freshness, is_stale, scope_days,
                      elapsed_seconds, source, scope_label }
← error             { message }
```

---

## 7. Nice-to-Have (Post-MVP)

- [ ] Export chart as PNG
- [ ] Export data table as CSV
- [ ] Dark mode toggle
- [ ] Keyboard shortcuts (Ctrl+Enter to send, Esc to clear)
- [ ] Session history (previous questions this session)
- [ ] Bookmarkable configs via URL params (`/app?config=teamX`)
- [ ] Typing indicator animation during LLM streaming

---

## 8. What's Already Working (from `/test` page)

The `/test` page at line 1042 of `app.py` has working implementations of:
- WebSocket connect/question flow
- Chart rendering (Plotly)
- Chip display + click
- Streaming tokens
- Meta line (elapsed, source, scope, freshness, stale warning)
- KQL display

This can serve as the reference implementation. The React `/app` just needs to be a proper, polished version of everything `/test` already does.

---

*Created: 2026-02-25 | Version: iter 9.2 | Backend: production-ready*
