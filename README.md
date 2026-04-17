# PULSE - Production Kusto Interface

**Config-Driven | Zero Hallucination | Multi-Cluster | Production-Ready**

---

## What Is PULSE?

PULSE is a natural language interface to Azure Data Explorer (Kusto) that eliminates hallucination risk through config-driven architecture.

**Key principle:** Config defines EVERYTHING structural. LLM only generates simple filters.

---

## Architecture

### The Flow

```
User: "Show me errors from yesterday"
    ↓
1. System reads CONFIG (which clusters, filters, strategy)
2. System prepares context FOR LLM from config
3. LLM generates simple WHERE clause
4. System builds full query FROM config + LLM output
5. System queries ALL clusters FROM config
6. System combines per strategy FROM config
7. Results to user
```

**Config controls:** Clusters, strategy, filters, auth, schema  
**LLM controls:** Simple WHERE/SUMMARIZE clauses only

### The Shape

```
┌─────────────────────────────────────────────────────┐
│  CLIENT   ─ Streamlit UI  OR  FastAPI + WebSocket   │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│  CORE ENGINE   (src/pulse/core/*)                   │
│    AI Orchestrator · Fast Router · Query Planner    │
│    Semantic Layer · Data Profile · DuckDB Cache     │
│    Narrative Engine · Visualizer · Org Enrichment   │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│  EXTERNAL   Azure OpenAI  +  Kusto cluster(s)       │
└─────────────────────────────────────────────────────┘
```

→ **Full walkthrough:** [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) covers the request flow end-to-end, the caching layers, every module in `core/`, the two server options, and how to plug in a new backend or client.

---

## Features

✅ **Zero Hallucination** - Config is the backbone  
✅ **Multi-Cluster** - Query 2+ clusters, union automatically  
✅ **All 3 Auth Methods** - Azure CLI, Managed Identity, Service Principal  
✅ **Rate Limiting** - 10/min, 100/hr, 1000/day per user  
✅ **DuckDB Caching** - 10-50x speedup on follow-ups  
✅ **Light Theme** - Accessible, WCAG AA compliant  
✅ **Pluggable Configs** - Teams maintain their own  
✅ **Strategy Auto-Infer** - Optional for single cluster

---

## Quick Start

### 1. Install
```bash
pip install -r requirements.txt
```

### 2. Configure
```bash
cp .env.example .env
# Edit .env with your Azure OpenAI credentials
```

### 3. Add Data Source
```bash
# Edit configs/example.yaml
# Update cluster URLs to your clusters
```

### 4. Run
```bash
streamlit run src/pulse/ui/app.py
```

### 5. Use
1. Select data source
2. Choose auth method
3. Click Connect
4. Ask questions!

---

## Config Structure

```yaml
metadata:
  id: "my-data-source"
  name: "My Data Source"
  owner: "team@example.com"
  version: "1.0.0"

clusters:
  - name: "EU-West"
    url: "https://xxx.kusto.windows.net"
    database: "MyDB"
    table: "MyTable"
  
  - name: "US-Central"
    url: "https://yyy.kusto.windows.net"
    database: "MyDB"
    table: "MyTable"

strategy: "union"  # Required for 2+ clusters

authentication:
  method: "azure_cli"

filters:
  mandatory:
    - "EntityType == 'ExampleApp'"

llm_context:
  additional_columns:
    - name: "custom_field"
      type: "string"
```

The config is the source of truth for everything structural — this is what
prevents the LLM from hallucinating clusters or schemas.
See [`docs/ARCHITECTURE.md#4-config-driven-core`](docs/ARCHITECTURE.md#4-config-driven-core)
for the design rationale.

---

## Default Schema

**System provides these 11 columns automatically:**

- Date (datetime)
- UserID (guid)
- TenantID (guid)
- UserIDType (string)
- ProductName (string)
- ServiceTreeID (guid)
- AdditionalInfoJson (string)
- UILocale (string)
- Geo (string)
- BrowserLocale (string)
- Region (string)

**Teams add custom columns via `llm_context.additional_columns`**

---

## Authentication

### Azure CLI (Local Development)
```bash
az login
az account show
```

Select "Azure CLI" in UI, click Connect.

### Managed Identity (Production)
Enable Managed Identity in Azure Portal.  
Grant permissions on Kusto.  
Select "Managed Identity" in UI.

### Service Principal (CI/CD)
Create App Registration.  
Set environment variable: `AZURE_CLIENT_SECRET=xxx`  
Add client_id and tenant_id to config.  
Select "Service Principal" in UI.

---

## Rate Limiting

**Per user question:**
- 10 questions/minute
- 100 questions/hour
- 1000 questions/day

Follow-up questions (DuckDB cache hits) count towards limit.

**UI shows rate limit status in sidebar.**

---

## Strategy (Single vs Multi-Cluster)

### Single Cluster
```yaml
clusters:
  - name: "My-Cluster"
    url: "..."

# No strategy needed! Auto-inferred as "single"
```

### Multi-Cluster
```yaml
clusters:
  - name: "EU"
    url: "..."
  - name: "US"
    url: "..."

strategy: "union"  # Required!
```

**Options:**
- `union` - Combine all clusters
- `single` - Use only first cluster
- `labeled_union` - Combine with source label

---

## DuckDB Caching

**First query:**  
Kusto (3s) → Load DuckDB (0.3s) = 3.3s

**Follow-up queries:**  
DuckDB (0.1s) = **30x faster!**

**Clear cache:** Click "Clear Cache" button in UI

---

## Adding Teams

### Create New Config

```bash
cp configs/example.yaml configs/sales.yaml
# Edit sales.yaml with your clusters
```

**That's it!** PULSE auto-discovers on next startup.

---

## Project Structure

```
pulse/
├── src/pulse/
│   ├── api/         FastAPI + WebSocket server
│   ├── ui/          Streamlit app (alternate client)
│   ├── core/        ~30 modules — orchestration, routing, query planning,
│   │                semantic layer, caching, narratives, visualizer, auth
│   └── utils/
├── configs/         YAML data-source definitions (add one per team)
├── docs/            Architecture, enrichment, integration guides
├── system_defaults.yaml
├── requirements.txt
├── .env.example
└── LICENSE
```

For the annotated module map (what each file in `core/` does and why it
exists), see
[`docs/ARCHITECTURE.md#5-module-map`](docs/ARCHITECTURE.md#5-module-map).

---

## Example Questions

```
"Show me errors from yesterday"
"Top 10 entities by count"
"Usage by region last week"
"Show me operations where Geo is EMEA"
"Count by tenant name"
```

---

## Troubleshooting

### Auth Fails
```bash
# Azure CLI
az login
az account show

# Check permissions on Kusto
# Need at least "Viewer" role
```

### Config Not Found
```bash
# Check configs/ directory
ls configs/

# Validate config
python -c "from pulse.core.config_loader import ConfigLoader; ConfigLoader().discover_and_load()"
```

### Rate Limit Hit
Wait specified time, or increase limits in `rate_limiter.py`

---

## Deployment

### Local
```bash
streamlit run src/pulse/ui/app.py
```

### Docker
```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt
CMD ["streamlit", "run", "src/pulse/ui/app.py"]
```

### Azure Web App
```bash
az webapp up --name pulse --runtime "PYTHON:3.12"
```

---

## Key Decisions

### 1. Config is Backbone
**Why:** Eliminates LLM hallucination on structural decisions  
**Trade-off:** Less flexible, more reliable

### 2. Strategy Auto-Infer
**Why:** Simpler configs for single-cluster projects  
**Trade-off:** Must explicitly specify for multi-cluster

### 3. Default Schema
**Why:** Consistent baseline across all configs  
**Trade-off:** Teams can only add, not remove defaults

### 4. Per-Question Rate Limiting
**Why:** Simple, clear, fair  
**Trade-off:** DuckDB hits count (could exempt)

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for how these decisions
shape the module layout and data flow.

---

## Scaling

The config-driven design means adding a new team or data source is a copy-and-edit
of a single YAML file — no code changes. Multi-cluster union is a first-class
feature, so fan-out across regions is a config flag rather than a refactor.

---

## License

MIT — see [`LICENSE`](LICENSE) for the full text.

---

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for development setup and pull-request
guidelines. Architecture deep-dive lives in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

---

## Status

This repository is a **reference implementation**. Running it requires your
own Azure Data Explorer cluster and Azure OpenAI credentials — the code ships
without hosted infrastructure.

---

**PULSE — Config-Driven, Low-Hallucination, Multi-Cluster**
