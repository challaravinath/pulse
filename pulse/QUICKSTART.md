# PULSE Quick Start Guide

## 5-Minute Setup

### Step 1: Install Dependencies (1 min)
```bash
cd pulse
pip install -r requirements.txt
```

### Step 2: Configure Azure OpenAI (1 min)
```bash
cp .env.example .env
```

Edit `.env`:
```
AZURE_OPENAI_ENDPOINT=https://your-endpoint.openai.azure.com/
AZURE_OPENAI_API_KEY=your-key-here
AZURE_OPENAI_MODEL=gpt-4o-mini
```

### Step 3: Update Config with Your Clusters (2 min)
Edit `configs/example.yaml`:
```yaml
clusters:
  - name: "Your-Cluster"
    url: "https://your-cluster.kusto.windows.net"  # YOUR URL
    database: "YourDatabase"                        # YOUR DB
    table: "YourTable"                              # YOUR TABLE
```

### Step 4: Authenticate (30 sec)
```bash
az login
az account show
```

### Step 5: Run (30 sec)
```bash
streamlit run src/pulse/ui/app.py
```

---

## First Query

1. **Select data source:** "ExampleApp Global Telemetry"
2. **Choose auth:** "Azure CLI (Local Dev)"
3. **Click:** "Connect"
4. **Ask:** "Show me data from yesterday"
5. **Click:** "Ask PULSE"

Done! 🎉

---

## Common Issues

### "Config validation failed"
- Check cluster URLs are https://
- Check URLs end with .kusto.windows.net
- Check owner email is valid

### "Authentication failed"
- Run `az login`
- Check `az account show`
- Verify permissions on Kusto cluster

### "No configs found"
- Check configs/ directory exists
- Check .yaml files are present
- Check file format is valid YAML

---

## Next Steps

1. **Add more clusters** - Edit config, add entries
2. **Create team configs** - Copy example.yaml, customize
3. **Try custom columns** - Add to llm_context.additional_columns
4. **Deploy** - See README.md for deployment options

---

## Getting Help

- **README.md** — Full documentation
- **docs/ARCHITECTURE.md** — How PULSE works under the hood (request flow,
  caching, module map, extension guide)
- **configs/example.yaml** — Example config with comments
- **system_defaults.yaml** — Default schema reference
