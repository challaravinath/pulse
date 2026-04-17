# PULSE Org Enrichment Module

> Sub-component of PULSE. For the overall system shape and where this module
> fits, see [`ARCHITECTURE.md`](ARCHITECTURE.md).

## What It Does

Replaces raw GUIDs with friendly display names in every PULSE response. Automatically.

**Before:** `11111111-1111-1111-1111-111111111111 — 1,500,000 events`  
**After:**  `Contoso (NAM) — 1.5M events`

## How It Works

1. PULSE builds profile tables from telemetry (as usual)
2. Enricher extracts distinct OrgIds from profile tables
3. Enricher queries organizations **only for those OrgIds** (fast — no full table scan)
4. Names cached in memory for the session
5. Every query result gets enriched automatically

**No CSV. No manual updates. Always fresh.**

## Files

| File | Purpose |
|------|---------|
| `org_enrichment.py` | Drop into `src/pulse/core/` — the enrichment engine |
| `integration_guide.py` | Reference — shows exactly where to wire into ai_orchestrator.py |

## Setup

### 1. Copy the module
```bash
cp org_enrichment.py  src/pulse/core/org_enrichment.py
```

### 2. Add to your YAML config
```yaml
enrichment:
  source: kusto
  cluster: https://example-org-us.kusto.windows.net
  database: OrgMetadataDB
  table: organizations
  filter: "| where DomainName !startswith 'deleted-'"
  mappings:
    org_id: OrgsId
    org_name: FriendlyName
    tenant_id: TenantId
    geo: Geo
```

### 3. Wire into ai_orchestrator.py

After profile build completes:
```python
# Fetch names for only the orgs in your telemetry — fast, no timeout
org_ids = data_profile.query("SELECT DISTINCT OrgId FROM profile_organization")['OrgId'].tolist()
self.enricher.load_for_orgs(org_ids, kusto_client=self.kusto_client)
```

See `integration_guide.py` for full walkthrough.

## For Other Teams

Each team points to their own reference table in YAML. No code changes:

```yaml
# Team B — different product, different reference
enrichment:
  source: kusto
  cluster: https://other-cluster.kusto.windows.net
  database: RefData
  table: CustomerOrgs
  mappings:
    org_id: customer_id
    org_name: customer_name
    geo: region
```

## Why load_for_orgs()

organizations has millions of rows — full query times out.
`load_for_orgs()` filters to only your active OrgIds. Fast, targeted, no timeout.

## Geo Backfill

Off by default. Enable after cluster owner confirms metadata geo = telemetry geo:
```yaml
enrichment:
  backfill_geo: true
```
