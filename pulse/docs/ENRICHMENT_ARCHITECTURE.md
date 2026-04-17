# PULSE Org Enrichment — Architecture & Data Flow

> Deep-dive on one sub-component. Start with [`ARCHITECTURE.md`](ARCHITECTURE.md)
> for the full-system picture before reading this document.

## The Problem

PULSE query results show raw GUIDs that mean nothing to users:

```
11111111-1111-1111-1111-111111111111 — 1,500,000 events
22222222-2222-2222-2222-222222222222 — 900,000 events
```

Users have to manually look up what these orgs are. That's not intelligence — it's extra work.


## The Solution

PULSE automatically resolves GUIDs to friendly names at startup. No manual files. No human in the loop.

```
Contoso (NAM)           — 1.5M events
Fabrikam Production (APAC)  — 900K events
```


## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    PULSE Startup                         │
│                                                         │
│  1. Connect to telemetry cluster                        │
│  2. Build profile tables (as usual)                     │
│  3. Extract distinct OrgIds from profile_organization   │
│  4. Pass OrgIds to OrgEnricher                          │
│                                                         │
└──────────────────────┬──────────────────────────────────┘
                       │
                       │ org_ids = [11111111..., 22222222..., ...]
                       ▼
┌─────────────────────────────────────────────────────────┐
│                   OrgEnricher                            │
│                                                         │
│  Reads YAML config → builds KQL:                        │
│                                                         │
│    union                                                │
│      cluster('example-org-eu').db('OrgMetadataDB').OrgInfo,   │
│      cluster('example-org-us').db('OrgMetadataDB').OrgInfo    │
│    | where OrgsId in~ ({org_ids})     ← only our orgs   │
│    | project OrgsId, FriendlyName, TenantId, Geo        │
│                                                         │
│  Runs query → caches result in memory                   │
│                                                         │
└──────────────────────┬──────────────────────────────────┘
                       │
                       │ lookup = {11111111 → Contoso, ...}
                       ▼
┌─────────────────────────────────────────────────────────┐
│                   Query Time                            │
│                                                         │
│  User: "Top 10 orgs by events"                          │
│                                                         │
│  FastRouter → DuckDB → DataFrame                        │
│       │                                                 │
│       ▼                                                 │
│  enricher.enrich(df)                                    │
│       │                                                 │
│       ├── Adds OrgName column (for data/context)        │
│       ├── Adds TenantName column (if available)         │
│       └── GeoName null stays null (backfill off)        │
│       │                                                 │
│       ▼                                                 │
│  enricher.enrich_for_display(df)                        │
│       │                                                 │
│       └── Replaces GUID columns with names (for charts) │
│       │                                                 │
│       ▼                                                 │
│  Visualizer + Narrative Engine                          │
│       │                                                 │
│       └── "Contoso (NAM) dominates at 1.5M events"   │
│                                                         │
└─────────────────────────────────────────────────────────┘
```


## Data Flow

```
Step 1: Profile Build (existing)
   Telemetry Kusto → DuckDB profile tables
   Result: profile_organization has OrgId + event counts

Step 2: Enrichment Load (new)
   profile_organization → extract distinct OrgIds
   OrgIds → organizations Kusto query (filtered, fast)
   Result: in-memory lookup table (OrgId → FriendlyName, TenantId, Geo)

Step 3: Query Enrichment (new, every query)
   Any DataFrame with OrgId column → enricher.enrich()
   Result: OrgName + TenantName columns added

Step 4: Display Enrichment (new, before charts)
   DataFrame → enricher.enrich_for_display()
   Result: GUID columns replaced with friendly names
```

Key point: Step 2 only queries organizations for orgs that exist in your telemetry. If you have 50 active orgs, it fetches 50 rows — not millions. No timeout.


## Two Enrichment Modes

**enrich(df)** — Adds columns, preserves GUIDs.
Used after every query. DuckDB can still join on the original OrgId.

```
OrgId                                  | OrgName            | GeoName | events
11111111-1111-1111-1111-111111111111   | Contoso         | NAM     | 1.5M
22222222-2222-2222-2222-222222222222   | Fabrikam Production | APAC    | 900K
33333333-3333-3333-3333-333333333333   |                    | IND     | 600K
```

**enrich_for_display(df)** — Replaces GUIDs with names.
Used only before rendering charts and tables to the user.

```
Organization       | GeoName | events
Contoso         | NAM     | 1.5M
Fabrikam Production | APAC    | 900K
33333333...        | IND     | 600K    ← unresolved = shortened GUID
```


## Configuration

Everything is driven by a `enrichment` block in the team's YAML config. No code changes needed.

### Your team (cross-cluster)

```yaml
enrichment:
  source: kusto
  cluster: https://example-org-us.kusto.windows.net
  database: OrgMetadataDB
  query: |
    union
      cluster('https://example-org-eu.kusto.windows.net').database('OrgMetadataDB').organizations,
      cluster('https://example-org-us.kusto.windows.net').database('OrgMetadataDB').organizations
    | where DomainName !startswith 'deleted-'
    | where OrgsId in~ ({org_ids})
    | project OrgsId, FriendlyName, TenantId, Geo
  mappings:
    org_id: OrgsId
    org_name: FriendlyName
    tenant_id: TenantId
    geo: Geo
```

### How it works

- `source: kusto` — tells PULSE to query a Kusto reference table
- `cluster` / `database` — connection details (connect to either cluster, Kusto federates the union)
- `query` — custom KQL with `{org_ids}` placeholder, replaced at runtime with active org IDs
- `mappings` — maps your reference table columns to PULSE's internal fields
- `{org_ids}` — PULSE injects this automatically from profile_organization


## How Other Teams Adapt

Each team has a different reference table with different column names. The `mappings` block handles this. No code changes — just YAML.

### Team with a single-cluster reference table

```yaml
enrichment:
  source: kusto
  cluster: https://other-cluster.kusto.windows.net
  database: RefData
  table: CustomerOrgs
  filter: "| where status == 'active'"
  mappings:
    org_id: customer_id
    org_name: customer_name
    tenant_id: tenant_guid
    geo: region
```

PULSE auto-generates the query:
```
CustomerOrgs | where status == 'active' | where customer_id in~ (...) | project customer_id, customer_name, tenant_guid, region
```

### Team with no reference table yet

```yaml
enrichment:
  source: none
```

PULSE works normally — just shows raw GUIDs. When they find their reference table, they update the YAML and restart. That's it.


## Config Field Reference

| Field | Required | Description |
|-------|----------|-------------|
| `source` | Yes | `kusto`, `csv`, or `none` |
| `cluster` | If kusto | Kusto cluster URL to connect through |
| `database` | If kusto | Kusto database name |
| `table` | If kusto (no query) | Reference table name |
| `filter` | No | KQL filter clause (e.g. `\| where status == 'active'`) |
| `query` | No | Custom KQL — overrides table+filter. Use `{org_ids}` placeholder |
| `mappings.org_id` | Yes | Column in reference table that matches telemetry OrgId |
| `mappings.org_name` | Yes | Column with the friendly display name |
| `mappings.tenant_id` | No | Column with tenant GUID |
| `mappings.geo` | No | Column with region/geo code |
| `backfill_geo` | No | `false` (default). If `true`, fills null GeoName from metadata |


## Onboarding Checklist for New Teams

1. Find your reference table (ask your data platform team: "Where do we store org display names?")
2. Identify the columns: which column has the org GUID? Which has the friendly name?
3. Add the `enrichment` block to your PULSE YAML config
4. Restart PULSE — enrichment loads automatically


## What We Validated

- organizations on `example-org-eu` and `example-org-us` clusters contains `OrgsId → FriendlyName` mappings
- Matched 10/10 top telemetry orgs against organizations — all resolved correctly
- Full table query times out (millions of rows) — filtered `in~()` query returns in seconds
- GeoName backfill is off by default — NaN stays NaN until cluster owner confirms semantics
- Pending: cluster owner confirmation that organizations is the canonical source
