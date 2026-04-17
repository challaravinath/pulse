"""
PULSE Enrichment Integration Patch
=====================================
Shows exactly what to change in ai_orchestrator.py to wire in org enrichment.

This is NOT a standalone file — it shows the diffs to apply.
"""

# ═══════════════════════════════════════════════════════════════════════════
# 1. ADD IMPORT (top of ai_orchestrator.py, near other imports)
# ═══════════════════════════════════════════════════════════════════════════

# After: from .narrative_engine import generate_smart_insight
# Add:
"""
from .org_enrichment import OrgEnricher, EnrichmentConfig
"""


# ═══════════════════════════════════════════════════════════════════════════
# 2. MODIFY __init__() — load enricher from config
# ═══════════════════════════════════════════════════════════════════════════

# In AIOrchestrator.__init__(), after self.fast_router setup, add:
"""
        # ── Org Enrichment ───────────────────────────────
        enrichment_cfg = EnrichmentConfig.from_dict(
            config.get("enrichment", {}) if config else {}
        )
        self.enricher = OrgEnricher(enrichment_cfg)
        # Don't load yet — we load after profile build with active org IDs
"""

# Then in your profile build completion callback (or after data_profile.build()):
"""
        # After profile tables are built, fetch names for active orgs only
        if self.enricher and self.enricher.config.source == "kusto":
            try:
                org_ids = self.data_profile.query(
                    "SELECT DISTINCT OrgId FROM profile_organization"
                )['OrgId'].tolist()
                self.enricher.load_for_orgs(org_ids, kusto_client=self.kusto_client)
            except Exception as e:
                logger.warning(f"Enrichment load failed: {e}")
        elif self.enricher:
            self.enricher.load()
"""


# ═══════════════════════════════════════════════════════════════════════════
# 3. ADD _enrich() HELPER METHOD
# ═══════════════════════════════════════════════════════════════════════════

# Add this method to AIOrchestrator class (after _guess_intent):
"""
    def _enrich(self, df: pd.DataFrame, for_display: bool = False) -> pd.DataFrame:
        \"\"\"Enrich DataFrame with org friendly names if enricher is loaded.\"\"\"
        if self.enricher and self.enricher.is_loaded:
            try:
                if for_display:
                    return self.enricher.enrich_for_display(df)
                return self.enricher.enrich(df)
            except Exception as e:
                logger.warning(f"Enrichment failed: {e}")
        return df
"""


# ═══════════════════════════════════════════════════════════════════════════
# 4. WIRE INTO ALL 4 EXECUTION PATHS
# ═══════════════════════════════════════════════════════════════════════════

# ── Path 1: FastRouter (_handle_fast_profile) ────────────────────────────
# After:  df = self.data_profile.query(result.sql)
# Add:    df = self._enrich(df)

# ── Path 2: Semantic Layer (_handle_profile) ─────────────────────────────
# After:  df = self.data_profile.query(sql)
# Add:    df = self._enrich(df)

# ── Path 3: Kusto Fetch (_validate_execute_visualize) ────────────────────
# After:  df = self.kusto_client.execute_query(kql)  (or however Kusto results come back)
# Add:    df = self._enrich(df)

# ── Path 4: Refine (_handle_refine) ──────────────────────────────────────
# After:  df = self.duckdb_engine.query(sql)
# Add:    df = self._enrich(df)


# ═══════════════════════════════════════════════════════════════════════════
# 5. ENRICH FOR DISPLAY IN VISUALIZER CALLS
# ═══════════════════════════════════════════════════════════════════════════

# Before each visualizer call, enrich for display so charts show names not GUIDs:
#
# BEFORE:
#   viz = self.visualizer.analyze_and_visualize(df, message, "", intent_hint=intent_hint)
#
# AFTER:
#   display_df = self._enrich(df, for_display=True)
#   viz = self.visualizer.analyze_and_visualize(display_df, message, "", intent_hint=intent_hint)
#
# The raw df (with GUID columns intact) stays in context for DuckDB queries.
# The display_df (with friendly names) goes to the visualizer for charts.


# ═══════════════════════════════════════════════════════════════════════════
# 6. YAML CONFIG — ADD metadata BLOCK
# ═══════════════════════════════════════════════════════════════════════════

YAML_EXAMPLE = """
# ── Your existing PULSE config ──
name: my-product
clusters:
  - url: https://example.kusto.windows.net
    database: telemetry
    table: Events

# ── NEW: Org Metadata Enrichment (cross-cluster) ──
# PULSE auto-queries at startup for active orgs only. No CSV to maintain.
enrichment:
  source: kusto
  cluster: https://example-org-us.kusto.windows.net   # connect via either cluster
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
  # backfill_geo: false   # Set true only after confirming metadata geo = telemetry geo

# ── For teams with a single reference cluster ──
# enrichment:
#   source: kusto
#   cluster: https://other-cluster.kusto.windows.net
#   database: RefData
#   table: CustomerOrgs
#   filter: "| where status == 'active'"
#   mappings:
#     org_id: customer_id
#     org_name: customer_name
#     geo: region

# ── Skip enrichment (default if metadata block is absent) ──
# enrichment:
#   source: none
"""
