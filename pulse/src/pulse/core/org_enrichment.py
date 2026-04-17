"""
Org Enrichment Module v1.0
============================
Replaces raw GUIDs with friendly display names in PULSE query results.

Configurable per team via YAML:
  enrichment:
    source: csv | kusto | none
    path: lookups/org_metadata.csv          # if source=csv
    cluster: https://example-org-us.kusto.windows.net  # if source=kusto
    database: OrgMetadataDB
    table: organizations
    mappings:
      org_id: OrgsId
      org_name: FriendlyName
      tenant_id: TenantId
      geo: Geo

Usage:
    enricher = OrgEnricher(config)
    enricher.load()                          # Load metadata once at startup
    df = enricher.enrich(df)                 # Enrich any DataFrame
    display_name = enricher.resolve("11111111-...")  # Single lookup

Author: PULSE Team
"""

import os
import re
import logging
import pandas as pd
from typing import Dict, Optional, List, Any
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


# ── Configuration ────────────────────────────────────────────────────────

@dataclass
class EnrichmentConfig:
    """Parsed metadata configuration from YAML."""
    source: str = "none"                     # csv | kusto | none
    path: str = ""                           # CSV/TSV file path
    cluster: str = ""                        # Kusto cluster URL
    database: str = ""                       # Kusto database
    table: str = ""                          # Kusto table name
    filter: str = ""                         # Optional KQL filter (e.g. "| where DomainName !startswith 'deleted-'")
    query: str = ""                          # Optional custom KQL (overrides table+filter)
    mappings: Dict[str, str] = field(default_factory=lambda: {
        "org_id": "OrgsId",
        "org_name": "FriendlyName",
        "tenant_id": "TenantId",
        "geo": "Geo",
    })
    cache_ttl_hours: int = 24                # Re-fetch interval for Kusto source
    display_format: str = "{org_name} ({geo})"  # How to render in UI
    backfill_geo: bool = False               # If True, fill null GeoName from metadata
                                             # Only enable after confirming metadata geo
                                             # matches telemetry geo semantics

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "EnrichmentConfig":
        """Parse from YAML dict."""
        if not d:
            return cls()
        return cls(
            source=d.get("source", "none"),
            path=d.get("path", ""),
            cluster=d.get("cluster", ""),
            database=d.get("database", ""),
            table=d.get("table", ""),
            filter=d.get("filter", ""),
            query=d.get("query", ""),
            mappings=d.get("mappings", {
                "org_id": "OrgsId",
                "org_name": "FriendlyName",
                "tenant_id": "TenantId",
                "geo": "Geo",
            }),
            cache_ttl_hours=d.get("cache_ttl_hours", 24),
            display_format=d.get("display_format", "{org_name} ({geo})"),
            backfill_geo=d.get("backfill_geo", False),
        )


# ── Metadata Store ───────────────────────────────────────────────────────

@dataclass
class OrgInfo:
    """Metadata for a single organization."""
    org_id: str
    org_name: str = ""
    tenant_id: str = ""
    tenant_name: str = ""
    geo: str = ""

    @property
    def display_name(self) -> str:
        """Friendly display string."""
        parts = []
        if self.org_name:
            parts.append(self.org_name)
        if self.geo:
            parts.append(f"({self.geo})")
        return " ".join(parts) if parts else self.short_id

    @property
    def short_id(self) -> str:
        """Truncated GUID for fallback display."""
        return self.org_id[:8] + "..." if len(self.org_id) > 12 else self.org_id


# ── Main Enricher ────────────────────────────────────────────────────────

class OrgEnricher:
    """
    Loads org metadata from CSV or Kusto and enriches DataFrames.

    Integration points:
    1. Orchestrator.__init__() → enricher = OrgEnricher(config); enricher.load()
    2. After any query returns a DataFrame → df = enricher.enrich(df)
    3. Narrative engine → enricher.resolve(org_id) for single lookups
    """

    # Columns in telemetry data that contain org/tenant GUIDs
    ORG_ID_COLUMNS = {"orgid", "org_id", "organizationid", "organization_id", "orgsid"}
    TENANT_ID_COLUMNS = {"aadtenantid", "aad_tenant_id", "tenantid", "tenant_id", "finaltenantid"}
    GEO_COLUMNS = {"geoname", "geo", "region", "geo_name"}

    def __init__(self, config: Optional[EnrichmentConfig] = None):
        self.config = config or EnrichmentConfig()
        self._lookup: Dict[str, OrgInfo] = {}         # org_id (lower) → OrgInfo
        self._tenant_lookup: Dict[str, str] = {}       # tenant_id (lower) → tenant_name
        self._loaded = False
        self._kusto_client = None                       # Cached direct connection to reference cluster

    # ── Loading ──────────────────────────────────────────────────────

    def load(self) -> bool:
        """Load metadata from configured source. Returns True on success."""
        if self.config.source == "none":
            logger.info("Enrichment: source=none, skipping metadata load")
            return True

        if self.config.source == "csv":
            return self._load_csv()
        elif self.config.source == "kusto":
            return self._load_kusto()
        else:
            logger.warning(f"Enrichment: unknown source '{self.config.source}', skipping")
            return False

    def load_for_orgs(self, org_ids: list, kusto_client=None) -> bool:
        """
        RECOMMENDED: Load metadata only for specific org IDs.

        Call this after profile build, passing the distinct OrgIds from telemetry.
        This avoids querying the entire organizations table (which times out)
        and only fetches names for orgs that actually appear in your data.

        Supports two modes:
        1. Auto-generated query: uses table + filter + mappings from config
        2. Custom query: uses config.query with {org_ids} placeholder
           Perfect for multi-cluster unions:
             union
               cluster('...weu').database('OrgMetadataDB').organizations,
               cluster('...cus').database('OrgMetadataDB').organizations
             | where OrgsId in~ ({org_ids})
             | project OrgsId, FriendlyName, TenantId, Geo

        Usage in orchestrator (during profile build):
            org_ids = profile.query("SELECT DISTINCT OrgId FROM profile_organization")['OrgId'].tolist()
            self.enricher.load_for_orgs(org_ids, kusto_client=self.kusto_client)
        """
        if self.config.source != "kusto" or not org_ids:
            return self.load()  # Fall back to standard load

        cluster = self.config.cluster
        database = self.config.database
        mappings = self.config.mappings

        if not cluster or not database:
            logger.warning("Enrichment: kusto source requires cluster and database")
            return False

        has_custom_query = bool(self.config.query and self.config.query.strip())

        if not has_custom_query and not self.config.table:
            logger.warning("Enrichment: kusto source requires either 'query' or 'table'")
            return False

        # Chunk org_ids (2000 per batch — cross-cluster union handles it fine)
        all_loaded = 0
        for chunk_start in range(0, len(org_ids), 2000):
            chunk = org_ids[chunk_start:chunk_start + 2000]
            clean_ids = [str(oid).strip() for oid in chunk if oid and str(oid).strip() not in ("", "nan")]
            if not clean_ids:
                continue

            ids_str = ", ".join(f"'{oid}'" for oid in clean_ids)

            # Build KQL — custom query or auto-generated
            if has_custom_query:
                # Replace {org_ids} placeholder in custom query
                kql = self.config.query.replace("{org_ids}", ids_str)
            else:
                # Auto-generate from table + filter + mappings
                org_id_col = mappings.get("org_id", "OrgsId")
                cols = ", ".join(mappings.values())
                filter_clause = self.config.filter or ""
                kql = f"{self.config.table} {filter_clause} | where {org_id_col} in~ ({ids_str}) | project {cols}"

            logger.info(f"Enrichment: fetching names for {len(clean_ids)} orgs (chunk {chunk_start // 2000 + 1})")

            try:
                if kusto_client:
                    # Use PULSE's existing Kusto client
                    df = kusto_client.execute_query(kql)
                else:
                    # Use azure-kusto-data SDK directly (cached connection)
                    from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
                    if not self._kusto_client:
                        kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(cluster)
                        self._kusto_client = KustoClient(kcsb)
                        logger.info(f"Enrichment: connected to {cluster}")
                    response = self._kusto_client.execute(database, kql)

                    rows = []
                    for row in response.primary_results[0]:
                        row_dict = {}
                        for k, v in mappings.items():
                            try:
                                row_dict[v] = row[v]
                            except (KeyError, IndexError):
                                row_dict[v] = ""
                        rows.append(row_dict)
                    df = pd.DataFrame(rows) if rows else pd.DataFrame()

                if not df.empty:
                    self._ingest_dataframe(df)
                    all_loaded += len(df)

            except Exception as e:
                logger.warning(f"Enrichment: chunk fetch failed: {e}")
                continue

        if all_loaded > 0:
            self._loaded = True
            logger.info(f"Enrichment: loaded {len(self._lookup)} orgs from Kusto (filtered to active orgs)")
            return True
        else:
            logger.warning("Enrichment: no orgs resolved from Kusto")
            return False

    def _load_csv(self) -> bool:
        """Load from CSV/TSV/XLSX file."""
        path = self.config.path
        if not path or not os.path.exists(path):
            logger.warning(f"Enrichment: metadata file not found: {path}")
            return False

        try:
            ext = os.path.splitext(path)[1].lower()
            if ext in (".xlsx", ".xls"):
                df = pd.read_excel(path)
            elif ext == ".tsv":
                df = pd.read_csv(path, sep="\t")
            else:
                # Auto-detect separator
                df = pd.read_csv(path, sep=None, engine="python")

            self._ingest_dataframe(df)
            logger.info(f"Enrichment: loaded {len(self._lookup)} orgs from {path}")
            return True

        except Exception as e:
            logger.error(f"Enrichment: failed to load CSV: {e}")
            return False

    def _load_kusto(self) -> bool:
        """
        Load from Kusto reference table at startup.
        This is the production path — runs once per session during profile build.
        No CSV, no manual updates. Always fresh.
        """
        cluster = self.config.cluster
        database = self.config.database
        table = self.config.table
        mappings = self.config.mappings

        if not all([cluster, database, table]):
            logger.warning("Enrichment: kusto source requires cluster, database, table")
            return False

        try:
            # Build query
            cols = ", ".join(mappings.values())
            kql = f"{table} | project {cols}"

            # Apply optional filter (e.g., exclude deleted orgs)
            if self.config.filter:
                kql = f"{table} {self.config.filter} | project {cols}"

            logger.info(f"Enrichment: querying {cluster}/{database}: {kql[:100]}...")

            # ── Try azure-kusto-data SDK ──
            try:
                from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
                if not self._kusto_client:
                    kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(cluster)
                    self._kusto_client = KustoClient(kcsb)
                response = self._kusto_client.execute(database, kql)

                rows = []
                for row in response.primary_results[0]:
                    row_dict = {}
                    for k, v in mappings.items():
                        try:
                            row_dict[v] = row[v]
                        except (KeyError, IndexError):
                            row_dict[v] = ""
                    rows.append(row_dict)

                if rows:
                    df = pd.DataFrame(rows)
                    self._ingest_dataframe(df)
                    logger.info(f"Enrichment: loaded {len(self._lookup)} orgs from Kusto ({cluster})")
                    return True
                else:
                    logger.warning("Enrichment: Kusto query returned 0 rows")
                    return False

            except ImportError:
                logger.warning("Enrichment: azure-kusto-data not installed, cannot query Kusto")
                return False

        except Exception as e:
            logger.error(f"Enrichment: Kusto load failed: {e}")
            return False

    def _ingest_dataframe(self, df: pd.DataFrame):
        """Parse a DataFrame into the internal lookup dict."""
        m = self.config.mappings

        # Find columns (case-insensitive)
        col_map = {}
        df_cols_lower = {c.lower(): c for c in df.columns}

        for key, expected_col in m.items():
            # Try exact match first, then case-insensitive
            if expected_col in df.columns:
                col_map[key] = expected_col
            elif expected_col.lower() in df_cols_lower:
                col_map[key] = df_cols_lower[expected_col.lower()]
            else:
                # Try matching against known column patterns
                for alias in self._get_aliases(key):
                    if alias in df_cols_lower:
                        col_map[key] = df_cols_lower[alias]
                        break

        org_col = col_map.get("org_id")
        if not org_col:
            logger.error(f"Enrichment: org_id column not found. Available: {list(df.columns)}")
            return

        # De-duplicate: keep first occurrence (in case of union from multiple clusters)
        df = df.drop_duplicates(subset=[org_col])

        for _, row in df.iterrows():
            org_id = str(row.get(org_col, "")).strip()
            if not org_id or org_id == "nan":
                continue

            info = OrgInfo(
                org_id=org_id,
                org_name=str(row.get(col_map.get("org_name", ""), "")).strip(),
                tenant_id=str(row.get(col_map.get("tenant_id", ""), "")).strip(),
                tenant_name=str(row.get(col_map.get("tenant_name", ""), "")).strip(),
                geo=str(row.get(col_map.get("geo", ""), "")).strip(),
            )

            # Clean up "nan" strings
            if info.org_name == "nan":
                info.org_name = ""
            if info.tenant_id == "nan":
                info.tenant_id = ""
            if info.tenant_name == "nan":
                info.tenant_name = ""
            if info.geo == "nan":
                info.geo = ""

            self._lookup[org_id.lower()] = info

            # Build tenant reverse lookup
            if info.tenant_id and info.tenant_name:
                self._tenant_lookup[info.tenant_id.lower()] = info.tenant_name

        self._loaded = True

    def _get_aliases(self, key: str) -> List[str]:
        """Return known aliases for a mapping key."""
        aliases = {
            "org_id": ["orgsid", "orgid", "org_id", "organizationid", "organization_id"],
            "org_name": ["friendlyname", "friendly_name", "orgname", "org_name", "displayname", "display_name", "name"],
            "tenant_id": ["tenantid", "tenant_id", "aadtenantid", "aad_tenant_id"],
            "tenant_name": ["tenantname", "tenant_name", "nameoftenant"],
            "geo": ["geo", "geoname", "geo_name", "region"],
        }
        return aliases.get(key, [])

    # ── Lookup ───────────────────────────────────────────────────────

    @property
    def is_loaded(self) -> bool:
        return self._loaded and len(self._lookup) > 0

    @property
    def org_count(self) -> int:
        return len(self._lookup)

    def resolve(self, org_id: str) -> Optional[OrgInfo]:
        """Look up org metadata by ID. Returns None if not found."""
        if not org_id or not self._loaded:
            return None
        return self._lookup.get(str(org_id).strip().lower())

    def get_display_name(self, org_id: str) -> str:
        """Get friendly display name for an org. Falls back to short GUID."""
        info = self.resolve(org_id)
        if info and info.org_name:
            return info.display_name
        # Fallback: truncate GUID
        org_id = str(org_id).strip()
        return org_id[:8] + "..." if len(org_id) > 12 else org_id

    def resolve_tenant(self, tenant_id: str) -> str:
        """Get tenant name from tenant ID. Falls back to short GUID."""
        if not tenant_id or not self._loaded:
            return str(tenant_id)[:8] + "..." if tenant_id and len(str(tenant_id)) > 12 else str(tenant_id)
        name = self._tenant_lookup.get(str(tenant_id).strip().lower(), "")
        if name:
            return name
        return str(tenant_id)[:8] + "..." if len(str(tenant_id)) > 12 else str(tenant_id)

    # ── DataFrame Enrichment ─────────────────────────────────────────

    def enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich a DataFrame by replacing GUID columns with friendly names.

        Detects OrgId, AadTenantId, GeoName columns automatically.
        Adds new columns: OrgName, TenantName (if mappable).
        Replaces GUID values in-place for display.

        Returns a new DataFrame (original is not modified).
        """
        if df is None or df.empty or not self._loaded:
            return df

        df = df.copy()

        # Find OrgId column
        org_col = self._find_column(df, self.ORG_ID_COLUMNS)
        tenant_col = self._find_column(df, self.TENANT_ID_COLUMNS)
        geo_col = self._find_column(df, self.GEO_COLUMNS)

        if not org_col and not tenant_col:
            # No GUID columns to enrich
            return df

        # ── Add OrgName column ───────────────────────────────────────
        if org_col:
            org_name_col = self._pick_new_column_name(df, "OrgName")
            df[org_name_col] = df[org_col].apply(self._resolve_org_name)

            # ★ v10.2: Skip adding name column if values are identical to
            # original (data already has readable names) or all empty
            resolved = df[org_name_col].astype(str).str.strip()
            original = df[org_col].astype(str).str.strip()
            all_empty = (resolved == '').all()
            all_same = (resolved == original).all()
            if all_empty or all_same:
                df = df.drop(columns=[org_name_col])
                logger.debug(f"Enrichment: skipped {org_name_col} (identical/empty)")
            else:
                # Reorder: put OrgName right after OrgId
                cols = list(df.columns)
                org_idx = cols.index(org_col)
                cols.remove(org_name_col)
                cols.insert(org_idx + 1, org_name_col)
                df = df[cols]

                logger.debug(f"Enrichment: added {org_name_col} column ({df[org_name_col].notna().sum()} resolved)")

        # ── Add TenantName column ────────────────────────────────────
        if tenant_col:
            tenant_name_col = self._pick_new_column_name(df, "TenantName")
            df[tenant_name_col] = df[tenant_col].apply(self._resolve_tenant_name)

            # ★ v10.2: Skip if identical/empty
            resolved_t = df[tenant_name_col].astype(str).str.strip()
            original_t = df[tenant_col].astype(str).str.strip()
            if (resolved_t == '').all() or (resolved_t == original_t).all():
                df = df.drop(columns=[tenant_name_col])
            else:
                cols = list(df.columns)
                tenant_idx = cols.index(tenant_col)
                cols.remove(tenant_name_col)
                cols.insert(tenant_idx + 1, tenant_name_col)
                df = df[cols]

        # ── Fill missing GeoName from metadata (only if explicitly enabled) ─
        if org_col and geo_col and self.config.backfill_geo:
            def fill_geo(row):
                geo = row.get(geo_col)
                if pd.isna(geo) or str(geo).strip() in ("", "nan"):
                    info = self.resolve(row.get(org_col, ""))
                    if info and info.geo:
                        return info.geo
                return geo
            df[geo_col] = df.apply(fill_geo, axis=1)
            logger.debug("Enrichment: geo backfill applied (backfill_geo=True)")

        return df

    def enrich_for_display(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enrichment variant for final UI display.
        Replaces GUID columns with friendly names instead of adding new columns.
        More aggressive — used just before rendering charts/tables.
        """
        if df is None or df.empty or not self._loaded:
            return df

        df = df.copy()

        org_col = self._find_column(df, self.ORG_ID_COLUMNS)
        tenant_col = self._find_column(df, self.TENANT_ID_COLUMNS)

        # ★ v10.2: Remove previously-added name columns (from enrich()) to avoid
        # duplicate columns when both enrich() and enrich_for_display() are called
        for drop_name in ('OrgName', 'orgname', 'TenantName', 'tenantname'):
            if drop_name in df.columns:
                df = df.drop(columns=[drop_name])

        if org_col:
            df[org_col] = df[org_col].apply(
                lambda x: self._resolve_org_name(x) or self._shorten_guid(x)
            )
            # Rename column for clarity
            df = df.rename(columns={org_col: "Organization"})

        if tenant_col:
            df[tenant_col] = df[tenant_col].apply(
                lambda x: self._resolve_tenant_name(x) or self._shorten_guid(x)
            )
            df = df.rename(columns={tenant_col: "Tenant"})

        return df

    # ── Private helpers ──────────────────────────────────────────────

    def _find_column(self, df: pd.DataFrame, candidates: set) -> Optional[str]:
        """Find a column in the DataFrame matching any of the candidate names."""
        for col in df.columns:
            if col.lower() in candidates:
                return col
        return None

    def _pick_new_column_name(self, df: pd.DataFrame, preferred: str) -> str:
        """Pick a column name that doesn't conflict with existing columns."""
        if preferred not in df.columns:
            return preferred
        for i in range(1, 10):
            alt = f"{preferred}_{i}"
            if alt not in df.columns:
                return alt
        return preferred

    def _resolve_org_name(self, org_id) -> str:
        """Resolve org ID to friendly name."""
        if pd.isna(org_id) or not org_id:
            return ""
        info = self.resolve(str(org_id))
        if info and info.org_name:
            return info.org_name
        return ""

    def _resolve_tenant_name(self, tenant_id) -> str:
        """Resolve tenant ID to name."""
        if pd.isna(tenant_id) or not tenant_id:
            return ""
        tid = str(tenant_id).strip().lower()
        # Check tenant lookup
        name = self._tenant_lookup.get(tid, "")
        if name:
            return name
        # Check if any org has this tenant with a name
        for info in self._lookup.values():
            if info.tenant_id.lower() == tid and info.tenant_name:
                return info.tenant_name
        return ""

    @staticmethod
    def _shorten_guid(val) -> str:
        """Shorten a GUID for display."""
        s = str(val).strip()
        if len(s) > 12 and re.match(r'^[a-f0-9-]+$', s, re.IGNORECASE):
            return s[:8] + "..."
        return s

    # ── Status ───────────────────────────────────────────────────────

    def get_status(self) -> Dict[str, Any]:
        """Return enrichment status for logging/UI."""
        return {
            "source": self.config.source,
            "loaded": self._loaded,
            "org_count": len(self._lookup),
            "tenant_count": len(self._tenant_lookup),
            "path": self.config.path if self.config.source == "csv" else "",
            "cluster": self.config.cluster if self.config.source == "kusto" else "",
        }

    def get_status_line(self) -> str:
        """One-line status for sidebar display."""
        if not self._loaded or self.config.source == "none":
            return "Enrichment: off (raw GUIDs)"
        return f"Enrichment: {len(self._lookup)} orgs mapped ({self.config.source})"
