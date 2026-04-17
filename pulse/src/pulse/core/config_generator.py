"""
Config Generator v1.0 — Auto-Discover & Generate Config
==========================================================

Given minimal input (cluster URL, database, table, auth),
connects to the cluster, discovers the schema, analyzes
column cardinality, and generates a complete PULSE config.

New team onboarding:
  1. Provide: cluster URL, database, table
  2. Run generator → outputs full config YAML
  3. Team reviews, tweaks, done

Author: PULSE Team
"""

import yaml
import logging
import os
from typing import Dict, List, Tuple, Optional, Any, Callable
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


# ── Column Classification Rules ──────────────────────────────────────────────

# Patterns that strongly indicate time columns
TIME_PATTERNS = ['time', 'date', 'timestamp', 'created', 'modified', 'updated']

# Patterns for ID columns (potential dcount metrics + grouping dimensions)
ID_PATTERNS = ['id', 'guid', 'key', 'identifier']

# Patterns for columns that are good group-by dimensions
DIMENSION_PATTERNS = [
    'name', 'type', 'category', 'status', 'level', 'tier',
    'region', 'geo', 'country', 'city', 'locale', 'language',
    'browser', 'os', 'device', 'platform', 'client',
    'module', 'component', 'feature', 'entity', 'action',
    'version', 'environment', 'channel', 'source', 'origin',
]

# Columns to skip entirely (internal/system columns)
# These are EXACT patterns matched against the full lowercase column name
SKIP_PATTERNS = [
    'correlationid', 'externalcorrelation', 'activityid',
    'pipelineinfo_ingestion', 'ingestiontime', 'internaltime',
    'clientip', 'useragent', 'sdkversion',
    'station', 'island',
]

# Patterns for columns likely to be metric-worthy IDs (dcount)
METRIC_ID_PATTERNS = [
    ('org', 'Active Organizations'),
    ('user', 'Active Users'),
    ('session', 'Sessions'),
    ('tenant', 'Active Tenants'),
    ('device', 'Active Devices'),
    ('account', 'Active Accounts'),
]


@dataclass
class ColumnInfo:
    """Discovered column with classification."""
    name: str
    data_type: str  # datetime, string, int, long, guid, etc.
    cardinality: int = 0  # dcount result
    sample_values: List[str] = field(default_factory=list)
    classification: str = 'skip'  # time, metric_id, dimension, numeric, skip


@dataclass
class GeneratedConfig:
    """The output of auto-discovery."""
    metrics: Dict[str, Dict]
    dimensions: Dict[str, Dict]
    time_column: str
    all_columns: Dict[str, ColumnInfo]
    yaml_text: str


class ConfigGenerator:
    """
    Auto-discovers schema and generates a complete PULSE config.

    Usage:
        gen = ConfigGenerator()
        result = gen.discover_and_generate(
            cluster_url="https://xyz.kusto.windows.net",
            database="MyDB",
            table="my_telemetry",
            auth_method="azure_cli",
        )
        print(result.yaml_text)  # Complete YAML ready to save
    """

    TYPE_MAP = {
        'System.DateTime': 'datetime',
        'System.String': 'string',
        'System.Int32': 'int',
        'System.Int64': 'long',
        'System.Double': 'decimal',
        'System.Boolean': 'bool',
        'System.Guid': 'guid',
        'System.Dynamic': 'dynamic',
        'System.TimeSpan': 'timespan',
    }

    def discover_and_generate(
        self,
        cluster_url: str,
        database: str,
        table: str,
        auth_method: str = "azure_cli",
        team_name: str = "My Team",
        owner_email: str = "team@example.com",
        client_id: str = None,
        tenant_id: str = None,
        progress_callback: Optional[Callable] = None,
    ) -> GeneratedConfig:
        """
        Main entry point. Connects, discovers, classifies, generates.

        Args:
            cluster_url: Kusto cluster URL
            database: Database name
            table: Table name
            auth_method: azure_cli | managed_identity | service_principal
            team_name: Display name for the config
            owner_email: Team owner email
            progress_callback: Optional fn(step, total, message)

        Returns:
            GeneratedConfig with the complete YAML
        """
        total_steps = 4

        # ── Step 1: Connect & get schema ──────────────────────────
        self._progress(progress_callback, 1, total_steps,
                       "Connecting & discovering schema…")

        client = self._create_client(cluster_url, auth_method, client_id, tenant_id)
        columns = self._discover_schema(client, database, table)
        logger.info(f"Discovered {len(columns)} columns")

        # ── Step 2: Check cardinality of promising columns ────────
        self._progress(progress_callback, 2, total_steps,
                       f"Analyzing {len(columns)} columns (checking cardinality)…")

        columns = self._analyze_cardinality(client, database, table, columns)

        # ── Step 3: Get sample values for dimensions ──────────────
        self._progress(progress_callback, 3, total_steps,
                       "Fetching sample values…")

        columns = self._fetch_samples(client, database, table, columns)

        # ── Step 4: Classify & generate config ────────────────────
        self._progress(progress_callback, 4, total_steps,
                       "Generating config…")

        columns = self._classify_columns(columns)
        config = self._generate_config(
            columns, cluster_url, database, table,
            auth_method, team_name, owner_email,
        )

        self._progress(progress_callback, total_steps, total_steps,
                       f"✓ Config generated: {len(config.metrics)} metrics, "
                       f"{len(config.dimensions)} dimensions")

        return config

    # ═══════════════════════════════════════════════════════════════════════════
    # Step 1: Schema Discovery
    # ═══════════════════════════════════════════════════════════════════════════

    def _create_client(self, cluster_url, auth_method, client_id=None, tenant_id=None):
        from .auth_manager import KustoAuthManager
        from .config_loader import AuthMethod

        method_map = {
            'azure_cli': AuthMethod.AZURE_CLI,
            'managed_identity': AuthMethod.MANAGED_IDENTITY,
            'service_principal': AuthMethod.SERVICE_PRINCIPAL,
        }
        auth = method_map.get(auth_method, AuthMethod.AZURE_CLI)

        return KustoAuthManager.create_client(
            cluster_url=cluster_url,
            auth_method=auth,
            client_id=client_id,
            client_secret=os.getenv("AZURE_CLIENT_SECRET"),
            tenant_id=tenant_id,
        )

    def _discover_schema(self, client, database, table) -> Dict[str, ColumnInfo]:
        from azure.kusto.data.helpers import dataframe_from_result_table
        query = f"{table} | getschema"
        result = client.execute(database, query)
        df = dataframe_from_result_table(result.primary_results[0])

        columns = {}
        for _, row in df.iterrows():
            name = row.get('ColumnName', '')
            raw_type = row.get('DataType', 'System.String')
            simple_type = self.TYPE_MAP.get(raw_type, 'string')
            columns[name] = ColumnInfo(name=name, data_type=simple_type)

        return columns

    # ═══════════════════════════════════════════════════════════════════════════
    # Step 2: Cardinality Analysis
    # ═══════════════════════════════════════════════════════════════════════════

    def _analyze_cardinality(
        self, client, database, table, columns: Dict[str, ColumnInfo]
    ) -> Dict[str, ColumnInfo]:
        """
        Run dcount on promising string/guid columns to understand cardinality.
        This tells us what's a useful dimension (low cardinality) vs what's an ID
        (high cardinality, useful for dcount metrics).
        """
        from azure.kusto.data.helpers import dataframe_from_result_table
        # Only check string and guid columns (not datetime, int, bool, etc.)
        candidates = [
            c for c in columns.values()
            if c.data_type in ('string', 'guid')
            and not self._should_skip(c.name)
        ]

        if not candidates:
            return columns

        # Build ONE query that dcounts all candidates at once (efficient)
        # Limit to 30 columns to keep query manageable
        batch = candidates[:30]
        dcount_exprs = [f"c_{i}=dcount({c.name})" for i, c in enumerate(batch)]

        query = (
            f"{table}\n"
            f"| where EventInfo_Time > ago(30d) or ingestion_time() > ago(30d)\n"
            f"| summarize {', '.join(dcount_exprs)}\n"
            f"| take 1"
        )

        try:
            # Try with EventInfo_Time first; fall back to just a time limit
            try:
                result = client.execute(database, query)
            except Exception:
                # Column might not exist — try without time filter (limit rows instead)
                query_fallback = (
                    f"{table}\n"
                    f"| sample 1000000\n"
                    f"| summarize {', '.join(dcount_exprs)}"
                )
                result = client.execute(database, query_fallback)

            df = dataframe_from_result_table(result.primary_results[0])

            if not df.empty:
                row = df.iloc[0]
                for i, col in enumerate(batch):
                    try:
                        col.cardinality = int(row[f"c_{i}"])
                    except (KeyError, ValueError, TypeError):
                        col.cardinality = 0

            logger.info(f"Cardinality analyzed for {len(batch)} columns")

        except Exception as e:
            logger.warning(f"Cardinality check failed: {e}")

        return columns

    # ═══════════════════════════════════════════════════════════════════════════
    # Step 3: Sample Values
    # ═══════════════════════════════════════════════════════════════════════════

    def _fetch_samples(
        self, client, database, table, columns: Dict[str, ColumnInfo]
    ) -> Dict[str, ColumnInfo]:
        """Fetch top 5 values for low-cardinality string columns (potential dimensions)."""
        from azure.kusto.data.helpers import dataframe_from_result_table
        # Only sample columns with cardinality 2-100 (likely dimensions)
        sample_candidates = [
            c for c in columns.values()
            if c.data_type in ('string', 'guid')
            and 2 <= c.cardinality <= 100
        ]

        for col in sample_candidates[:10]:  # Max 10 columns
            try:
                query = (
                    f"{table}\n"
                    f"| summarize c=count() by {col.name}\n"
                    f"| order by c desc\n"
                    f"| take 5\n"
                    f"| project {col.name}"
                )
                result = client.execute(database, query)
                df = dataframe_from_result_table(result.primary_results[0])
                col.sample_values = df[col.name].astype(str).tolist()
            except Exception as e:
                logger.debug(f"Sample fetch failed for {col.name}: {e}")

        return columns

    # ═══════════════════════════════════════════════════════════════════════════
    # Step 4: Classification
    # ═══════════════════════════════════════════════════════════════════════════

    def _classify_columns(self, columns: Dict[str, ColumnInfo]) -> Dict[str, ColumnInfo]:
        """Classify each column: time, metric_id, dimension, numeric, or skip."""
        for col in columns.values():
            col.classification = self._classify_one(col)
        return columns

    def _classify_one(self, col: ColumnInfo) -> str:
        name_lower = col.name.lower()

        # Skip system/internal columns
        if self._should_skip(col.name):
            return 'skip'

        # DateTime → time dimension
        if col.data_type == 'datetime':
            if any(p in name_lower for p in TIME_PATTERNS):
                return 'time'
            return 'skip'  # Probably an internal timestamp

        # Boolean → skip (not useful as metric or dimension)
        if col.data_type in ('bool', 'timespan', 'dynamic'):
            return 'skip'

        # Numeric → could be a pre-computed metric
        if col.data_type in ('int', 'long', 'decimal'):
            return 'numeric'

        # String/Guid — classify by cardinality + name patterns
        if col.data_type in ('string', 'guid'):
            # High cardinality ID columns → dcount metrics
            if col.cardinality > 1000 or any(p in name_lower for p in ID_PATTERNS):
                # But check if it's a KNOWN metric-worthy ID
                for pattern, _ in METRIC_ID_PATTERNS:
                    if pattern in name_lower:
                        return 'metric_id'
                # High cardinality but not a known pattern → skip
                if col.cardinality > 10000:
                    return 'skip'
                return 'metric_id'

            # Low cardinality → dimension (good for group-by)
            if 1 < col.cardinality <= 500:
                return 'dimension'

            # Very low cardinality (0-1) → skip (probably empty or constant)
            if col.cardinality <= 1:
                return 'skip'

            # Name matches dimension patterns → dimension even if cardinality unknown
            if any(p in name_lower for p in DIMENSION_PATTERNS):
                return 'dimension'

        return 'skip'

    def _should_skip(self, col_name: str) -> bool:
        name_lower = col_name.lower()
        return any(p in name_lower for p in SKIP_PATTERNS)

    # ═══════════════════════════════════════════════════════════════════════════
    # Step 5: Generate Config
    # ═══════════════════════════════════════════════════════════════════════════

    def _generate_config(
        self,
        columns: Dict[str, ColumnInfo],
        cluster_url: str,
        database: str,
        table: str,
        auth_method: str,
        team_name: str,
        owner_email: str,
    ) -> GeneratedConfig:
        """Build the complete YAML config from classified columns."""

        # ── Find time column ──────────────────────────────────────
        time_cols = [c for c in columns.values() if c.classification == 'time']
        # Prefer "EventInfo_Time" or column with "event" and "time" in name
        time_col = None
        for tc in time_cols:
            if 'event' in tc.name.lower() and 'time' in tc.name.lower():
                time_col = tc
                break
        if not time_col and time_cols:
            time_col = time_cols[0]
        time_col_name = time_col.name if time_col else "Timestamp"

        # ── Build metrics ─────────────────────────────────────────
        metrics = {}

        # Always add events count
        metrics['events'] = {
            'display_name': 'Events',
            'kql': 'count()',
            'description': 'Total telemetry events',
        }

        # Add dcount metrics for ID columns
        metric_cols = [c for c in columns.values() if c.classification == 'metric_id']
        for col in metric_cols:
            metric_id = self._to_metric_id(col.name)
            display_name = self._metric_display_name(col.name)
            metrics[metric_id] = {
                'display_name': display_name,
                'kql': f'dcount({col.name})',
                'description': f'Distinct {display_name.lower()} (cardinality: ~{col.cardinality:,})',
            }

        # ── Build dimensions ──────────────────────────────────────
        dimensions = {}

        # Always add time dimension
        dimensions['time'] = {
            'column': time_col_name,
            'display_name': 'Time',
            'kql_group': f'Day=startofday({time_col_name})',
            'sql_column': 'Day',
            'description': 'Event timestamp',
            'granularities': {
                'day': f'startofday({time_col_name})',
                'week': f'startofweek({time_col_name})',
                'month': f'startofmonth({time_col_name})',
            },
        }

        # Add discovered dimensions
        dim_cols = [c for c in columns.values() if c.classification == 'dimension']
        # Sort by cardinality (low first = more useful as dimensions)
        dim_cols.sort(key=lambda c: c.cardinality)

        for col in dim_cols[:15]:  # Cap at 15 dimensions
            dim_id = self._to_dim_id(col.name)
            display_name = self._to_display_name(col.name, 'dimension')
            samples_comment = ""
            if col.sample_values:
                samples_comment = f" (e.g. {', '.join(col.sample_values[:3])})"

            dimensions[dim_id] = {
                'column': col.name,
                'display_name': display_name,
                'kql_group': col.name,
                'sql_column': col.name,
                'description': f'{display_name}{samples_comment}',
            }

        # ── Also note ID columns as potential dimensions ──────────
        # Only add as dimensions if cardinality is manageable (< 5000)
        # OrgId (~12K) is borderline but useful; UserId/SessionId (~100K+) are not
        DIMENSION_CARDINALITY_LIMIT = 50000
        for col in metric_cols:
            if col.cardinality > DIMENSION_CARDINALITY_LIMIT:
                continue  # Too high cardinality — metric only, not a dimension
            dim_id = self._to_dim_id(col.name)
            if dim_id not in dimensions:
                display_name = self._metric_display_name(col.name)
                # Use singular form for dimension: "Active Organizations" → "Organization"
                dim_display = display_name.replace('Active ', '').rstrip('s')
                dimensions[dim_id] = {
                    'column': col.name,
                    'display_name': dim_display,
                    'kql_group': col.name,
                    'sql_column': col.name,
                    'description': f'{dim_display} (high cardinality: ~{col.cardinality:,})',
                }

        # ── Generate YAML ─────────────────────────────────────────
        config_id = team_name.lower().replace(' ', '-').replace('_', '-')

        config_dict = {
            'metadata': {
                'id': config_id,
                'name': team_name,
                'description': f'Auto-discovered config for {table}. Each row is one telemetry event.',
                'owner': owner_email,
                'version': '1.0.0',
            },
            'clusters': [{
                'name': 'Primary',
                'url': cluster_url,
                'database': database,
                'table': table,
            }],
            'authentication': {'method': auth_method},
            'filters': {'mandatory': []},
            'profile': {
                'time_scope': '365d',
                'time_column': time_col_name,
            },
            'metrics': metrics,
            'dimensions': dimensions,
        }

        # Pretty YAML with comments
        yaml_text = self._render_yaml(config_dict, columns)

        return GeneratedConfig(
            metrics=metrics,
            dimensions=dimensions,
            time_column=time_col_name,
            all_columns=columns,
            yaml_text=yaml_text,
        )

    # ═══════════════════════════════════════════════════════════════════════════
    # Helpers
    # ═══════════════════════════════════════════════════════════════════════════

    def _to_metric_id(self, col_name: str) -> str:
        """OrgId → active_orgs, SessionId → sessions, UserId → active_users"""
        name = col_name.lower()
        for pattern, display in METRIC_ID_PATTERNS:
            if pattern in name:
                return display.lower().replace(' ', '_')
        # Generic: strip 'id', 'guid' suffixes
        clean = name.replace('id', '').replace('guid', '').strip('_')
        return f"active_{clean}s" if clean else f"distinct_{name}"

    def _metric_display_name(self, col_name: str) -> str:
        """OrgId → Active Organizations, SessionId → Sessions"""
        name = col_name.lower()
        for pattern, display in METRIC_ID_PATTERNS:
            if pattern in name:
                return display
        # Generic fallback
        return self._to_display_name(col_name, 'metric')

    def _to_dim_id(self, col_name: str) -> str:
        """GeoName → geo, EntityName → entity, DeviceInfo_BrowserName → browser, OrgId → organization"""
        name = col_name.lower()
        # Remove common prefixes
        for prefix in ['deviceinfo_', 'pipelineinfo_', 'eventinfo_', 'userinfo_', 'appinfo_']:
            name = name.replace(prefix, '')
        # Remove common suffixes
        name = name.replace('name', '').replace('id', '').replace('guid', '').strip('_')
        # Handle type suffix
        if name.endswith('type'):
            name = name[:-4].strip('_') + '_type' if name != 'type' else 'type'
        # Map known short names to better IDs
        dim_id_map = {'org': 'organization', 'geo': 'region', 'user': 'user', 'session': 'session'}
        return dim_id_map.get(name, name) if name else col_name.lower()

    def _to_display_name(self, col_name: str, kind: str) -> str:
        """Convert column name to human-readable display name."""
        name = col_name
        # Remove common prefixes
        for prefix in ['DeviceInfo_', 'PipelineInfo_', 'EventInfo_', 'UserInfo_', 'AppInfo_']:
            name = name.replace(prefix, '')

        # Split on camelCase and underscore
        import re
        parts = re.findall(r'[A-Z][a-z]+|[a-z]+|[A-Z]+(?=[A-Z]|$)', name)
        if not parts:
            parts = name.split('_')

        display = ' '.join(p.capitalize() for p in parts if p.lower() not in ('id', 'guid', 'info'))
        return display if display else col_name

    def _render_yaml(self, config: Dict, columns: Dict[str, ColumnInfo]) -> str:
        """Render config dict to YAML with helpful comments."""
        lines = [
            "# -----------------------------------------------------------",
            f"# PULSE Config - {config['metadata']['name']}",
            "# -----------------------------------------------------------",
            "#",
            "# AUTO-GENERATED by PULSE Config Generator",
            "# Review and adjust metrics/dimensions as needed.",
            "#",
            "# Profile queries are auto-generated from metrics x dimensions.",
            "# Add/remove metrics and dimensions to customize the profile.",
            "# -----------------------------------------------------------",
            "",
        ]

        # Dump the config body
        yaml_body = yaml.dump(
            config, default_flow_style=False, sort_keys=False, allow_unicode=False,
        )
        lines.append(yaml_body)

        # Add a commented section showing ALL discovered columns
        lines.append("")
        lines.append("# -----------------------------------------------------------")
        lines.append(f"# ALL DISCOVERED COLUMNS ({len(columns)} total)")
        lines.append("# -----------------------------------------------------------")
        lines.append("# Classification: [+] = included, [~] = available, [-] = skipped")
        lines.append("#")

        for col in sorted(columns.values(), key=lambda c: c.name):
            if col.classification in ('metric_id', 'dimension', 'time'):
                symbol = "[+]"
            elif col.classification == 'numeric':
                symbol = "[~]"
            else:
                symbol = "[-]"

            card_str = f" (cardinality: {col.cardinality:,})" if col.cardinality > 0 else ""
            samples_str = ""
            if col.sample_values:
                samples_str = f" - e.g. {', '.join(col.sample_values[:3])}"

            lines.append(
                f"# {symbol} {col.name} ({col.data_type}){card_str}{samples_str}"
            )

        return "\n".join(lines)

    @staticmethod
    def _progress(callback, step, total, message):
        if callback:
            callback(step, total, message)
        logger.info(f"[{step}/{total}] {message}")
