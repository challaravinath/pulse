"""
Config Loader - Discovers and validates data source configs
============================================================

Features:
- Auto-discovers configs from configs/ directory
- Validates all fields
- Auto-infers strategy for single cluster
- Loads default schema + custom additions
- Zero hallucination architecture

Author: PULSE Team
"""

import yaml
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class StrategyType(Enum):
    """How to combine multi-cluster results"""
    UNION = "union"
    SINGLE = "single"
    LABELED_UNION = "labeled_union"


class AuthMethod(Enum):
    """Supported authentication methods"""
    AZURE_CLI = "azure_cli"
    MANAGED_IDENTITY = "managed_identity"
    SERVICE_PRINCIPAL = "service_principal"


@dataclass
class ClusterConfig:
    """Single cluster configuration"""
    name: str
    url: str
    database: str
    table: str
    region: Optional[str] = None
    
    def validate(self) -> List[str]:
        """Validate cluster config"""
        errors = []
        
        if not self.url.startswith("https://"):
            errors.append(f"Cluster '{self.name}': URL must start with https://")
        
        if not ".kusto.windows.net" in self.url:
            errors.append(f"Cluster '{self.name}': URL must contain .kusto.windows.net")
        
        if not self.database:
            errors.append(f"Cluster '{self.name}': Database name required")
        
        if not self.table:
            errors.append(f"Cluster '{self.name}': Table name required")
        
        return errors


@dataclass
class ColumnDef:
    """Column definition"""
    name: str
    type: str
    description: str = ""


@dataclass
class DataSourceConfig:
    """Complete data source configuration"""
    
    # Metadata
    id: str
    name: str
    description: str
    owner: str
    version: str
    
    # Clusters (the backbone)
    clusters: List[ClusterConfig]
    
    # Strategy (optional for single cluster)
    strategy: Optional[StrategyType] = None
    
    # Authentication
    auth_method: AuthMethod = AuthMethod.AZURE_CLI
    auth_client_id: Optional[str] = None
    auth_tenant_id: Optional[str] = None
    
    # Filters (always applied)
    mandatory_filters: List[str] = field(default_factory=list)
    
    # Columns (default + custom)
    default_columns: List[ColumnDef] = field(default_factory=list)
    custom_columns: List[ColumnDef] = field(default_factory=list)
    source_path: Optional[str] = None  # Track which file this was loaded from
    enrichment: Dict[str, Any] = field(default_factory=dict)  # Org metadata enrichment config
    
    def get_effective_strategy(self) -> StrategyType:
        """Get strategy that will actually be used"""
        if len(self.clusters) == 1:
            return StrategyType.SINGLE  # Auto-inferred
        elif self.strategy:
            return self.strategy
        else:
            raise ValueError("Strategy required for multiple clusters")
    
    def get_all_columns(self) -> List[ColumnDef]:
        """Get all columns (default + custom)"""
        return self.default_columns + self.custom_columns
    
    def validate(self) -> List[str]:
        """Comprehensive validation"""
        errors = []
        
        # Validate metadata
        if not self.id or not self.id.replace('-', '').replace('_', '').islower():
            errors.append(f"Invalid id: '{self.id}' (use lowercase-hyphen format)")
        
        if not self.owner or '@' not in self.owner:
            errors.append(f"Invalid owner email: '{self.owner}'")
        
        if not self.version or len(self.version.split('.')) != 3:
            errors.append(f"Invalid version: '{self.version}' (use X.Y.Z format)")
        
        # Validate clusters
        if not self.clusters:
            errors.append("At least one cluster required")
        
        if len(self.clusters) > 20:
            errors.append(f"Too many clusters: {len(self.clusters)} (max 20)")
        
        for cluster in self.clusters:
            errors.extend(cluster.validate())
        
        # Validate strategy based on cluster count
        if len(self.clusters) == 1:
            if self.strategy and self.strategy != StrategyType.SINGLE:
                logger.warning(
                    f"Config '{self.id}': Strategy '{self.strategy.value}' with 1 cluster "
                    f"is meaningless. Will use 'single' automatically."
                )
        elif len(self.clusters) > 1:
            if not self.strategy:
                errors.append(
                    f"Strategy required for {len(self.clusters)} clusters. "
                    f"Specify: 'union', 'single', or 'labeled_union'"
                )
        
        # Validate filters don't contain dangerous commands
        dangerous = ['.drop', '.delete', '.alter', '.create', 'cluster(']
        for filter_expr in self.mandatory_filters:
            filter_lower = filter_expr.lower()
            for cmd in dangerous:
                if cmd in filter_lower:
                    errors.append(f"Dangerous command '{cmd}' in filter: '{filter_expr}'")
        
        # Validate auth
        if self.auth_method == AuthMethod.SERVICE_PRINCIPAL:
            if not self.auth_client_id:
                errors.append("Service Principal auth requires client_id")
            if not self.auth_tenant_id:
                errors.append("Service Principal auth requires tenant_id")
        
        return errors


class ConfigLoader:
    """
    Discovers and loads data source configs
    
    Features:
    - Auto-discovers configs from directory
    - Loads default schema automatically
    - Validates all configs
    - Auto-infers strategy for single cluster
    """
    
    def __init__(self, config_dir: str = "./configs", defaults_file: str = "./system_defaults.yaml"):
        self.config_dir = Path(config_dir)
        self.defaults_file = Path(defaults_file)
        self.loaded_configs: List[DataSourceConfig] = []
        self.default_columns: List[ColumnDef] = []
        
        # Load default schema
        self._load_default_schema()
    
    def _load_default_schema(self) -> None:
        """Load system default columns"""
        if not self.defaults_file.exists():
            logger.warning(f"Default schema file not found: {self.defaults_file}")
            return
        
        try:
            with open(self.defaults_file, 'r', encoding='utf-8') as f:
                defaults = yaml.safe_load(f)
            
            for col in defaults.get('default_columns', []):
                self.default_columns.append(ColumnDef(
                    name=col['name'],
                    type=col['type'],
                    description=col.get('description', '')
                ))
            
            logger.info(f"Loaded {len(self.default_columns)} default columns")
        
        except Exception as e:
            logger.error(f"Failed to load default schema: {e}")
    
    def discover_and_load(self) -> List[DataSourceConfig]:
        """Discover all config files and load them"""
        
        if not self.config_dir.exists():
            logger.warning(f"Config directory not found: {self.config_dir}")
            self.config_dir.mkdir(parents=True, exist_ok=True)
            return []
        
        config_files = list(self.config_dir.glob("*.yaml")) + list(self.config_dir.glob("*.yml"))
        
        if not config_files:
            logger.warning(f"No config files found in {self.config_dir}")
            return []
        
        logger.info(f"Discovering configs in {self.config_dir}...")
        logger.info(f"Found {len(config_files)} config file(s)")
        
        loaded = []
        for config_file in config_files:
            try:
                config = self.load_config(config_file)
                if config:
                    loaded.append(config)
                    logger.info(f"✓ Loaded: {config.name} ({config_file.name})")
            except Exception as e:
                logger.error(f"✗ Failed to load {config_file.name}: {e}")
        
        self.loaded_configs = loaded
        logger.info(f"Successfully loaded {len(loaded)} data source(s)")
        return loaded
    
    def load_config(self, config_file: Path) -> Optional[DataSourceConfig]:
        """Load and validate a single config file"""
        
        with open(config_file, 'r', encoding='utf-8') as f:
            raw_config = yaml.safe_load(f)
        
        # Parse config
        config = self._parse_config(raw_config)
        config.source_path = str(config_file)  # Track source file
        
        # Add default columns
        config.default_columns = self.default_columns.copy()
        
        # Validate
        errors = config.validate()
        if errors:
            error_msg = "\n  - ".join(errors)
            raise ValueError(f"Validation failed:\n  - {error_msg}")
        
        return config
    
    def _parse_config(self, raw: Dict[str, Any]) -> DataSourceConfig:
        """Parse raw YAML into DataSourceConfig"""
        
        # Required fields
        metadata = raw.get('metadata', {})
        clusters_raw = raw.get('clusters', [])
        strategy_raw = raw.get('strategy')
        auth_raw = raw.get('authentication', {})
        
        # Parse clusters
        clusters = []
        for c in clusters_raw:
            clusters.append(ClusterConfig(
                name=c.get('name', 'Unknown'),
                url=c.get('url', ''),
                database=c.get('database', ''),
                table=c.get('table', ''),
                region=c.get('region')
            ))
        
        # Parse strategy (optional)
        strategy = None
        if strategy_raw:
            try:
                strategy = StrategyType(strategy_raw)
            except ValueError:
                raise ValueError(f"Invalid strategy: '{strategy_raw}' (use: union, single, labeled_union)")
        
        # Parse auth
        auth_method_str = auth_raw.get('method', 'azure_cli')
        try:
            auth_method = AuthMethod(auth_method_str)
        except ValueError:
            raise ValueError(f"Invalid auth method: '{auth_method_str}'")
        
        # Parse custom columns
        custom_columns = []
        llm_context = raw.get('llm_context', {})
        for col in llm_context.get('additional_columns', []):
            custom_columns.append(ColumnDef(
                name=col.get('name', ''),
                type=col.get('type', 'string'),
                description=col.get('description', '')
            ))
        
        # Optional fields
        filters_raw = raw.get('filters', {})
        
        return DataSourceConfig(
            id=metadata.get('id', ''),
            name=metadata.get('name', 'Unnamed'),
            description=metadata.get('description', ''),
            owner=metadata.get('owner', ''),
            version=metadata.get('version', '1.0.0'),
            clusters=clusters,
            strategy=strategy,
            auth_method=auth_method,
            auth_client_id=auth_raw.get('client_id'),
            auth_tenant_id=auth_raw.get('tenant_id'),
            mandatory_filters=filters_raw.get('mandatory', []),
            custom_columns=custom_columns,
            enrichment=raw.get('enrichment', {}),
        )
    
    def get_config_by_id(self, config_id: str) -> Optional[DataSourceConfig]:
        """Get a specific config by ID"""
        for config in self.loaded_configs:
            if config.id == config_id:
                return config
        return None
    
    def get_all_configs(self) -> List[DataSourceConfig]:
        """Get all loaded configs"""
        return self.loaded_configs
