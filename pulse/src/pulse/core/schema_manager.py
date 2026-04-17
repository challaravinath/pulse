"""
Schema Manager - Auto-Discover Table Schemas
=============================================

Automatically fetches schema from Kusto clusters and saves to config.
Teams only need to add hints for important columns.

Usage:
    pulse discover-schema example.yaml

Author: PULSE Team
"""

import yaml
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple
from azure.kusto.data.helpers import dataframe_from_result_table
from .auth_manager import KustoAuthManager
from .config_loader import ConfigLoader

logger = logging.getLogger(__name__)


class SchemaManager:
    """Manages schema discovery and caching"""
    
    def __init__(self):
        self.type_mapping = {
            'System.DateTime': 'datetime',
            'System.String': 'string',
            'System.Int32': 'int',
            'System.Int64': 'int',
            'System.Double': 'decimal',
            'System.Boolean': 'bool',
            'System.Guid': 'guid',
            'System.Dynamic': 'dynamic'
        }
    
    def discover_and_save(self, config_path: str) -> Dict[str, any]:
        """
        Main entry point - discovers schema and saves to config
        
        Args:
            config_path: Path to config file (e.g., 'configs/example.yaml')
        
        Returns:
            {
                'success': bool,
                'columns_discovered': int,
                'clusters_scanned': int,
                'message': str
            }
        """
        
        logger.info(f"Discovering schema for {config_path}")
        
        # Load config
        try:
            config_loader = ConfigLoader()
            config = config_loader.load_config(config_path)
        except Exception as e:
            return {
                'success': False,
                'columns_discovered': 0,
                'clusters_scanned': 0,
                'message': f"Failed to load config: {str(e)}"
            }
        
        # Discover schema from first cluster
        try:
            schema = self._discover_schema(config)
        except Exception as e:
            return {
                'success': False,
                'columns_discovered': 0,
                'clusters_scanned': 0,
                'message': f"Failed to discover schema: {str(e)}"
            }
        
        # Validate schema across all clusters
        try:
            validation = self._validate_schema_across_clusters(config, schema)
        except Exception as e:
            logger.warning(f"Schema validation failed: {e}")
            validation = {'all_match': False, 'warnings': [str(e)]}
        
        # Save to config
        try:
            self._save_schema_to_config(config_path, schema, validation)
        except Exception as e:
            return {
                'success': False,
                'columns_discovered': len(schema),
                'clusters_scanned': len(config.clusters),
                'message': f"Failed to save schema: {str(e)}"
            }
        
        message = f"✓ Discovered {len(schema)} columns from {len(config.clusters)} cluster(s)"
        if not validation['all_match']:
            message += f"\n⚠️ {len(validation.get('warnings', []))} warning(s) - schemas may differ across clusters"
        
        return {
            'success': True,
            'columns_discovered': len(schema),
            'clusters_scanned': len(config.clusters),
            'message': message,
            'warnings': validation.get('warnings', [])
        }
    
    def _discover_schema(self, config) -> Dict[str, str]:
        """Discover schema from first cluster"""
        
        cluster = config.clusters[0]
        logger.info(f"Discovering schema from {cluster.name}...")
        
        # Create Kusto client
        client = KustoAuthManager.create_client(
            cluster_url=cluster.url,
            auth_method=config.auth_method,
            client_id=config.auth_client_id,
            client_secret=None,
            tenant_id=config.auth_tenant_id
        )
        
        # Execute getschema query
        query = f"{cluster.table} | getschema"
        result = client.execute(cluster.database, query)
        df = dataframe_from_result_table(result.primary_results[0])
        
        # Convert to schema dict
        schema = {}
        for _, row in df.iterrows():
            col_name = row['ColumnName']
            col_type = row['DataType']
            
            # Map Kusto type to simple type
            simple_type = self.type_mapping.get(col_type, 'string')
            schema[col_name] = simple_type
        
        logger.info(f"✓ Found {len(schema)} columns")
        return schema
    
    def _validate_schema_across_clusters(self, config, reference_schema: Dict[str, str]) -> Dict:
        """Validate that all clusters have the same schema"""
        
        if len(config.clusters) == 1:
            return {'all_match': True, 'warnings': []}
        
        warnings = []
        
        for cluster in config.clusters[1:]:
            try:
                logger.info(f"Validating schema on {cluster.name}...")
                
                client = KustoAuthManager.create_client(
                    cluster_url=cluster.url,
                    auth_method=config.auth_method,
                    client_id=config.auth_client_id,
                    client_secret=None,
                    tenant_id=config.auth_tenant_id
                )
                
                query = f"{cluster.table} | getschema"
                result = client.execute(cluster.database, query)
                df = dataframe_from_result_table(result.primary_results[0])
                
                cluster_schema = {}
                for _, row in df.iterrows():
                    col_name = row['ColumnName']
                    col_type = row['DataType']
                    simple_type = self.type_mapping.get(col_type, 'string')
                    cluster_schema[col_name] = simple_type
                
                # Compare schemas
                ref_cols = set(reference_schema.keys())
                cluster_cols = set(cluster_schema.keys())
                
                missing = ref_cols - cluster_cols
                extra = cluster_cols - ref_cols
                
                if missing:
                    warnings.append(f"{cluster.name}: Missing columns: {', '.join(list(missing)[:5])}")
                if extra:
                    warnings.append(f"{cluster.name}: Extra columns: {', '.join(list(extra)[:5])}")
                
                if not missing and not extra:
                    logger.info(f"✓ {cluster.name} schema matches")
            
            except Exception as e:
                warnings.append(f"{cluster.name}: Could not validate - {str(e)}")
        
        return {
            'all_match': len(warnings) == 0,
            'warnings': warnings
        }
    
    def _save_schema_to_config(self, config_path: str, schema: Dict[str, str], validation: Dict):
        """Save discovered schema to config file"""
        
        # Load existing config
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        
        # Add schema section
        config_data['schema'] = {
            'discovered_at': datetime.now().isoformat(),
            'columns': schema,
            'validation': {
                'all_clusters_match': validation['all_match'],
                'warnings': validation.get('warnings', [])
            }
        }
        
        # Add column_hints section if it doesn't exist
        if 'column_hints' not in config_data:
            config_data['column_hints'] = {
                '# Add hints for important columns here': None,
                '# Example:': None,
                '# OrgId': 'Organization ID - use for grouping by org',
                '# EventInfo_Time': 'Event timestamp - use for date filters'
            }
        
        # Save back to file
        with open(config_path, 'w') as f:
            yaml.dump(config_data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
        
        logger.info(f"✓ Saved schema to {config_path}")
    
    def get_schema_context(self, config_path: str) -> str:
        """
        Get schema context for LLM (combines discovered schema + user hints)
        
        Returns formatted string with all columns and hints
        """
        
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        
        schema = config_data.get('schema', {}).get('columns', {})
        hints = config_data.get('column_hints', {})
        
        if not schema:
            return "No schema discovered yet. Run: pulse discover-schema <config-name>"
        
        lines = ["Available columns:\n"]
        
        # Important columns first (those with hints)
        if hints:
            lines.append("KEY COLUMNS:")
            for col, hint in hints.items():
                if col.startswith('#'):  # Skip comments
                    continue
                if col in schema:
                    lines.append(f"- {col} ({schema[col]}): {hint}")
            lines.append("")
        
        # All columns
        lines.append(f"ALL COLUMNS ({len(schema)} total):")
        for col, col_type in schema.items():
            if col not in hints:  # Don't repeat key columns
                lines.append(f"- {col} ({col_type})")
        
        return "\n".join(lines)


def main():
    """CLI entry point"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python -m pulse.core.schema_manager <config-file>")
        print("Example: python -m pulse.core.schema_manager configs/example.yaml")
        sys.exit(1)
    
    config_path = sys.argv[1]
    
    if not Path(config_path).exists():
        print(f"Error: Config file not found: {config_path}")
        sys.exit(1)
    
    print("=" * 60)
    print("PULSE Schema Discovery")
    print("=" * 60)
    print()
    
    manager = SchemaManager()
    result = manager.discover_and_save(config_path)
    
    print(result['message'])
    
    if result.get('warnings'):
        print("\nWarnings:")
        for warning in result['warnings']:
            print(f"  ⚠️ {warning}")
    
    print()
    print("=" * 60)
    print("Next steps:")
    print(f"1. Open {config_path}")
    print("2. Add hints for important columns in the 'column_hints' section")
    print("3. Restart PULSE")
    print("=" * 60)
    
    sys.exit(0 if result['success'] else 1)


if __name__ == '__main__':
    main()
