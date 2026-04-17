#!/usr/bin/env python3
"""
PULSE Schema Discovery CLI
===========================

Auto-discover table schemas from Kusto clusters.

Usage:
    python discover_schema.py <config-file>

Example:
    python discover_schema.py configs/example.yaml

Author: PULSE Team
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from pulse.core.schema_manager import SchemaManager
from pulse.utils.logger import setup_logging

def main():
    """CLI entry point"""
    
    if len(sys.argv) < 2:
        print("=" * 60)
        print("PULSE Schema Discovery")
        print("=" * 60)
        print()
        print("Usage:")
        print("  python discover_schema.py <config-file>")
        print()
        print("Example:")
        print("  python discover_schema.py configs/example.yaml")
        print()
        print("=" * 60)
        sys.exit(1)
    
    config_path = sys.argv[1]
    
    if not Path(config_path).exists():
        print(f"❌ Error: Config file not found: {config_path}")
        sys.exit(1)
    
    # Setup logging
    setup_logging()
    
    print("=" * 60)
    print("PULSE Schema Discovery")
    print("=" * 60)
    print()
    print(f"Config: {config_path}")
    print()
    
    manager = SchemaManager()
    result = manager.discover_and_save(config_path)
    
    print(result['message'])
    
    if result.get('warnings'):
        print("\n⚠️ Warnings:")
        for warning in result['warnings']:
            print(f"  • {warning}")
    
    print()
    print("=" * 60)
    print("✅ Next Steps:")
    print("=" * 60)
    print(f"1. Open {config_path}")
    print("2. Review the auto-discovered schema")
    print("3. Add hints in 'column_hints' section for important columns")
    print("4. Restart PULSE to use the new schema")
    print()
    print("Example hint:")
    print("  column_hints:")
    print("    EventInfo_Time: 'Use this for date filters'")
    print("    OrgId: 'Organization ID for grouping'")
    print("=" * 60)
    
    sys.exit(0 if result['success'] else 1)


if __name__ == '__main__':
    main()
