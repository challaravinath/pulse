"""
Schema Validator - Validates and Suggests Column Names
=======================================================

Features:
- Validates column names in KQL
- Fuzzy matching for typos
- Helpful error messages with suggestions

Author: PULSE Team
"""

import re
import difflib
import logging
from typing import List, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


class ColumnSuggestion:
    """Column suggestion with confidence"""
    def __init__(self, column: str, confidence: float):
        self.column = column
        self.confidence = confidence


class SchemaValidator:
    """
    Validates KQL queries against schema and provides helpful suggestions
    """
    
    def __init__(self, schema: Dict[str, str]):
        """
        Args:
            schema: Dict mapping column names to types
        """
        self.schema = schema
        self.column_names = list(schema.keys())
        self.column_names_lower = [c.lower() for c in self.column_names]
    
    def validate_kql(self, kql: str) -> Tuple[bool, Optional[str], List[Dict]]:
        """
        Validate KQL query against schema
        
        Returns:
            (is_valid, error_message, suggestions)
        """
        
        # Extract column names from KQL
        columns_in_query = self._extract_columns_from_kql(kql)
        
        invalid_columns = []
        suggestions_list = []
        
        for col in columns_in_query:
            if not self._is_valid_column(col):
                # Find suggestions
                suggestions = self._find_suggestions(col)
                
                invalid_columns.append(col)
                suggestions_list.append({
                    'invalid': col,
                    'suggestions': [s.column for s in suggestions[:3]]
                })
        
        if invalid_columns:
            error_msg = self._format_error_message(invalid_columns, suggestions_list)
            return False, error_msg, suggestions_list
        
        return True, None, []
    
    def _extract_columns_from_kql(self, kql: str) -> List[str]:
        """
        Extract column names from KQL query
        
        Looks for:
        - summarize ... by ColumnName
        - where ColumnName == ...
        - project ColumnName, ...
        - extend ColumnName = ...
        - order by ColumnName
        """
        
        columns = set()
        
        # Pattern: word characters that start with uppercase or after operators
        # This is a simple heuristic - KQL parser would be better
        
        # After 'by'
        by_pattern = r'\bby\s+([A-Za-z_][A-Za-z0-9_]*)'
        columns.update(re.findall(by_pattern, kql))
        
        # After 'where'
        where_pattern = r'\bwhere\s+([A-Za-z_][A-Za-z0-9_]*)'
        columns.update(re.findall(where_pattern, kql))
        
        # In project list
        project_pattern = r'\bproject\s+([A-Za-z_][A-Za-z0-9_]*(?:\s*,\s*[A-Za-z_][A-Za-z0-9_]*)*)'
        project_matches = re.findall(project_pattern, kql)
        for match in project_matches:
            cols = [c.strip() for c in match.split(',')]
            columns.update(cols)
        
        # After order by
        order_pattern = r'\border\s+by\s+([A-Za-z_][A-Za-z0-9_]*)'
        columns.update(re.findall(order_pattern, kql))
        
        # Common aggregation functions
        agg_pattern = r'(?:count|sum|avg|min|max|dcount)\(([A-Za-z_][A-Za-z0-9_]*)\)'
        columns.update(re.findall(agg_pattern, kql))
        
        # Filter out KQL keywords
        kql_keywords = {
            'count', 'sum', 'avg', 'min', 'max', 'dcount', 'by', 'where',
            'project', 'extend', 'order', 'take', 'limit', 'top', 'summarize',
            'join', 'union', 'let', 'as', 'on', 'asc', 'desc', 'and', 'or',
            'ago', 'startofday', 'endofday', 'bin', 'between', 'in', 'has'
        }
        
        columns = {c for c in columns if c.lower() not in kql_keywords}
        
        return list(columns)
    
    def _is_valid_column(self, column: str) -> bool:
        """Check if column exists in schema"""
        # Exact match
        if column in self.column_names:
            return True
        
        # Case-insensitive match
        if column.lower() in self.column_names_lower:
            return True
        
        return False
    
    def find_column(self, requested: str) -> Optional[str]:
        """
        Find exact column name (case-insensitive)
        
        Returns the actual column name from schema
        """
        # Exact match
        if requested in self.column_names:
            return requested
        
        # Case-insensitive
        requested_lower = requested.lower()
        for i, col_lower in enumerate(self.column_names_lower):
            if col_lower == requested_lower:
                return self.column_names[i]
        
        return None
    
    def _find_suggestions(self, invalid_column: str) -> List[ColumnSuggestion]:
        """Find similar column names using fuzzy matching"""
        
        suggestions = []
        
        # Use difflib for fuzzy matching
        matches = difflib.get_close_matches(
            invalid_column,
            self.column_names,
            n=5,
            cutoff=0.6
        )
        
        for match in matches:
            # Calculate similarity score
            similarity = difflib.SequenceMatcher(
                None,
                invalid_column.lower(),
                match.lower()
            ).ratio()
            
            suggestions.append(ColumnSuggestion(match, similarity))
        
        # Sort by confidence
        suggestions.sort(key=lambda x: x.confidence, reverse=True)
        
        # If no good fuzzy matches, suggest columns with similar prefix
        if not suggestions or suggestions[0].confidence < 0.7:
            prefix = invalid_column[:3].lower()
            prefix_matches = [
                c for c in self.column_names
                if c.lower().startswith(prefix)
            ]
            
            for match in prefix_matches[:3]:
                if match not in [s.column for s in suggestions]:
                    suggestions.append(ColumnSuggestion(match, 0.5))
        
        return suggestions
    
    def _format_error_message(
        self,
        invalid_columns: List[str],
        suggestions_list: List[Dict]
    ) -> str:
        """Format user-friendly error message"""
        
        lines = []
        
        if len(invalid_columns) == 1:
            col = invalid_columns[0]
            sugg = suggestions_list[0]['suggestions']
            
            if sugg:
                lines.append(f"❌ Column '{col}' not found.")
                lines.append(f"💡 Did you mean: {', '.join(sugg)}?")
            else:
                lines.append(f"❌ Column '{col}' not found.")
                lines.append(f"💡 Available columns: {', '.join(self.column_names[:5])}")
                if len(self.column_names) > 5:
                    lines.append(f"   ... and {len(self.column_names) - 5} more")
        else:
            lines.append(f"❌ {len(invalid_columns)} columns not found:")
            for item in suggestions_list[:3]:
                col = item['invalid']
                sugg = item['suggestions']
                if sugg:
                    lines.append(f"  • '{col}' → Try: {', '.join(sugg)}")
                else:
                    lines.append(f"  • '{col}' → No suggestions")
        
        return "\n".join(lines)
    
    def suggest_corrections(self, kql: str) -> Optional[str]:
        """
        Suggest corrected KQL with proper column names
        
        Returns corrected KQL if possible, None otherwise
        """
        
        is_valid, _, suggestions_list = self.validate_kql(kql)
        
        if is_valid:
            return kql
        
        corrected_kql = kql
        
        for item in suggestions_list:
            invalid = item['invalid']
            suggestions = item['suggestions']
            
            if suggestions and len(suggestions) > 0:
                # Use top suggestion
                correct = suggestions[0]
                
                # Replace in KQL (word boundaries)
                pattern = r'\b' + re.escape(invalid) + r'\b'
                corrected_kql = re.sub(pattern, correct, corrected_kql)
        
        # Verify correction worked
        is_valid, _, _ = self.validate_kql(corrected_kql)
        
        if is_valid:
            return corrected_kql
        
        return None
