"""
KQL Detector - Identifies Direct KQL Queries
=============================================

Detects if user input is KQL vs natural language

Author: PULSE Team
"""

import re
from typing import Tuple


class KQLDetector:
    """
    Detects if user input is direct KQL or natural language
    """
    
    def __init__(self):
        # KQL operators (unambiguous - rarely used in natural language)
        self.strong_kql_operators = [
            'summarize', 'project', 'extend', 'join', 'union', 
            'let', 'make-series', 'render', 'evaluate', 'invoke',
            'parse', 'mvexpand', 'mv-expand'
        ]
        
        # Ambiguous operators (can be both KQL and natural language)
        self.ambiguous_operators = [
            'where', 'order', 'top', 'take', 'limit', 'sort',
            'distinct', 'count', 'sample', 'by', 'as', 'in', 'on'
        ]
        
        # Natural language indicators
        self.nl_indicators = [
            'show me', 'what', 'how many', 'which', 'when', 'why',
            'give me', 'find', 'list', 'get', 'tell me', 'explain',
            'interesting', 'compare', 'analyze', 'summarize for',
            'can you', 'please', 'i want', 'i need'
        ]
    
    def is_kql(self, user_input: str) -> Tuple[bool, str]:
        """
        Determine if input is KQL or natural language
        
        Returns:
            (is_kql: bool, reason: str)
        """
        
        input_lower = user_input.lower().strip()
        
        # Empty input
        if not input_lower:
            return False, "Empty input"
        
        # STRONG KQL INDICATORS (almost always KQL)
        
        # Check for pipe operator (very strong KQL indicator)
        if '|' in input_lower:
            return True, "Contains pipe operator"
        
        # Check for KQL functions with parentheses
        kql_functions = [
            r'\bcount\(\)', r'\bsum\(', r'\bavg\(', r'\bmin\(', r'\bmax\(',
            r'\bdcount\(', r'\bstdev\(', r'\bpercentile\(', r'\bmake_list\(',
            r'\bbin\(', r'\bstartofday\(', r'\bendofday\(', r'\bformat_datetime\(',
            r'\bago\(', r'\bnow\(', r'\bdatetime\(', r'\btostring\('
        ]
        
        for func_pattern in kql_functions:
            if re.search(func_pattern, input_lower):
                return True, f"Contains KQL function: {func_pattern}"
        
        # Check for comparison operators (==, !=, >=, <=)
        # But NOT single > or < (can be "greater than" in NL)
        if re.search(r'==|!=|>=|<=', user_input):
            return True, "Contains comparison operators"
        
        # Check for strong KQL operators
        for operator in self.strong_kql_operators:
            if operator in input_lower:
                return True, f"Contains strong KQL operator: {operator}"
        
        # NATURAL LANGUAGE INDICATORS (strong NL signals)
        
        # Questions (ends with ?)
        if input_lower.endswith('?'):
            return False, "Ends with question mark"
        
        # Starts with natural language phrases
        for indicator in self.nl_indicators:
            if input_lower.startswith(indicator):
                return False, f"Starts with NL indicator: {indicator}"
        
        # AMBIGUOUS KEYWORD HANDLING (context-aware)
        
        # Check if starts with ambiguous operator
        for operator in self.ambiguous_operators:
            if input_lower.startswith(operator):
                is_kql, reason = self._check_ambiguous_operator(operator, input_lower, user_input)
                if is_kql is not None:
                    return is_kql, reason
        
        # Default: If it has multiple KQL keywords, probably KQL
        kql_keyword_count = sum(1 for op in self.strong_kql_operators if op in input_lower)
        if kql_keyword_count >= 2:
            return True, f"Multiple KQL keywords ({kql_keyword_count})"
        
        # Default to natural language
        return False, "Appears to be natural language"
    
    def _check_ambiguous_operator(self, operator: str, input_lower: str, original_input: str) -> Tuple[bool, str]:
        """
        Check context for ambiguous operators
        Returns (is_kql, reason) or (None, None) if still ambiguous
        """
        
        # Special handling for "where"
        if operator == 'where':
            # Natural: "Where are the errors?", "Where is the data?"
            # KQL: "where EventType == 'Error'"
            if any(word in input_lower for word in ['are', 'is', 'was', 'were', 'the', '?']):
                return False, "Natural language question with 'where'"
            if '==' in original_input or '!=' in original_input:
                return True, "KQL where clause with comparison"
            # Ambiguous - check further context
            return None, None
        
        # Special handling for "top"
        if operator == 'top':
            # Natural: "Top 10 orgs", "Top 5 users by activity"
            # KQL: "top 10 by Count desc", "top 5 by Name asc"
            
            # Real KQL pattern: "top N by X desc/asc"
            if ' by ' in input_lower and (' desc' in input_lower or ' asc' in input_lower):
                return True, "KQL top with by/desc/asc syntax"
            
            # Natural language pattern: "top N <word>" or "top <word>"
            if re.match(r'^top\s+(\d+\s+)?[a-z]+', input_lower):
                return False, "Natural language starting with 'top'"
            
            return None, None
        
        # Special handling for "count"
        if operator == 'count':
            # Natural: "Count the events", "Count how many"
            # KQL: "count()", "count() by OrgId"
            if 'count()' in input_lower:
                return True, "KQL count() function"
            if any(word in input_lower for word in ['the', 'how many', 'total']):
                return False, "Natural language count request"
            return None, None
        
        # Special handling for "order"
        if operator == 'order':
            # Natural: "Order by region please", "Order these by size"
            # KQL: "order by Count desc"
            if ' by ' in input_lower and (' desc' in input_lower or ' asc' in input_lower):
                return True, "KQL order by with desc/asc"
            if any(word in input_lower for word in ['please', 'these', 'them', 'the', '?']):
                return False, "Natural language ordering request"
            return None, None
        
        # Special handling for "take"
        if operator == 'take':
            # Natural: "Take the top 10", "Take a look at"
            # KQL: "take 10", "take 100"
            if re.match(r'^take\s+\d+\s*$', input_lower):
                return True, "KQL take N syntax"
            if any(word in input_lower for word in ['the', 'a look', 'please']):
                return False, "Natural language with 'take'"
            return None, None
        
        # Special handling for "limit"
        if operator == 'limit':
            # Natural: "Limit to 5 results", "Limit the output"
            # KQL: "limit 10"
            if re.match(r'^limit\s+\d+\s*$', input_lower):
                return True, "KQL limit N syntax"
            if any(word in input_lower for word in ['to', 'the', 'results']):
                return False, "Natural language with 'limit'"
            return None, None
        
        # Special handling for "sort"
        if operator == 'sort':
            # Natural: "Sort by name", "Sort these results"
            # KQL: "sort by Count desc"
            if ' by ' in input_lower and (' desc' in input_lower or ' asc' in input_lower):
                return True, "KQL sort by with desc/asc"
            if any(word in input_lower for word in ['please', 'these', 'the', 'results']):
                return False, "Natural language sorting request"
            return None, None
        
        # Special handling for "distinct"
        if operator == 'distinct':
            # Natural: "Distinct values of region"
            # KQL: "distinct OrgId"
            if ' of ' in input_lower or ' values' in input_lower:
                return False, "Natural language with 'distinct'"
            # If it's just "distinct ColumnName" - likely KQL
            if re.match(r'^distinct\s+[A-Z][a-zA-Z0-9_]*\s*$', original_input):
                return True, "KQL distinct column syntax"
            return None, None
        
        # Special handling for "sample"
        if operator == 'sample':
            # Natural: "Sample some data", "Sample of events"
            # KQL: "sample 100"
            if re.match(r'^sample\s+\d+\s*$', input_lower):
                return True, "KQL sample N syntax"
            if any(word in input_lower for word in ['some', 'of', 'the']):
                return False, "Natural language with 'sample'"
            return None, None
        
        # For other ambiguous operators, still ambiguous
        return None, None
    
    def extract_kql(self, user_input: str) -> str:
        """
        Extract and clean KQL from user input
        """
        # Remove common prefixes
        kql = user_input.strip()
        
        # Remove "KQL:", "Query:", etc. prefixes
        prefixes = ['kql:', 'query:', 'run:']
        for prefix in prefixes:
            if kql.lower().startswith(prefix):
                kql = kql[len(prefix):].strip()
        
        return kql
