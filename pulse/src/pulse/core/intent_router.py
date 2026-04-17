"""
Intent Router v2.2 — Smarter Intent Classification
=====================================================

Improvements:
- Much better follow-up detection ("break that down by", "show just X")
- Smarter cache-vs-fetch decision when data is already loaded
- Explicit fetch markers ("show me", "get", "from Kusto")
- Reduced LLM calls — heuristics handle most cases

Author: PULSE Team
"""

import logging
import re
import json
from enum import Enum
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


class IntentType(Enum):
    FETCH_DATA = "fetch_data"
    COMPLEX_QUERY = "complex_query"     # ★ NEW: multi-step planning
    ANALYZE_CACHE = "analyze_cache"
    REFINE_CACHE = "refine_cache"
    META_QUESTION = "meta_question"
    REDIRECT = "redirect"


class IntentRouter:

    def __init__(self, openai_client, model: str = 'gpt-4o-mini'):
        self.client = openai_client
        self._model = model

        # ── Keyword banks ────────────────────────────────────────────

        # Strong fetch signals — user explicitly wants NEW data
        self.fetch_strong = [
            "show me", "get me", "fetch", "pull", "query",
            "from kusto", "from the database", "load",
        ]

        # Fetch signals — but weaker, can be overridden by cache
        self.fetch_weak = [
            "top", "how many", "which", "who", "count",
            "list", "find", "what are", "what is",
        ]

        # Analysis — user wants insights on what they already have
        self.analysis_kw = [
            "interesting", "insight", "summary", "summarize", "summarise",
            "analyze", "analyse", "explain", "what does", "why",
            "tell me about", "health", "overview", "trend", "pattern",
            "anomal", "unusual", "surprising", "worried",
            "highlight", "tldr", "tl;dr", "manager", "executive",
            "important", "going well", "going wrong",
        ]

        # Refine — user wants to slice / filter current data.
        # ★ "break down by", "now by", "split by", "drill down" removed here —
        # those are dimension lookups handled by fast_router/universal_resolve
        # before intent_router ever runs. Keeping them here caused them to be
        # classified as REFINE_CACHE which then skipped profile and hit Kusto.
        self.refine_kw = [
            "just", "only", "filter", "exclude", "show just",
            "narrow", "focus on", "zoom in",
            "but only", "limit to",
            "remove", "without", "except", "subset",
        ]

        # Meta — about the system itself
        self.meta_kw = [
            "what can you", "help me", "how do i", "what data",
            "what columns", "capabilities", "what tables",
        ]

        # ★ Complex queries — need multi-step planning
        self.complex_patterns = [
            r"breakdown of the top", r"breakdown of top",
            r"detail on the top", r"deep dive into",
            r"but not in", r"exclusive to", r"unique to",
            r"crosstab", r"cross-tab", r"pivot", r"matrix of",
            r"heatmap of .* vs",
            r"compare top .* vs bottom", r"compare .* with .* and",
            r"top .* versus bottom", r"power users vs",
            r"active in multiple", r"appeared in more than one",
            r"used all .* entity", r"used every",
            r"growing fastest", r"declining fastest",
            r"most improved", r"biggest change", r"rate of change",
            r"orgs that have more than \d+ entity",
            r"orgs that use .* and .*",
            r"entities? used in .* but not",
        ]

    # ── Public API ───────────────────────────────────────────────────────────

    def classify_intent(
        self,
        user_message: str,
        has_cached_data: bool,
        conversation_history: list = None,
    ) -> Dict[str, Any]:
        """Classify user intent using layered heuristics + LLM fallback."""

        msg = user_message.lower().strip()

        # ── 1. Meta questions ───────────────────────────────────────
        if any(k in msg for k in self.meta_kw):
            return self._result(IntentType.META_QUESTION, 0.92, "Meta keyword")

        # ── 1.5 Complex query detection ───────────────────────────
        if self._is_complex(msg):
            return self._result(IntentType.COMPLEX_QUERY, 0.90, "Complex multi-step query")

        # ── 2. If NO cached data → must fetch ──────────────────────
        if not has_cached_data:
            # Unless it's purely conversational / off-topic
            if self._is_offtopic(msg):
                return self._result(IntentType.REDIRECT, 0.85, "Off-topic, no data")
            return self._result(IntentType.FETCH_DATA, 0.90, "No cache → fetch")

        # ── From here, we HAVE cached data ──────────────────────────

        # ── 3. Strong refine signals ────────────────────────────────
        if any(k in msg for k in self.refine_kw):
            return self._result(IntentType.REFINE_CACHE, 0.90, "Refine keyword")

        # ── 4. Analysis signals ─────────────────────────────────────
        if any(k in msg for k in self.analysis_kw):
            return self._result(IntentType.ANALYZE_CACHE, 0.90, "Analysis keyword")

        # ── 5. Strong fetch signals (even with cache) ───────────────
        if any(k in msg for k in self.fetch_strong):
            return self._result(IntentType.FETCH_DATA, 0.88, "Strong fetch keyword")

        # ── 6. Weak fetch — is it a NEW topic or a follow-up? ──────
        # If the user's question looks like a completely new topic,
        # fetch new data. If it looks like a continuation, analyse.
        if any(k in msg for k in self.fetch_weak):
            if self._looks_like_followup(msg, conversation_history):
                return self._result(IntentType.REFINE_CACHE, 0.75, "Follow-up → refine")
            return self._result(IntentType.FETCH_DATA, 0.80, "Weak fetch, new topic")

        # ── 7. Off-topic check ──────────────────────────────────────
        if self._is_offtopic(msg):
            return self._result(IntentType.REDIRECT, 0.80, "Off-topic")

        # ── 8. LLM fallback ─────────────────────────────────────────
        return self._classify_with_llm(user_message, has_cached_data, conversation_history)

    # ── Heuristics ───────────────────────────────────────────────────────────

    def _looks_like_followup(self, msg: str, history: list) -> bool:
        """Check if the question references previous context."""
        followup_signals = [
            "that", "this", "those", "these", "it", "them",
            "the same", "above", "previous", "last",
            "now ", "also ", "and ", "but ",
        ]
        return any(s in msg for s in followup_signals)

    def _is_offtopic(self, msg: str) -> bool:
        """Quick check for obviously off-topic messages."""
        offtopic = [
            "weather", "joke", "recipe", "poem", "song",
            "who are you", "what are you", "hello", "hi ",
            "thanks", "thank you", "bye", "goodbye",
        ]
        # Very short greetings
        if len(msg.split()) <= 2 and any(msg.startswith(w) for w in ["hi", "hey", "hello"]):
            return True
        return any(k in msg for k in offtopic)

    def _is_complex(self, msg: str) -> bool:
        """Check if a question needs multi-step query planning."""
        # Check explicit complexity patterns
        for pattern in self.complex_patterns:
            if re.search(pattern, msg):
                return True

        # Count analytical dimensions
        dims = 0
        dim_map = [
            ["org", "organization"],
            ["region", "geo", "country", "emea", "apac", "eu ", "us "],
            ["entity", "feature", "module", "component"],
            ["day", "week", "trend", "over time", "daily", "growth"],
            ["session"],
        ]
        for keywords in dim_map:
            if any(k in msg for k in keywords):
                dims += 1

        # 3+ dimensions almost always needs planning
        if dims >= 3:
            return True

        # Multiple "and" connecting analyses
        if msg.count(" and ") >= 2 and dims >= 2:
            return True

        return False

    # ── LLM Fallback ─────────────────────────────────────────────────────────

    def _classify_with_llm(
        self, message: str, has_cache: bool, history: list
    ) -> Dict:
        """Use LLM for truly ambiguous cases."""

        system = f"""Classify the user's intent into ONE category.
The user is interacting with a telemetry data analysis tool.
They {"HAVE data loaded" if has_cache else "have NO data loaded yet"}.

Categories:
- FETCH_DATA: wants NEW data from database (simple query, 1 step)
- COMPLEX_QUERY: complex question needing multiple steps (comparisons, breakdowns, set operations, multi-dimensional analysis)
- ANALYZE_CACHE: wants insights on existing loaded data
- REFINE_CACHE: wants to filter/slice existing data
- META_QUESTION: asking about system capabilities
- REDIRECT: off-topic

Respond with ONLY JSON: {{"intent": "...", "confidence": 0.0-1.0}}"""

        try:
            resp = self.client.chat.completions.create(
                model=self._model,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": message},
                ],
                temperature=0.1,
                max_tokens=80,
            )
            result = json.loads(resp.choices[0].message.content)
            intent_map = {
                "FETCH_DATA": IntentType.FETCH_DATA,
                "COMPLEX_QUERY": IntentType.COMPLEX_QUERY,
                "ANALYZE_CACHE": IntentType.ANALYZE_CACHE,
                "REFINE_CACHE": IntentType.REFINE_CACHE,
                "META_QUESTION": IntentType.META_QUESTION,
                "REDIRECT": IntentType.REDIRECT,
            }
            intent = intent_map.get(result.get("intent"), IntentType.FETCH_DATA)
            return self._result(intent, result.get("confidence", 0.6), "LLM classification")

        except Exception as e:
            logger.error(f"LLM intent classification failed: {e}")
            # ★ Safer fallback: if we have data, default to analysis (cheap).
            # If no data, default to fetch.
            if has_cache:
                return self._result(IntentType.ANALYZE_CACHE, 0.5, "Fallback → analyse")
            return self._result(IntentType.FETCH_DATA, 0.5, "Fallback → fetch")

    # ── Helpers ──────────────────────────────────────────────────────────────

    @staticmethod
    def _result(intent: IntentType, confidence: float, reason: str) -> Dict:
        return {
            'intent': intent,
            'confidence': confidence,
            'reasoning': reason,
            'should_query_kusto': intent == IntentType.FETCH_DATA,
        }
