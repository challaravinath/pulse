"""
Predictive Cache v1.0 — YouTube-Style Pre-computation
=======================================================

After every answer, pre-compute 2-3 likely follow-ups in background.
When the user asks one → instant response, zero wait.

This is the single biggest perceived-speed improvement you can make.

Flow:
  1. User asks "top 10 orgs"
  2. We answer from profile (fast)
  3. Background: prefetch "trend", "by region", "bottom 10"
  4. User asks "show the trend" → already computed → 0ms

Architecture:
  - Stores pre-computed DataFrames + narrative text
  - Thread-safe (uses threading.Lock)
  - Auto-expires entries after 5 minutes
  - Max 10 entries to limit memory

Author: PULSE Team
"""

import re
import time
import logging
import threading
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# How long prefetched results stay valid (seconds)
PREFETCH_TTL = 300  # 5 minutes
MAX_ENTRIES = 10


@dataclass
class CacheEntry:
    key: str
    intent: str
    sql: str
    table: str
    df: Any = None  # pd.DataFrame
    narrative: str = ""
    viz: Any = None  # pre-built visualization dict
    created_at: float = field(default_factory=time.time)

    @property
    def is_expired(self) -> bool:
        return (time.time() - self.created_at) > PREFETCH_TTL


class PredictiveCache:
    """
    Pre-computes likely follow-up queries after each answer.
    Thread-safe. Auto-expiring.
    """

    # What to prefetch after each intent type
    FOLLOW_UP_MAP = {
        "ranking": [
            {
                "key": "trend",
                "match": ["trend", "over time", "daily", "growing"],
                "table": "profile_daily",
                "sql": "SELECT * FROM profile_daily ORDER BY day ASC",
                "intent": "trend",
            },
            {
                "key": "by_region",
                "match": ["region", "geo", "where", "geographic"],
                "table": "profile_region",
                "sql": "SELECT * FROM profile_region ORDER BY events DESC",
                "intent": "lookup",
            },
            {
                "key": "bottom",
                "match": ["bottom", "least", "lowest", "smallest"],
                "table": "profile_organization",
                "sql": "SELECT * FROM profile_organization ORDER BY events ASC LIMIT 10",
                "intent": "ranking",
            },
        ],
        "trend": [
            {
                "key": "top_orgs",
                "match": ["top org", "top 10", "biggest", "most active"],
                "table": "profile_organization",
                "sql": "SELECT * FROM profile_organization ORDER BY events DESC LIMIT 10",
                "intent": "ranking",
            },
            {
                "key": "by_region",
                "match": ["region", "geo", "break down by region"],
                "table": "profile_region",
                "sql": "SELECT * FROM profile_region ORDER BY events DESC",
                "intent": "lookup",
            },
            {
                "key": "health",
                "match": ["issue", "anomal", "problem", "health", "spike", "drop"],
                "table": "profile_daily",
                "sql": "SELECT * FROM profile_daily ORDER BY day ASC",
                "intent": "health",
            },
        ],
        "overview": [
            {
                "key": "trend",
                "match": ["trend", "daily", "over time"],
                "table": "profile_daily",
                "sql": "SELECT * FROM profile_daily ORDER BY day ASC",
                "intent": "trend",
            },
            {
                "key": "top_orgs",
                "match": ["top org", "top 10", "biggest"],
                "table": "profile_organization",
                "sql": "SELECT * FROM profile_organization ORDER BY events DESC LIMIT 10",
                "intent": "ranking",
            },
            {
                "key": "by_region",
                "match": ["region", "geo"],
                "table": "profile_region",
                "sql": "SELECT * FROM profile_region ORDER BY events DESC",
                "intent": "lookup",
            },
        ],
        "total": [
            {
                "key": "trend",
                "match": ["trend", "daily", "over time", "growing"],
                "table": "profile_daily",
                "sql": "SELECT * FROM profile_daily ORDER BY day ASC",
                "intent": "trend",
            },
            {
                "key": "by_region",
                "match": ["region", "break down"],
                "table": "profile_region",
                "sql": "SELECT * FROM profile_region ORDER BY events DESC",
                "intent": "lookup",
            },
            {
                "key": "top_orgs",
                "match": ["top org", "top 10"],
                "table": "profile_organization",
                "sql": "SELECT * FROM profile_organization ORDER BY events DESC LIMIT 10",
                "intent": "ranking",
            },
        ],
        "health": [
            {
                "key": "trend",
                "match": ["trend", "full trend", "show trend"],
                "table": "profile_daily",
                "sql": "SELECT * FROM profile_daily ORDER BY day ASC",
                "intent": "trend",
            },
            {
                "key": "top_orgs",
                "match": ["top org", "which org"],
                "table": "profile_organization",
                "sql": "SELECT * FROM profile_organization ORDER BY events DESC LIMIT 10",
                "intent": "ranking",
            },
            {
                "key": "by_region",
                "match": ["region", "compare region"],
                "table": "profile_region",
                "sql": "SELECT * FROM profile_region ORDER BY events DESC",
                "intent": "lookup",
            },
        ],
        "lookup": [
            {
                "key": "top_orgs",
                "match": ["top", "biggest", "most"],
                "table": "profile_organization",
                "sql": "SELECT * FROM profile_organization ORDER BY events DESC LIMIT 10",
                "intent": "ranking",
            },
            {
                "key": "trend",
                "match": ["trend", "over time"],
                "table": "profile_daily",
                "sql": "SELECT * FROM profile_daily ORDER BY day ASC",
                "intent": "trend",
            },
        ],
    }

    def __init__(self, data_profile=None, visualizer=None):
        self._entries: Dict[str, CacheEntry] = {}
        self._lock = threading.Lock()
        self._profile = data_profile
        self._visualizer = visualizer

    def get_if_ready(self, question: str) -> Optional[Dict[str, Any]]:
        """
        Check if this question matches a prefetched result.
        Returns orchestrator-compatible response dict, or None.
        """
        q = question.lower().strip()

        with self._lock:
            # Evict expired
            self._evict_expired()

            for key, entry in self._entries.items():
                if entry.df is None:
                    continue
                # Check if question matches this entry's trigger words
                for follow_up_group in self.FOLLOW_UP_MAP.values():
                    for fu in follow_up_group:
                        if fu["key"] == key:
                            if any(m in q for m in fu["match"]):
                                logger.info(f"Predictive cache HIT: {key}")
                                result = self._build_response(entry)
                                return result

        return None

    def prefetch_after(self, intent: str, question: str):
        """
        Background: pre-compute likely follow-ups for this intent.
        Called in a thread — must be thread-safe.
        """
        if not self._profile:
            return

        follow_ups = self.FOLLOW_UP_MAP.get(intent, [])
        if not follow_ups:
            return

        available = set(self._profile.list_tables()) if hasattr(self._profile, 'list_tables') else set()

        for fu in follow_ups:
            key = fu["key"]
            table = fu["table"]

            # Skip if already cached and fresh
            with self._lock:
                if key in self._entries and not self._entries[key].is_expired:
                    continue

            # Skip if table not available
            if table not in available:
                continue

            try:
                df = self._profile.query(fu["sql"])
                if df is not None and not df.empty:
                    from pulse.core.narrative_engine import generate_smart_insight
                    narrative = generate_smart_insight(df, fu["intent"], f"{len(df)} results")

                    # Build visualization now so hit is truly instant
                    viz = None
                    if self._visualizer:
                        try:
                            intent_hint = (
                                'ranking_bottom' if 'ASC' in fu["sql"] and fu["intent"] == 'ranking'
                                else fu["intent"]
                            )
                            viz = self._visualizer.analyze_and_visualize(
                                df, fu.get("key", ""), "", intent_hint=intent_hint
                            )
                        except Exception:
                            pass

                    entry = CacheEntry(
                        key=key,
                        intent=fu["intent"],
                        sql=fu["sql"],
                        table=table,
                        df=df,
                        narrative=narrative,
                        viz=viz,
                    )

                    with self._lock:
                        self._entries[key] = entry
                        self._enforce_max_entries()

                    logger.debug(f"Prefetched: {key} ({len(df)} rows)")

            except Exception as e:
                logger.debug(f"Prefetch failed for {key}: {e}")

    def list_ready(self) -> List[str]:
        """Return keys of currently cached (non-expired) entries."""
        with self._lock:
            self._evict_expired()
            return [k for k, v in self._entries.items() if v.df is not None]

    def clear(self):
        with self._lock:
            self._entries.clear()

    # ── Internal ──

    def _build_response(self, entry: CacheEntry) -> Dict[str, Any]:
        """Convert cache entry to orchestrator response format."""
        return {
            "response_type": "data",
            "intent": entry.intent,
            "data": entry.df,
            "message": entry.narrative,
            "visualization": entry.viz,  # pre-built — zero extra compute on hit
            "kql": None,
            "profile_sql": entry.sql,
            "suggestions_list": [],
            "_prefetched": True,
        }

    def _evict_expired(self):
        """Remove expired entries. Must hold lock."""
        expired = [k for k, v in self._entries.items() if v.is_expired]
        for k in expired:
            del self._entries[k]

    def _enforce_max_entries(self):
        """Remove oldest entries if over limit. Must hold lock."""
        if len(self._entries) > MAX_ENTRIES:
            sorted_keys = sorted(
                self._entries.keys(),
                key=lambda k: self._entries[k].created_at,
            )
            for k in sorted_keys[: len(self._entries) - MAX_ENTRIES]:
                del self._entries[k]
