"""
MemoryStore v1.0 — Persistent memory of notable findings across sessions
=========================================================================

What it does:
  - Saves notable snapshots (health state, key metrics, concentrations) after
    each session to a JSON file in ./cache/memory/
  - On next connect, loads previous snapshot and computes what CHANGED
  - Surfaces deltas in the proactive digest and inline responses

This is what gives PULSE the "analyst who was here yesterday" feeling:
  "EMEA concentration was 70% last week — now it's 74%. Trending up."
  "3 new orgs in the top 10 since you last looked."
  "NA was dead last week, now it has 1.2B events — something changed."

File format: JSON, one file per config, stored in ./cache/memory/{config_id}.json

Author: PULSE Team
"""

import json
import logging
import hashlib
from pathlib import Path
from typing import Optional, Dict, List, Any
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

MEMORY_DIR = Path("./cache/memory")


# ─────────────────────────────────────────────────────────────────────────────
# Snapshot schema
# ─────────────────────────────────────────────────────────────────────────────

def _empty_snapshot() -> Dict:
    return {
        'saved_at': None,
        'daily': {
            'avg_7d': None,
            'avg_prev_7d': None,
            'trend_word': None,
            'peak_val': None,
            'peak_date': None,
        },
        'regions': [],         # [{name, events, share_pct}]
        'top_orgs': [],        # [{name, events, share_pct}]
        'totals': {
            'events_30d': None,
            'active_orgs': None,
        },
        'concentration': {
            'top1_name': None,
            'top1_pct': None,
            'top3_pct': None,
        },
        'health': {
            'status': None,    # 'green', 'amber', 'red'
            'finding': None,
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

class MemoryStore:
    """
    Persistent cross-session memory for a single team config.
    """

    def __init__(self, config_id: str):
        self.config_id   = config_id
        self.memory_path = MEMORY_DIR / f"{_safe_name(config_id)}.json"
        self.previous: Optional[Dict] = None
        self.current:  Optional[Dict] = None
        self._load_previous()

    def has_previous(self) -> bool:
        return self.previous is not None and self.previous.get('saved_at') is not None

    def age_hours(self) -> Optional[float]:
        """How many hours since the last snapshot was saved."""
        if not self.has_previous():
            return None
        try:
            saved = datetime.fromisoformat(self.previous['saved_at'])
            return (datetime.now() - saved).total_seconds() / 3600
        except Exception:
            return None

    def save_snapshot(self, data_profile) -> bool:
        """
        Build and save a snapshot from the current data profile.
        Called at end of session (or after profile build completes).
        Returns True if saved successfully.
        """
        try:
            snap = self._build_snapshot(data_profile)
            snap['saved_at'] = datetime.now().isoformat()
            self.current = snap

            MEMORY_DIR.mkdir(parents=True, exist_ok=True)
            with open(self.memory_path, 'w') as f:
                json.dump(snap, f, indent=2, default=str)

            logger.info(f"Memory saved: {self.config_id} → {self.memory_path}")
            return True
        except Exception as e:
            logger.warning(f"Memory save failed: {e}")
            return False

    def compute_deltas(self) -> List[Dict]:
        """
        Compare previous snapshot to current data.
        Returns list of notable delta dicts, sorted by significance.

        Each delta: {type, headline, detail, severity, dimension}
        """
        if not self.has_previous() or self.current is None:
            return []

        prev = self.previous
        curr = self.current
        deltas = []

        # ── Daily volume change ───────────────────────────────────────────────
        prev_avg = prev.get('daily', {}).get('avg_7d')
        curr_avg = curr.get('daily', {}).get('avg_7d')
        if prev_avg and curr_avg and prev_avg > 0:
            delta_pct = (curr_avg - prev_avg) / prev_avg * 100
            if abs(delta_pct) >= 10:
                direction = "up" if delta_pct > 0 else "down"
                severity  = 'red' if abs(delta_pct) > 25 else 'amber'
                age = self.age_hours() or 0
                period = f"{age/24:.0f}d ago" if age >= 24 else f"{age:.0f}h ago"
                deltas.append({
                    'type': 'volume_change',
                    'dimension': 'trend',
                    'headline': (
                        f"Daily events {direction} {abs(delta_pct):.0f}% since last session "
                        f"({_fmt(prev_avg)}/day → {_fmt(curr_avg)}/day, {period})"
                    ),
                    'detail': '',
                    'severity': severity,
                })

        # ── Regional concentration shift ──────────────────────────────────────
        prev_top1_name = prev.get('concentration', {}).get('top1_name')
        prev_top1_pct  = prev.get('concentration', {}).get('top1_pct')
        curr_top1_pct  = curr.get('concentration', {}).get('top1_pct')
        curr_top1_name = curr.get('concentration', {}).get('top1_name')

        if prev_top1_pct and curr_top1_pct:
            pct_shift = curr_top1_pct - prev_top1_pct
            if abs(pct_shift) >= 3:
                direction = "grew" if pct_shift > 0 else "fell"
                severity  = 'amber'
                name = curr_top1_name or prev_top1_name or "Top region"
                deltas.append({
                    'type': 'concentration_shift',
                    'dimension': 'region',
                    'headline': (
                        f"**{name}** concentration {direction} from "
                        f"{prev_top1_pct:.0f}% → {curr_top1_pct:.0f}%"
                    ),
                    'detail': 'Trend: ' + ('increasing dominance' if pct_shift > 0 else 'diversifying'),
                    'severity': severity,
                })

        # ── New top region ────────────────────────────────────────────────────
        if (prev_top1_name and curr_top1_name
                and prev_top1_name.upper() != curr_top1_name.upper()):
            deltas.append({
                'type': 'new_top_region',
                'dimension': 'region',
                'headline': (
                    f"Top region changed: **{prev_top1_name}** → **{curr_top1_name}**"
                ),
                'detail': 'Worth investigating — leadership changes are rare.',
                'severity': 'amber',
            })

        # ── Org roster changes ────────────────────────────────────────────────
        prev_orgs = {o['name'] for o in prev.get('top_orgs', [])}
        curr_orgs = {o['name'] for o in curr.get('top_orgs', [])}
        new_orgs  = curr_orgs - prev_orgs
        gone_orgs = prev_orgs - curr_orgs

        if new_orgs:
            names = ", ".join(sorted(new_orgs)[:3])
            deltas.append({
                'type': 'new_top_orgs',
                'dimension': 'org',
                'headline': f"{len(new_orgs)} new org(s) entered top 10: **{names}**",
                'detail': 'New entrants in the top 10 often signal onboarding or feature adoption.',
                'severity': 'green',
            })
        if gone_orgs:
            names = ", ".join(sorted(gone_orgs)[:3])
            deltas.append({
                'type': 'dropped_orgs',
                'dimension': 'org',
                'headline': f"{len(gone_orgs)} org(s) dropped out of top 10: **{names}**",
                'detail': 'Worth checking if these orgs are still active.',
                'severity': 'amber',
            })

        # Sort by severity
        order = {'red': 0, 'amber': 1, 'green': 2}
        deltas.sort(key=lambda d: order.get(d['severity'], 3))
        return deltas

    def format_delta_narrative(self) -> str:
        """
        Build the "since you last looked" narrative from deltas.
        Returns '' if nothing notable changed.
        """
        deltas = self.compute_deltas()
        if not deltas:
            return ''

        age = self.age_hours()
        if age is None:
            period_str = "since last session"
        elif age < 24:
            period_str = f"in the last {age:.0f} hours"
        elif age < 48:
            period_str = "since yesterday"
        else:
            period_str = f"since {age/24:.0f} days ago"

        lines = [f"**📋 Since you last looked ({period_str}):**\n"]
        for d in deltas[:4]:  # cap at 4 deltas
            icon = {'red': '🔴', 'amber': '🟡', 'green': '🟢'}.get(d['severity'], '⚪')
            lines.append(f"{icon} {d['headline']}")
            if d.get('detail'):
                lines.append(f"   *{d['detail']}*")

        return "\n".join(lines)

    # ─────────────────────────────────────────────────────────────────────────
    # Internal
    # ─────────────────────────────────────────────────────────────────────────

    def _load_previous(self):
        """Load previous snapshot from disk."""
        try:
            if self.memory_path.exists():
                with open(self.memory_path) as f:
                    self.previous = json.load(f)
                age = self.age_hours()
                logger.info(
                    f"Memory loaded: {self.config_id} "
                    f"(saved {age:.0f}h ago)" if age else f"(saved at {self.previous.get('saved_at')})"
                )
            else:
                self.previous = None
                logger.debug(f"No previous memory for {self.config_id}")
        except Exception as e:
            logger.warning(f"Memory load failed: {e}")
            self.previous = None

    def _build_snapshot(self, data_profile) -> Dict:
        """Build a snapshot dict from the current data profile."""
        snap = _empty_snapshot()

        try:
            # ── Daily ──────────────────────────────────────────────────────
            if data_profile.has_table('profile_daily'):
                df = data_profile.query_safe(
                    "SELECT * FROM profile_daily ORDER BY day DESC"
                )
                if df is not None and not df.empty:
                    v_col = _find_col(df, ['events', 'event_count', 'total_events'])
                    if v_col:
                        vals = pd.to_numeric(df[v_col], errors='coerce').fillna(0)
                        snap['daily']['avg_7d']      = float(vals.head(7).mean())
                        snap['daily']['avg_prev_7d'] = float(vals.iloc[7:14].mean()) if len(vals) >= 14 else None
                        snap['daily']['peak_val']    = float(vals.max())

        except Exception as e:
            logger.debug(f"Snapshot daily failed: {e}")

        try:
            # ── Regions ────────────────────────────────────────────────────
            if data_profile.has_table('profile_region'):
                df = data_profile.query_safe(
                    "SELECT * FROM profile_region ORDER BY events DESC LIMIT 10"
                )
                if df is not None and not df.empty:
                    v_col = _find_col(df, ['events', 'event_count', 'total_events'])
                    l_col = _find_col(df, ['geoname', 'geo', 'region', 'country'])
                    if v_col and l_col:
                        df[v_col] = pd.to_numeric(df[v_col], errors='coerce').fillna(0)
                        total = df[v_col].sum()
                        regions = []
                        for _, row in df.head(5).iterrows():
                            val  = float(row[v_col])
                            name = str(row[l_col])
                            regions.append({
                                'name': name,
                                'events': val,
                                'share_pct': round(val / total * 100, 1) if total > 0 else 0,
                            })
                        snap['regions'] = regions

                        if regions:
                            snap['concentration']['top1_name'] = regions[0]['name']
                            snap['concentration']['top1_pct']  = regions[0]['share_pct']
                            top3_pct = sum(r['share_pct'] for r in regions[:3])
                            snap['concentration']['top3_pct']  = top3_pct

        except Exception as e:
            logger.debug(f"Snapshot region failed: {e}")

        try:
            # ── Orgs ───────────────────────────────────────────────────────
            if data_profile.has_table('profile_organization'):
                df = data_profile.query_safe(
                    "SELECT * FROM profile_organization ORDER BY events DESC LIMIT 10"
                )
                if df is not None and not df.empty:
                    v_col = _find_col(df, ['events', 'event_count', 'total_events'])
                    l_col = _find_col(df, ['orgid', 'org_id', 'organization', 'tenantid'])
                    if v_col and l_col:
                        df[v_col] = pd.to_numeric(df[v_col], errors='coerce').fillna(0)
                        total = df[v_col].sum()
                        orgs = []
                        for _, row in df.head(10).iterrows():
                            val  = float(row[v_col])
                            name = str(row[l_col])[:40]
                            orgs.append({
                                'name': name,
                                'events': val,
                                'share_pct': round(val / total * 100, 1) if total > 0 else 0,
                            })
                        snap['top_orgs'] = orgs

        except Exception as e:
            logger.debug(f"Snapshot org failed: {e}")

        return snap


# ─────────────────────────────────────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────────────────────────────────────

def _safe_name(s: str) -> str:
    """Make a string safe for use as a filename."""
    return "".join(c if c.isalnum() or c in '-_' else '_' for c in s)[:64]


def _find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for name in candidates:
        for c in df.columns:
            if c.lower() == name:
                return c
    return None


def _fmt(n) -> str:
    if n is None or (isinstance(n, float) and np.isnan(n)):
        return "—"
    n = float(n)
    if abs(n) >= 1e12: return f"{n/1e12:.1f}T"
    if abs(n) >= 1e9:  return f"{n/1e9:.1f}B"
    if abs(n) >= 1e6:  return f"{n/1e6:.1f}M"
    if abs(n) >= 1e3:  return f"{n/1e3:.1f}K"
    return f"{n:,.0f}"
