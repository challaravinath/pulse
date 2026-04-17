"""
BusinessContext v1.0 — "What's normal for YOUR data"
======================================================

Reads a context.yaml file (lives next to the team's config file) that
lets teams tell PULSE what normal looks like:

  - baseline daily events (so "2B today" means something)
  - expected regional distribution
  - known seasonality (Q1 dips, Dec slowdown)
  - KPI thresholds (what counts as a drop worth alerting)
  - product events to watch

Without this: PULSE says "dropped 59%"
With this:    PULSE says "dropped 59% — below your 2B/day baseline.
               Note: January is typically 15% slower, so effective
               drop is ~44% vs seasonally-adjusted baseline."

The context.yaml is OPTIONAL. If absent, everything degrades gracefully
to the current behaviour.

Author: PULSE Team
"""

import logging
import yaml
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime, date

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Data model
# ─────────────────────────────────────────────────────────────────────────────

class BusinessContext:
    """
    Loaded business context for a team's data.
    All fields have safe defaults so missing context never breaks anything.
    """

    def __init__(self, raw: Dict = None):
        raw = raw or {}

        # ── Identity ──────────────────────────────────────────────────────────
        self.product_name: str = raw.get('product_name', 'your product')
        self.team_name:    str = raw.get('team_name', 'your team')

        # ── Baselines ─────────────────────────────────────────────────────────
        baselines = raw.get('baselines', {})
        self.baseline_daily_events:   Optional[float] = baselines.get('daily_events')
        self.baseline_weekly_events:  Optional[float] = baselines.get('weekly_events')
        self.baseline_active_orgs:    Optional[int]   = baselines.get('active_orgs')
        self.baseline_active_users:   Optional[int]   = baselines.get('active_users')

        # ── Thresholds ────────────────────────────────────────────────────────
        thresholds = raw.get('thresholds', {})
        self.alert_drop_pct:    float = thresholds.get('alert_drop_pct', 25.0)
        self.warn_drop_pct:     float = thresholds.get('warn_drop_pct', 10.0)
        self.alert_spike_pct:   float = thresholds.get('alert_spike_pct', 100.0)
        self.concentration_warn: float = thresholds.get('concentration_warn_pct', 70.0)

        # ── Regional expectations ─────────────────────────────────────────────
        regions_raw = raw.get('regions', {})
        # e.g. {EMEA: {share_pct: 70, note: "primary market"}, NA: {share_pct: 20}}
        self.region_expectations: Dict[str, Dict] = regions_raw

        # ── Seasonality ───────────────────────────────────────────────────────
        seasonality_raw = raw.get('seasonality', {})
        # e.g. {january: -15, december: -20, q4: +10}
        self.monthly_adjustments: Dict[str, float] = seasonality_raw.get('monthly_pct', {})
        self.quarterly_adjustments: Dict[str, float] = seasonality_raw.get('quarterly_pct', {})
        self.known_quiet_periods: List[str] = seasonality_raw.get('quiet_periods', [])

        # ── KPIs to watch ─────────────────────────────────────────────────────
        kpis_raw = raw.get('kpis', [])
        self.kpis: List[Dict] = kpis_raw
        # e.g. [{name: "Daily active orgs", metric: "active_orgs", target: 500}]

        # ── Known events calendar ─────────────────────────────────────────────
        events_raw = raw.get('events', [])
        self.known_events: List[Dict] = events_raw
        # e.g. [{date: "2025-03-15", name: "v2.0 launch", expected_impact: "+20%"}]

        # ── Notes / context ───────────────────────────────────────────────────
        self.notes: List[str] = raw.get('notes', [])

        self.loaded = bool(raw)

    # ─────────────────────────────────────────────────────────────────────────
    # Framing helpers — used by narrative engine
    # ─────────────────────────────────────────────────────────────────────────

    def frame_daily_value(self, value: float) -> str:
        """
        Frame a daily event count against the team's baseline.
        Returns a short contextual string, or '' if no baseline set.

        e.g. "2.1B today — slightly above your 2B/day baseline"
        """
        if not self.baseline_daily_events or value <= 0:
            return ''

        baseline = self.baseline_daily_events
        diff_pct = (value - baseline) / baseline * 100

        # Seasonal adjustment for current month
        seasonal_adj = self._get_seasonal_adjustment()
        adj_note = ''
        if abs(seasonal_adj) >= 5:
            direction = "slower" if seasonal_adj < 0 else "faster"
            adj_note = f" (note: {abs(seasonal_adj):.0f}% {direction} than usual this month)"

        if abs(diff_pct) < 5:
            return f"roughly in line with your {_fmt(baseline)}/day baseline{adj_note}"
        elif diff_pct > 0:
            return f"{diff_pct:+.0f}% above your {_fmt(baseline)}/day baseline{adj_note}"
        else:
            severity = "well below" if diff_pct < -20 else "below"
            return f"{abs(diff_pct):.0f}% {severity} your {_fmt(baseline)}/day baseline{adj_note}"

    def frame_wow_change(self, pct_change: float) -> str:
        """
        Frame a week-over-week change against configured thresholds.
        Returns severity label + context.
        """
        if pct_change is None:
            return ''

        seasonal_adj = self._get_seasonal_adjustment()
        adjusted_change = pct_change - seasonal_adj  # seasonal-adjust the change

        if adjusted_change < -self.alert_drop_pct:
            return f"significant drop (your alert threshold is {self.alert_drop_pct:.0f}%)"
        elif adjusted_change < -self.warn_drop_pct:
            return f"notable dip (warning threshold: {self.warn_drop_pct:.0f}%)"
        elif adjusted_change > self.alert_spike_pct:
            return f"major spike — verify this is real"
        elif abs(adjusted_change) < 5:
            return "within normal range"
        return ''

    def get_region_context(self, region_name: str) -> Optional[str]:
        """Return context note for a specific region if configured."""
        for name, info in self.region_expectations.items():
            if name.upper() == region_name.upper():
                share = info.get('share_pct')
                note  = info.get('note', '')
                parts = []
                if share:
                    parts.append(f"expected ~{share}% share")
                if note:
                    parts.append(note)
                return ", ".join(parts) if parts else None
        return None

    def get_upcoming_events(self, days_ahead: int = 7) -> List[Dict]:
        """Return known events in the next N days."""
        upcoming = []
        today = date.today()
        for ev in self.known_events:
            try:
                ev_date = date.fromisoformat(str(ev.get('date', '')))
                delta = (ev_date - today).days
                if 0 <= delta <= days_ahead:
                    upcoming.append({**ev, 'days_away': delta})
            except (ValueError, TypeError):
                pass
        return upcoming

    def _get_seasonal_adjustment(self) -> float:
        """Get the seasonal adjustment % for the current month."""
        now = datetime.now()
        month_name = now.strftime('%B').lower()    # 'january', 'february', ...
        quarter = f"q{(now.month - 1) // 3 + 1}"  # 'q1', 'q2', ...

        adj = self.monthly_adjustments.get(month_name, 0)
        if adj == 0:
            adj = self.quarterly_adjustments.get(quarter, 0)
        return float(adj)

    def summary_line(self) -> str:
        """One-line summary of what context is loaded."""
        parts = []
        if self.baseline_daily_events:
            parts.append(f"baseline {_fmt(self.baseline_daily_events)}/day")
        if self.region_expectations:
            parts.append(f"{len(self.region_expectations)} regions configured")
        if self.monthly_adjustments or self.quarterly_adjustments:
            parts.append("seasonality aware")
        if self.kpis:
            parts.append(f"{len(self.kpis)} KPIs tracked")
        if not parts:
            return "no business context loaded"
        return ", ".join(parts)


# ─────────────────────────────────────────────────────────────────────────────
# Loader
# ─────────────────────────────────────────────────────────────────────────────

def load(config_path: str) -> BusinessContext:
    """
    Load business context from a context.yaml file.

    Looks for context.yaml in the same directory as the team's config file.
    e.g. configs/example.yaml → configs/example_context.yaml

    Returns an empty BusinessContext (all defaults) if file not found.
    """
    if not config_path:
        return BusinessContext()

    config_dir  = Path(config_path).parent
    config_stem = Path(config_path).stem

    # Try team-specific context first, then generic
    candidates = [
        config_dir / f"{config_stem}_context.yaml",
        config_dir / "context.yaml",
    ]

    for path in candidates:
        if path.exists():
            try:
                with open(path) as f:
                    raw = yaml.safe_load(f) or {}
                ctx = BusinessContext(raw)
                logger.info(f"Business context loaded from {path}: {ctx.summary_line()}")
                return ctx
            except Exception as e:
                logger.warning(f"Failed to load business context from {path}: {e}")

    logger.debug("No business context file found — using defaults")
    return BusinessContext()


def create_example(output_path: str):
    """Write an example context.yaml to help teams get started."""
    example = """\
# ─────────────────────────────────────────────────────────────────
# PULSE Business Context — tells PULSE what "normal" looks like
# Save as: configs/YOUR_CONFIG_context.yaml
# ─────────────────────────────────────────────────────────────────

product_name: "ExampleApp"
team_name: "ExampleApp Engineering"

# ── What's normal ──────────────────────────────────────────────
baselines:
  daily_events: 2_000_000_000    # 2B events/day is normal
  weekly_events: 14_000_000_000  # ~14B/week
  active_orgs: 450               # ~450 orgs active per week
  active_users: 12000            # ~12K users active per week

# ── When to alert ──────────────────────────────────────────────
thresholds:
  alert_drop_pct: 25      # 🔴 flag if WoW drops more than 25%
  warn_drop_pct: 10       # 🟡 warn if WoW drops more than 10%
  alert_spike_pct: 100    # 🔴 flag if WoW spikes more than 100%
  concentration_warn_pct: 70  # 🟡 warn if top region > 70% share

# ── Regional expectations ──────────────────────────────────────
regions:
  EMEA:
    share_pct: 70
    note: "primary market, always dominant"
  NA:
    share_pct: 20
    note: "secondary market"
  GBR:
    share_pct: 15
    note: "subset of EMEA, key customer base"

# ── Seasonality ───────────────────────────────────────────────
# These adjustments are applied when framing WoW changes.
# e.g. if january is -15%, a 10% drop in January is actually normal.
seasonality:
  monthly_pct:
    january: -15      # January is typically 15% slower
    august: -10       # Summer slowdown
    december: -25     # Holiday period, very quiet
  quarterly_pct:
    q1: -10           # Q1 overall slower
    q4: +5            # Q4 slightly higher
  quiet_periods:
    - "Christmas week (Dec 24 – Jan 1)"
    - "Diwali week (varies)"

# ── KPIs to track ─────────────────────────────────────────────
kpis:
  - name: "Daily active orgs"
    metric: "active_orgs"
    target: 500
    note: "OKR target for FY25"
  - name: "Daily events"
    metric: "events"
    target: 2_000_000_000

# ── Known events calendar ─────────────────────────────────────
# PULSE will mention these when reviewing data near these dates
events:
  - date: "2025-03-15"
    name: "v2.0 feature launch"
    expected_impact: "+20%"
  - date: "2025-06-01"
    name: "Regional rollout to ANZ"
    expected_impact: "+5%"

# ── General notes ─────────────────────────────────────────────
# Shown in executive briefings and health checks
notes:
  - "EMAA (not EMEA) is a legacy label for some orgs — treat as equivalent"
  - "Pipeline gaps on weekends are expected — look at 7-day rolling avg"
  - "GBR and FRA are subsets of EMEA, included separately for drill-down"
"""
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        f.write(example)
    logger.info(f"Example context written to {output_path}")


# ─────────────────────────────────────────────────────────────────────────────
# Utility
# ─────────────────────────────────────────────────────────────────────────────

def _fmt(n) -> str:
    if n is None:
        return "—"
    n = float(n)
    if abs(n) >= 1e12: return f"{n/1e12:.1f}T"
    if abs(n) >= 1e9:  return f"{n/1e9:.1f}B"
    if abs(n) >= 1e6:  return f"{n/1e6:.1f}M"
    if abs(n) >= 1e3:  return f"{n/1e3:.1f}K"
    return f"{n:,.0f}"
