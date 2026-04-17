"""
Smart Visualizer v3.0 — Premium Analytics Charts
===================================================

Upgrade from v2.2:
  - Gradient bar charts (light→dark based on value)
  - Line charts with area fill + peak/trough annotations
  - Modern donut charts with center label
  - Horizontal bars with gradient + value labels
  - Clean grid styling (subtle, no clutter)
  - Auto-annotation of notable data points
  - Responsive heights based on data shape
  - Professional typography throughout

Same detection logic. Same API. Just much better looking.

Author: PULSE Team
"""

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, Any, Optional, Tuple, List
import logging
import re

logger = logging.getLogger(__name__)

# ★ v9.2: Dynamic date boundary — current year + 1, not hardcoded 2035
_CURRENT_YEAR = __import__('datetime').datetime.now().year
_MAX_VALID_YEAR = _CURRENT_YEAR + 1

# ── Color System ─────────────────────────────────────────────────────────────

# Primary gradient (for single-series bars — value-mapped)
GRADIENT = [
    "#DBEAFE", "#BFDBFE", "#93C5FD", "#60A5FA",
    "#3B82F6", "#2563EB", "#1D4ED8",
]

# Multi-series palette (vibrant, accessible, never monotone)
PALETTE = [
    "#3B82F6",  # blue
    "#10B981",  # emerald
    "#F59E0B",  # amber
    "#EF4444",  # red
    "#8B5CF6",  # violet
    "#EC4899",  # pink
    "#06B6D4",  # cyan
    "#F97316",  # orange
    "#14B8A6",  # teal
    "#6366F1",  # indigo
]

# Single accent
ACCENT = "#2563EB"
ACCENT_LIGHT = "rgba(37,99,235,0.08)"
ACCENT_MED = "rgba(37,99,235,0.15)"

# Annotation style
ANNO_FONT = dict(size=11, color="#64748B", family="system-ui")
ANNO_FONT_BOLD = dict(size=12, color="#0F172A", family="system-ui")


class VisualizationType:
    BAR_CHART = "bar"
    HORIZONTAL_BAR = "hbar"
    GROUPED_BAR = "grouped_bar"
    STACKED_BAR = "stacked_bar"
    LINE_CHART = "line"
    MULTI_LINE = "multi_line"
    PIE_CHART = "pie"
    TABLE_ONLY = "table"
    EXECUTIVE_SUMMARY = "summary"



def _safe_parse_dates(series: 'pd.Series') -> 'pd.Series':
    """
    Parse a series to datetime, handling:
    - timezone-aware values (strip tz)
    - .NET ticks (100ns since year 0001) — detected by values > 6e17
    - Unix ms — detected by reasonable ms range
    - String dates
    v6.0: Filters outlier dates instead of re-interpreting entire column.
    """
    import pandas as _pd
    s = series.copy()

    # Already datetime — just strip tz and filter outliers
    if _pd.api.types.is_datetime64_any_dtype(s):
        try:
            if hasattr(s.dt, 'tz') and s.dt.tz is not None:
                s = s.dt.tz_localize(None)
        except Exception:
            pass

        # v6.0: Check if MOST dates are sane (using median, not max).
        # Only attempt .NET ticks if the MEDIAN is out of range — meaning
        # the whole column is wrong, not just a few outliers.
        try:
            years = s.dropna().dt.year
            median_year = years.median()

            if median_year > _MAX_VALID_YEAR or median_year < 2000:
                # Majority of dates are wrong — try .NET ticks conversion
                numeric = s.astype('int64')
                NET_EPOCH_TICKS = 621355968000000000
                if numeric.median() > 6e17:
                    unix_ns = (numeric - NET_EPOCH_TICKS) * 100
                    s = _pd.to_datetime(unix_ns, unit='ns', errors='coerce')
            else:
                # Most dates are fine — just NaN the outliers
                bad_mask = (years < 2000) | (years > _MAX_VALID_YEAR)
                if bad_mask.any():
                    import logging as _log
                    _log.getLogger(__name__).warning(
                        f"_safe_parse_dates: nullifying {bad_mask.sum()} outlier date(s) "
                        f"(years {years[bad_mask].min()}-{years[bad_mask].max()})"
                    )
                    s = s.copy()
                    s.loc[s.dt.year > _MAX_VALID_YEAR] = _pd.NaT
                    s.loc[s.dt.year < 2000] = _pd.NaT
        except Exception:
            pass
        return s

    # Numeric — detect format
    try:
        numeric = _pd.to_numeric(s, errors='coerce')
        if numeric.notna().all():
            med = numeric.median()
            NET_EPOCH_TICKS = 621355968000000000
            if med > 6e17:  # .NET ticks
                unix_ns = (numeric - NET_EPOCH_TICKS) * 100
                return _pd.to_datetime(unix_ns, unit='ns', errors='coerce')
            elif med > 1e12:  # milliseconds
                return _pd.to_datetime(numeric, unit='ms', errors='coerce')
            elif med > 1e9:   # seconds
                return _pd.to_datetime(numeric, unit='s', errors='coerce')
    except Exception:
        pass

    # String — standard parse + tz strip
    try:
        result = _pd.to_datetime(s, errors='coerce', utc=True)
        if hasattr(result.dt, 'tz') and result.dt.tz is not None:
            result = result.dt.tz_localize(None)
        return result
    except Exception:
        pass

    return s


class SmartVisualizer:
    """Premium chart generation with automatic type detection."""

    def __init__(self):
        self._columns_hint = None  # ★ iter14.2: explicit column mapping
        self.ranking_kw = [
            "top", "bottom", "highest", "lowest", "most", "least",
            "largest", "smallest", "biggest", "best", "worst",
        ]
        self.time_kw = [
            "per day", "over time", "daily", "trend", "growing",
            "timeline", "day over day", "day-over-day", "week",
            "trajectory", "per week", "pattern",
        ]
        self.distribution_kw = [
            "by region", "by entity", "by geo", "breakdown",
            "distribution", "split", "across", "per region",
            "per entity",
        ]
        self.comparison_kw = [
            "compare", "vs", "versus", "first half", "second half",
            "crosstab", "cross-tab",
        ]
        self.pie_kw = [
            "percentage", "share", "proportion", "what percentage",
            "what fraction", "pie",
        ]
        self.exec_kw = [
            "summary", "manager", "tldr", "tl;dr", "health",
            "overview", "brief", "executive", "highlight",
        ]
        self.bar_kw = [
            "events per region", "orgs per region", "sessions per region",
            "how many events", "event counts", "count by",
            "most active", "most diverse", "most used",
        ]

    # ── Public API ───────────────────────────────────────────────────────────

    def analyze_and_visualize(
        self, df: pd.DataFrame, query: str, kql: str,
        intent_hint: str = '',
        columns_hint: dict = None,
    ) -> Dict[str, Any]:
        """
        columns_hint: optional dict with {'value': 'col_name', 'label': 'col_name'}
                      When provided, bypasses _val_lbl() heuristic entirely.
        """
        if df.empty:
            return self._empty()

        # ★ iter14.2: Inject column hints into DataFrame metadata
        # so _val_lbl() doesn't have to guess
        if columns_hint:
            self._columns_hint = columns_hint
        else:
            self._columns_hint = None

        # ★ iter16: Force numpy types → Python native for Plotly/JSON safety
        for col in df.columns:
            if hasattr(df[col], 'dtype'):
                import numpy as np
                if np.issubdtype(df[col].dtype, np.integer):
                    df[col] = df[col].astype(int)
                elif np.issubdtype(df[col].dtype, np.floating):
                    df[col] = df[col].astype(float)

        if intent_hint:
            chart_type = self._type_from_intent(df, intent_hint)
        else:
            chart_type = self._detect(df, query, kql)

        logger.info(f"Viz type: {chart_type} | intent={intent_hint or 'none'} | query: {query[:60]}")

        dispatch = {
            VisualizationType.BAR_CHART: self._bar,
            VisualizationType.HORIZONTAL_BAR: self._hbar,
            VisualizationType.GROUPED_BAR: self._grouped_bar,
            VisualizationType.STACKED_BAR: self._stacked_bar,
            VisualizationType.LINE_CHART: self._line,
            VisualizationType.MULTI_LINE: self._multi_line,
            VisualizationType.PIE_CHART: self._pie,
            VisualizationType.EXECUTIVE_SUMMARY: self._executive,
        }

        handler = dispatch.get(chart_type)
        if handler:
            _tagged_query = f"[{intent_hint}] {query}" if intent_hint else query
            result = handler(df, _tagged_query)
            if result and result.get("chart") is not None:
                return result
            # Handler explicitly gave up (bad dates, wrong shape) — respect it,
            # don't fall to _auto_fallback which would render a broken bar chart.
            if result and result.get("type") == VisualizationType.TABLE_ONLY:
                return result

        return self._auto_fallback(df, query)

    def _type_from_intent(self, df: pd.DataFrame, intent: str) -> str:
        if intent in ('trend', 'summary', 'health'):
            if self._has_time_col(df):
                return VisualizationType.LINE_CHART
            return VisualizationType.BAR_CHART

        if intent in ('ranking', 'ranking_bottom'):
            return VisualizationType.HORIZONTAL_BAR

        if intent in ('total', 'overview'):
            return VisualizationType.EXECUTIVE_SUMMARY

        if intent == 'lookup':
            if 1 < len(df) <= 6:
                return VisualizationType.PIE_CHART
            return VisualizationType.BAR_CHART

        if intent == 'compare':
            num_cols = df.select_dtypes(include='number').columns.tolist()
            if len(num_cols) >= 2:
                return VisualizationType.GROUPED_BAR
            return VisualizationType.HORIZONTAL_BAR

        return self._detect(df, '', '')

    # ── Detection (unchanged logic) ──────────────────────────────────────────

    def _detect(self, df: pd.DataFrame, query: str, kql: str) -> str:
        q = query.lower()
        k = kql.lower() if kql else ""

        if any(w in q for w in self.exec_kw):
            return VisualizationType.EXECUTIVE_SUMMARY

        has_time = self._has_time_col(df)

        if has_time:
            group_cols = self._non_time_non_numeric_cols(df)
            num_cols = df.select_dtypes(include="number").columns.tolist()
            if group_cols and num_cols:
                unique_groups = df[group_cols[0]].nunique()
                if unique_groups <= 12 and any(w in q for w in self.time_kw + self.distribution_kw + self.comparison_kw):
                    return VisualizationType.MULTI_LINE

            if any(w in q for w in self.time_kw):
                return VisualizationType.LINE_CHART
            if "bin(" in k or "startofday" in k or "startofweek" in k:
                return VisualizationType.LINE_CHART

        if any(w in q for w in self.pie_kw):
            if 1 < len(df) <= 8:
                return VisualizationType.PIE_CHART

        if any(w in q for w in self.comparison_kw):
            num_cols = df.select_dtypes(include="number").columns.tolist()
            if len(num_cols) >= 2:
                return VisualizationType.GROUPED_BAR
            return VisualizationType.BAR_CHART

        if any(w in q for w in self.ranking_kw):
            return VisualizationType.HORIZONTAL_BAR

        if any(w in q for w in self.distribution_kw + self.bar_kw):
            if 1 < len(df) <= 6 and len(df.columns) == 2:
                return VisualizationType.PIE_CHART
            return VisualizationType.BAR_CHART

        if "summarize" in k and "by" in k:
            if "order" in k and "desc" in k:
                return VisualizationType.HORIZONTAL_BAR
            return VisualizationType.BAR_CHART

        if len(df.columns) == 2:
            val, lbl = self._val_lbl(df)
            if val and lbl:
                if len(df) <= 6:
                    return VisualizationType.PIE_CHART
                return VisualizationType.BAR_CHART

        return VisualizationType.TABLE_ONLY

    # ═════════════════════════════════════════════════════════════════════════
    # ★ PREMIUM CHART BUILDERS
    # ═════════════════════════════════════════════════════════════════════════

    def _bar(self, df: pd.DataFrame, query: str) -> Dict:
        """Vertical bar chart with value-mapped gradient coloring."""
        val, lbl = self._val_lbl(df)
        if not val or not lbl:
            return self._table_only(df)

        dfs = df.copy()
        dfs[val] = pd.to_numeric(dfs[val], errors='coerce').fillna(0)

        # ★ iter14.2: Sanity check — if max value is suspiciously low,
        # _val_lbl probably picked the wrong column. Try alternatives.
        if dfs[val].max() < 10 and len(dfs) > 3:
            # Try other numeric columns for a better value column
            for c in dfs.select_dtypes(include='number').columns:
                if c == val or c.lower() in ('day', 'date', 'timestamp'):
                    continue
                test_max = dfs[c].max()
                if test_max > dfs[val].max() * 100:  # Much larger → better candidate
                    logger.info(f"_bar: Swapped val {val} (max={dfs[val].max()}) → {c} (max={test_max})")
                    val = c
                    break

        dfs = dfs.sort_values(val, ascending=False).head(20)
        dfs[lbl] = dfs[lbl].astype(str).str[:30]

        # ★ iter13: Single-item stat card instead of sad 1-bar chart
        if len(dfs) <= 2:
            return self._stat_card(dfs, val, lbl, query)

        # Value-mapped colors: highest = darkest blue
        max_val = dfs[val].max()
        min_val = dfs[val].min()
        if max_val > min_val:
            normalized = (dfs[val] - min_val) / (max_val - min_val)
        else:
            normalized = pd.Series([0.5] * len(dfs), index=dfs.index)

        colors = [
            f"rgba(37,99,235,{0.3 + 0.7 * v})" for v in normalized
        ]

        fig = go.Figure(go.Bar(
            x=dfs[lbl],
            y=dfs[val],
            marker=dict(
                color=colors,
                line=dict(width=0),
                cornerradius=4,
            ),
            text=dfs[val].apply(self._fmt),
            textposition="outside",
            textfont=dict(size=12, color="#374151", family="system-ui"),
            hovertemplate="<b>%{x}</b><br>%{y:,.0f}<extra></extra>",
        ))

        fig.update_layout(**self._premium_layout(query, height=420))
        fig.update_xaxes(tickangle=-35, tickfont=dict(size=11))
        fig.update_yaxes(
            showgrid=True, gridcolor="#F1F5F9", gridwidth=1,
            zeroline=True, zerolinecolor="#E2E8F0",
            rangemode="tozero",
            exponentformat="SI",
            tickformat="~s",
        )

        return self._result(VisualizationType.BAR_CHART, fig, self._bar_insights(dfs, val, lbl))

    def _hbar(self, df: pd.DataFrame, query: str) -> Dict:
        """Horizontal bar chart — the money chart for rankings."""
        val, lbl = self._val_lbl(df)
        if not val or not lbl:
            return self._table_only(df)

        dfs = df.copy()
        dfs[val] = pd.to_numeric(dfs[val], errors='coerce').fillna(0)

        # ★ iter14.2: Same sanity check as _bar
        if dfs[val].max() < 10 and len(dfs) > 3:
            for c in dfs.select_dtypes(include='number').columns:
                if c == val or c.lower() in ('day', 'date', 'timestamp'):
                    continue
                test_max = dfs[c].max()
                if test_max > dfs[val].max() * 100:
                    logger.info(f"_hbar: Swapped val {val} (max={dfs[val].max()}) → {c} (max={test_max})")
                    val = c
                    break
        dfs = dfs.head(20)

        # ★ iter13: Single-item stat card
        if len(dfs) <= 2:
            return self._stat_card(dfs, val, lbl, query)

        # Detect bottom query BEFORE manipulating display order
        is_bottom = '[ranking_bottom]' in query or 'bottom' in query.lower()

        if is_bottom:
            # Reverse so lowest-activity org sits at TOP of chart.
            # Plotly hbar draws y[0] at bottom, y[-1] at top.
            # Without reversal, the highest-value org dominates at top
            # and the chart looks identical to a top-10 chart.
            dfs = dfs.sort_values(val, ascending=True)   # lowest first in data
            dfs = dfs.iloc[::-1].reset_index(drop=True)  # flip: lowest now at top
        else:
            dfs = dfs.sort_values(val, ascending=True)   # Plotly hbar looks best low→high

        dfs[lbl] = dfs[lbl].astype(str).str[:30]

        n = len(dfs)
        max_val = dfs[val].max()
        min_val = dfs[val].min()

        # Use amber for bottom queries, blue for top (is_bottom set above)
        if max_val > min_val:
            normalized = (dfs[val] - min_val) / (max_val - min_val)
        else:
            normalized = pd.Series([0.5] * n, index=dfs.index)

        if is_bottom:
            # Amber gradient: highest in list = most amber, lowest = pale amber
            colors = [f"rgba(217,119,6,{0.3 + 0.7 * v})" for v in normalized]
        else:
            # Blue gradient for top queries
            colors = [f"rgba(37,99,235,{0.25 + 0.75 * v})" for v in normalized]

        fig = go.Figure(go.Bar(
            x=dfs[val],
            y=dfs[lbl],
            orientation="h",
            marker=dict(
                color=colors,
                line=dict(width=0),
                cornerradius=3,
            ),
            text=dfs[val].apply(self._fmt),
            textposition="outside",
            textfont=dict(size=12, color="#374151", family="system-ui"),
            hovertemplate="<b>%{y}</b><br>%{x:,.0f}<extra></extra>",
        ))

        chart_height = max(380, n * 36 + 100)
        layout = self._premium_layout(query, height=chart_height)
        layout['margin'] = dict(l=180, r=80, t=60, b=40)
        fig.update_layout(**layout)

        fig.update_xaxes(
            showgrid=True, gridcolor="#F1F5F9", gridwidth=1,
            zeroline=True, zerolinecolor="#E2E8F0",
            rangemode="tozero",
            exponentformat="SI",
            tickformat="~s",
        )
        fig.update_yaxes(
            showgrid=False,
            tickfont=dict(size=12),
        )

        return self._result(VisualizationType.HORIZONTAL_BAR, fig,
                            self._bar_insights(dfs.sort_values(val, ascending=False), val, lbl))

    def _grouped_bar(self, df: pd.DataFrame, query: str) -> Dict:
        """Grouped bar chart for comparisons — multi-color, clean."""
        num_cols = df.select_dtypes(include="number").columns.tolist()
        lbl_cols = df.select_dtypes(exclude="number").columns.tolist()
        if not num_cols or not lbl_cols:
            return self._bar(df, query)

        lbl = lbl_cols[0]
        dfs = df.head(15).copy()
        dfs[lbl] = dfs[lbl].astype(str).str[:25]

        fig = go.Figure()
        for i, nc in enumerate(num_cols[:5]):
            fig.add_trace(go.Bar(
                x=dfs[lbl], y=dfs[nc], name=nc,
                marker=dict(
                    color=PALETTE[i % len(PALETTE)],
                    line=dict(width=0),
                    cornerradius=3,
                ),
                text=dfs[nc].apply(self._fmt),
                textposition="outside",
                textfont=dict(size=11),
                hovertemplate=f"<b>%{{x}}</b><br>{nc}: %{{y:,.0f}}<extra></extra>",
            ))

        layout = self._premium_layout(query, height=450)
        layout['barmode'] = 'group'
        layout['legend'] = dict(
            orientation="h", y=-0.18, x=0.5, xanchor="center",
            font=dict(size=12), bgcolor="rgba(0,0,0,0)",
        )
        fig.update_layout(**layout)
        fig.update_xaxes(tickangle=-25)
        fig.update_yaxes(showgrid=True, gridcolor="#F1F5F9")

        insights = [f"Comparing **{', '.join(num_cols[:5])}** across **{lbl}**"]
        return self._result(VisualizationType.GROUPED_BAR, fig, insights)

    def _stacked_bar(self, df: pd.DataFrame, query: str) -> Dict:
        r = self._grouped_bar(df, query)
        if r.get("chart"):
            r["chart"].update_layout(barmode="stack")
            r["type"] = VisualizationType.STACKED_BAR
        return r

    def _line(self, df: pd.DataFrame, query: str) -> Dict:
        """Line chart with area fill, peak/trough annotations, clean grid."""
        tcol = self._time_col(df)
        vcol = self._val_col(df)
        if not tcol or not vcol:
            return self._table_only(df)

        dfs = df.copy()

        dfs[tcol] = _safe_parse_dates(dfs[tcol])

        dfs = dfs.dropna(subset=[tcol])
        if len(dfs) < 2:
            return self._table_only(df)

        dfs = dfs.sort_values(tcol)
        dfs[vcol] = pd.to_numeric(dfs[vcol], errors='coerce')

        # Sanity check: if all dates cluster at the same point (e.g. 1970-01-01 nanoseconds),
        # this is not a real time series — fall back to table view
        try:
            _span = (dfs[tcol].max() - dfs[tcol].min()).total_seconds()
            _min_year = dfs[tcol].dt.year.min()
            if _span < 60 or _min_year < 2000:
                # Dates are garbage — return table only
                import logging as _log
                _log.getLogger(__name__).warning(
                    f"_line: date column looks invalid (span={_span:.0f}s, min_year={_min_year}) — table only"
                )
                return self._table_only(df)
        except Exception:
            pass

        # ── Clamp dates — drop future rows and far-past outliers ──
        # Future rows (tomorrow onwards) are zero-filled placeholders from Kusto.
        # They create a flat "Mar 1 → Mar 15" line that sends a wrong signal.
        # Bad timestamps (epoch overflow, .NET ticks) stretch axis to 2030.
        try:
            _now = pd.Timestamp.now(tz='UTC').normalize().tz_localize(None)
            _min_valid = _now - pd.DateOffset(years=5)
            _max_valid = _now + pd.DateOffset(days=1)  # include today fully
            # Strip tz from data if present (compare apples to apples)
            _dates = dfs[tcol]
            if hasattr(_dates.dt, 'tz') and _dates.dt.tz is not None:
                _dates = _dates.dt.tz_localize(None)
            _before = len(dfs)
            _filtered = dfs[(_dates >= _min_valid) & (_dates <= _max_valid)]
            # Safety net: NEVER drop all rows — if filter empties, keep originals
            if len(_filtered) >= 2:
                dfs = _filtered
                if len(dfs) < _before:
                    import logging as _log
                    _log.getLogger(__name__).info(
                        f"_line: dropped {_before - len(dfs)} rows outside valid range"
                    )
            elif _before > 0:
                import logging as _log
                _log.getLogger(__name__).warning(
                    f"_line: date filter would drop ALL {_before} rows — keeping originals"
                )
        except Exception:
            pass

        if len(dfs) < 2:
            return self._table_only(df)

        # ── Main line + area fill ──
        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=dfs[tcol], y=dfs[vcol],
            mode="lines+markers",
            name=vcol,
            line=dict(color=ACCENT, width=3, shape="spline"),
            marker=dict(size=7, color=ACCENT, line=dict(width=2, color="white")),
            fill="tozeroy",
            fillcolor=ACCENT_LIGHT,
            hovertemplate="%{x|%b %d}<br><b>%{y:,.0f}</b><extra></extra>",
        ))

        # ── Drop detection — annotate if recent values fell sharply ──────────
        # For health/drop questions, show exactly WHERE the cliff is
        _is_drop_question = any(
            w in query.lower() for w in
            ['drop','fell','decline','decrease','down','lower','why','issue','problem','wrong']
        )
        try:
            if _is_drop_question and len(dfs) >= 5:
                _vals = dfs[vcol].dropna()
                _recent = _vals.iloc[-3:].mean()
                _baseline = _vals.iloc[:-3].mean()
                _pct = (_recent - _baseline) / _baseline * 100 if _baseline > 0 else 0
                if _pct < -20:  # real drop
                    # Find the first day that fell below 50% of baseline
                    _threshold = _baseline * 0.5
                    _drop_mask = _vals < _threshold
                    if _drop_mask.any():
                        _drop_idx = _drop_mask.idxmax()
                        _drop_date = dfs.loc[_drop_idx, tcol]
                        _drop_val  = dfs.loc[_drop_idx, vcol]
                        fig.add_vline(
                            x=_drop_date,
                            line=dict(color="#EF4444", width=2, dash="dash"),
                            annotation_text=f"Drop starts",
                            annotation_position="top right",
                            annotation_font=dict(size=11, color="#EF4444"),
                        )
                        fig.add_annotation(
                            x=_drop_date, y=_drop_val,
                            text=f"↓ {abs(_pct):.0f}% drop",
                            showarrow=True, arrowhead=2,
                            arrowcolor="#EF4444", arrowwidth=2,
                            ax=40, ay=-40,
                            font=dict(size=12, color="#991B1B", family="system-ui", bold=True),
                            bgcolor="rgba(254,226,226,0.95)",
                            bordercolor="#EF4444", borderwidth=1.5, borderpad=5,
                        )
        except Exception:
            pass

        # ── Average line ──
        avg_val = dfs[vcol].mean()
        fig.add_hline(
            y=avg_val,
            line=dict(color="#94A3B8", width=1.5, dash="dot"),
            annotation=dict(
                text=f"Avg: {self._fmt(avg_val)}",
                font=ANNO_FONT,
                xanchor="left",
            ),
        )

        # ── Peak annotation ──
        if len(dfs) >= 3:
            peak_idx = dfs[vcol].idxmax()
            peak_val = dfs[vcol].loc[peak_idx]
            peak_date = dfs[tcol].loc[peak_idx]

            fig.add_annotation(
                x=peak_date, y=peak_val,
                text=f"Peak: {self._fmt(peak_val)}",
                showarrow=True,
                arrowhead=0,
                arrowcolor="#10B981",
                arrowwidth=1.5,
                ax=0, ay=-35,
                font=dict(size=11, color="#065F46", family="system-ui"),
                bgcolor="rgba(209,250,229,0.9)",
                bordercolor="#10B981",
                borderwidth=1,
                borderpad=4,
            )

            # ── Trough annotation (only if significantly different) ──
            trough_idx = dfs[vcol].idxmin()
            trough_val = dfs[vcol].loc[trough_idx]
            trough_date = dfs[tcol].loc[trough_idx]

            if avg_val > 0 and (avg_val - trough_val) / avg_val > 0.2:
                fig.add_annotation(
                    x=trough_date, y=trough_val,
                    text=f"Low: {self._fmt(trough_val)}",
                    showarrow=True,
                    arrowhead=0,
                    arrowcolor="#EF4444",
                    arrowwidth=1.5,
                    ax=0, ay=30,
                    font=dict(size=11, color="#991B1B", family="system-ui"),
                    bgcolor="rgba(254,226,226,0.9)",
                    bordercolor="#EF4444",
                    borderwidth=1,
                    borderpad=4,
                )

        # ── Layout ──
        layout = self._premium_layout(query, height=420)
        layout['hovermode'] = 'x unified'
        fig.update_layout(**layout)

        # Smart tickformat based on actual date span
        try:
            _span_days = (dfs[tcol].max() - dfs[tcol].min()).days
        except Exception:
            _span_days = 0
        if _span_days > 180:
            _tick_fmt = "%b %Y"
        elif _span_days > 60:
            _tick_fmt = "%b %d '%y"
        else:
            _tick_fmt = "%b %d"

        fig.update_xaxes(
            showgrid=True, gridcolor="#F1F5F9", gridwidth=1,
            type="date",
            tickformat=_tick_fmt,
            dtick="D1" if len(dfs) <= 14 else None,
        )
        fig.update_yaxes(
            showgrid=True, gridcolor="#F1F5F9", gridwidth=1,
            zeroline=True, zerolinecolor="#E2E8F0",
            rangemode="tozero",
            exponentformat="SI",
            tickformat="~s",
        )

        return self._result(VisualizationType.LINE_CHART, fig,
                            self._time_insights(dfs, tcol, vcol))

    def _multi_line(self, df: pd.DataFrame, query: str) -> Dict:
        """Multi-series line chart — each series gets its own color."""
        tcol = self._time_col(df)
        if not tcol:
            return self._line(df, query)

        group_cols = self._non_time_non_numeric_cols(df)
        vcol = self._val_col(df)
        if not group_cols or not vcol:
            return self._line(df, query)

        gcol = group_cols[0]
        groups = df[gcol].unique()[:10]

        fig = go.Figure()
        for i, g in enumerate(groups):
            sub = df[df[gcol] == g].sort_values(tcol)
            color = PALETTE[i % len(PALETTE)]
            fig.add_trace(go.Scatter(
                x=sub[tcol], y=sub[vcol],
                mode="lines+markers",
                name=str(g),
                line=dict(color=color, width=2.5, shape="spline"),
                marker=dict(size=5, color=color, line=dict(width=1.5, color="white")),
                hovertemplate=f"<b>{g}</b><br>%{{x|%b %d}}: %{{y:,.0f}}<extra></extra>",
            ))

        layout = self._premium_layout(query, height=450)
        layout['hovermode'] = 'x unified'
        layout['legend'] = dict(
            orientation="h", y=-0.18, x=0.5, xanchor="center",
            font=dict(size=12), bgcolor="rgba(0,0,0,0)",
        )
        fig.update_layout(**layout)
        fig.update_xaxes(showgrid=True, gridcolor="#F1F5F9", tickformat="%b %d")
        fig.update_yaxes(showgrid=True, gridcolor="#F1F5F9")

        insights = [f"Showing **{len(groups)}** series by **{gcol}** over **{tcol}**"]
        return self._result(VisualizationType.MULTI_LINE, fig, insights)

    def _pie(self, df: pd.DataFrame, query: str) -> Dict:
        """Modern donut chart with center total label."""
        val, lbl = self._val_lbl(df)
        if not val or not lbl:
            return self._bar(df, query)

        dfs = df.copy()
        dfs[val] = pd.to_numeric(dfs[val], errors='coerce').fillna(0)
        total = dfs[val].sum()

        fig = go.Figure(go.Pie(
            labels=dfs[lbl].astype(str),
            values=dfs[val],
            hole=0.55,
            marker=dict(
                colors=PALETTE[:len(dfs)],
                line=dict(color="white", width=2.5),
            ),
            textinfo="label+percent",
            textfont=dict(size=13, family="system-ui"),
            hovertemplate="<b>%{label}</b><br>%{value:,.0f} (%{percent})<extra></extra>",
            sort=False,
        ))

        # Center annotation with total
        fig.add_annotation(
            text=f"<b>{self._fmt(total)}</b><br><span style='font-size:11px;color:#64748B'>Total</span>",
            x=0.5, y=0.5,
            xref="paper", yref="paper",
            showarrow=False,
            font=dict(size=20, color="#0F172A", family="system-ui"),
        )

        layout = self._premium_layout(query, height=450)
        layout['margin'] = dict(l=20, r=20, t=60, b=20)
        layout['showlegend'] = True
        layout['legend'] = dict(
            orientation="h", y=-0.1, x=0.5, xanchor="center",
            font=dict(size=12),
        )
        fig.update_layout(**layout)

        try:
            top_idx = dfs[val].idxmax()
            pct = float(dfs[val][top_idx]) / total * 100 if total else 0
            insights = [
                f"**{dfs[lbl][top_idx]}** leads at {pct:.1f}%",
                f"Total: {self._fmt(total)}",
            ]
        except Exception:
            insights = [f"**{len(dfs)}** categories"]
        return self._result(VisualizationType.PIE_CHART, fig, insights)

    def _executive(self, df: pd.DataFrame, query: str) -> Dict:
        """★ iter15: KPI card for wide-row data (e.g. profile_totals: 1 row × N cols)."""
        nc = df.select_dtypes(include="number").columns.tolist()

        # If single row with multiple numeric columns → multi-KPI indicator
        if len(df) <= 2 and len(nc) >= 2:
            return self._totals_kpi(df, nc, query)

        # Fallback: text-only summary
        insights = ["📊 **Executive Summary**", ""]
        insights.append(f"**Records:** {len(df):,}  •  **Columns:** {df.shape[1]}")
        for col in nc[:5]:
            t, a = df[col].sum(), df[col].mean()
            insights.append(f"**{col}:** {self._fmt(t)} total, {self._fmt(a)} avg")
        return self._result(VisualizationType.EXECUTIVE_SUMMARY, None, insights)

    def _totals_kpi(self, df: pd.DataFrame, num_cols: list, query: str) -> Dict:
        """★ iter15: Multi-KPI indicator card for profile_totals (1 row × N numeric cols)."""
        # Clean title
        title = re.sub(r'^\[[^\]]+\]\s*', '', query.strip())
        if title:
            title = title[0].upper() + title[1:]
            if len(title) > 65:
                title = title[:62] + "…"

        # Friendly column names
        _friendly = {
            'events': 'Total Events', 'event_count': 'Total Events',
            'active_orgs': 'Active Orgs', 'entity_types': 'Entity Types',
            'total_events': 'Total Events', 'sessions': 'Sessions',
            'active_users': 'Active Users', 'total_orgs': 'Total Orgs',
        }

        cols_to_show = num_cols[:4]  # Max 4 KPI cards
        n = len(cols_to_show)
        indicators = []

        for idx, col in enumerate(cols_to_show):
            val = float(df[col].iloc[0]) if len(df) > 0 else 0
            friendly_name = _friendly.get(col.lower(), col.replace('_', ' ').title())

            x_start = idx / n + 0.01
            x_end = (idx + 1) / n - 0.01

            # Pick format based on magnitude
            if val >= 1e9:
                vfmt = ",.2s"
            elif val >= 1e6:
                vfmt = ",.2s"
            elif val >= 1000:
                vfmt = ",.0f"
            else:
                vfmt = ",.1f"

            indicators.append(go.Indicator(
                mode="number",
                value=val,
                title=dict(text=friendly_name, font=dict(size=13, color="#64748b")),
                number=dict(
                    font=dict(size=34, color="#0f172a", family="DM Sans, system-ui"),
                    valueformat=vfmt,
                ),
                domain=dict(x=[x_start, x_end], y=[0.1, 0.85]),
            ))

        fig = go.Figure(data=indicators)
        fig.update_layout(
            title=dict(
                text=title or "Key Metrics",
                font=dict(size=15, color="#0F172A", family="DM Sans, system-ui"),
                x=0.02, xanchor="left", y=0.97,
                automargin=True,
            ),
            height=170,
            autosize=True,
            margin=dict(l=10, r=10, t=45, b=5),
            paper_bgcolor="white",
            plot_bgcolor="white",
            font=dict(family="DM Sans, system-ui", color="#334155"),
        )

        # Text insights as well
        insights = []
        for col in cols_to_show:
            val = float(df[col].iloc[0]) if len(df) > 0 else 0
            friendly_name = _friendly.get(col.lower(), col.replace('_', ' ').title())
            insights.append(f"**{friendly_name}:** {self._fmt(val)}")

        return self._result("stat_card", fig, insights)

    # ── Fallback ─────────────────────────────────────────────────────────────

    def _auto_fallback(self, df: pd.DataFrame, query: str) -> Dict:
        val, lbl = self._val_lbl(df)
        if val and lbl:
            if len(df) <= 2:
                return self._stat_card(df, val, lbl, query)
            if len(df) <= 30:
                return self._bar(df, query)
        return self._table_only(df)

    # ═════════════════════════════════════════════════════════════════════════
    # LAYOUT + HELPERS
    # ═════════════════════════════════════════════════════════════════════════

    def _premium_layout(self, query: str, height: int = 420) -> dict:
        """Clean, modern layout — no clutter, professional typography."""
        title = query.strip()
        # Strip internal intent tags like [ranking_bottom], [lookup] etc from visible title
        title = re.sub(r'^\[[^\]]+\]\s*', '', title).strip()
        if title:
            title = title[0].upper() + title[1:]
            if len(title) > 65:
                title = title[:62] + "…"

        return dict(
            title=dict(
                text=title,
                font=dict(size=16, color="#0F172A", family="DM Sans, system-ui"),
                x=0.01, xanchor="left",
                y=0.97,
                automargin=True,
            ),
            height=height,
            margin=dict(l=60, r=40, t=60, b=60),
            plot_bgcolor="white",
            paper_bgcolor="white",
            font=dict(
                family="DM Sans, system-ui, -apple-system, sans-serif",
                color="#334155",
                size=12,
            ),
            hoverlabel=dict(
                bgcolor="white",
                bordercolor="#E2E8F0",
                font=dict(size=13, family="DM Sans, system-ui", color="#0F172A"),
            ),
            # No legend by default (single series doesn't need it)
            showlegend=False,
        )

    # ── Column detection helpers (unchanged) ─────────────────────────────────

    def _has_time_col(self, df: pd.DataFrame) -> bool:
        for c in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[c]):
                return True
            if any(x in c.lower() for x in ["date", "time", "timestamp", "day"]):
                try:
                    pd.to_datetime(df[c].head(5), errors="raise")
                    return True
                except Exception:
                    pass
        return False

    def _time_col(self, df: pd.DataFrame) -> Optional[str]:
        for c in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[c]):
                return c
        for c in df.columns:
            if any(x in c.lower() for x in ["date", "time", "timestamp", "day"]):
                try:
                    pd.to_datetime(df[c].head(5), errors="raise")
                    df[c] = pd.to_datetime(df[c])
                    return c
                except Exception:
                    pass
        return None

    def _val_col(self, df: pd.DataFrame) -> Optional[str]:
        nc = df.select_dtypes(include="number").columns
        skip = {'day', 'date', 'time', 'timestamp', 'eventinfo_time'}
        nc = [c for c in nc if c.lower() not in skip]
        if len(nc) == 0:
            return None
        for c in nc:
            if any(x in c.lower() for x in ["count", "total", "sum", "events", "sessions", "amount"]):
                return c
        return nc[0]

    def _val_lbl(self, df: pd.DataFrame) -> Tuple[Optional[str], Optional[str]]:
        # ★ iter14.2: Use explicit column hints if available
        if self._columns_hint:
            v = self._columns_hint.get('value')
            l = self._columns_hint.get('label')
            if v and v in df.columns and l and l in df.columns:
                # Force numeric conversion on value column
                if not pd.api.types.is_numeric_dtype(df[v]):
                    df[v] = pd.to_numeric(df[v], errors='coerce').fillna(0)
                logger.info(f"_val_lbl: using hints val={v}, lbl={l}")
                return v, l

        date_cols = {'day', 'date', 'time', 'timestamp', 'eventinfo_time',
                     'first_seen', 'last_seen', 'first_event', 'last_event'}

        for c in df.columns:
            if c.lower() in date_cols:
                continue
            if not pd.api.types.is_numeric_dtype(df[c]) and not pd.api.types.is_datetime64_any_dtype(df[c]):
                try:
                    converted = pd.to_numeric(df[c], errors='coerce')
                    if converted.notna().sum() > len(df) * 0.5:
                        df[c] = converted
                except Exception:
                    pass

        nc = df.select_dtypes(include="number").columns.tolist()
        nc = [c for c in nc if c.lower() not in date_cols]

        lc = [c for c in df.columns
              if not pd.api.types.is_numeric_dtype(df[c])
              and not pd.api.types.is_datetime64_any_dtype(df[c])
              and c.lower() not in date_cols]

        if not nc:
            return None, None

        val = nc[0]
        for c in nc:
            if any(x in c.lower() for x in ["count", "total", "sum", "events", "sessions"]):
                val = c
                break

        lbl = lc[0] if lc else df.columns[0]
        for c in lc:
            if any(x in c.lower() for x in ["name", "id", "org", "region", "entity", "geo", "type"]):
                lbl = c
                break
        return val, lbl

    def _non_time_non_numeric_cols(self, df: pd.DataFrame) -> List[str]:
        out = []
        for c in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[c]):
                continue
            if any(x in c.lower() for x in ["date", "time", "timestamp"]):
                continue
            if not pd.api.types.is_numeric_dtype(df[c]):
                out.append(c)
        return out

    @staticmethod
    def _fmt(num) -> str:
        if pd.isna(num):
            return "N/A"
        num = float(num)
        if abs(num) >= 1e9:
            return f"{num/1e9:.1f}B"
        if abs(num) >= 1e6:
            return f"{num/1e6:.1f}M"
        if abs(num) >= 1e3:
            return f"{num/1e3:.1f}K"
        return f"{num:,.0f}"

    # ── Insight generators ───────────────────────────────────────────────────

    def _bar_insights(self, df: pd.DataFrame, val: str, lbl: str) -> list:
        try:
            vals = pd.to_numeric(df[val], errors='coerce')
            total = float(vals.sum())
            top_val = float(vals.iloc[0]) if len(vals) > 0 else 0
            top_lbl = df[lbl].iloc[0] if len(df) > 0 else "N/A"
            pct = top_val / total * 100 if total else 0
            ins = [
                f"**Total {val}:** {self._fmt(total)}",
                f"**#1:** {top_lbl} — {self._fmt(top_val)} ({pct:.1f}%)",
            ]
            if len(df) >= 3:
                t3 = float(vals.head(3).sum())
                ins.append(f"**Top 3** = {t3/total*100:.1f}% of total" if total else "")
            return [i for i in ins if i]
        except Exception:
            return [f"**{len(df)}** items"]

    def _time_insights(self, df: pd.DataFrame, tcol: str, vcol: str) -> list:
        try:
            vals = pd.to_numeric(df[vcol], errors='coerce')
            f_val, l_val = float(vals.iloc[0]), float(vals.iloc[-1])
            ch = ((l_val - f_val) / f_val * 100) if f_val else 0
            e = "📈" if ch > 0 else "📉" if ch < 0 else "➡️"
            mx_idx = vals.idxmax()
            mn_idx = vals.idxmin()
            return [
                f"{e} **Overall:** {ch:+.1f}%",
                f"**Peak:** {self._fmt(vals[mx_idx])} on {df[tcol].iloc[mx_idx]}",
                f"**Low:** {self._fmt(vals[mn_idx])} on {df[tcol].iloc[mn_idx]}",
                f"**Avg:** {self._fmt(vals.mean())}",
            ]
        except Exception:
            return [f"**{len(df)}** data points"]

    # ── Result helpers ───────────────────────────────────────────────────────

    @staticmethod
    def _result(vtype, fig, insights) -> Dict[str, Any]:
        return {"type": vtype, "chart": fig, "insights": insights, "show_table": True}

    def _stat_card(self, df, val_col, lbl_col, query: str) -> Dict:
        """★ iter14.2: Improved stat card — full width, no truncation, proper height."""
        title = re.sub(r'^\[[^\]]+\]\s*', '', query.strip())
        if title:
            title = title[0].upper() + title[1:]
            if len(title) > 65:
                title = title[:62] + "…"

        indicators = []
        n = len(df.head(3))
        for idx, (_, row) in enumerate(df.head(3).iterrows()):
            name = str(row[lbl_col])[:40]
            value = float(row[val_col])
            x_start = idx / max(n, 1)
            x_end = (idx + 1) / max(n, 1)
            indicators.append(go.Indicator(
                mode="number",
                value=value,
                title=dict(text=name, font=dict(size=14, color="#64748b")),
                number=dict(
                    font=dict(size=36, color="#0f172a", family="DM Sans, system-ui"),
                    valueformat=",.0f",
                ),
                domain=dict(x=[x_start + 0.02, x_end - 0.02], y=[0.05, 0.75]),
            ))

        fig = go.Figure(data=indicators)
        fig.update_layout(
            title=dict(
                text=title,
                font=dict(size=15, color="#0F172A", family="DM Sans, system-ui"),
                x=0.02, xanchor="left", y=0.95,
                automargin=True,
            ),
            height=200,
            autosize=True,
            margin=dict(l=20, r=20, t=50, b=10),
            paper_bgcolor="white",
            plot_bgcolor="white",
            font=dict(family="DM Sans, system-ui", color="#334155"),
        )

        insights = []
        for _, row in df.iterrows():
            insights.append(f"**{row[lbl_col]}:** {self._fmt(float(row[val_col]))}")
        return self._result("stat_card", fig, insights)

    @staticmethod
    def _table_only(df) -> Dict[str, Any]:
        ins = [f"**{len(df):,} rows** returned"]
        for c in df.select_dtypes(include="number").columns[:3]:
            ins.append(f"**{c}:** {df[c].sum():,.0f} total")
        return {"type": VisualizationType.TABLE_ONLY, "chart": None, "insights": ins, "show_table": True}

    @staticmethod
    def _empty() -> Dict[str, Any]:
        return {"type": VisualizationType.TABLE_ONLY, "chart": None,
                "insights": ["No data returned"], "show_table": True}
