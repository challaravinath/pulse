"""
Analysis Engine v2.2 — Real Statistical Analysis
==================================================

Does ACTUAL computation before asking the LLM:
- Anomaly detection (z-score based)
- Trend analysis (slope via linear regression)
- Concentration metrics (Gini / Pareto)
- Period-over-period comparison
- Health scoring

The LLM is used to *narrate* the findings, not to *compute* them.

Author: PULSE Team
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


def _fmt(num: float) -> str:
    """Format large numbers with K/M/B suffixes."""
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


class AnalysisEngine:
    """Computes real statistics and uses LLM to narrate them."""

    def __init__(self, openai_client):
        self.client = openai_client

    # ── Public API ───────────────────────────────────────────────────────────

    def generate_insights(
        self,
        df: pd.DataFrame,
        user_question: str,
        context: str = ""
    ) -> Dict[str, Any]:
        """
        Main entry: compute stats, then let LLM narrate.

        Returns: {'summary': str, 'insights': [str], 'recommendations': [str]}
        """
        if df.empty:
            return {
                'summary': "No data to analyse.",
                'insights': [],
                'recommendations': ["Try fetching data first."]
            }

        # Step 1: compute everything we can from the data
        stats = self._compute_full_stats(df)

        # Step 2: let LLM narrate (with fallback if LLM fails)
        try:
            raw = self._ask_llm(df, user_question, stats, context)
            parsed = self._parse_analysis(raw, stats)
            return parsed
        except Exception as e:
            logger.error(f"LLM analysis failed: {e}")
            return {
                'summary': stats.get('executive_oneliner', f"Dataset: {len(df):,} rows."),
                'insights': stats.get('auto_insights', []),
                'recommendations': stats.get('auto_recommendations', []),
            }

    def generate_executive_summary(
        self,
        df: pd.DataFrame,
        original_query: str = ""
    ) -> str:
        """Generate executive-ready summary (no LLM needed)."""
        stats = self._compute_full_stats(df)
        lines = ["## 📊 Executive Summary\n"]

        lines.append(f"**Dataset:** {len(df):,} rows × {df.shape[1]} columns")
        if original_query:
            lines.append(f"**Query:** {original_query}")
        lines.append("")

        # Key metrics
        for col in df.select_dtypes(include=np.number).columns[:5]:
            t, a = df[col].sum(), df[col].mean()
            lines.append(f"- **{col}:** {_fmt(t)} total, {_fmt(a)} avg")

        # Auto insights
        for ins in stats.get('auto_insights', [])[:5]:
            lines.append(f"- {ins}")

        return "\n".join(lines)

    # ── Statistical Computation ──────────────────────────────────────────────

    def _compute_full_stats(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Compute all useful statistics from the DataFrame."""
        stats: Dict[str, Any] = {
            'row_count': len(df),
            'col_count': len(df.columns),
            'columns': list(df.columns),
            'auto_insights': [],
            'auto_recommendations': [],
        }

        # ── Numeric column stats ────────────────────────────────────
        num_cols = df.select_dtypes(include=np.number).columns.tolist()
        stats['numeric_stats'] = {}
        for col in num_cols[:8]:
            s = df[col].dropna()
            if len(s) == 0:
                continue
            col_stats = {
                'sum': float(s.sum()),
                'mean': float(s.mean()),
                'median': float(s.median()),
                'min': float(s.min()),
                'max': float(s.max()),
                'std': float(s.std()) if len(s) > 1 else 0,
            }
            stats['numeric_stats'][col] = col_stats

            # Anomaly: z-score outliers
            if col_stats['std'] > 0 and len(s) > 3:
                z = (s - s.mean()) / s.std()
                outliers = (z.abs() > 2).sum()
                if outliers > 0:
                    stats['auto_insights'].append(
                        f"**{col}** has {outliers} outlier(s) (>2σ from mean)."
                    )

        # ── Concentration (Pareto) ──────────────────────────────────
        if num_cols:
            val_col = num_cols[0]
            for c in num_cols:
                if any(x in c.lower() for x in ['count', 'events', 'total', 'sessions']):
                    val_col = c
                    break
            total = df[val_col].sum()
            if total > 0 and len(df) > 3:
                sorted_vals = df[val_col].sort_values(ascending=False)
                cumsum = sorted_vals.cumsum()
                n_for_80 = (cumsum <= total * 0.8).sum() + 1
                pct_for_80 = n_for_80 / len(df) * 100
                stats['concentration'] = {
                    'val_col': val_col,
                    'n_for_80pct': n_for_80,
                    'pct_for_80': pct_for_80,
                }
                if pct_for_80 < 30:
                    stats['auto_insights'].append(
                        f"**High concentration:** Top {n_for_80} of {len(df)} "
                        f"({pct_for_80:.0f}%) account for 80% of **{val_col}**."
                    )

        # ── Categorical column stats ────────────────────────────────
        cat_cols = df.select_dtypes(include=['object', 'string']).columns.tolist()
        stats['categorical_stats'] = {}
        for col in cat_cols[:5]:
            vc = df[col].value_counts()
            stats['categorical_stats'][col] = {
                'unique': len(vc),
                'top_values': vc.head(5).to_dict(),
            }
            if len(vc) > 0:
                top_name, top_count = list(vc.items())[0]
                pct = top_count / len(df) * 100
                if pct > 50:
                    stats['auto_insights'].append(
                        f"**{col}:** '{top_name}' dominates at {pct:.0f}% of all rows."
                    )

        # ── Time series trend ───────────────────────────────────────
        time_col = self._find_time_col(df)
        if time_col and num_cols:
            val_col = num_cols[0]
            trend = self._compute_trend(df, time_col, val_col)
            stats['trend'] = trend
            if trend:
                direction = "📈 growing" if trend['slope'] > 0 else "📉 declining"
                stats['auto_insights'].append(
                    f"**Trend ({val_col}):** {direction} at "
                    f"{abs(trend['slope_pct']):.1f}% per period. "
                    f"Peak on {trend['peak_date']} ({_fmt(trend['peak_val'])})."
                )

        # ── Period-over-period ──────────────────────────────────────
        if time_col and num_cols:
            pop = self._period_over_period(df, time_col, num_cols[0])
            if pop:
                stats['period_comparison'] = pop
                chg = pop['change_pct']
                emoji = "📈" if chg > 0 else "📉"
                stats['auto_insights'].append(
                    f"{emoji} **Period comparison:** Second half is "
                    f"{abs(chg):.1f}% {'higher' if chg > 0 else 'lower'} "
                    f"than first half ({_fmt(pop['first_half'])} → {_fmt(pop['second_half'])})."
                )

        # ── Always-present basic insights ───────────────────────────
        if not stats['auto_insights']:
            # Generate basic insights if nothing special was detected
            if num_cols:
                for col in num_cols[:3]:
                    s = stats['numeric_stats'].get(col)
                    if s:
                        stats['auto_insights'].append(
                            f"**{col}:** {_fmt(s['sum'])} total, "
                            f"{_fmt(s['mean'])} avg, range {_fmt(s['min'])}–{_fmt(s['max'])}"
                        )
            for col in cat_cols[:2]:
                cs = stats['categorical_stats'].get(col)
                if cs and cs['top_values']:
                    top_name = list(cs['top_values'].keys())[0]
                    top_count = list(cs['top_values'].values())[0]
                    stats['auto_insights'].append(
                        f"**{col}:** {cs['unique']} unique values, "
                        f"most common is '{top_name}' ({top_count:,} rows)"
                    )

        # ── Executive one-liner ─────────────────────────────────────
        parts = [f"{len(df):,} rows"]
        if num_cols:
            parts.append(f"{_fmt(df[num_cols[0]].sum())} total {num_cols[0]}")
        stats['executive_oneliner'] = " | ".join(parts)

        # ── Auto recommendations ────────────────────────────────────
        if (stats.get('concentration') or {}).get('pct_for_80', 100) < 20:
            stats['auto_recommendations'].append(
                "Usage is very concentrated — investigate whether a few power users "
                "are skewing metrics."
            )
        if (stats.get('trend') or {}).get('slope', 0) < 0:
            stats['auto_recommendations'].append(
                "Usage is trending down — consider investigating recent changes "
                "or reaching out to declining orgs."
            )

        return stats

    def _compute_trend(
        self, df: pd.DataFrame, time_col: str, val_col: str
    ) -> Optional[Dict]:
        """Compute linear trend over a time series."""
        try:
            dfc = df[[time_col, val_col]].dropna().copy()
            if len(dfc) < 3:
                return None

            dfc[time_col] = pd.to_datetime(dfc[time_col])
            dfc = dfc.sort_values(time_col)

            # Convert dates to numeric (days from first)
            first = dfc[time_col].iloc[0]
            x = (dfc[time_col] - first).dt.total_seconds().values
            y = dfc[val_col].values.astype(float)

            if x[-1] == 0:
                return None

            # Simple linear regression
            n = len(x)
            sx, sy, sxy, sx2 = x.sum(), y.sum(), (x * y).sum(), (x * x).sum()
            denom = n * sx2 - sx * sx
            if denom == 0:
                return None
            slope = (n * sxy - sx * sy) / denom

            # Percentage slope relative to mean
            y_mean = y.mean()
            slope_pct = (slope * (x[-1] - x[0]) / y_mean * 100) if y_mean != 0 else 0

            # Peak / valley
            peak_idx = int(np.argmax(y))
            valley_idx = int(np.argmin(y))

            return {
                'slope': float(slope),
                'slope_pct': float(slope_pct),
                'peak_date': str(dfc[time_col].iloc[peak_idx])[:10],
                'peak_val': float(y[peak_idx]),
                'valley_date': str(dfc[time_col].iloc[valley_idx])[:10],
                'valley_val': float(y[valley_idx]),
                'first_val': float(y[0]),
                'last_val': float(y[-1]),
            }
        except Exception as e:
            logger.warning(f"Trend computation failed: {e}")
            return None

    def _period_over_period(
        self, df: pd.DataFrame, time_col: str, val_col: str
    ) -> Optional[Dict]:
        """Compare first half vs second half of the data."""
        try:
            dfc = df[[time_col, val_col]].dropna().copy()
            dfc[time_col] = pd.to_datetime(dfc[time_col])

            mid = dfc[time_col].min() + (dfc[time_col].max() - dfc[time_col].min()) / 2
            first = dfc[dfc[time_col] < mid][val_col].sum()
            second = dfc[dfc[time_col] >= mid][val_col].sum()

            if first == 0:
                return None

            return {
                'first_half': float(first),
                'second_half': float(second),
                'change_pct': float((second - first) / first * 100),
            }
        except Exception:
            return None

    def _find_time_col(self, df: pd.DataFrame) -> Optional[str]:
        for c in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[c]):
                return c
        for c in df.columns:
            if any(x in c.lower() for x in ['date', 'time', 'timestamp', 'day']):
                try:
                    pd.to_datetime(df[c].head(5), errors='raise')
                    return c
                except Exception:
                    pass
        return None

    # ── Streaming Insights (NEW) ────────────────────────────────────────────

    def stream_insights(
        self,
        df: pd.DataFrame,
        user_question: str,
        context: str = ""
    ):
        """
        ★ NEW: Stream LLM analysis token-by-token.

        Yields dicts:
          {'type': 'stats', 'stats': {...}}         — pre-computed stats (instant)
          {'type': 'token', 'text': '...'}          — LLM token
          {'type': 'done',  'full_text': '...'}     — final complete text

        Usage in Streamlit:
            placeholder = st.empty()
            accumulated = ""
            for chunk in engine.stream_insights(df, question, ctx):
                if chunk['type'] == 'token':
                    accumulated += chunk['text']
                    placeholder.markdown(accumulated)
        """
        if df.empty:
            yield {'type': 'done', 'full_text': "No data to analyse."}
            return

        # Step 1: compute stats (instant, no LLM)
        stats = self._compute_full_stats(df)
        yield {'type': 'stats', 'stats': stats}

        # Step 2: stream LLM narration
        try:
            messages = self._build_llm_messages(df, user_question, stats, context)

            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=messages,
                temperature=0.25,
                max_tokens=800,
                stream=True,
            )

            full_text = ""
            for chunk in response:
                if chunk.choices and chunk.choices[0].delta.content:
                    token = chunk.choices[0].delta.content
                    full_text += token
                    yield {'type': 'token', 'text': token}

            yield {'type': 'done', 'full_text': full_text}

        except Exception as e:
            logger.error(f"LLM streaming failed: {e}")
            # Fallback to pre-computed stats
            fallback = stats.get('executive_oneliner', f"Dataset: {len(df):,} rows.")
            if stats.get('auto_insights'):
                fallback += "\n\n" + "\n".join(f"- {i}" for i in stats['auto_insights'])
            yield {'type': 'done', 'full_text': fallback}

    # ── LLM Narration ────────────────────────────────────────────────────────

    def _build_llm_messages(self, df, question, stats, context):
        """Build the messages array for the analysis LLM call."""
        data_block = self._build_data_summary(stats)

        prompt = (
            f"You are a senior data analyst. Answer the user's question using ONLY "
            f"the pre-computed statistics below. Be specific with numbers.\n\n"
            f"DATA:\n{data_block}\n\n"
        )
        if context:
            prompt += f"CONTEXT:\n{context}\n\n"
        prompt += (
            f"USER QUESTION: {question}\n\n"
            f"Respond with:\n"
            f"SUMMARY: [2-3 sentence answer]\n"
            f"INSIGHTS:\n- [finding]\n- [finding]\n"
            f"RECOMMENDATIONS:\n- [action]"
        )

        return [
            {"role": "system", "content": "You are a senior data analyst. Use the provided stats."},
            {"role": "user", "content": prompt}
        ]

    def _build_data_summary(self, stats):
        """Build dense data summary string from pre-computed stats."""
        summary_lines = []
        summary_lines.append(f"Rows: {stats['row_count']:,}  Columns: {stats['col_count']}")

        if stats.get('numeric_stats'):
            summary_lines.append("\nNumeric columns:")
            for col, s in list(stats['numeric_stats'].items())[:5]:
                summary_lines.append(
                    f"  {col}: sum={_fmt(s['sum'])}, mean={_fmt(s['mean'])}, "
                    f"min={_fmt(s['min'])}, max={_fmt(s['max'])}, std={_fmt(s['std'])}"
                )

        if stats.get('categorical_stats'):
            summary_lines.append("\nCategorical columns:")
            for col, s in list(stats['categorical_stats'].items())[:3]:
                top = list(s['top_values'].items())[:3]
                top_str = ", ".join(f"'{k}': {v}" for k, v in top)
                summary_lines.append(f"  {col}: {s['unique']} unique — top: {top_str}")

        if stats.get('trend'):
            t = stats['trend']
            summary_lines.append(
                f"\nTrend: slope_pct={t['slope_pct']:.1f}%, "
                f"peak={_fmt(t['peak_val'])} on {t['peak_date']}, "
                f"first={_fmt(t['first_val'])}, last={_fmt(t['last_val'])}"
            )

        if stats.get('period_comparison'):
            p = stats['period_comparison']
            summary_lines.append(
                f"Period comparison: first_half={_fmt(p['first_half'])}, "
                f"second_half={_fmt(p['second_half'])}, change={p['change_pct']:+.1f}%"
            )

        if stats.get('concentration'):
            c = stats['concentration']
            summary_lines.append(
                f"Concentration: top {c['n_for_80pct']} ({c['pct_for_80']:.0f}%) "
                f"account for 80% of {c['val_col']}"
            )

        if stats.get('auto_insights'):
            summary_lines.append("\nAuto-detected insights:")
            for ins in stats['auto_insights']:
                summary_lines.append(f"  • {ins}")

        return "\n".join(summary_lines)

    def _ask_llm(
        self,
        df: pd.DataFrame,
        question: str,
        stats: Dict,
        context: str
    ) -> str:
        """Ask LLM to narrate pre-computed stats (non-streaming)."""
        messages = self._build_llm_messages(df, question, stats, context)

        response = self.client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            temperature=0.25,
            max_tokens=800
        )
        return response.choices[0].message.content

    def _parse_analysis(self, text: str, stats: Dict) -> Dict[str, Any]:
        """Parse LLM response into structured format."""
        lines = text.strip().split('\n')
        summary_lines, insights, recommendations = [], [], []
        section = None

        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue

            low = stripped.lower()
            if low.startswith('summary:'):
                section = 'summary'
                rest = stripped[8:].strip()
                if rest:
                    summary_lines.append(rest)
                continue
            elif low.startswith('insight'):
                section = 'insights'
                continue
            elif low.startswith('recommend') or low.startswith('suggestion'):
                section = 'recommendations'
                continue

            # Parse list items
            cleaned = stripped.lstrip('-•*0123456789. ').strip()
            if not cleaned:
                continue

            if section == 'summary':
                summary_lines.append(cleaned)
            elif section == 'insights':
                insights.append(cleaned)
            elif section == 'recommendations':
                recommendations.append(cleaned)
            elif section is None:
                summary_lines.append(cleaned)

        return {
            'summary': ' '.join(summary_lines) if summary_lines
                       else stats.get('executive_oneliner', ''),
            'insights': insights if insights
                        else stats.get('auto_insights', []),
            'recommendations': recommendations if recommendations
                               else stats.get('auto_recommendations', []),
        }
