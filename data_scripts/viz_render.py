"""
data_scripts/viz_render.py
Plotly and HTML/Pandas rendering routines for main.ipynb analytical cockpit:
- View 1: Data Lakehouse Health Console
- View 2: Macro Regime & Top-Down News View
- View 3: Constituent Churn & Weight Matrix View (Holdings)
- View 4: Top 50 Value-Weighted Performance View (Strategy)
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Dark-themed financial palette
COLORS = {
    "background": "#0f172a",
    "paper": "#1e293b",
    "text": "#f8fafc",
    "grid": "#334155",
    "strategy": "#38bdf8",     # Sky blue
    "benchmark": "#f59e0b",    # Amber
    "drawdown": "#ef4444",     # Red
    "accent": "#10b981",       # Emerald
}


def render_lakehouse_health(telemetry_dict: dict) -> pd.DataFrame:
    """Format and style data lakehouse health telemetry as a DataFrame."""
    tables = telemetry_dict.get("tables", {})
    rows = []
    for tbl_name, info in tables.items():
        rows.append({
            "Lakehouse Domain": tbl_name.upper(),
            "Total Rows": f"{info.get('rows', 0):,}",
            "Entity/Series Count": info.get("unique_tickers", info.get("series_count", "-")),
            "Min Date": str(info.get("min_date", info.get("min_timestamp", "-")))[:10],
            "Max Date": str(info.get("max_date", info.get("max_timestamp", "-")))[:10],
            "Health Status": "HEALTHY",
        })
    df_health = pd.DataFrame(rows)
    return df_health


def render_macro_overlay(
    df_prices: pd.DataFrame,
    df_macro: pd.DataFrame,
    benchmark_ticker: str = "SPY",
    macro_series_id: str = "FEDFUNDS",
    macro_series_name: str = "Federal Funds Rate (%)",
    event_dates: dict = None,
):
    """Dual-axis plot: Benchmark Price (SPY) vs Macro Rate with historical policy event markers."""
    spy_df = df_prices[df_prices["ticker"] == benchmark_ticker].sort_values("date").copy()
    macro_df = df_macro[df_macro["series_id"] == macro_series_id].sort_values("date").copy()

    # Align macro date range with benchmark price timeframe (e.g. 1999~present)
    if not spy_df.empty:
        spy_df["date"] = pd.to_datetime(spy_df["date"]).dt.date
        macro_df["date"] = pd.to_datetime(macro_df["date"]).dt.date
        min_date = spy_df["date"].min()
        max_date = spy_df["date"].max()
        macro_df = macro_df[(macro_df["date"] >= min_date) & (macro_df["date"] <= max_date)]
    else:
        min_date, max_date = None, None

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Left Y: SPY Price
    fig.add_trace(
        go.Scatter(
            x=spy_df["date"],
            y=spy_df["adj_close"],
            name=f"{benchmark_ticker} Price",
            line=dict(color=COLORS["benchmark"], width=2),
        ),
        secondary_y=False,
    )

    # Right Y: Macro Series
    fig.add_trace(
        go.Scatter(
            x=macro_df["date"],
            y=macro_df["value"],
            name=macro_series_name,
            line=dict(color=COLORS["strategy"], width=1.5, dash="dot"),
        ),
        secondary_y=True,
    )

    # Vertical Event Markers for FOMC / Macro Shifts
    if event_dates is None:
        event_dates = {
            "2001-01-03": "Dot-Com Easing",
            "2008-09-15": "GFC Lehman Failure",
            "2020-03-15": "COVID-19 Emergency Cut",
            "2022-03-16": "Tightening Liftoff",
            "2024-09-18": "Fed 50bp Pivot",
        }

    for edate, elabel in event_dates.items():
        fig.add_vline(
            x=edate,
            line_width=1,
            line_dash="dash",
            line_color="#64748b",
        )
        fig.add_annotation(
            x=edate,
            y=1.03,
            yref="paper",
            text=elabel,
            showarrow=False,
            font=dict(size=10, color="#cbd5e1"),
            textangle=-45,
            xanchor="left",
        )

    fig.update_layout(
        title=f"Macro Context: {benchmark_ticker} Price vs {macro_series_name} with Policy Regimes",
        template="plotly_dark",
        plot_bgcolor=COLORS["background"],
        paper_bgcolor=COLORS["paper"],
        font=dict(color=COLORS["text"]),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="right", x=1),
        margin=dict(l=40, r=40, t=80, b=40),
    )

    fig.update_xaxes(
        showgrid=True,
        gridcolor=COLORS["grid"],
        range=[min_date, max_date] if min_date and max_date else None,
    )
    fig.update_yaxes(title_text=f"{benchmark_ticker} ($)", secondary_y=False, showgrid=True, gridcolor=COLORS["grid"])
    fig.update_yaxes(title_text=macro_series_name, secondary_y=True, showgrid=False)

    return fig


def render_news_feed(
    df_news: pd.DataFrame,
    keyword: str = None,
    category: str = None,
    limit: int = 15,
) -> pd.DataFrame:
    """Filter and format news feed for programmatic inspection."""
    df = df_news.copy().sort_values("timestamp", ascending=False)
    
    if category:
        df = df[df["category"].str.lower() == category.lower()]
    if keyword:
        kw = keyword.lower()
        df = df[df["headline"].str.lower().str.contains(kw, na=False) | df["summary"].str.lower().str.contains(kw, na=False)]

    df_out = df.head(limit)[["timestamp", "source", "category", "headline", "summary"]].copy()
    df_out["timestamp"] = pd.to_datetime(df_out["timestamp"]).dt.strftime("%Y-%m-%d %H:%M")
    return df_out.reset_index(drop=True)


def render_news_feed_html(
    df_news: pd.DataFrame,
    keyword: str = None,
    category: str = None,
    limit: int = 15,
) -> str:
    """Render a styled, readable dark-mode HTML table for notebook news inspection."""
    df = render_news_feed(df_news, keyword=keyword, category=category, limit=limit)
    if df.empty:
        return "<div style='color: #94a3b8; padding: 12px;'>No matching articles found.</div>"

    rows_html = []
    for _, r in df.iterrows():
        cat = str(r["category"]).lower()
        badge_color = "#3b82f6" if cat == "macro" else ("#10b981" if cat == "sector" else "#f59e0b")
        row = f"""
        <tr style="border-bottom: 1px solid #1e293b;">
            <td style="padding: 10px 8px; white-space: nowrap; color: #94a3b8; font-family: monospace; vertical-align: top;">{r['timestamp']}</td>
            <td style="padding: 10px 8px; vertical-align: top;">
                <span style="background-color: {badge_color}22; color: {badge_color}; border: 1px solid {badge_color}55; padding: 2px 6px; border-radius: 4px; font-size: 11px; text-transform: uppercase; font-weight: bold;">{r['category']}</span>
            </td>
            <td style="padding: 10px 8px; color: #cbd5e1; font-weight: 500; font-size: 12px; white-space: nowrap; vertical-align: top;">{r['source']}</td>
            <td style="padding: 10px 8px; vertical-align: top;">
                <div style="color: #f1f5f9; font-weight: 600; font-size: 13px; margin-bottom: 4px;">{r['headline']}</div>
                <div style="color: #94a3b8; font-size: 12px; line-height: 1.4;">{r['summary']}</div>
            </td>
        </tr>
        """
        rows_html.append(row)

    table_html = f"""
    <div style="background-color: #0b0f19; border: 1px solid #1e293b; border-radius: 8px; overflow-x: auto; padding: 8px; margin-top: 10px;">
        <table style="width: 100%; border-collapse: collapse; text-align: left; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
            <thead>
                <tr style="border-bottom: 2px solid #334155; color: #64748b; font-size: 12px; text-transform: uppercase; letter-spacing: 0.05em;">
                    <th style="padding: 8px;">Timestamp</th>
                    <th style="padding: 8px;">Category</th>
                    <th style="padding: 8px;">Source</th>
                    <th style="padding: 8px;">Headline & Summary</th>
                </tr>
            </thead>
            <tbody>
                {''.join(rows_html)}
            </tbody>
        </table>
    </div>
    """
    return table_html


def render_weights_treemap(df_weights: pd.DataFrame, target_date: str = None):
    """Plotly treemap depicting Top 50 portfolio weights allocation on target date."""
    df = df_weights.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    if not target_date:
        target_date = df["date"].max()

    sub = df[df["date"] == target_date].sort_values("weight", ascending=False).head(50).copy()
    sub["weight_pct"] = (sub["weight"] * 100.0).round(2)
    sub["label"] = sub["ticker"] + "<br>" + sub["weight_pct"].astype(str) + "%"

    fig = px.treemap(
        sub,
        path=["ticker"],
        values="weight",
        color="weight_pct",
        color_continuous_scale="Blues",
        title=f"S&P 500 Top 50 Portfolio Allocation ({target_date})",
    )

    fig.update_layout(
        template="plotly_dark",
        plot_bgcolor=COLORS["background"],
        paper_bgcolor=COLORS["paper"],
        font=dict(color=COLORS["text"]),
        margin=dict(l=20, r=20, t=50, b=20),
    )
    return fig


def render_churn_table(df_weights: pd.DataFrame, date_t0: str = None, date_t1: str = None) -> pd.DataFrame:
    """Format member ingress/egress churn matrix between consecutive rebalancing dates."""
    df = df_weights.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    dates = sorted(df["date"].unique())

    if len(dates) < 2:
        return pd.DataFrame()

    if not date_t1:
        date_t1 = dates[-1]
    if not date_t0:
        date_t0 = dates[-2]

    w0 = df[df["date"] == date_t0].set_index("ticker")["weight"]
    w1 = df[df["date"] == date_t1].set_index("ticker")["weight"]

    diff = pd.concat([w0, w1], axis=1, keys=[f"Weight ({date_t0})", f"Weight ({date_t1})"]).fillna(0.0)
    diff["Delta (%)"] = (diff[f"Weight ({date_t1})"] - diff[f"Weight ({date_t0})"]) * 100.0
    diff[f"Weight ({date_t0})"] = (diff[f"Weight ({date_t0})"] * 100.0).round(2)
    diff[f"Weight ({date_t1})"] = (diff[f"Weight ({date_t1})"] * 100.0).round(2)
    diff["Delta (%)"] = diff["Delta (%)"].round(2)

    diff["Status"] = "MAINTAINED"
    diff.loc[diff[f"Weight ({date_t0})"] == 0.0, "Status"] = "NEW INGRESS (ADD)"
    diff.loc[diff[f"Weight ({date_t1})"] == 0.0, "Status"] = "EGRESS (EXCLUDE)"

    churn_df = diff[diff["Status"] != "MAINTAINED"].sort_values("Delta (%)", ascending=False).reset_index()
    churn_df = churn_df.rename(columns={"index": "Ticker"})
    return churn_df


def render_performance_dashboard(detailed_df: pd.DataFrame):
    """Dual-panel Plotly figure: Top NAV vs SPY, Bottom MDD Underwater Subplot."""
    df = detailed_df.sort_values("date")

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.06,
        row_heights=[0.7, 0.3],
        subplot_titles=("Cumulative NAV Growth ($100 Base)", "Portfolio Drawdown (Underwater Area)"),
    )

    # Panel 1: Strategy NAV vs Benchmark NAV
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["strategy_nav"],
            name="Top 50 Value-Weighted",
            line=dict(color=COLORS["strategy"], width=2.2),
        ),
        row=1,
        col=1,
    )

    if "benchmark_nav" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=df["benchmark_nav"],
                name="SPY Benchmark",
                line=dict(color=COLORS["benchmark"], width=1.8),
            ),
            row=1,
            col=1,
        )

    # Panel 2: Underwater Drawdown Area
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["strategy_drawdown"] * 100.0,
            name="Strategy Drawdown",
            fill="tozeroy",
            line=dict(color=COLORS["drawdown"], width=1.2),
            fillcolor="rgba(239, 68, 68, 0.25)",
        ),
        row=2,
        col=1,
    )

    if "benchmark_drawdown" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=df["benchmark_drawdown"] * 100.0,
                name="SPY Drawdown",
                line=dict(color=COLORS["benchmark"], width=1.0, dash="dash"),
            ),
            row=2,
            col=1,
        )

    fig.update_layout(
        template="plotly_dark",
        plot_bgcolor=COLORS["background"],
        paper_bgcolor=COLORS["paper"],
        font=dict(color=COLORS["text"]),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=40, r=40, t=70, b=40),
    )

    fig.update_xaxes(showgrid=True, gridcolor=COLORS["grid"])
    fig.update_yaxes(title_text="NAV ($)", row=1, col=1, showgrid=True, gridcolor=COLORS["grid"])
    fig.update_yaxes(title_text="Drawdown (%)", row=2, col=1, showgrid=True, gridcolor=COLORS["grid"])

    return fig
