"""
data_scripts/portfolio_calc.py
Pure computational engine for S&P 500 Top 50 value-weighted performance calculations:
- Value weights normalization
- Daily portfolio return compounding
- Strategy vs Benchmark metrics: NAV, MDD, Alpha, Beta, Sharpe, Rebalancing Turnover
"""

import numpy as np
import pandas as pd


def compute_weights(df_constituents: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure value-weighted shares sum to 1.0 for each rebalancing date.
    Required columns: ['date', 'ticker', 'market_cap']
    """
    df = df_constituents.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    
    # Calculate weights by date partition
    total_caps = df.groupby("date")["market_cap"].transform("sum")
    df["weight"] = df["market_cap"] / total_caps
    df["rank"] = df.groupby("date")["market_cap"].rank(ascending=False, method="first").astype(int)
    return df.sort_values(["date", "rank"]).reset_index(drop=True)


def compute_portfolio_returns(
    df_prices: pd.DataFrame,
    df_weights: pd.DataFrame,
    benchmark_ticker: str = "SPY",
) -> pd.DataFrame:
    """
    Compute daily portfolio returns from piecewise constant rebalancing weights.
    df_prices requires: ['date', 'ticker', 'adj_close']
    df_weights requires: ['date', 'ticker', 'weight']
    """
    prices = df_prices.copy()
    prices["date"] = pd.to_datetime(prices["date"]).dt.date
    
    # Calculate individual asset returns
    prices = prices.sort_values(["ticker", "date"]).reset_index(drop=True)
    prices["daily_return"] = prices.groupby("ticker")["adj_close"].pct_change()
    
    # Extract benchmark daily return
    bench_df = prices[prices["ticker"] == benchmark_ticker][["date", "daily_return"]].copy()
    bench_df = bench_df.rename(columns={"daily_return": "benchmark_return"}).dropna()

    # Rebalancing schedule forward-fill
    weights = df_weights.copy()
    weights["date"] = pd.to_datetime(weights["date"]).dt.date
    rebal_dates = sorted(weights["date"].unique())
    all_trading_dates = sorted(prices["date"].unique())

    # Map each trading date to its active rebalancing interval
    date_to_rebal = {}
    cur_idx = 0
    for t_date in all_trading_dates:
        while cur_idx + 1 < len(rebal_dates) and t_date >= rebal_dates[cur_idx + 1]:
            cur_idx += 1
        if t_date >= rebal_dates[cur_idx]:
            date_to_rebal[t_date] = rebal_dates[cur_idx]

    df_mapping = pd.DataFrame(list(date_to_rebal.items()), columns=["date", "rebal_date"])
    
    # Merge active weights to trading dates
    trading_weights = df_mapping.merge(
        weights[["date", "ticker", "weight"]].rename(columns={"date": "rebal_date"}),
        on="rebal_date",
        how="inner",
    )

    # Join with daily price returns
    port_prices = prices.merge(trading_weights, on=["date", "ticker"], how="inner")
    port_prices["weighted_return"] = port_prices["daily_return"] * port_prices["weight"]
    
    # Daily portfolio return = sum(w_i * r_i)
    port_daily = port_prices.groupby("date")["weighted_return"].sum().reset_index()
    port_daily = port_daily.rename(columns={"weighted_return": "strategy_return"})

    # Merge benchmark returns
    result = port_daily.merge(bench_df, on="date", how="left").sort_values("date").reset_index(drop=True)
    result = result.dropna(subset=["strategy_return"])
    return result


def compute_performance_metrics(
    df_returns: pd.DataFrame,
    risk_free_rate: float = 0.02,
) -> dict:
    """
    Produce cumulative NAV, Drawdown, MDD, Sharpe, Alpha, Beta.
    df_returns requires: ['date', 'strategy_return', 'benchmark_return']
    """
    df = df_returns.copy().dropna()
    if df.empty:
        return {}

    # Cumulative NAV (Base = 100)
    df["strategy_nav"] = 100.0 * (1.0 + df["strategy_return"]).cumprod()
    df["strategy_peak"] = df["strategy_nav"].cummax()
    df["strategy_drawdown"] = (df["strategy_nav"] - df["strategy_peak"]) / df["strategy_peak"]

    if "benchmark_return" in df.columns:
        df["benchmark_nav"] = 100.0 * (1.0 + df["benchmark_return"]).cumprod()
        df["benchmark_peak"] = df["benchmark_nav"].cummax()
        df["benchmark_drawdown"] = (df["benchmark_nav"] - df["benchmark_peak"]) / df["benchmark_peak"]

    # Summary Statistics
    n_days = len(df)
    years = max(n_days / 252.0, 1e-4)

    strat_total_ret = (df["strategy_nav"].iloc[-1] / 100.0) - 1.0
    strat_cagr = (df["strategy_nav"].iloc[-1] / 100.0) ** (1.0 / years) - 1.0
    strat_ann_vol = df["strategy_return"].std() * np.sqrt(252.0)
    strat_mdd = df["strategy_drawdown"].min()
    strat_sharpe = (strat_cagr - risk_free_rate) / strat_ann_vol if strat_ann_vol > 0 else np.nan

    metrics = {
        "n_trading_days": n_days,
        "strategy_cagr": float(strat_cagr),
        "strategy_total_return": float(strat_total_ret),
        "strategy_annualized_vol": float(strat_ann_vol),
        "strategy_mdd": float(strat_mdd),
        "strategy_sharpe": float(strat_sharpe),
        "detailed_df": df,
    }

    if "benchmark_return" in df.columns:
        bench_total_ret = (df["benchmark_nav"].iloc[-1] / 100.0) - 1.0
        bench_cagr = (df["benchmark_nav"].iloc[-1] / 100.0) ** (1.0 / years) - 1.0
        bench_ann_vol = df["benchmark_return"].std() * np.sqrt(252.0)
        bench_mdd = df["benchmark_drawdown"].min()
        bench_sharpe = (bench_cagr - risk_free_rate) / bench_ann_vol if bench_ann_vol > 0 else np.nan

        # Beta & Alpha vs Benchmark
        cov_matrix = np.cov(df["strategy_return"], df["benchmark_return"])
        bench_var = cov_matrix[1, 1]
        beta = cov_matrix[0, 1] / bench_var if bench_var > 0 else np.nan
        alpha = strat_cagr - (risk_free_rate + beta * (bench_cagr - risk_free_rate))

        metrics.update({
            "benchmark_cagr": float(bench_cagr),
            "benchmark_total_return": float(bench_total_ret),
            "benchmark_annualized_vol": float(bench_ann_vol),
            "benchmark_mdd": float(bench_mdd),
            "benchmark_sharpe": float(bench_sharpe),
            "beta": float(beta),
            "alpha": float(alpha),
        })

    return metrics


def compute_turnover_rate(df_weights: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate portfolio turnover between consecutive rebalancing periods.
    Turnover = 0.5 * sum(|w_{i, t} - w_{i, t-1}|)
    """
    df = df_weights.copy()
    dates = sorted(df["date"].unique())
    turnover_rows = []

    for i in range(1, len(dates)):
        d_prev, d_curr = dates[i - 1], dates[i]
        w_prev = df[df["date"] == d_prev].set_index("ticker")["weight"]
        w_curr = df[df["date"] == d_curr].set_index("ticker")["weight"]

        combined = pd.concat([w_prev, w_curr], axis=1, keys=["prev", "curr"]).fillna(0.0)
        turnover = 0.5 * (combined["curr"] - combined["prev"]).abs().sum()

        added = combined[(combined["prev"] == 0.0) & (combined["curr"] > 0.0)].index.tolist()
        removed = combined[(combined["prev"] > 0.0) & (combined["curr"] == 0.0)].index.tolist()

        turnover_rows.append({
            "date": d_curr,
            "turnover_rate": float(turnover),
            "additions_count": len(added),
            "exclusions_count": len(removed),
            "additions": added,
            "exclusions": removed,
        })

    return pd.DataFrame(turnover_rows)
