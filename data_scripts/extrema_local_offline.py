"""
data_scripts/extrema_local_offline.py
Offline local extrema detection and ground-truth labeling for market indices (^GSPC, ^NDX, ^DJI).
Converted from TradingView Pine Script (v6) indicator by primenumbersam.

Labels produced:
- extrema_type: +1 (Peak/High), -1 (Trough/Low), 0 (None)
- extrema_price: price of the peak or trough
- swing_regime: +1 (Bullish swing expansion), -1 (Bearish swing contraction)
- next_extrema_type: type of the next upcoming extrema (+1 / -1)
- bars_to_next_extrema: trading days until next extrema
- return_to_next_extrema: cumulative return to next extrema price
- week_ahead_direction: sign of 5-day future price change (+1, -1, 0)
"""

import argparse
from pathlib import Path
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def compute_atr(df: pd.DataFrame, length: int = 10) -> pd.Series:
    """Wilder smoothed Average True Range (RMA with alpha = 1 / length)."""
    high = df["high"]
    low = df["low"]
    prev_close = df["close"].shift(1)

    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    # Wilder RMA exponential moving average
    atr = tr.ewm(alpha=1.0 / length, adjust=False).mean()
    return atr


class ExtremaPoint:
    def __init__(self, bar_idx: int, price: float, point_type: int, date):
        self.bar_idx = bar_idx
        self.price = price
        self.point_type = point_type
        self.date = date


def label_ticker_extrema(
    df_ticker: pd.DataFrame,
    left_bars: int = 10,
    right_bars: int = 10,
    atr_length: int = 10,
    atr_mult: float = 1.5,
) -> pd.DataFrame:
    """
    Detect local extrema and generate target labels for a single ticker series.
    """
    df = df_ticker.sort_values("date").reset_index(drop=True).copy()
    n = len(df)
    if n <= left_bars + right_bars:
        return df

    df["atr"] = compute_atr(df, length=atr_length)

    highs = df["high"].values
    lows = df["low"].values
    closes = df["close"].values
    atrs = df["atr"].values
    dates = df["date"].values

    extrema_history = []

    # Sequential bar traversal replicating Pine Script runtime
    for i in range(left_bars + right_bars, n):
        target_idx = i - right_bars
        target_high = highs[target_idx]
        target_low = lows[target_idx]
        target_atr = atrs[target_idx]

        # Check Pivot High
        is_pivot_high = True
        for k in range(target_idx - left_bars, target_idx + right_bars + 1):
            if k != target_idx:
                if highs[k] > target_high:
                    is_pivot_high = False
                    break
                elif highs[k] == target_high and k < target_idx:
                    is_pivot_high = False
                    break

        # Check Pivot Low
        is_pivot_low = True
        for k in range(target_idx - left_bars, target_idx + right_bars + 1):
            if k != target_idx:
                if lows[k] < target_low:
                    is_pivot_low = False
                    break
                elif lows[k] == target_low and k < target_idx:
                    is_pivot_low = False
                    break

        # Process High Candidate
        if is_pivot_high:
            cand_price = float(target_high)
            if len(extrema_history) == 0:
                extrema_history.append(ExtremaPoint(target_idx, cand_price, 1, dates[target_idx]))
            else:
                last_pt = extrema_history[-1]
                price_delta = abs(cand_price - last_pt.price)
                min_threshold = target_atr * atr_mult

                if last_pt.point_type == 1:
                    if cand_price > last_pt.price:
                        last_pt.bar_idx = target_idx
                        last_pt.price = cand_price
                        last_pt.date = dates[target_idx]
                else:
                    if price_delta >= min_threshold:
                        extrema_history.append(ExtremaPoint(target_idx, cand_price, 1, dates[target_idx]))

        # Process Low Candidate
        if is_pivot_low:
            cand_price = float(target_low)
            if len(extrema_history) == 0:
                extrema_history.append(ExtremaPoint(target_idx, cand_price, -1, dates[target_idx]))
            else:
                last_pt = extrema_history[-1]
                price_delta = abs(cand_price - last_pt.price)
                min_threshold = target_atr * atr_mult

                if last_pt.point_type == -1:
                    if cand_price < last_pt.price:
                        last_pt.bar_idx = target_idx
                        last_pt.price = cand_price
                        last_pt.date = dates[target_idx]
                else:
                    if price_delta >= min_threshold:
                        extrema_history.append(ExtremaPoint(target_idx, cand_price, -1, dates[target_idx]))

    # Initialize output columns
    df["extrema_type"] = np.int8(0)
    df["extrema_price"] = np.nan
    df["swing_regime"] = np.int8(0)
    df["next_extrema_type"] = np.int8(0)
    df["bars_to_next_extrema"] = np.nan
    df["return_to_next_extrema"] = np.nan

    # Mark exact extrema points
    for ep in extrema_history:
        df.loc[ep.bar_idx, "extrema_type"] = np.int8(ep.point_type)
        df.loc[ep.bar_idx, "extrema_price"] = ep.price

    # Backfill forward-looking regime and next extrema targets
    if len(extrema_history) >= 2:
        for idx_e in range(len(extrema_history) - 1):
            curr_ep = extrema_history[idx_e]
            next_ep = extrema_history[idx_e + 1]

            # Swing regime between curr_ep and next_ep
            regime_val = np.int8(1) if curr_ep.point_type == -1 and next_ep.point_type == 1 else np.int8(-1)
            start_i = curr_ep.bar_idx
            end_i = next_ep.bar_idx

            df.loc[start_i:end_i, "swing_regime"] = regime_val
            for bar_i in range(start_i, end_i):
                df.loc[bar_i, "next_extrema_type"] = np.int8(next_ep.point_type)
                df.loc[bar_i, "bars_to_next_extrema"] = int(end_i - bar_i)
                df.loc[bar_i, "return_to_next_extrema"] = (next_ep.price - closes[bar_i]) / closes[bar_i]

    # Week-Ahead Future Direction (5 trading days lookahead)
    future_close_5d = df["close"].shift(-5)
    close_diff = future_close_5d - df["close"]
    df["week_ahead_direction"] = np.sign(close_diff).fillna(0).astype(np.int8)

    df = df.drop(columns=["atr"])
    return df


def process_indices_extrema(
    input_path: str,
    output_path: str,
    left_bars: int = 10,
    right_bars: int = 10,
    atr_length: int = 10,
    atr_mult: float = 1.5,
):
    """Run extrema labeling across all market indices in Parquet."""
    in_file = Path(input_path)
    out_file = Path(output_path)

    if not in_file.exists():
        raise FileNotFoundError(f"Input file not found: {in_file}")

    print(f"[Extrema] Loading index data from: {in_file}")
    df = pd.read_parquet(in_file)

    labeled_chunks = []
    tickers = df["ticker"].unique()

    print(f"[Extrema] Parameters: L={left_bars}, R={right_bars}, ATR_Len={atr_length}, ATR_Mult={atr_mult}")
    for ticker in tickers:
        df_t = df[df["ticker"] == ticker]
        print(f"\nProcessing {ticker} ({len(df_t):,} bars)...")
        labeled_t = label_ticker_extrema(
            df_t,
            left_bars=left_bars,
            right_bars=right_bars,
            atr_length=atr_length,
            atr_mult=atr_mult,
        )

        n_peaks = (labeled_t["extrema_type"] == 1).sum()
        n_troughs = (labeled_t["extrema_type"] == -1).sum()
        bullish_bars = (labeled_t["swing_regime"] == 1).sum()
        bearish_bars = (labeled_t["swing_regime"] == -1).sum()
        up_5d = (labeled_t["week_ahead_direction"] == 1).sum()
        down_5d = (labeled_t["week_ahead_direction"] == -1).sum()

        print(f"  - Confirmed Extrema: {n_peaks} Peaks, {n_troughs} Troughs (Total: {n_peaks + n_troughs})")
        print(f"  - Swing Regime Coverage: Bullish {bullish_bars:,} bars ({bullish_bars/len(df_t)*100:.1f}%) | Bearish {bearish_bars:,} bars ({bearish_bars/len(df_t)*100:.1f}%)")
        print(f"  - 5-Day Ahead Direction: +1 (Up) {up_5d:,} ({up_5d/len(df_t)*100:.1f}%) | -1 (Down) {down_5d:,} ({down_5d/len(df_t)*100:.1f}%)")

        labeled_chunks.append(labeled_t)

    df_out = pd.concat(labeled_chunks, ignore_index=True)
    df_out = df_out.sort_values(["ticker", "date"]).reset_index(drop=True)

    table = pa.Table.from_pandas(df_out, preserve_index=False)
    pq.write_table(table, out_file, compression="zstd")

    size_kb = out_file.stat().st_size / 1024
    print(f"\n[Extrema] Successfully written labeled dataset to {out_file} ({len(df_out):,} rows, {size_kb:.1f} KB).")


def main():
    parser = argparse.ArgumentParser(description="Offline Local Extrema Detection & Labeling")
    parser.add_argument("--input", type=str, default="yfinance/indices.parquet", help="Input indices parquet file")
    parser.add_argument("--output", type=str, default="yfinance/indices.parquet", help="Output parquet file path")
    parser.add_argument("--left-bars", type=int, default=10, help="Left bars (L)")
    parser.add_argument("--right-bars", type=int, default=10, help="Right bars (R)")
    parser.add_argument("--atr-length", type=int, default=10, help="ATR length")
    parser.add_argument("--atr-mult", type=float, default=1.5, help="ATR multiplier threshold")
    args = parser.parse_args()

    process_indices_extrema(
        input_path=args.input,
        output_path=args.output,
        left_bars=args.left_bars,
        right_bars=args.right_bars,
        atr_length=args.atr_length,
        atr_mult=args.atr_mult,
    )


if __name__ == "__main__":
    main()
