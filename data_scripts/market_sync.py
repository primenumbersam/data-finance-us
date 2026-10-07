"""
data_scripts/market_sync.py
Incremental daily ETL pipeline for 2011~present:
1. Macro indicators (FRED API & official endpoints based on arena/data)
2. S&P 500 Top 50 + Buffer pool + SPY benchmark incremental OHLCV & Market Cap (yfinance)
3. Dynamic constituents resolution (constituents_current.parquet)
4. Telemetry and state synchronization (sync_state.json)
"""

import argparse
import io
import json
import os
from datetime import datetime, timezone
import time
from pathlib import Path
import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import yfinance as yf

ROOT_DIR = Path(__file__).resolve().parent.parent
LAKEHOUSE_DIR = ROOT_DIR
FRED_DIR = LAKEHOUSE_DIR / "fred"
YFINANCE_DIR = LAKEHOUSE_DIR / "yfinance"
NEWS_DIR = LAKEHOUSE_DIR / "news"

HEADERS = {"User-Agent": "Mozilla/5.0"}

# Top-Down Macro Indicator Registry (Adapted from arena/data/_index.csv)
FRED_INDICATORS = {
    # Credit & Term Premiums
    "BAMLH0A0HYM2": {"category": "credit", "name": "ICE BofA US High Yield Index Option-Adjusted Spread"},
    "THREEFYTP10": {"category": "credit", "name": "Term Premium on a 10 Year Zero Coupon Bond"},
    "T10Y2Y": {"category": "credit", "name": "10-Year Minus 2-Year Treasury Constant Maturity"},
    "M2SL": {"category": "credit", "name": "M2 Money Stock"},
    "GFDEBTN": {"category": "credit", "name": "Federal Debt: Total Public Debt"},
    # Inflation & Interest Rates
    "FEDFUNDS": {"category": "inflation", "name": "Federal Funds Effective Rate"},
    "DGS10": {"category": "inflation", "name": "Market Yield on U.S. Treasury Securities at 10-Year Constant Maturity"},
    "DFII10": {"category": "inflation", "name": "10-Year Treasury Inflation-Indexed Security"},
    "T5YIE": {"category": "inflation", "name": "5-Year Breakeven Inflation Rate"},
    "EXPINF5YR": {"category": "inflation", "name": "5-Year Expected Inflation (Cleveland Fed)"},
    "EXPINF10YR": {"category": "inflation", "name": "10-Year Expected Inflation (Cleveland Fed)"},
    "MICH": {"category": "inflation", "name": "University of Michigan: Inflation Expectation"},
    "CPIAUCSL": {"category": "inflation", "name": "Consumer Price Index for All Urban Consumers: All Items"},
    "PCEPILFE": {"category": "inflation", "name": "Personal Consumption Expenditures Excluding Food and Energy"},
    "PPIACO": {"category": "inflation", "name": "Producer Price Index by Commodity: All Commodities"},
    # Growth & Activity
    "GDPC1": {"category": "growth", "name": "Real Gross Domestic Product"},
    "PAYEMS": {"category": "growth", "name": "All Employees: Total Nonfarm"},
    "INDPRO": {"category": "growth", "name": "Industrial Production Index"},
    "HOUST": {"category": "growth", "name": "New Privately-Owned Housing Units Started: Total Units"},
    "UNRATE": {"category": "growth", "name": "Unemployment Rate"},
    "NFCI": {"category": "growth", "name": "Chicago Fed National Financial Conditions Index"},
    "STLFSI4": {"category": "growth", "name": "St. Louis Fed Financial Stress Index"},
    # Recession Risk & Market Regime
    "USREC": {"category": "recession", "name": "NBER based Recession Indicators for the United States"},
    "RECRISKUSP50": {"category": "recession", "name": "Recession Probability for the United States (Median Estimate)"},
    "RECPROUSM156N": {"category": "recession", "name": "Smoothed U.S. Recession Probabilities"},
    "SAHMREALTIME": {"category": "recession", "name": "Real-time Sahm Rule Recession Indicator"},
    "VIXCLS": {"category": "market", "name": "CBOE Volatility Index: VIX"},
    "NCBEILQ027S": {"category": "market", "name": "Nonfinancial Corporate Business: Corporate Equities"},
}


def get_fred_api_key():
    """Resolve FRED API key from environment, Kaggle secrets, .env file, or fallback."""
    key = os.environ.get("FRED_API_KEY")
    if key:
        return key.strip()
    try:
        from kaggle_secrets import UserSecretsClient
        user_secrets = UserSecretsClient()
        key = user_secrets.get_secret("FRED_API_KEY")
        if key:
            return key.strip()
    except Exception:
        pass
    env_file = ROOT_DIR / ".env"
    if env_file.exists():
        try:
            with open(env_file) as f:
                for line in f:
                    if line.startswith("FRED_API_KEY="):
                        return line.split("=", 1)[1].strip().strip('"').strip("'")
        except Exception:
            pass
    return "eab5c4fa956bada7be846a1f9052b64c"


def fetch_single_fred_series(series_id, meta, start_date, api_key, session):
    for attempt in range(2):
        try:
            if api_key:
                url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={api_key}&file_type=json"
                if start_date:
                    url += f"&observation_start={start_date}"
                resp = session.get(url, timeout=8)
                if resp.status_code == 200:
                    data = resp.json()
                    obs = data.get("observations", [])
                    if not obs:
                        return None
                    df = pd.DataFrame(obs)
                    df = df[df["value"] != "."]
                    if df.empty:
                        return None
                    df["date"] = pd.to_datetime(df["date"]).dt.date
                    df["series_id"] = series_id
                    df["value"] = pd.to_numeric(df["value"], errors="coerce")
                    df["category"] = meta["category"]
                    df["name"] = meta["name"]
                    return df[["date", "series_id", "value", "category", "name"]].dropna(subset=["value"])
            else:
                url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
                if start_date:
                    url += f"&cosd={start_date}"
                resp = session.get(url, timeout=8)
                if resp.status_code == 200:
                    df = pd.read_csv(io.StringIO(resp.text), na_values=".")
                    date_col = "observation_date" if "observation_date" in df.columns else "DATE"
                    if date_col in df.columns and series_id in df.columns:
                        df = df.dropna(subset=[series_id, date_col])
                        df["date"] = pd.to_datetime(df[date_col]).dt.date
                        df["series_id"] = series_id
                        df["value"] = pd.to_numeric(df[series_id], errors="coerce")
                        df["category"] = meta["category"]
                        df["name"] = meta["name"]
                        return df[["date", "series_id", "value", "category", "name"]].dropna(subset=["value"])
        except Exception:
            if attempt < 1:
                time.sleep(1.0)
    print(f"  Warning: failed to fetch {series_id} (timeout or network)")
    return None


def sync_fred_indicators():
    """Fetch macro series from FRED (high-speed REST API or CSV) and consolidate into indicators.parquet."""
    FRED_DIR.mkdir(parents=True, exist_ok=True)
    out_file = FRED_DIR / "indicators.parquet"

    start_date = None
    if out_file.exists():
        con = duckdb.connect()
        max_date = con.execute(f"SELECT MAX(date) FROM '{out_file}'").fetchone()[0]
        con.close()
        if max_date:
            start_date = (pd.to_datetime(max_date) - pd.Timedelta(days=30)).strftime("%Y-%m-%d")

    api_key = get_fred_api_key()
    mode = "Official REST API" if api_key else "Web CSV"
    date_info = f"from {start_date}" if start_date else "full history"
    print(f"[FRED] Fetching {len(FRED_INDICATORS)} macro indicators ({mode}, {date_info})...")

    session = requests.Session()
    session.headers.update(HEADERS)

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [
            executor.submit(fetch_single_fred_series, sid, meta, start_date, api_key, session)
            for sid, meta in FRED_INDICATORS.items()
        ]
        all_records = [f.result() for f in futures if f.result() is not None]

    if not all_records:
        print("[FRED] Warning: No new records to merge.")
        return

    df_incoming = pd.concat(all_records, ignore_index=True)

    if out_file.exists():
        print("[FRED] Merging incoming records with existing indicators.parquet...")
        con = duckdb.connect()
        con.register("incoming", df_incoming)
        con.execute(f"CREATE TABLE existing AS SELECT * FROM read_parquet('{out_file}')")
        merged_query = f"""
        COPY (
            SELECT date, series_id, value, category, name FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY date, series_id ORDER BY date) as rn
                FROM (
                    SELECT * FROM existing
                    UNION ALL
                    SELECT * FROM incoming
                )
            ) WHERE rn = 1
            ORDER BY series_id, date
        ) TO '{out_file}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');
        """
        con.execute(merged_query)
        con.close()
    else:
        table = pa.Table.from_pandas(df_incoming, preserve_index=False)
        pq.write_table(table, out_file, compression="zstd")

    con = duckdb.connect()
    row_count = con.execute(f"SELECT COUNT(*) FROM '{out_file}'").fetchone()[0]
    date_range = con.execute(f"SELECT MIN(date), MAX(date) FROM '{out_file}'").fetchone()
    con.close()

    print(f"[FRED] Successfully updated {out_file.name}: {row_count:,} records ({date_range[0]} ~ {date_range[1]}).")


def get_current_sp500_tickers() -> list:
    """Fetch live S&P 500 members list from Wikipedia."""
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    resp = requests.get(url, headers=HEADERS, timeout=20)
    tables = pd.read_html(io.StringIO(resp.text))
    tickers = tables[0]["Symbol"].str.replace(".", "-", regex=False).tolist()
    return tickers


from concurrent.futures import ThreadPoolExecutor


def sync_equities_and_constituents(start_date: str = None, full_scan: bool = False):
    """
    1. Resolve 2024+ Top 50 constituents and current weights (constituents_current.parquet)
    2. Extract Buffer universe (~70 tickers) + SPY benchmark
    3. Ingest daily OHLCV and market cap increments (prices_incremental.parquet)
    """
    YFINANCE_DIR.mkdir(parents=True, exist_ok=True)
    LAKEHOUSE_DIR.mkdir(parents=True, exist_ok=True)
    constituents_current_path = LAKEHOUSE_DIR / "constituents_current.parquet"
    prices_inc_path = YFINANCE_DIR / "prices_incremental.parquet"

    def fetch_single_cap(ticker_sym):
        try:
            t = yf.Ticker(ticker_sym)
            mcap = t.fast_info.market_cap
            if mcap and mcap > 0:
                return {"ticker": ticker_sym, "market_cap": float(mcap)}
        except Exception:
            pass
        return None

    if not full_scan and constituents_current_path.exists():
        print("[Equities] Fast-updating existing constituents weights...")
        df_prev = pd.read_parquet(constituents_current_path)
        buffer_universe = df_prev["ticker"].unique().tolist()
        if "SPY" not in buffer_universe:
            buffer_universe.append("SPY")

        with ThreadPoolExecutor(max_workers=10) as executor:
            cap_records = list(filter(None, executor.map(fetch_single_cap, buffer_universe)))

        if cap_records:
            df_caps = pd.DataFrame(cap_records).sort_values("market_cap", ascending=False).reset_index(drop=True)
            df_caps["rank"] = range(1, len(df_caps) + 1)
            top50 = df_caps.head(50).copy()
            total_top50_mcap = top50["market_cap"].sum()
            top50["weight"] = top50["market_cap"] / total_top50_mcap
            top50["date"] = datetime.now(timezone.utc).date()
            table_constituents = pa.Table.from_pandas(
                top50[["date", "ticker", "market_cap", "rank", "weight"]],
                preserve_index=False,
            )
            pq.write_table(table_constituents, constituents_current_path, compression="zstd")
            print(f"[Equities] Saved current constituents: {constituents_current_path.name} (50 rows, total cap: )")
    else:
        print("[Equities] Resolving current S&P 500 constituents and market caps across all members...")
        all_sp500 = get_current_sp500_tickers()
        print(f"Total current S&P 500 constituents: {len(all_sp500)}")
        with ThreadPoolExecutor(max_workers=15) as executor:
            cap_records = list(filter(None, executor.map(fetch_single_cap, all_sp500)))

        if not cap_records:
            print("[Equities] Error: Failed to resolve market caps.")
            return

        df_caps = pd.DataFrame(cap_records).sort_values("market_cap", ascending=False).reset_index(drop=True)
        df_caps["rank"] = range(1, len(df_caps) + 1)
        top50 = df_caps.head(50).copy()
        total_top50_mcap = top50["market_cap"].sum()
        top50["weight"] = top50["market_cap"] / total_top50_mcap
        top50["date"] = datetime.now(timezone.utc).date()
        table_constituents = pa.Table.from_pandas(
            top50[["date", "ticker", "market_cap", "rank", "weight"]],
            preserve_index=False,
        )
        pq.write_table(table_constituents, constituents_current_path, compression="zstd")
        print(f"[Equities] Saved current constituents: {constituents_current_path.name} (50 rows, total cap: )")
        buffer_universe = df_caps.head(70)["ticker"].tolist()
        if "SPY" not in buffer_universe:
            buffer_universe.append("SPY")

    if start_date is None:
        if prices_inc_path.exists():
            con = duckdb.connect()
            max_date = con.execute(f"SELECT MAX(date) FROM '{prices_inc_path}'").fetchone()[0]
            con.close()
            if max_date:
                start_date = (pd.to_datetime(max_date) - pd.Timedelta(days=5)).strftime("%Y-%m-%d")
            else:
                start_date = "2024-01-01"
        else:
            start_date = "2024-01-01"

    print(f"[Equities] Ingesting incremental OHLCV for Buffer Universe ({len(buffer_universe)} tickers, start={start_date})...")
    df_download = yf.download(
        tickers=buffer_universe,
        start=start_date,
        interval="1d",
        auto_adjust=False,
        threads=True,
        progress=False,
    )
    if df_download.empty:
        print("[Equities] Error: Downloaded price dataset is empty.")
        return

    # Transform multi-index columns to long format
    df_stacked = df_download.stack(level="Ticker", future_stack=True).reset_index()
    date_col = "Date" if "Date" in df_stacked.columns else df_stacked.columns[0]
    
    # Standardize column naming
    mcap_map = dict(zip(df_caps["ticker"], df_caps["market_cap"]))
    
    df_incoming = pd.DataFrame()
    df_incoming["date"] = pd.to_datetime(df_stacked[date_col]).dt.date
    df_incoming["ticker"] = df_stacked["Ticker"].astype(str)
    df_incoming["open"] = pd.to_numeric(df_stacked.get("Open"), errors="coerce")
    df_incoming["high"] = pd.to_numeric(df_stacked.get("High"), errors="coerce")
    df_incoming["low"] = pd.to_numeric(df_stacked.get("Low"), errors="coerce")
    df_incoming["close"] = pd.to_numeric(df_stacked.get("Close"), errors="coerce")
    df_incoming["adj_close"] = pd.to_numeric(df_stacked.get("Adj Close", df_stacked.get("Close")), errors="coerce")
    df_incoming["volume"] = pd.to_numeric(df_stacked.get("Volume"), errors="coerce").fillna(0).astype("int64")
    df_incoming["market_cap"] = df_incoming["ticker"].map(mcap_map).astype(float)

    df_incoming_prices = df_incoming.dropna(subset=["close", "date"]).sort_values(["ticker", "date"]).reset_index(drop=True)
    prices_inc_path = YFINANCE_DIR / "prices_incremental.parquet"

    if prices_inc_path.exists():
        print(f"[Equities] Merging incoming records into {prices_inc_path.name}...")
        con = duckdb.connect()
        con.register("incoming", df_incoming_prices)
        con.execute(f"CREATE TABLE existing AS SELECT * FROM read_parquet('{prices_inc_path}')")
        merged_query = f"""
        COPY (
            SELECT date, ticker, open, high, low, close, adj_close, volume, market_cap FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY date, ticker ORDER BY date) as rn
                FROM (
                    SELECT * FROM existing
                    UNION ALL
                    SELECT * FROM incoming
                )
            ) WHERE rn = 1
            ORDER BY ticker, date
        ) TO '{prices_inc_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');
        """
        con.execute(merged_query)
        con.close()
    else:
        table_prices = pa.Table.from_pandas(df_incoming_prices, preserve_index=False)
        pq.write_table(table_prices, prices_inc_path, compression="zstd")

    con = duckdb.connect()
    row_count = con.execute(f"SELECT COUNT(*) FROM '{prices_inc_path}'").fetchone()[0]
    symbols_count = con.execute(f"SELECT COUNT(DISTINCT ticker) FROM '{prices_inc_path}'").fetchone()[0]
    date_range = con.execute(f"SELECT MIN(date), MAX(date) FROM '{prices_inc_path}'").fetchone()
    con.close()

    print(f"[Equities] Saved {prices_inc_path.name}: {row_count:,} rows across {symbols_count} tickers ({date_range[0]} ~ {date_range[1]}).")



def sync_indices(tickers: list = None, start_date: str = None):
    """
    Fetch and synchronize primary market benchmark indices (^GSPC, ^NDX, ^DJI)
    into data-finance-us/yfinance/indices.parquet for ex-post labeling and regime analysis.
    """
    if tickers is None:
        tickers = ["^GSPC", "^NDX", "^DJI"]

    YFINANCE_DIR.mkdir(parents=True, exist_ok=True)
    out_file = YFINANCE_DIR / "indices.parquet"

    if start_date is None:
        if out_file.exists():
            con = duckdb.connect()
            max_date = con.execute(f"SELECT MAX(date) FROM '{out_file}'").fetchone()[0]
            con.close()
            if max_date:
                start_date = (pd.to_datetime(max_date) - pd.Timedelta(days=5)).strftime("%Y-%m-%d")
            else:
                start_date = "1999-01-01"
        else:
            start_date = "1999-01-01"

    print(f"[Indices] Fetching market benchmark indices: {tickers} from {start_date}...")

    raw = yf.download(tickers, start=start_date, auto_adjust=False, progress=False)
    if raw.empty:
        print("[Indices] Warning: No index data returned from yfinance.")
        return

    records = []
    for t in tickers:
        try:
            sub = pd.DataFrame({
                "date": pd.to_datetime(raw.index).date,
                "ticker": t,
                "open": raw["Open"][t].astype(float),
                "high": raw["High"][t].astype(float),
                "low": raw["Low"][t].astype(float),
                "close": raw["Close"][t].astype(float),
                "adj_close": raw["Adj Close"][t].astype(float),
                "volume": raw["Volume"][t].fillna(0).astype("int64"),
            })
            records.append(sub.dropna(subset=["close"]))
        except Exception as e:
            print(f"[Indices] Error processing {t}: {e}")

    if not records:
        return

    df_indices = pd.concat(records, ignore_index=True)
    df_indices = df_indices.sort_values(["ticker", "date"]).reset_index(drop=True)

    if out_file.exists():
        con = duckdb.connect()
        con.register("incoming", df_indices)
        con.execute(f"CREATE TABLE existing AS SELECT * FROM read_parquet('{out_file}')")
        merged_query = f"""
        COPY (
            SELECT date, ticker, open, high, low, close, adj_close, volume FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY date, ticker ORDER BY date) as rn
                FROM (
                    SELECT * FROM existing UNION ALL SELECT * FROM incoming
                )
            ) WHERE rn = 1
            ORDER BY ticker, date
        ) TO '{out_file}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');
        """
        con.execute(merged_query)
        total_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out_file}')").fetchone()[0]
        con.close()
        print(f"[Indices] Successfully updated {out_file.name}: {total_rows:,} rows.")
    else:
        table = pa.Table.from_pandas(df_indices, preserve_index=False)
        pq.write_table(table, out_file, compression="zstd")
        print(f"[Indices] Successfully created {out_file.name}: {len(df_indices):,} rows.")


def update_sync_state():
    """Produce global synchronization state telemetry (sync_state.json)."""
    sync_state_path = LAKEHOUSE_DIR / "sync_state.json"
    con = duckdb.connect()

    telemetry = {
        "last_sync_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "tables": {},
    }

    # Inspect constituents
    const_files = list(LAKEHOUSE_DIR.glob("constituents_*.parquet"))
    if const_files:
        info = con.execute(f"""
            SELECT 
                COUNT(*) as rows, 
                COUNT(DISTINCT ticker) as unique_tickers, 
                MIN(date) as min_date, 
                MAX(date) as max_date 
            FROM read_parquet('{LAKEHOUSE_DIR}/constituents_*.parquet', union_by_name=true)
        """).df().to_dict(orient="records")[0]
        telemetry["tables"]["constituents"] = info

    # Inspect prices
    price_files = list(YFINANCE_DIR.glob("prices_*.parquet"))
    if price_files:
        info = con.execute(f"""
            SELECT 
                COUNT(*) as rows, 
                COUNT(DISTINCT ticker) as unique_tickers, 
                MIN(date) as min_date, 
                MAX(date) as max_date 
            FROM read_parquet('{YFINANCE_DIR}/prices_*.parquet', union_by_name=true)
        """).df().to_dict(orient="records")[0]
        telemetry["tables"]["prices"] = info

    # Inspect fred indicators
    fred_file = FRED_DIR / "indicators.parquet"
    if fred_file.exists():
        info = con.execute(f"""
            SELECT 
                COUNT(*) as rows, 
                COUNT(DISTINCT series_id) as series_count, 
                MIN(date) as min_date, 
                MAX(date) as max_date 
            FROM read_parquet('{fred_file}')
        """).df().to_dict(orient="records")[0]
        telemetry["tables"]["macro_indicators"] = info

    # Inspect market indices
    indices_file = YFINANCE_DIR / "indices.parquet"
    if indices_file.exists():
        info = con.execute(f"""
            SELECT 
                COUNT(*) as rows, 
                COUNT(DISTINCT ticker) as unique_tickers, 
                MIN(date) as min_date, 
                MAX(date) as max_date 
            FROM read_parquet('{indices_file}')
        """).df().to_dict(orient="records")[0]
        telemetry["tables"]["indices"] = info


    # Inspect news
    news_files = list(NEWS_DIR.glob("news_*.parquet"))
    if news_files:
        info = con.execute(f"""
            SELECT 
                COUNT(*) as rows, 
                MIN(timestamp) as min_timestamp, 
                MAX(timestamp) as max_timestamp 
            FROM read_parquet('{NEWS_DIR}/news_*.parquet', union_by_name=true)
        """).df().to_dict(orient="records")[0]
        # Convert timestamp to str for JSON serialization
        for k in ["min_timestamp", "max_timestamp"]:
            if info.get(k):
                info[k] = str(info[k])
        telemetry["tables"]["news"] = info

    con.close()

    # Convert dates to string
    for table_name, data in telemetry["tables"].items():
        for k, v in data.items():
            if isinstance(v, (datetime, pd.Timestamp)):
                data[k] = v.isoformat()
            elif hasattr(v, "strftime"):
                data[k] = str(v)

    with open(sync_state_path, "w", encoding="utf-8") as f:
        json.dump(telemetry, f, indent=2, ensure_ascii=False)

    print(f"\n[Telemetry] Synchronized state catalog written to: {sync_state_path}")


import re
import xml.etree.ElementTree as ET

MACRO_PATTERNS = re.compile(
    r"\b(?:fed|fomc|inflation|cpi|interest rate|rates|recession|treasury|yield|gdp|unemployment|jobs|economy|central bank|powell|yellen|monetary|tariff)\b",
    re.IGNORECASE,
)
SECTOR_PATTERNS = re.compile(
    r"\b(?:semiconductor|chip|chips|energy|crude|oil|gas|banking|banks|pharma|biotech|tech|retail|telecom|automotive|defense|aerospace)\b",
    re.IGNORECASE,
)


def classify_headline(title: str):
    """Categorize headline into macro, sector, or market with extracted tags."""
    title_str = str(title or "")
    macro_matches = set(re.findall(r"\b([a-zA-Z]+)\b", title_str.lower())).intersection({
        "fed", "fomc", "inflation", "cpi", "rate", "rates", "recession", "treasury", "yield",
        "gdp", "unemployment", "jobs", "economy", "powell", "yellen", "tariff"
    })
    sector_matches = set(re.findall(r"\b([a-zA-Z]+)\b", title_str.lower())).intersection({
        "semiconductor", "chip", "energy", "oil", "gas", "banking", "banks", "pharma",
        "biotech", "tech", "retail", "telecom", "automotive"
    })

    tags = []
    category = "market"

    if macro_matches:
        category = "macro"
        tags.extend([m.upper() for m in macro_matches])
    elif sector_matches:
        category = "market"
        tags.extend([s.upper() for s in sector_matches])

    return category, tags


def is_valid_news_record(headline: str, summary: str) -> bool:
    """
    Validate that an article summary provides substantive distinct context.
    Drops records where summary is missing, empty, or essentially identical to headline.
    """
    if not headline or not summary:
        return False
    h = headline.strip().lower()
    s = summary.strip().lower()
    if not s or h == s:
        return False
    h_norm = re.sub(r"[^\w]", "", h)
    s_norm = re.sub(r"[^\w]", "", s)
    if not s_norm or h_norm == s_norm:
        return False
    if len(s_norm) <= len(h_norm) + 5 and (s_norm in h_norm or h_norm in s_norm):
        return False
    return True


def sync_news_feed(backfill_from: str = None):
    """
    Collect comprehensive top-down financial news (2011~present):
    1. Bi-weekly historical window queries via Google News Macro to cover EVERY trading day
    2. Real-time yf.Search top macro keywords (filtered for distinct summaries)
    3. Curated economic RSS feeds (CNBC Economy, MarketWatch)
    Saves to: news/news_incremental.parquet
    """
    NEWS_DIR.mkdir(parents=True, exist_ok=True)
    out_file = NEWS_DIR / "news_incremental.parquet"

    if backfill_from is None:
        if out_file.exists():
            con = duckdb.connect()
            latest_ts = con.execute(f"SELECT MAX(timestamp) FROM '{out_file}'").fetchone()[0]
            con.close()
            if latest_ts:
                start_dt = pd.to_datetime(latest_ts) - pd.Timedelta(days=7)
                backfill_from = start_dt.strftime("%Y-%m-%d")
            else:
                backfill_from = "2024-01-01"
        else:
            backfill_from = "2011-01-01"

    print(f"\n[News] Collecting daily top-down financial & macro news feeds (Backfill from {backfill_from})...")

    records = []

    # 1. Historical Window Backfill (2024-01-01 to Present in 14-day intervals)
    cur_date = pd.to_datetime(backfill_from)
    end_limit = pd.to_datetime(datetime.now(timezone.utc).date())
    
    windows = []
    while cur_date < end_limit:
        next_date = min(cur_date + pd.Timedelta(days=14), end_limit)
        windows.append((cur_date.strftime("%Y-%m-%d"), next_date.strftime("%Y-%m-%d")))
        cur_date = next_date

    print(f"[News] Executing historical date-window queries across {len(windows)} intervals ({backfill_from}~present)...")
    for idx, (s_date, e_date) in enumerate(windows):
        url = (
            f"https://news.google.com/rss/search?q=(Federal+Reserve+OR+inflation+OR+economy+OR+stocks)"
            f"+after:{s_date}+before:{e_date}&hl=en-US&gl=US&ceid=US:en"
        )
        try:
            r = requests.get(url, headers=HEADERS, timeout=12)
            if r.status_code == 200:
                root = ET.fromstring(r.content)
                for item in root.findall(".//item"):
                    title_elem = item.find("title")
                    desc_elem = item.find("description")
                    date_elem = item.find("pubDate")
                    source_elem = item.find("source")

                    title = title_elem.text.strip() if title_elem is not None and title_elem.text else ""
                    desc = desc_elem.text.strip() if desc_elem is not None and desc_elem.text else ""
                    desc = re.sub(r"<[^>]+>", "", desc).strip()
                    summary_text = desc[:300].strip()
                    source_name = source_elem.text.strip() if source_elem is not None and source_elem.text else "Google News"

                    if not title or not is_valid_news_record(title, summary_text):
                        continue

                    if date_elem is not None and date_elem.text:
                        try:
                            ts = pd.to_datetime(date_elem.text, utc=True).tz_localize(None).floor("D")
                        except Exception:
                            ts = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                    else:
                        ts = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

                    if re.search(r"[а-яА-ЯёЁ]", title):
                        continue

                    cat, tags = classify_headline(title)
                    records.append({
                        "timestamp": ts,
                        "source": source_name,
                        "category": cat,
                        "tags": tags,
                        "headline": title,
                        "summary": summary_text,
                    })
        except Exception:
            pass
        if (idx + 1) % 25 == 0 or idx == len(windows) - 1:
            print(f"  [{idx + 1}/{len(windows)}] Window {s_date} ~ {e_date} | Total records: {len(records):,}")
        time.sleep(0.05)

    print(f"[News] Accumulated {len(records):,} articles from date-window backfill.")

    # 2. Yahoo Finance Search News (Latest)
    search_queries = ["Federal Reserve", "US Inflation", "S&P 500 Market", "Treasury Yields"]
    for q in search_queries:
        try:
            res = yf.Search(q, news_count=25)
            for item in res.news:
                title = item.get("title", "").strip()
                if not title:
                    continue
                summary_text = str(item.get("summary") or item.get("description") or "").strip()
                summary_text = re.sub(r"<[^>]+>", "", summary_text).strip()
                if not is_valid_news_record(title, summary_text):
                    continue

                pub_time = item.get("providerPublishTime")
                if pub_time:
                    ts = pd.to_datetime(pub_time, unit="s", utc=True).tz_localize(None).floor("D")
                else:
                    ts = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

                if re.search(r"[а-яА-ЯёЁ]", title):
                    continue

                cat, tags = classify_headline(title)
                records.append({
                    "timestamp": ts,
                    "source": str(item.get("publisher", "Yahoo Finance")).strip(),
                    "category": cat,
                    "tags": tags,
                    "headline": title,
                    "summary": summary_text,
                })
        except Exception as e:
            print(f"  Warning: yfinance search for '{q}' failed: {e}")

    # 3. Curated Economic RSS Feeds (Latest)
    rss_feeds = [
        ("CNBC Economy", "https://www.cnbc.com/id/20910258/device/rss/rss.html"),
        ("MarketWatch", "https://feeds.content.dowjones.io/public/rss/mw_topstories"),
    ]

    for source_name, feed_url in rss_feeds:
        try:
            r = requests.get(feed_url, headers=HEADERS, timeout=15)
            if r.status_code != 200:
                continue
            root = ET.fromstring(r.content)
            for item in root.findall(".//item"):
                title_elem = item.find("title")
                desc_elem = item.find("description")
                date_elem = item.find("pubDate")

                title = title_elem.text.strip() if title_elem is not None and title_elem.text else ""
                desc = desc_elem.text.strip() if desc_elem is not None and desc_elem.text else ""
                desc = re.sub(r"<[^>]+>", "", desc).strip()
                summary_text = desc[:300].strip()

                if not title or not is_valid_news_record(title, summary_text):
                    continue

                if date_elem is not None and date_elem.text:
                    try:
                        ts = pd.to_datetime(date_elem.text, utc=True).tz_localize(None).floor("D")
                    except Exception:
                        ts = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                else:
                    ts = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

                if re.search(r"[а-яА-ЯёЁ]", title):
                    continue

                cat, tags = classify_headline(title)
                records.append({
                    "timestamp": ts,
                    "source": source_name,
                    "category": cat,
                    "tags": tags,
                    "headline": title,
                    "summary": summary_text,
                })
        except Exception as e:
            print(f"  Warning: RSS feed '{source_name}' failed: {e}")

    if not records:
        print("[News] Warning: No new articles fetched.")
        return

    df_incoming = pd.DataFrame(records)
    print(f"[News] Total raw incoming articles to merge: {len(df_incoming):,}")

    schema = pa.schema([
        ("timestamp", pa.timestamp("ms")),
        ("source", pa.string()),
        ("category", pa.string()),
        ("tags", pa.list_(pa.string())),
        ("headline", pa.string()),
        ("summary", pa.string()),
    ])

    if out_file.exists():
        print(f"[News] Merging incoming articles into {out_file.name}...")
        con = duckdb.connect()
        con.register("incoming", df_incoming)
        con.execute(f"CREATE TABLE existing AS SELECT * FROM read_parquet('{out_file}')")
        merged_query = f"""
        COPY (
            SELECT DATE_TRUNC('day', timestamp)::TIMESTAMP as timestamp, source, category, tags, headline, summary FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY headline 
                    ORDER BY timestamp ASC
                ) as rn
                FROM (
                    SELECT * FROM existing
                    UNION ALL
                    SELECT * FROM incoming
                )
            ) 
            WHERE rn = 1
              AND summary IS NOT NULL
              AND LENGTH(TRIM(summary)) > 0
              AND TRIM(LOWER(headline)) != TRIM(LOWER(summary))
            ORDER BY timestamp ASC, headline ASC
        ) TO '{out_file}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');
        """
        con.execute(merged_query)
        con.close()
    else:
        table = pa.Table.from_pandas(df_incoming, schema=schema, preserve_index=False)
        pq.write_table(table, out_file, compression="zstd")

    con = duckdb.connect()
    row_count = con.execute(f"SELECT COUNT(*) FROM '{out_file}'").fetchone()[0]
    date_range = con.execute(f"SELECT MIN(timestamp), MAX(timestamp) FROM '{out_file}'").fetchone()
    con.close()

    print(f"[News] Successfully updated {out_file.name}: {row_count:,} articles ({date_range[0]} ~ {date_range[1]}).")


def main():
    parser = argparse.ArgumentParser(description="Market Sync: Macro & Equity Incremental Ingestion")
    parser.add_argument("--skip-macro", action="store_true", help="Skip FRED macro indicators sync")
    parser.add_argument("--skip-equities", action="store_true", help="Skip yfinance equity sync")
    parser.add_argument("--skip-news", action="store_true", help="Skip incremental news sync")
    parser.add_argument("--skip-indices", action="store_true", help="Skip primary indices sync")
    parser.add_argument("--backfill-from", type=str, default=None, help="Backfill news from date (YYYY-MM-DD). If omitted, syncs dynamically from latest timestamp.")
    args = parser.parse_args()

    if not args.skip_macro:
        sync_fred_indicators()

    if not args.skip_equities:
        sync_equities_and_constituents()

    if not args.skip_indices:
        sync_indices()

    if not args.skip_news:
        sync_news_feed(backfill_from=args.backfill_from)

    update_sync_state()
    print("\n[Market Sync] All incremental synchronization routines completed successfully.")


if __name__ == "__main__":
    main()
