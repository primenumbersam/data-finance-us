"""
data_scripts/finspid_diet.py
Historical FINSPID archive filtration and lightweight Parquet lakehouse generation.
Filters 1999-2023 full equity history to S&P 500 Top 50 universe and curated news.
"""

import argparse
import os
import re
import subprocess
import sys
import zipfile
from datetime import datetime
from pathlib import Path
import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ROOT_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT_DIR / "raw_data"
LAKEHOUSE_DIR = ROOT_DIR
YFINANCE_DIR = LAKEHOUSE_DIR / "yfinance"
NEWS_DIR = LAKEHOUSE_DIR / "news"

NEWS_URLS = {
    "All_external.csv": "https://huggingface.co/datasets/Zihan1004/FNSPID/resolve/main/Stock_news/All_external.csv",
    "nasdaq_exteral_data.csv": "https://huggingface.co/datasets/Zihan1004/FNSPID/resolve/main/Stock_news/nasdaq_exteral_data.csv",
}

MACRO_PATTERNS = re.compile(
    r"\b(?:fed|fomc|inflation|cpi|interest rate|rates|recession|treasury|yield|gdp|unemployment|jobs|economy|central bank|powell|yellen|monetary|tariff)\b",
    re.IGNORECASE,
)
SECTOR_PATTERNS = re.compile(
    r"\b(?:semiconductor|chip|chips|energy|crude|oil|gas|banking|banks|pharma|biotech|tech|retail|telecom|automotive|defense|aerospace)\b",
    re.IGNORECASE,
)


def load_sp500_pit_membership(csv_path: Path) -> pd.DataFrame:
    """Load point-in-time S&P 500 constituents history (1999-2023)."""
    df = pd.read_csv(csv_path)
    df["date"] = pd.to_datetime(df["date"])
    df = df[(df["date"] >= "1999-01-01") & (df["date"] <= "2023-12-31")]
    df = df.sort_values("date").reset_index(drop=True)
    return df


def extract_top50_constituents_and_prices(
    zip_path: Path,
    sp500_components_path: Path,
):
    """
    Extract Top 50 S&P 500 stocks across rebalancing periods (1999-2023).
    Saves:
      - data-finance-us/constituents_1999_2023_finspid.parquet
      - data-finance-us/yfinance/prices_1999_2023_finspid.parquet
    """
    YFINANCE_DIR.mkdir(parents=True, exist_ok=True)
    LAKEHOUSE_DIR.mkdir(parents=True, exist_ok=True)

    print("[1/5] Loading historical S&P 500 membership records...")
    df_sp500 = load_sp500_pit_membership(sp500_components_path)

    all_sp500_tickers = set()
    for row in df_sp500["tickers"].dropna():
        all_sp500_tickers.update([t.strip().upper() for t in row.split(",") if t.strip()])

    print(f"Total unique S&P 500 tickers (1999-2023): {len(all_sp500_tickers)}")

    print("[2/5] Inspecting raw_data/full_history.zip...")
    with zipfile.ZipFile(zip_path, "r") as zf:
        zip_namelist = set(zf.namelist())
        available_tickers = {}
        for ticker in all_sp500_tickers:
            cand = f"full_history/{ticker}.csv"
            if cand in zip_namelist:
                available_tickers[ticker] = cand

        print(f"Available S&P 500 tickers in archive: {len(available_tickers)}")

        print("[3/5] Ingesting ticker OHLCV records and computing size rankings...")
        ticker_dfs = []
        con = duckdb.connect()

        count = 0
        total = len(available_tickers)
        for ticker, zip_entry in available_tickers.items():
            count += 1
            if count % 100 == 0 or count == total:
                print(f"  Reading tickers: {count}/{total} ({(count/total)*100:.1f}%)")

            try:
                with zf.open(zip_entry) as f:
                    df_t = pd.read_csv(f)
            except Exception:
                continue

            if df_t.empty or "date" not in df_t.columns or "close" not in df_t.columns:
                continue

            df_t["date"] = pd.to_datetime(df_t["date"])
            df_t = df_t[(df_t["date"] >= "1999-01-01") & (df_t["date"] <= "2023-12-31")]
            if df_t.empty:
                continue

            df_t["ticker"] = ticker
            df_t["open"] = pd.to_numeric(df_t.get("open"), errors="coerce")
            df_t["high"] = pd.to_numeric(df_t.get("high"), errors="coerce")
            df_t["low"] = pd.to_numeric(df_t.get("low"), errors="coerce")
            df_t["close"] = pd.to_numeric(df_t.get("close"), errors="coerce")
            df_t["adj_close"] = pd.to_numeric(df_t.get("adj close", df_t["close"]), errors="coerce")
            df_t["volume"] = pd.to_numeric(df_t.get("volume"), errors="coerce").fillna(0).astype("int64")
            df_t["dollar_volume"] = df_t["close"] * df_t["volume"]

            ticker_dfs.append(df_t[["date", "ticker", "open", "high", "low", "close", "adj_close", "volume", "dollar_volume"]])

    print("Registering price records in DuckDB for quarterly aggregation...")
    df_all_prices = pd.concat(ticker_dfs, ignore_index=True)
    con.register("raw_prices", df_all_prices)

    rebal_dates = (
        pd.date_range(start="1999-01-01", end="2023-12-31", freq="QE")
        .strftime("%Y-%m-%d")
        .tolist()
    )

    print(f"[4/5] Computing quarterly S&P 500 Top 50 portfolios across {len(rebal_dates)} periods...")
    constituents_rows = []
    top50_union = set()

    for r_date_str in rebal_dates:
        r_date = pd.to_datetime(r_date_str)
        sub_sp = df_sp500[df_sp500["date"] <= r_date]
        if sub_sp.empty:
            continue
        active_tickers_str = sub_sp.iloc[-1]["tickers"]
        active_tickers = set([t.strip().upper() for t in active_tickers_str.split(",") if t.strip()])

        query = f"""
        SELECT 
            ticker,
            MEDIAN(dollar_volume) AS med_dollar_volume
        FROM raw_prices
        WHERE date <= '{r_date_str}' AND date >= '{r_date_str}'::DATE - INTERVAL 60 DAY
        GROUP BY ticker
        HAVING med_dollar_volume > 0
        ORDER BY med_dollar_volume DESC
        """
        df_rank = con.execute(query).df()
        df_rank = df_rank[df_rank["ticker"].isin(active_tickers)].reset_index(drop=True)

        df_top50 = df_rank.head(50).copy()
        df_top50["rank"] = range(1, len(df_top50) + 1)
        total_size = df_top50["med_dollar_volume"].sum()
        df_top50["weight"] = df_top50["med_dollar_volume"] / total_size
        df_top50["date"] = r_date.date()
        df_top50["market_cap"] = df_top50["med_dollar_volume"]

        for _, row in df_top50.iterrows():
            top50_union.add(row["ticker"])
            constituents_rows.append({
                "date": row["date"],
                "ticker": row["ticker"],
                "market_cap": float(row["market_cap"]),
                "rank": int(row["rank"]),
                "weight": float(row["weight"]),
            })

    print(f"Top 50 cumulative union universe size (U_target): {len(top50_union)} unique tickers")

    df_constituents = pd.DataFrame(constituents_rows)
    constituents_parquet_path = LAKEHOUSE_DIR / "constituents_1999_2023_finspid.parquet"
    table_constituents = pa.Table.from_pandas(df_constituents, preserve_index=False)
    pq.write_table(table_constituents, constituents_parquet_path, compression="zstd")
    print(f"Saved: {constituents_parquet_path} ({len(df_constituents)} rows)")

    print(f"[5/5] Filtering price histories to U_target ({len(top50_union)} symbols)...")
    df_target_prices = df_all_prices[df_all_prices["ticker"].isin(top50_union)].copy()
    df_target_prices = df_target_prices.drop(columns=["dollar_volume"]).sort_values(["ticker", "date"]).reset_index(drop=True)
    df_target_prices["date"] = df_target_prices["date"].dt.date

    prices_parquet_path = YFINANCE_DIR / "prices_1999_2023_finspid.parquet"
    table_prices = pa.Table.from_pandas(df_target_prices, preserve_index=False)
    pq.write_table(table_prices, prices_parquet_path, compression="zstd")
    print(f"Saved: {prices_parquet_path} ({len(df_target_prices)} rows)")

    con.close()
    return top50_union


def classify_article(title: str):
    """Categorize headline into macro or market with extracted tags."""
    title_str = str(title or "")
    macro_matches = set(MACRO_PATTERNS.findall(title_str))
    sector_matches = set(SECTOR_PATTERNS.findall(title_str))

    tags = []
    category = "market"

    if macro_matches:
        category = "macro"
        tags.extend([m.upper() for m in macro_matches])
    elif sector_matches:
        category = "market"
        tags.extend([s.upper() for s in sector_matches])

    return category, tags


def process_news_csv(
    csv_path: Path,
    target_tickers: set,
    intermediate_parquet_path: Path,
    chunk_size: int = 100_000,
):
    """
    Stream-filter a large raw news CSV and append records into Parquet.
    Filters:
      - Stock_symbol in Top 50 universe OR headline matches macro/sector patterns
      - Retains headline, summary, publisher, timestamp, category, tags
    """
    print(f"Processing {csv_path.name} in chunks of {chunk_size:,}...")

    schema = pa.schema([
        ("timestamp", pa.timestamp("ms")),
        ("source", pa.string()),
        ("category", pa.string()),
        ("tags", pa.list_(pa.string())),
        ("headline", pa.string()),
        ("summary", pa.string()),
    ])

    writer = pq.ParquetWriter(intermediate_parquet_path, schema=schema, compression="zstd")

    total_read = 0
    total_retained = 0

    # Read CSV in chunks
    for chunk in pd.read_csv(
        csv_path,
        chunksize=chunk_size,
        low_memory=False,
        on_bad_lines="skip",
    ):
        total_read += len(chunk)

        # Standardize column names
        chunk.columns = [c.strip() for c in chunk.columns]

        # Stock_symbol filter
        symbol_col = "Stock_symbol" if "Stock_symbol" in chunk.columns else None
        title_col = "Article_title" if "Article_title" in chunk.columns else "headline"
        date_col = "Date" if "Date" in chunk.columns else "date"
        pub_col = "Publisher" if "Publisher" in chunk.columns else "source"
        summary_col = "Lsa_summary" if "Lsa_summary" in chunk.columns else "summary"

        chunk[title_col] = chunk[title_col].astype(str)
        chunk[date_col] = pd.to_datetime(chunk[date_col], errors="coerce", utc=True)
        chunk = chunk.dropna(subset=[date_col, title_col])

        # Mask: symbol in Top 50 universe OR macro keyword in headline
        is_target_symbol = False
        if symbol_col and symbol_col in chunk.columns:
            clean_symbols = chunk[symbol_col].fillna("").str.strip().str.upper()
            is_target_symbol = clean_symbols.isin(target_tickers)

        has_macro_keyword = chunk[title_col].str.contains(MACRO_PATTERNS, na=False)
        retained_mask = is_target_symbol | has_macro_keyword
        filtered = chunk[retained_mask].copy()

        # Drop non-English (Cyrillic) articles
        filtered = filtered[~filtered[title_col].astype(str).str.contains(r"[а-яА-ЯёЁ]", regex=True, na=False)]

        if filtered.empty:
            continue

        # Classify each row
        cats, tags_list = [], []
        for title in filtered[title_col]:
            cat, tags = classify_article(title)
            cats.append(cat)
            tags_list.append(tags)

        # Build clean dataframe
        df_clean = pd.DataFrame()
        df_clean["timestamp"] = filtered[date_col].dt.tz_localize(None).dt.floor("D")
        df_clean["source"] = filtered[pub_col].fillna("Unknown").astype(str).str.strip()
        df_clean["category"] = cats
        df_clean["tags"] = tags_list
        df_clean["headline"] = filtered[title_col].str.strip()
        df_clean["summary"] = filtered[summary_col].fillna(filtered[title_col]).astype(str).str.strip()

        table = pa.Table.from_pandas(df_clean, schema=schema, preserve_index=False)
        writer.write_table(table)
        total_retained += len(df_clean)

        print(f"  Processed {total_read:,} rows | Retained: {total_retained:,} rows ({total_retained/total_read*100:.1f}%)")

    writer.close()
    print(f"Completed {csv_path.name}: {total_read:,} read -> {total_retained:,} retained.")
    return total_retained


def download_and_process_news(target_tickers: set, source_filter: str = None):
    """
    Download and process All_external.csv and/or nasdaq_exteral_data.csv,
    deleting raw files after ingestion to conserve disk space.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    NEWS_DIR.mkdir(parents=True, exist_ok=True)

    intermediate_files = []

    active_urls = NEWS_URLS
    if source_filter:
        active_urls = {k: v for k, v in NEWS_URLS.items() if source_filter.lower() in k.lower()}
        if not active_urls:
            print(f"Unknown news source filter: {source_filter}. Choices: all_external, nasdaq", file=sys.stderr)
            return

    for filename, url in active_urls.items():
        csv_file = RAW_DIR / filename
        inter_parquet = NEWS_DIR / f"temp_{filename.replace('.csv', '')}.parquet"

        print(f"\n==========================================")
        print(f"Starting pipeline for: {filename}")
        print(f"==========================================")

        # Download using curl with resume support (-C -)
        print(f"Downloading {filename} via curl (resuming if partial file exists)...")
        cmd = ["curl", "-L", "-C", "-", "--retry", "5", "--retry-delay", "2", "-o", str(csv_file), url]
        ret = subprocess.run(cmd)
        if ret.returncode != 0:
            print(f"Error downloading {filename}", file=sys.stderr)
            continue

        # Process and convert to intermediate Parquet
        process_news_csv(csv_file, target_tickers, inter_parquet)
        intermediate_files.append(inter_parquet)

        # Remove raw CSV immediately to reclaim disk space
        if csv_file.exists():
            print(f"Removing raw file {csv_file} to reclaim disk space...")
            csv_file.unlink()

    # Final Deduplication & Consolidation with DuckDB
    final_news_parquet = NEWS_DIR / "news_1999_2010_finspid.parquet"
    if final_news_parquet.exists():
        existing_backup = NEWS_DIR / "temp_existing_base.parquet"
        final_news_parquet.rename(existing_backup)
        intermediate_files.append(existing_backup)

    print("\n[Final Consolidation] Consolidating and deduplicating news records in DuckDB...")

    con = duckdb.connect()
    parquet_glob = str(NEWS_DIR / "temp_*.parquet")

    dedup_query = f"""
    COPY (
        SELECT 
            timestamp,
            source,
            category,
            tags,
            headline,
            summary
        FROM read_parquet('{parquet_glob}')
        WHERE timestamp < '2011-01-01'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY headline, timestamp, source 
            ORDER BY LENGTH(summary) DESC
        ) = 1
        ORDER BY timestamp DESC
    ) TO '{final_news_parquet}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');
    """
    con.execute(dedup_query)
    con.close()

    # Cleanup temp intermediate parquet files
    for inter_p in intermediate_files:
        if inter_p.exists():
            inter_p.unlink()

    print(f"Final curated news lakehouse table saved: {final_news_parquet}")


def main():
    parser = argparse.ArgumentParser(description="FINSPID Diet & Data Lakehouse ETL")
    parser.add_argument("--skip-prices", action="store_true", help="Skip price & constituent extraction")
    parser.add_argument("--skip-news", action="store_true", help="Skip news extraction")
    parser.add_argument("--news-source", type=str, default=None, help="Process specific news source: all_external or nasdaq")
    args = parser.parse_args()

    zip_p = RAW_DIR / "full_history.zip"
    comp_p = RAW_DIR / "sp500_historical_components.csv"
    constituents_p = LAKEHOUSE_DIR / "constituents_1999_2023_finspid.parquet"

    # Step 1: Constituents & Prices
    if not args.skip_prices:
        if not zip_p.exists() or not comp_p.exists():
            print("Error: Missing prerequisites for prices", file=sys.stderr)
            sys.exit(1)
        target_universe = extract_top50_constituents_and_prices(zip_p, comp_p)
    else:
        # Load existing target universe from constituents parquet
        if constituents_p.exists():
            df_c = pd.read_parquet(constituents_p)
            target_universe = set(df_c["ticker"].unique())
            print(f"Loaded existing Top 50 universe: {len(target_universe)} symbols")
        else:
            print(f"Error: {constituents_p} not found. Run without --skip-prices first.", file=sys.stderr)
            sys.exit(1)

    # Step 2: News Processing
    if not args.skip_news:
        download_and_process_news(target_universe, source_filter=args.news_source)

    print("\nAll pipeline tasks finished successfully.")


if __name__ == "__main__":
    main()
