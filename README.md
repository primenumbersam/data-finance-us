# data-finance-us

A high-performance, local financial data lakehouse designed for quantitative research, macro regime analysis, and portfolio backtesting on US equities and economic indicators.

The lakehouse consolidates historical archives from FINSPID (1999–2023) with an ultra-fast, cache-aware incremental synchronization engine (2024–present). Analytical exploration and visualization operate directly inside an interactive Jupyter Notebook cockpit (`main.ipynb`) via DuckDB and Plotly, with zero remote server daemons or web framework overhead.

---

## 1. Architectural Highlights

- **Pure Local Execution**: Eliminates cloud datacenter IP throttling, rate-limiting, and CAPTCHA blocks from data providers (Yahoo Finance, St. Louis Fed FRED, Google News). Running directly on a local workstation completes a full incremental sync across all domains in under 20 seconds.
- **Intelligent Cache & State Management**: Before fetching new data, the sync engine queries local Parquet tables using DuckDB to identify the latest existing timestamps (`MAX(date)` / `MAX(timestamp)`). It automatically calculates dynamic date windows with a conservative overlap buffer, backfilling only missing intervals.
- **Idempotent Upsert Deduplication**: Newly ingested records are merged into existing Parquet tables using DuckDB window deduplication (`ROW_NUMBER() OVER (PARTITION BY primary_keys ORDER BY date DESC)`). Historical long-term series are preserved without truncation or data duplication.
- **Zero-Frontend Server Cockpit**: Analytical queries and visual widgets run directly inside Jupyter Notebook (`main.ipynb`) using embedded DuckDB queries and Plotly graphics.
- **Wildcard Zero-Copy Queries**: Historical (FINSPID) and incremental files share unified Parquet schemas, allowing multi-decade scans with a single DuckDB glob pattern (`*.parquet`, `union_by_name = true`).

---

## 2. Directory Structure

```text
data-finance-us/                            # Project Root
├── main.ipynb                              # Primary interactive research cockpit & dashboard
├── market_sync.py                          # Local incremental synchronization engine
├── requirements.txt                        # Python dependencies
├── README.md                               # Project technical documentation
├── sync_state.json                         # Automated sync telemetry and catalog metadata
├── constituents_1999_2023_finspid.parquet # Historical S&P 500 Top 50 constituents & weights (1999–2023)
├── constituents_current.parquet           # 2024~present Top 50 constituents & live market caps
├── data_analyze/                           # Analytical notebooks & research documentation
│   ├── extrema_local_offline.md            # Local extrema detection documentation
│   └── extrema_ndx.ipynb                   # NDX extrema analysis notebook
├── data_scripts/                           # Computational & helper modules
│   ├── extrema_local_offline.py            # Ground-truth extrema labeling script
│   ├── finspid_diet.py                     # Historical FINSPID archive filtration
│   ├── portfolio_calc.py                   # Value-weighted performance calculation engine
│   └── viz_render.py                       # Plotly & tabular rendering helpers
├── yfinance/                               # Market price series & benchmark indices
│   ├── indices.parquet                     # Market benchmark indices (^GSPC, ^NDX, ^DJI)
│   ├── prices_1999_2023_finspid.parquet    # Historical daily OHLCV for Top 50 universe + SPY (1999–2023)
│   └── prices_incremental.parquet          # Incremental daily OHLCV (2024~present)
├── fred/                                   # Macroeconomic indicators
│   └── indicators.parquet                  # Consolidated macro series (FEDFUNDS, CPI, 10Y Yield, etc.)
└── news/                                   # Financial news feeds
    ├── news_1999_2010_finspid.parquet      # Curated legacy FINSPID macro news (2007–2010)
    └── news_incremental.parquet            # Institutional news stream (2011~present)
```

---

## 3. Schema Definitions

### 3.1 Constituents (`constituents_*.parquet`)
- `date`: DATE (Rebalancing or effective date)
- `ticker`: VARCHAR (Equity symbol)
- `market_cap`: DOUBLE (Market capitalization in USD)
- `rank`: INTEGER (Market cap ranking, 1 to 50)
- `weight`: DOUBLE (Value-weighted portfolio share, sum = 1.0)

### 3.2 Price Series (`yfinance/*.parquet`)
- `date`: DATE (Trading session date)
- `ticker`: VARCHAR (Equity symbol)
- `open`: DOUBLE
- `high`: DOUBLE
- `low`: DOUBLE
- `close`: DOUBLE
- `adj_close`: DOUBLE
- `volume`: BIGINT
- `market_cap`: DOUBLE (Market capitalization in USD; populated for incremental records)

### 3.3 Macroeconomic Indicators (`fred/indicators.parquet`)
- `date`: DATE (Observation or release date)
- `series_id`: VARCHAR (FRED series identifier: FEDFUNDS, CPIAUCSL, DGS10, T10Y2Y, etc.)
- `value`: DOUBLE (Numeric index or rate value)
- `category`: VARCHAR (credit, inflation, growth, recession, market)
- `name`: VARCHAR (Descriptive series name)

### 3.4 Top-Down News Stream (`news/*.parquet`)
- `timestamp`: TIMESTAMP (Article publication date, normalized to 00:00:00 UTC)
- `source`: VARCHAR (Verified wire/outlet: Reuters, CNBC, Federal Reserve, WSJ, etc.)
- `category`: VARCHAR (macro, market)
- `tags`: VARCHAR[] (Topic flags: FED, INFLATION, YIELD, JOBS, ECONOMY, etc.)
- `headline`: VARCHAR (Article title)
- `summary`: VARCHAR (Lead article summary, max 300 chars, distinct from headline)

---

## 4. Cache & Incremental Synchronization

The synchronization engine in `market_sync.py` manages incremental data collection with native caching.

```text
[Local Synchronization Engine: market_sync.py]
  │
  ├── 1. FRED Macro Series
  │      Inspects fred/indicators.parquet MAX(date)
  │      Fetches (max_date - 14d) ~ present via official FRED REST API (ThreadPoolExecutor, 8 workers)
  │      Deduplicates and merges via DuckDB into indicators.parquet (~2-3 sec)
  │
  ├── 2. Equities & Constituents
  │      Fast-Path: checks constituents_current.parquet
  │      Updates market caps for current universe (~50 tickers) concurrently
  │      Inspects yfinance/prices_incremental.parquet MAX(date)
  │      Batch downloads (max_date - 3d) ~ present via yfinance
  │      Deduplicates and merges via DuckDB (~4-5 sec)
  │
  ├── 3. Market Benchmark Indices
  │      Inspects yfinance/indices.parquet MAX(date)
  │      Fetches (max_date - 3d) ~ present for ^GSPC, ^NDX, ^DJI
  │      Deduplicates and merges via DuckDB into indices.parquet (~2 sec)
  │
  ├── 4. Top-Down News Stream
  │      Inspects news/news_incremental.parquet MAX(timestamp)
  │      Queries Google News date-windows, Yahoo Finance search, and RSS feeds
  │      Filters invalid summaries and non-English text
  │      Deduplicates by headline and merges via DuckDB (~5-7 sec)
  │
  └── 5. State Telemetry
         Audits total row counts, date bounds, and table metrics
         Writes fresh catalog to sync_state.json
```

### Execution Benchmarks (Local Workstation)
- **FRED Macro Sync**: ~2.5 seconds
- **Equities Ingestion**: ~4.5 seconds
- **Indices Ingestion**: ~2.0 seconds
- **News Feed Sync**: ~6.5 seconds
- **Total Pipeline Execution**: ~15–18 seconds

---

## 5. Usage & CLI Options

### 5.1 Prerequisites & Installation

```bash
# Clone the repository
git clone https://github.com/primenumbersam/data-finance-us.git
cd data-finance-us

# Create and activate Python virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 5.2 Environment Variables (Optional)

Create a `.env` file at the repository root if you wish to configure a custom FRED API key:

```ini
FRED_API_KEY="your_fred_api_key_here"
```

*(If omitted, the engine uses the built-in project key).*

### 5.3 Running Market Sync

Execute standard daily incremental sync (fast-path, resumes from existing cache):
```bash
python market_sync.py
```

Optional execution flags:
```bash
# Full re-scan of all 503 S&P 500 constituents to re-rank weights (e.g. quarterly)
python market_sync.py --full-scan

# Skip specific domains during targeted testing
python market_sync.py --skip-news
python market_sync.py --skip-macro
python market_sync.py --skip-equities
python market_sync.py --skip-indices

# Custom historical news backfill from a specific date
python market_sync.py --backfill-from 2026-01-01
```

---

## 6. Interactive Research Cockpit (`main.ipynb`)

Launch JupyterLab to interact with the data lakehouse:

```bash
jupyter lab main.ipynb
```

### 6.1 Embedded DuckDB Setup
DuckDB queries all historical and incremental Parquet partitions with zero-copy speed:

```python
import duckdb

con = duckdb.connect()

# Full prices (1999–present)
con.execute("CREATE VIEW v_prices AS SELECT * FROM read_parquet('yfinance/*.parquet', union_by_name = true)")

# Benchmark indices
con.execute("CREATE VIEW v_indices AS SELECT * FROM read_parquet('yfinance/indices.parquet')")

# Macroeconomic indicators
con.execute("CREATE VIEW v_macro AS SELECT * FROM read_parquet('fred/indicators.parquet')")

# Full news stream (2007–present)
con.execute("CREATE VIEW v_news AS SELECT * FROM read_parquet('news/*.parquet', union_by_name = true)")

# Constituents & weights
con.execute("CREATE VIEW v_constituents AS SELECT * FROM read_parquet('constituents_*.parquet', union_by_name = true)")
```

### 6.2 Sequential Dashboard Views
1. **View 1 (Health Console)**: Validates `sync_state.json`, checks for missing trading sessions, null counts, and partition sizes.
2. **View 2 (Macro Context)**: Overlays Federal Reserve monetary policy rates (FEDFUNDS) and yield curve spreads (T10Y2Y) with market index turning points.
3. **View 3 (Constituent Churn & Treemap)**: Renders a Plotly market-cap treemap for any selected rebalancing date and audits member entries and exits.
4. **View 4 (Strategy Evaluation)**: Computes the 50-stock capitalization-weighted cumulative NAV curve against SPY, including annualized Alpha, Sharpe ratio, and Maximum Drawdown (MDD) underwater analysis.
