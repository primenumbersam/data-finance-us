# data-finance-us

This document specifies the technical design for a zero-cost financial data lakehouse (`data-finance-us`). The system leverages Kaggle free-tier infrastructure (Scheduled Notebooks and Private Datasets) to maintain a lightweight historical archive from FINSPID (1999–2023) and execute daily incremental ingestions (2024–present) for S&P 500 Top 50 equities, macro indicators, and top-down market news. Web dashboard frameworks (FastAPI, HTMX) are replaced by an interactive, zero-overhead Jupyter Notebook environment using DuckDB and Plotly.

---

## 0. Prerequisites: Kaggle Secrets Configuration

Before deploying scheduled notebooks on Kaggle, register the following credentials under `Add-ons` -> `Secrets` in the Kaggle Notebook interface:

1. `KAGGLE_API_TOKEN`: Kaggle API token (current standard, starting with `KGAT_`).
2. `KAGGLE_USERNAME`: Kaggle account username (for dataset path resolution).
3. `FRED_API_KEY`: St. Louis Fed FRED API key for macroeconomic series.

Runtime Environment Injection Snippet (run at notebook start):
```python
import os
from kaggle_secrets import UserSecretsClient

secrets = UserSecretsClient()
os.environ["KAGGLE_API_TOKEN"] = secrets.get_secret("KAGGLE_API_TOKEN")
os.environ["KAGGLE_USERNAME"] = secrets.get_secret("KAGGLE_USERNAME")
os.environ["FRED_API_KEY"] = secrets.get_secret("FRED_API_KEY")
```

---

## 1. Core Principles & Naming Conventions

### 1.1 Architectural Principles
- Zero-Frontend Server: Eliminates web server daemon and REST endpoint maintenance. Analytical queries and visual widgets run directly inside Jupyter Notebook (`main.ipynb`) via DuckDB and Plotly.
- Zero-Cost Free-Tier Maximization: Kaggle Scheduled Notebooks provide daily execution runtimes; Kaggle Private Datasets store versioned Parquet data (up to 20GB–100GB free), avoiding Git LFS quotas and CI minute depletion on GitHub/GitLab.
- Clean Workspace Organization: All computational and ingestion scripts are grouped under `data_scripts/`, data tables reside under root lakehouse layout, and `main.ipynb` sits at the repository root as the immediate interactive entry point.
- Wildcard Query Optimization: Historical (FINSPID) and incremental files coexist inside domain folders, enabling unified full-history scans with a single DuckDB glob pattern (`*.parquet`).
- Root Master Placement: Portfolio benchmark constituents and global sync state (`sync_state.json`) reside at the data lakehouse root for immediate discoverability.
- Separation of Logic and Presentation: Financial mathematics (`data_scripts/portfolio_calc.py`) and visualization rendering (`data_scripts/viz_render.py`) are decoupled from notebook cells.

### 1.2 Naming Conventions ({what}_{action})
All Python scripts adhere to `{domain/target}_{action/role}` snake_case and reside under `data_scripts/`:

- `data_scripts/finspid_diet.py`: Filters historical FINSPID (1999–2023) archives to S&P 500 Top 50 universe and curated Bloomberg/Reuters macro news.
- `data_scripts/market_sync.py`: Ingests daily incremental OHLCV (yfinance), economic indicators (FRED), and top-down news feeds (backfilling from 2011~present).
- `data_scripts/portfolio_calc.py`: Pure computational engine for value-weighted weights, daily returns, NAV, MDD, and turnover.
- `data_scripts/viz_render.py`: Plotly and Pandas Styler rendering routines.
- `data_scripts/extrema_local_offline.py`: Ground-truth extrema labeling routine for market benchmark indices.
- `main.ipynb`: Primary interactive research and validation notebook at the project root.
- Target Lakehouse & Dataset Identifier: `data-finance-us`

---

## 2. Project Directory Structure

```text
data-finance-us/                            # Project Root
├── main.ipynb                              # Primary interactive research frontend & dashboard
├── requirements.txt                        # Python dependencies
├── README.md                               # Project documentation
├── constituents_1999_2023_finspid.parquet # Historical S&P 500 Top 50 constituents & weights
├── constituents_current.parquet           # 2024~present Top 50 constituents & latest market caps
├── sync_state.json                        # Last synchronization timestamps and schema metadata
├── data_analyze/                           # Analytical notebooks & research documentation
│   ├── extrema_local_offline.md            # Local extrema detection documentation
│   └── extrema_ndx.ipynb                   # NDX extrema analysis notebook
├── data_scripts/                           # Python operational & utility scripts
│   ├── dataset_publish.py                  # Automated Kaggle Dataset publisher (kagglehub API)
│   ├── extrema_local_offline.py            # Local extrema labeling script
│   ├── finspid_diet.py                     # Historical archive filtration
│   ├── market_sync.py                      # Incremental daily ETL & news backfill engine
│   ├── portfolio_calc.py                   # Value-weighted performance math engine
│   └── viz_render.py                       # Plotly & table rendering helpers
├── yfinance/                               # Market price series & indices
│   ├── indices.parquet                    # Primary market benchmark indices (^GSPC, ^NDX, ^DJI)
│   ├── prices_1999_2023_finspid.parquet   # Historical daily OHLCV for Top 50 universe + SPY
│   └── prices_incremental.parquet         # Incremental daily OHLCV (2024~present)
├── fred/                                   # Macroeconomic indicators
│   └── indicators.parquet                 # Macro series (FEDFUNDS, CPI, 10Y Yield, etc.)
└── news/                                   # Financial news feed
    ├── news_1999_2010_finspid.parquet     # Curated legacy FINSPID macro news (2007–2010)
    └── news_incremental.parquet           # High-provenance institutional news stream (2011~present)
```

---

## 3. Schema Definitions

### 3.1 constituents (Lakehouse Root)
- `date`: DATE (Rebalancing or effective date)
- `ticker`: VARCHAR (Equity symbol)
- `market_cap`: DOUBLE (Market capitalization in USD)
- `rank`: INTEGER (Market cap ranking, 1 to 50)
- `weight`: DOUBLE (Value-weighted portfolio share, sum = 1.0)

### 3.2 yfinance (OHLCV Price Series)
- `date`: DATE (Trading session date)
- `ticker`: VARCHAR (Equity symbol)
- `open`: DOUBLE
- `high`: DOUBLE
- `low`: DOUBLE
- `close`: DOUBLE
- `adj_close`: DOUBLE
- `volume`: BIGINT
- `market_cap`: DOUBLE (Market capitalization in USD; populated for 2024+ incremental updates via yfinance, NULL for historical FINSPID)

### 3.3 fred (Macroeconomic Indicators)
- `date`: DATE (Observation or release date)
- `series_id`: VARCHAR (FRED series identifier: FEDFUNDS, CPIAUCSL, DGS10, etc.)
- `value`: DOUBLE (Numeric index or percentage value)

### 3.4 news (Top-Down Textual Stream)
Focuses exclusively on macro regimes and broad market movements. Single-ticker micro headlines are filtered out, and sector classifications are consolidated directly into market.
- `timestamp`: TIMESTAMP (Article date normalized to 00:00:00 UTC)
- `source`: VARCHAR (`Unknown` (71.5%), retail contributor networks (Seeking Alpha, Zacks, GuruFocus), official Fed RSS, Reuters, CNBC, etc.)
- `category`: VARCHAR (macro, market)
- `tags`: VARCHAR[] (Whitelist tags: FED, MACRO, TOP/MKT, Economy, Central Banks)
- `headline`: VARCHAR (Article title)
- `summary`: VARCHAR (Lead 2–3 sentences)

### 3.5 News Stream Diagnostics & Balancing Specification

#### 3.5.1 Dataset Diagnostics & Known Biases
1. Category Consolidation:
   - Sector category is merged into `market` to eliminate category sparsity.
   - Post-consolidation distribution:
     - FINSPID Legacy Archive (2007–2010): Macro 82.74% (8,645 articles), Market 17.26% (1,803 articles) across 10,448 articles.
     - Institutional Backfilled Stream (2011~present): Macro 55.84% (14,649 articles), Market 44.16% (11,585 articles) across 26,234 verified articles.
2. Timestamp Normalization & Monotonicity:
   - All article timestamps are unified to day-level resolution (`00:00:00`) across historical archives and daily incremental pipelines.
   - This prevents intraday timestamp inversions, timezone misalignment, and ordering artifacts when merging historical daily feeds with incremental streaming feeds.
3. Source Provenance & Quality Bias:
   - High unverified rate in FINSPID: 71.50% (31,682 articles) in historical FINSPID are labeled as `Unknown`, precluding verification against primary tier-1 financial wires (Bloomberg, Reuters).
   - Retail contributor skew in FINSPID: Identified historical publishers skew heavily toward contributor/blog platforms—Seeking Alpha (6.04%), Zacks (2.26%), GuruFocus (1.30%)—which exhibit lower institutional rigour for macro regime detection.
   - Institutional authority in 2011~present backfill: Over 2,641 verified sources without provenance deficits, anchored by CNBC (5.04%), Reuters (4.95%), The New York Times (4.04%), Federal Reserve regional banks (St. Louis 2.99%, Minneapolis 2.08%, Dallas 1.72%, SF 1.44%, Kansas City 1.40%), WSJ (2.07%), PBS (1.99%), and Brookings (1.67%).
   - Non-English purging: All non-English articles (8 Russian articles in FINSPID, including non-financial sports and ICANN domain notices) have been purged from the dataset.
4. Temporal Discontinuity & Segmentation:
   - 1999–2006 archival void: Raw FINSPID contained only 1 record in 2001, 1 in 2004, and 4 in 2005 (all non-English noise). After non-English elimination, continuous clean English news coverage begins on 2007-01-01.
   - 2007–2010 residual FINSPID legacy: Maintained as an auxiliary index for pre-2011 historical turning points.
   - 2011–present institutional news replacement: Due to FINSPID's high provenance deficits (71.5% Unknown), Google News 14-day date-window queries backfill authoritative institutional news (Reuters, Fed RSS, WSJ, CNBC) starting from 2011-01-01, replacing low-quality FINSPID scrapings for the modern quantitative backtest regime.
5. Summary Field Deficit & Asymmetry (Headline Duplication):
   - FINSPID Legacy Archive (2007–2010): 97.70% (10,208 / 10,448 articles) lacked abstractive summaries in the raw dataset (`Lsa_summary` null), causing the ETL ingestion pipeline to fall back on copying the headline into the summary column. The legacy archive functions essentially as a headline-only event marker.
   - Institutional Backfilled Stream (2011~present): 100% of retained articles (26,234 / 26,234 articles) maintain verified, distinct lead summaries (up to 300 characters from RSS and Google News feeds), with 0% headline-summary identity. Articles lacking independent summary text (such as unsummarized ticker news from Yahoo Finance) are strictly filtered out at ingestion.

#### 3.5.2 News Balancing & Pipeline Rules
1. Weekly Window Aggregation & Volume Bounds:
   - Aggregations are conducted on weekly windows rather than daily caps.
   - Volume is bounded between a minimum of 10 articles and a maximum of 50 articles per week.
2. Stratified Sampling Ratio:
   - Baseline target distribution maintains Macro ~55–60% and Market ~40–45%, dynamically adjusting to available volume while enforcing the weekly bounds.
3. Deduplication:
   - Complete deduplication by headline (`headline` unique primary key) retains the earliest chronological observation and removes redundant syndication.
4. Backfill Horizon:
   - The ingestion pipeline (`data_scripts/market_sync.py`) defaults to `--backfill-from 2011-01-01`, capturing over 15 years of uninterrupted institutional news coverage.
5. Sentiment Score Decoupling:
   - The `sentiment_score` column is removed from the lakehouse ingestion schema to maintain raw textual purity. Decision model directional signals and confidence scores are computed downstream.
6. Distinct Summary Enforcement:
   - The incremental ingestion pipeline enforces strict summary validation (`is_valid_news_record`). Any article where `summary` is empty, null, or identical/near-identical to `headline` (e.g. normalized character match or trivial headline repeats) is dropped, ensuring zero redundancy in downstream NLP and sentiment modeling.

#### 3.5.3 Dataset Utility & Methodological Assessment
1. Quantitative Data Reliability:
   - Historical S&P 500 Top 50 constituents ([constituents_1999_2023_finspid.parquet](file:///home/sam/github/data-finance-us/constituents_1999_2023_finspid.parquet)) and price series ([yfinance/prices_1999_2023_finspid.parquet](file:///home/sam/github/data-finance-us/yfinance/prices_1999_2023_finspid.parquet)) maintain high institutional validity and effectively protect backtests against survivorship bias.
2. Textual Stream Downgrade to Auxiliary Index:
   - Due to provenance deficits (`Unknown` 71.50%) and contributor biases in FINSPID, the historical text stream should not be utilized as a standalone alpha signal or ground-truth sentiment driver.
   - It is appropriately downgraded to an auxiliary narrative timeline for auditing market context around major historical turning points.
3. Downstream Decision Modeling Anchor:
   - Future proprietary decision and sentiment models should anchor predominantly on 2024+ incremental feeds—prioritizing official Federal Reserve releases and verified primary financial news streams over uncurated historical web scrapings.

---

## 4. Backend ETL Pipeline Modules

### 4.1 data_scripts/finspid_diet.py
- Input: Raw FINSPID archive.
- Universe Filtering: Reconstructs point-in-time S&P 500 members for each rebalancing interval (1999–2023), computes market cap rank, and isolates the cumulative union of Top 50 constituents (~250–350 unique symbols).
- Text Filtering: Drops company-specific rumor feeds. Retains Bloomberg and Reuters articles tagged with macroeconomic or sector-wide keywords. Extracts headlines and lead summaries.
- Outputs:
  - `constituents_1999_2023_finspid.parquet`
  - `yfinance/prices_1999_2023_finspid.parquet`
  - `news/news_1999_2010_finspid.parquet`

### 4.2 data_scripts/market_sync.py
- Runtime: Kaggle Scheduled Notebook or local CLI runner.
- State Detection: Inspects `sync_state.json` to identify the latest ingested date per source.
- Extraction Logic:
  - Equities (Buffer Universe Optimization): Avoids querying all 503 S&P 500 tickers daily. Instead, queries a dynamic buffer pool (~70 tickers) consisting of the current Top 50 plus ranks 51–70. The baseline universe and market caps for 2024+ are resolved via direct Yahoo Finance (`yfinance`) large-cap market cap queries (`fast_info['market_cap']`). Full universe scans occur only on quarterly rebalancing dates, reducing daily API calls by >85% and preventing Yahoo Finance rate limits.
  - Macro: Queries official FRED REST endpoints for new observations (`fred/indicators.parquet`).
  - Indices: Syncs primary benchmark index daily series (^GSPC, ^NDX, ^DJI) into `yfinance/indices.parquet` for ex-post labeling and market regime modeling.
  - News: Collects macro and market RSS/API feeds from verified financial sources (`news/news_incremental.parquet`), supporting historical backfills from 2011~present via `--backfill-from 2011-01-01`.
- Idempotency: Merges incoming records against existing Parquet tables using unique primary keys (`date + ticker` for prices, `timestamp + headline` for news).

---

## 5. Notebook Frontend Architecture (main.ipynb)

`main.ipynb` acts as the operational and analytical cockpit. It imports mathematical logic from `data_scripts.portfolio_calc` and visualization routines from `data_scripts.viz_render`.

### 5.1 Supporting Modules

#### data_scripts/portfolio_calc.py
- `compute_weights(df_constituents)`: Calculates value weights $w_{i, t} = \frac{MC_{i, t-1}}{\sum MC_{j, t-1}}$ for $i \in \text{Top50}_t$.
- `compute_portfolio_returns(df_prices, df_weights)`: Derives daily portfolio return $R_{p, t} = \sum w_{i, t} R_{i, t}$.
- `compute_performance_metrics(series_returns, benchmark_returns)`: Produces cumulative NAV ($NAV_0 = 100$), Maximum Drawdown (MDD), Alpha vs SPY, annualized Sharpe ratio, and rebalancing turnover rate.

#### data_scripts/viz_render.py
- `render_lakehouse_health(df_summary)`: Displays Parquet partition sizes, date ranges, and null-value audits.
- `render_macro_overlay(df_prices, df_macro, event_dates)`: Plots benchmark prices overlaid with FRED federal funds rates and vertical event markers.
- `render_news_feed(df_news, keyword, category, limit)`: Renders a scrollable table of filtered macro headlines and sentiment indicators.
- `render_performance_dashboard(df_perf)`: Creates dual synchronized Plotly subplots displaying cumulative NAV on top and MDD underwater area below.
- `render_weights_treemap(df_weights, target_date)`: Generates a treemap depicting the Top 50 market-cap distribution on a given date.
- `render_churn_table(df_weights, date_t0, date_t1)`: Formats a color-coded table highlighting portfolio additions (green) and exclusions (red).

### 5.2 Sequential Notebook Dashboard Views

To support methodical validation, `main.ipynb` organizes its views in a logical 1 -> 2 -> 3 -> 4 progression (Data Health -> Macro Context -> Portfolio Constituents & Holdings -> Strategy Performance):

```text
[main.ipynb Execution Flow]
┌─────────────────────────────────────────────────────────────────┐
│ Section 0: DuckDB Initialization & Wildcard View Registrations  │
├─────────────────────────────────────────────────────────────────┤
│ View 1 (Health): Data Lakehouse Health Console                  │
│ - Null-value audit, date continuity checks, storage size tally  │
├─────────────────────────────────────────────────────────────────┤
│ View 2 (Macro): Macro Regime & Top-Down News View               │
│ - Policy rate overlays, FOMC event flags, Bloomberg/Reuters feed│
├─────────────────────────────────────────────────────────────────┤
│ View 3 (Holdings): Constituent Churn & Weight Matrix View       │
│ - Top 50 market cap treemap, member ingress/egress churn table  │
├─────────────────────────────────────────────────────────────────┤
│ View 4 (Strategy): Top 50 Value-Weighted Performance View       │
│ - Cumulative NAV vs SPY, Alpha, Sharpe, interactive MDD subplot │
└─────────────────────────────────────────────────────────────────┘
```

#### View 1: Data Lakehouse Health Console (System Integrity)
- Audits `sync_state.json` and verifies that all Parquet partitions load without schema corruption.
- Runs DuckDB integrity checks for duplicate timestamps, missing business dates, or non-positive volume entries.
- Summarizes total lakehouse disk consumption against Kaggle storage allowances.

#### View 2: Macro Regime & Top-Down News View (Macro Context)
- Cross-references equity index movements with FRED monetary indicators (`v_macro`).
- Marks major Federal Reserve rate adjustment dates directly on the price timeline.
- Offers an interactive text filter across Bloomberg and Reuters articles (`v_news`) categorized by macro, market, and sector tags.

#### View 3: Constituent Churn & Weight Matrix View (Portfolio Composition)
- Accepts a rebalancing date input to display current Top 50 weight allocations via an interactive treemap.
- Calculates portfolio turnover and outputs a styled DataFrame identifying newly entered vs discarded equities.

#### View 4: Top 50 Value-Weighted Performance View (Strategy Evaluation)
- Loads `v_prices` and `v_constituents` via DuckDB to compute the 50-stock capitalization-weighted equity curve.
- Renders an interactive Plotly chart comparing strategy NAV against SPY from 1999 to the present.
- Links an underwater drawdown panel to inspect capital preservation during market crises (2000 Dot-com, 2008 GFC, 2020 COVID).

---

## 6. DuckDB Wildcard Views (Section 0 Setup in main.ipynb)

With identical folder schemas, DuckDB executes zero-copy queries across full historical spans without SQL `UNION` operations:

```python
import duckdb

con = duckdb.connect()

# Full price history (1999~present) via single wildcard with schema union
con.execute("CREATE VIEW v_prices AS SELECT * FROM read_parquet('data-finance-us/yfinance/*.parquet', union_by_name = true)")

# Full curated news history (1999~present) via single wildcard
con.execute("CREATE VIEW v_news AS SELECT * FROM read_parquet('data-finance-us/news/*.parquet', union_by_name = true)")

# Macro indicators and constituents
con.execute("CREATE VIEW v_macro AS SELECT * FROM read_parquet('data-finance-us/fred/indicators.parquet')")
con.execute("CREATE VIEW v_constituents AS SELECT * FROM read_parquet('data-finance-us/constituents_*.parquet', union_by_name = true)")
```

---

## 7. Kaggle Automation Pipeline

```text
[Kaggle Scheduler (Daily at UTC 22:30)]
       │
       ▼ (1) Mount existing dataset
[Kaggle Dataset: data-finance-us-kaggle]
       │
       ▼ (2) Execute incremental ETL (python data_scripts/market_sync.py)
       ├── constituents ──> data-finance-us/constituents_current.parquet
       ├── sync state ────> data-finance-us/sync_state.json
       ├── yfinance ──────> data-finance-us/yfinance/prices_incremental.parquet
       ├── FRED API ──────> data-finance-us/fred/indicators.parquet
       └── News Feed ─────> data-finance-us/news/news_incremental.parquet
       │
       ▼ (3) Verify output tree
[/kaggle/working/data-finance-us]
       │
       ▼ (4) Commit new version (python data_scripts/dataset_publish.py)
[Kaggle Private Dataset (New Version)]
       │
       ▼ (5) Pull to local workstation on demand
[Local .venv: kagglehub.dataset_download("<username>/data-finance-us-kaggle")]
```

1. Execution Schedule: Scheduled daily at 22:30 UTC, allowing complete settlement of US regular market sessions (20:00 UTC / 16:00 EDT).
2. Credential Isolation: `FRED_API_KEY` and Kaggle API credentials reside inside Kaggle Notebook Secrets (`UserSecretsClient`).
3. Fault Tolerance: Failed incremental jobs do not corrupt previous dataset releases, providing automatic rollback stability.

---

## 8. Local Environment Setup

```bash
# Create and activate Python virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install required packages
pip install duckdb pandas pyarrow yfinance requests plotly jupyterlab kaggle
```
