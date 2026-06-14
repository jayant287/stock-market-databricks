# Stock Market Lakehouse on Databricks

## Project Overview

This project is an end-to-end data engineering pipeline built on **Databricks** using the **Medallion Architecture** (Bronze, Silver, Gold layers) to ingest, validate, transform, and serve financial market data for analytical dashboards.

The platform combines two complementary data flows from **Finnhub**, a financial market data provider:

1. **Batch/near-real-time reference and fundamental data** — company profiles, daily quotes, historical price candles, analyst recommendations, financial statements, and valuation metrics — pulled via Finnhub's REST API, orchestrated through **Azure Data Factory**, landed in **Azure Data Lake Storage (ADLS Gen2)**, and processed through Databricks **Spark Declarative Pipelines** (Lakeflow Declarative Pipelines / DLT).
2. **Real-time tick-level trade data** — streamed via the **Finnhub WebSocket API**, published to **Azure Event Hubs** (Kafka-compatible endpoint), and consumed continuously by a Databricks streaming pipeline to produce minute-level aggregated trading metrics.

All datasets are governed centrally through **Unity Catalog**, with every table organized under a single catalog (`stock_market`) and three schemas representing the medallion layers (`bronze`, `silver`, `gold`), plus a dedicated `quarantine` schema for records that fail data quality checks. The final Gold-layer tables — dimensional and fact tables — feed BI dashboards that present stock performance, company fundamentals, valuation ratios, analyst sentiment, and live intraday trading activity.

The project demonstrates several modern lakehouse engineering practices: schema evolution handling with Auto Loader, declarative streaming pipelines, data quality enforcement with quarantine routing, Slowly Changing Dimension (SCD) handling via Auto CDC flows, structured streaming with watermarking for real-time aggregation, and SQL-based materialized views for analytics-ready Gold tables.

---

## Data Source: Finnhub

[Finnhub](https://finnhub.io) is a financial data API provider offering real-time and historical stock market data. This project consumes the following Finnhub endpoints/streams:

| Finnhub Data | Description | Ingestion Method |
|---|---|---|
| **Company Profile** | Static reference data about a company — name, ticker, exchange, industry, market capitalization, shares outstanding, IPO date, country, currency | REST API → ADF → ADLS (JSON) |
| **Quote** | Daily snapshot of a stock's current price quote — open, high, low, close, previous close, change, percent change | REST API → ADF → ADLS (JSON) |
| **Stock Candles** | Historical daily OHLCV (Open, High, Low, Close, Volume) price bars per symbol | REST API → ADF → ADLS (CSV) |
| **Recommendation Trends** | Monthly analyst recommendation counts (Strong Buy, Buy, Hold, Sell, Strong Sell) per stock per period | REST API → ADF → ADLS (JSON) |
| **Financials Reported** | Raw "as-reported" financial statement line items (Balance Sheet, Income Statement, Cash Flow Statement) from SEC filings, by fiscal period | REST API → ADF → ADLS (JSON) |
| **Basic Financial Metrics** | A large set of valuation, profitability, growth, and risk metrics (P/E, P/B, ROE, ROA, Beta, margins, 52-week highs/lows, etc.), available as a current snapshot and as annual/quarterly time series | REST API → ADF → ADLS (JSON) |
| **Trades (WebSocket)** | Live tick-by-tick trade events — symbol, trade price, volume, trade conditions, and timestamp — pushed in real time | WebSocket → Azure Event Hub (Kafka) → Databricks Structured Streaming |

The watchlist of symbols used for real-time trade streaming includes major large-cap stocks such as AAPL, MSFT, GOOGL, AMZN, TSLA, NVDA, META, NFLX, JPM, BAC, GS, WMT, KO, DIS, CTSH, and JNJ.

---

## Architecture Diagram

The diagram below describes the full flow of data through the platform, from ingestion to dashboards.

<img width="1553" height="748" alt="image" src="https://github.com/user-attachments/assets/229ac788-40f6-4179-96cc-fa8f2edf6f4c" />


### Architecture Narrative

1. **Ingestion**: Five Finnhub REST datasets (quote, recommendation, financials-reported, profile, metric) are orchestrated by Azure Data Factory on a schedule and landed as files in an ADLS Gen2 "destination" container. A separate `stock_candles` dataset is also landed directly in ADLS as CSV files. In parallel, a Python WebSocket client (`quote_eventhub_stream.py`) subscribes to Finnhub's live trade stream and republishes each trade event to an Azure Event Hub, which exposes a Kafka-compatible endpoint.
2. **Bronze layer**: Databricks Spark Declarative Pipelines use **Auto Loader** (`cloudFiles` format) to incrementally and idempotently load new files from ADLS into Bronze Delta tables, preserving the raw structure and adding ingestion metadata (file name, file size, file path, ingest timestamp). The real-time trade stream is read directly from Event Hubs via the Kafka connector into a Bronze Delta table.
3. **Silver layer**: Each Bronze dataset passes through a validation step driven by a centralized **data quality rules file** (`dq_rules.json`). Records are flattened/normalized (e.g., exploding nested arrays for financials and recommendations, unpivoting metric time series), type-cast, and enriched. Records failing **critical** rules are routed to **Quarantine** tables; records passing validation flow into Silver tables. Several Silver tables use **Auto CDC flows (`create_auto_cdc_flow`)** to manage Slowly Changing Dimensions (SCD Type 1 or Type 2).
4. **Gold layer**: Gold tables are built as **materialized views** (SQL) or batch/streaming tables (PySpark) on top of Silver, applying business logic — financial ratio calculations, technical indicators, analyst consensus scoring, market cap segmentation, and real-time minute-level OHLCV aggregations with anomaly detection.
5. **Quarantine**: A dedicated schema captures records that fail critical data quality expectations, along with the specific reason(s) for failure, enabling data quality monitoring and remediation without blocking the pipeline.
6. **Governance**: **Unity Catalog** governs all tables across Bronze, Silver, Gold, and Quarantine under the `stock_market` catalog, providing centralized access control, lineage, and discovery.
7. **Consumption**: Gold-layer fact and dimension tables are consumed by BI dashboards to present company fundamentals, daily and intraday price action, valuation ratios, analyst sentiment, and live trading activity.

---

## Catalog & Schema Organization

All tables live under a single Unity Catalog catalog named **`stock_market`**, organized into the following schemas:

| Schema | Purpose |
|---|---|
| `bronze` | Raw, ingested data with minimal transformation, schema-on-read, enriched with ingestion metadata |
| `silver` | Cleansed, validated, deduplicated, conformed, and (where applicable) SCD-managed data |
| `quarantine` | Records that failed critical data quality rules during Silver processing, with failure reasons |
| `gold` | Business-ready dimensional and fact tables, aggregates, and derived metrics consumed by dashboards |

---

## Pipeline Modules

The repository is organized into independent pipeline modules, each corresponding to a Finnhub data domain and following the Bronze → Silver → Gold pattern:

| Module | Source Data | Ingestion Pattern |
|---|---|---|
| **Stock Market Quote** | Daily quote snapshots (JSON) | Auto Loader (batch files from ADLS) |
| **Stock Market Quote Eventhub** | Live tick trades (Kafka/Event Hub) | Structured Streaming (Kafka connector) |
| **Stock Market Candles** | Historical daily OHLCV (CSV) | Auto Loader (batch files from ADLS) with SCD Type 1 CDC |
| **Stock Market Financials** | As-reported financial statements (JSON) | Auto Loader with SCD Type 1 CDC |
| **Stock Market Metric** | Valuation/fundamental metrics (snapshot, annual series, quarterly series) (JSON) | Auto Loader, snapshot table + SCD Type 1 CDC for time series |
| **Stock Market Profile Ingestion** | Company profile/reference data (JSON) | Auto Loader with SCD Type 2 CDC |
| **Stock Market Recommendation** | Analyst recommendation trends (JSON) | Auto Loader with SCD Type 2 CDC |

A shared `dq_rules/dq_rules.json` file centralizes the **critical** (hard-fail, quarantine-triggering) and **warn** (soft, logged-only) data quality expectations for every dataset, ensuring consistent validation logic is applied across all Silver-layer transformations.

---

## Bronze Layer Design

The Bronze layer captures source data as close to its raw form as possible, with light enrichment for lineage and auditability. Every Bronze table (except the Event Hub stream) is populated using **Databricks Auto Loader** with `schemaEvolutionMode = "rescue"`, allowing the pipeline to tolerate upstream schema changes without failing, and adds the following common metadata columns:

| Column | Description |
|---|---|
| `file_name` | Name of the source file that produced the row |
| `file_size` | Size in bytes of the source file |
| `file_path` | Full ADLS path of the source file |
| `ingest_timestamp` | Timestamp at which the row was ingested into Bronze |

### Bronze Tables

| Table | Source Format | Description |
|---|---|---|
| `bronze.bronze_quote` | JSON (Auto Loader) | Raw daily quote payloads per symbol (open, high, low, close, previous close, change, percent change, quote timestamp) plus file metadata |
| `bronze.bronze_quote_eh` | Kafka (Event Hubs) | Raw live trade events consumed from the Event Hub via the Kafka protocol; key and value byte arrays are cast to strings (`key_str`, `value_str`) for downstream JSON parsing |
| `bronze.bronze_stock_candles` | CSV (Auto Loader) | Raw historical daily OHLCV candle data per symbol plus file metadata |
| `bronze.bronze_financials_reported` | JSON (Auto Loader, explicit schema) | Raw "as-reported" SEC financial filings per symbol, containing nested arrays for Balance Sheet (`bs`), Income Statement (`ic`), and Cash Flow Statement (`cf`) line items, organized by fiscal year/quarter |
| `bronze.bronze_metric` | JSON (Auto Loader, explicit schema) | Raw fundamental metrics payload per symbol, containing a flat `metric` snapshot object and a nested `series` object with `annual` and `quarterly` time series for dozens of valuation/profitability/growth ratios |
| `bronze.bronze_profile` | JSON (Auto Loader) | Raw company profile/reference attributes per symbol (name, ticker, exchange, industry, country, currency, IPO date, market cap, shares outstanding, website, phone) |
| `bronze.bronze_recommendation` | JSON (Auto Loader) | Raw analyst recommendation trend data per symbol, containing an array of monthly recommendation counts (`buy`, `sell`, `hold`, `strongBuy`, `strongSell`) |

All file-based Bronze tables read from ADLS Gen2 paths under the `destination` container (e.g., `abfss://destination@.../quote`, `.../stock_candles`, `.../financials_reported`, `.../metric`, `.../profile`, `.../recommendation`), with Auto Loader checkpoint and schema location metadata stored under the `bronze` container.

---

## Silver Layer Design

The Silver layer is responsible for **cleansing, validating, flattening, type-casting, deduplicating, and conforming** Bronze data into analytics-friendly structures. A consistent validation pattern is applied across nearly every module:

1. Load the centralized `dq_rules.json` data quality rules for the relevant dataset.
2. Build a combined SQL expression representing all **critical** rules; any row that fails one or more critical rules is flagged.
3. Build a `quarantine_reason` (or `critical_reason`) string that lists which specific rule(s) failed, using `concat_ws`.
4. Apply both **critical** and **warn** expectations via `@dp.expect_all` for pipeline-level monitoring of data quality metrics.
5. Split the validated stream: rows passing all critical checks flow to the Silver table; rows failing critical checks flow to the corresponding Quarantine table.
6. For dimension-like or slowly-changing datasets, use `create_auto_cdc_flow` to apply **SCD Type 1** (overwrite) or **SCD Type 2** (full history with `__START_AT` / `__END_AT`) merge semantics.

### Silver Tables

| Table | Type / SCD | Key Transformations |
|---|---|---|
| `silver.silver_quote` | Streaming table, no SCD (append) | Casts raw Finnhub quote fields (`c`, `d`, `dp`, `h`, `l`, `o`, `pc`, `t`) to typed columns (`close_price`, `change`, `percent_change`, `high_price`, `low_price`, `open_price`, `previous_close_price`, `event_timestamp`); converts Unix epoch timestamp to America/New York timezone; rows failing critical rules (e.g., null symbol, non-positive close price, null event timestamp) are quarantined |
| `silver.silver_quote_eh` | Streaming table, no SCD (append), auto-optimized | Parses the Kafka `value_str` JSON payload (symbol, price, volume, conditions array, timestamp) into structured columns; converts millisecond epoch timestamp to a proper timestamp and derives `session_date`; performs a stateful **left join** against the latest `previous_close_price` from `silver_quote` (per symbol, most recent `event_timestamp`) to enrich each trade tick with the prior day's closing price |
| `silver.silver_stock_candles` | Streaming table, **SCD Type 1** (`create_auto_cdc_flow`, keys = `symbol, date`) | Casts OHLCV columns to proper numeric types and date to `date` type; deduplicates and merges by `symbol + date`, keeping the latest version per key based on `ingest_timestamp`; failed records routed to `quarantine.quarantine_stock_candles` |
| `silver.silver_financials_reported` | Streaming table, **SCD Type 1** (keys = `symbol, cik, accessNumber, year, quarter, concept`) | Explodes the nested Balance Sheet, Income Statement, and Cash Flow Statement arrays into a long/narrow format (one row per financial line-item `concept` per filing period); unions the three statement types; casts `value` to double; deduplicates on the natural key; failed records routed to `quarantine.quarantine_financial_reported` |
| `silver.silver_metric_snapshot` | Table (batch overwrite per run) | Flattens the `metric` struct (current-point-in-time valuation/profitability/risk ratios) into top-level columns; converts the snapshot timestamp to America/New York and derives `metric_date`; deduplicates on `symbol + metric_date`; flags rows failing critical rules (e.g., non-positive market cap/enterprise value, unrealistic beta) with `is_critical` and `critical_reason`, but does **not** route to a separate quarantine table — filtering happens downstream in Gold |
| `silver.silver_metric_annual` | Streaming table, **SCD Type 1** (keys = `symbol, period`) | Unpivots (via `stack`) the wide `series.annual` struct — dozens of named annual metric series (e.g., `eps`, `pe`, `roe`, `grossMargin`) — into a long format with `metric_type`, `period`, and `value` columns; flags critical rule failures |
| `silver.silver_metric_quarterly` | Streaming table, **SCD Type 1** (keys = `symbol, period`) | Same unpivot pattern as `silver_metric_annual`, applied to the `series.quarterly` struct, producing `metric_type`, `period`, `value` per symbol |
| `silver.silver_profile` | Streaming table, **SCD Type 2** (keys = `symbol`, history tracked except `ingest_timestamp`) | Casts `marketCapitalization` and `shareOutstanding` to double; renames `symbol_name` to `symbol`; selects core company attributes (country, currency, exchange, industry, IPO date, ticker, web URL, name, estimate currency); tracks full history of changes to company attributes over time via `__START_AT` / `__END_AT` |
| `silver.silver_recommendation` | Streaming table, **SCD Type 2** (keys = `symbol, period`, history tracked except `ingest_timestamp`) | Explodes the array of monthly recommendation records into rows with `period`, `buy`, `sell`, `hold`, `strongBuy`, `strongSell` cast to integers; tracks history of recommendation changes per symbol/period |

### Quarantine Tables

| Table | Populated From | Contents |
|---|---|---|
| `quarantine.quarantine_quote` | Silver Quote validation | Quote rows failing critical rules (null symbol, non-positive close price, missing event timestamp), with `quarantine_reason` |
| `quarantine.quarantine_stock_candles` | Silver Candles validation | Candle rows failing critical rules (null symbol/date, non-positive open/close), with `quarantine_reason` |
| `quarantine.quarantine_financial_reported` | Silver Financials validation | Financial line items failing critical rules (null symbol/cik/accessNumber, invalid year/quarter, null concept/value), with `quarantine_reason` |

---

## Gold Layer Design

The Gold layer applies business logic to Silver data to produce **dimension tables**, **fact tables**, and **derived analytical metrics** ready for dashboard consumption. Gold objects are implemented either as SQL **materialized views** (for batch dimensional/fact logic) or as Spark tables (for batch enrichment and real-time streaming aggregation).

### Gold Tables

| Table | Type | Description |
|---|---|---|
| `gold.gold_dim_company` | Materialized view (dimension) | Company dimension derived from the current (`__END_AT IS NULL`) and valid (`is_critical = False`) records in `silver_profile`; adds a `market_cap_category` classification (Mega cap, Large cap, Mid cap, Small cap, Micro cap) based on market capitalization thresholds; exposes symbol, name, exchange, industry, market cap, and shares outstanding |
| `gold.gold_stock_candles` | Table (batch) | Enriches `silver_stock_candles` by computing `previous_close_price` per symbol using a window function (`lag` over `symbol` ordered by `date`), defaulting to 0 for the first available date per symbol; provides the historical daily price series used both standalone and as an input to the daily quote fact table |
| `gold.gold_fact_daily_quote` | Materialized view (fact) | Combines the latest daily snapshot from `silver_quote` (per session date, based on max `ingest_timestamp`) with historical data from `gold_stock_candles` into a unified daily quote fact; computes `intraday_range` (high − low), `intraday_pct` (intraday range as % of open), `gap_flag` (gap_up / gap_down / no_gap based on open vs. previous close), and `candle_direction` (bullish / bearish); deduplicates to one row per `symbol + session_date` |
| `gold.gold_financial_ratio` | Materialized view (fact) | Pivots the long-format `silver_financials_reported` line items back into a wide format per `symbol/year/quarter/filing`, extracting key concepts (revenue, net income, operating income, EPS basic/diluted, total assets, total debt, cash and equivalents, shareholders' equity, current assets/liabilities, operating cash flow, capex, dividends paid, buybacks) using prioritized concept-name fallbacks via `COALESCE`/`FIRST`; computes derived ratios including **free cash flow**, **debt-to-equity**, **current ratio**, **net margin**, **operating margin**, **FCF margin**, and a human-readable `period_label` (e.g., `FY2024` or `Q2-2024`) |
| `gold.gold_fact_stock_metrics` | Table (batch) | Filters `silver_metric_snapshot` to valid rows (`is_critical = False`) and selects a curated set of valuation, profitability, and risk metrics (P/E TTM, P/B, Beta, P/S TTM, EV/EBITDA TTM, ROE TTM, ROA TTM, 52-week high/low, 13/26/52-week returns, dividend yield, gross/operating/net margins, market capitalization); derives a `risk_category` (High volatility / Above Market / Low Volatility) from Beta thresholds and a composite `profitability_score` weighted from ROE, ROA, net profit margin, and gross margin |
| `gold.gold_fact_analyst_signal` | Materialized view (fact) | Aggregates `silver_recommendation` (filtered to `is_critical = False`) per `symbol/period` into `total_analysts` and a weighted `raw_score` (Strong Buy ×2, Buy ×1, Sell ×−1, Strong Sell ×−2); normalizes this into a `bullish_score` on a 1–5 scale and computes `pct_strong_buy`; derives a `consensus_label` (Strong buy / Buy / Hold / Sell / Strong sell) based on the bullish score |
| `gold.fact_realtime_agg` | Streaming table, auto-optimized | Real-time minute-level OHLCV aggregation from `silver_quote_eh`; applies a 30-second watermark on trade `timestamp` and groups trades into 1-minute tumbling windows per `symbol`, computing `open_price`/`close_price` (first/last trade by time), `high_price`/`low_price` (excluding trades with non-regular conditions per Finnhub's trade condition codes), `volume` (summed), and `snapshot_count`; computes `change`, `percent_change`, `intrawindow_spread`, and `price_drift_pct` relative to the previous day's close; flags `price_anomaly_flag = TRUE` when the absolute price drift exceeds 3% within the window |

### Gold Layer Relationships

| Fact / Dimension | Grain | Key Joins / Dependencies |
|---|---|---|
| `gold_dim_company` | One row per company (`symbol`) | Source dimension referenced by all fact tables via `symbol` |
| `gold_stock_candles` | One row per `symbol` + `date` | Feeds `gold_fact_daily_quote` |
| `gold_fact_daily_quote` | One row per `symbol` + `session_date` | Combines `silver_quote` (intraday latest snapshot) and `gold_stock_candles` (historical) |
| `gold_financial_ratio` | One row per `symbol` + `year` + `quarter` + filing | Derived from pivoted `silver_financials_reported` |
| `gold_fact_stock_metrics` | One row per `symbol` + `metric_date` | Derived from `silver_metric_snapshot` |
| `gold_fact_analyst_signal` | One row per `symbol` + `period` | Derived from `silver_recommendation` |
| `fact_realtime_agg` | One row per `symbol` + 1-minute window | Derived from `silver_quote_eh`, enriched with `previous_close_price` sourced from `silver_quote` |

---

## Real-Time Streaming Component (`quote_eventhub_stream.py`)

This standalone Python script acts as the bridge between Finnhub's live market and the Databricks lakehouse:

- Connects to **Finnhub's WebSocket API** using an API key and subscribes to trade updates for a fixed watchlist of 16 large-cap symbols (AAPL, MSFT, GOOGL, AMZN, TSLA, NVDA, META, NFLX, JPM, BAC, GS, WMT, KO, DIS, CTSH, JNJ).
- For each incoming `trade` message, extracts the relevant fields (`symbol`, `price`, `volume`, `conditions`, `timestamp`) into a normalized JSON payload.
- Batches these normalized trade events and publishes them to an **Azure Event Hub** using the `EventHubProducerClient`, handling batch size limits by flushing and creating new batches as needed.
- The Event Hub's Kafka-compatible endpoint is then consumed directly by the `bronze_quote_eh` Databricks streaming pipeline, completing the real-time path from market tick to Bronze Delta table.

---

## Data Quality Framework (`dq_rules.json`)

A single centralized JSON configuration file defines **critical** and **warn** data quality rules for every dataset in the pipeline (`quote`, `quote_eh`, `stock_candles`, `financials_reported`, `metric_annual`, `metric_quarterly`, `metric_snapshot`, `profile`, `recommendation`). Each rule is expressed as a SQL boolean expression.

| Rule Category | Behavior |
|---|---|
| **Critical** | If any critical rule fails for a row, the row is flagged (`is_quarantined` / `is_critical`) with a descriptive reason and routed to the corresponding Quarantine table (or excluded downstream in Gold for datasets without a dedicated quarantine table) |
| **Warn** | Tracked via Databricks pipeline expectations (`@dp.expect_all`) for monitoring and alerting, but does not block rows from reaching Silver |

Examples of critical rules include: non-null symbols and timestamps, positive prices/market caps, valid fiscal year/quarter ranges, and realistic beta ranges. Examples of warn rules include: high ≥ low/close consistency checks, non-negative volumes, sane IPO dates, and finite metric values.

---

## Dashboards

The Gold layer is designed to power BI dashboards covering:

- **Company fundamentals** — sector, exchange, market cap category, shares outstanding (`gold_dim_company`)
- **Daily and historical price action** — OHLC, previous close, intraday range, gap analysis, candle direction (`gold_fact_daily_quote`, `gold_stock_candles`)
- **Financial health and valuation ratios** — revenue, net income, margins, free cash flow, debt-to-equity, current ratio (`gold_financial_ratio`)
- **Stock valuation and risk metrics** — P/E, P/B, Beta-based risk category, profitability score, dividend yield, multi-period returns (`gold_fact_stock_metrics`)
- **Analyst sentiment** — consensus rating, bullish score, percentage of strong buy recommendations (`gold_fact_analyst_signal`)
- **Live intraday trading activity** — minute-by-minute OHLCV, price drift, and anomaly flags for real-time monitoring (`fact_realtime_agg`)

---

## Technology Stack

| Component | Technology |
|---|---|
| Compute & Orchestration (transformations) | Databricks Spark Declarative Pipelines (Lakeflow Declarative Pipelines) |
| Storage | Delta Lake on Azure Data Lake Storage Gen2 (ADLS Gen2) |
| Batch Ingestion Orchestration | Azure Data Factory |
| File Ingestion Mechanism | Databricks Auto Loader (`cloudFiles`) with schema evolution (`rescue` mode) |
| Real-Time Ingestion | Finnhub WebSocket → Azure Event Hubs (Kafka protocol) → Spark Structured Streaming |
| Governance & Cataloging | Unity Catalog (`stock_market` catalog; `bronze`, `silver`, `gold`, `quarantine` schemas) |
| Data Quality | Centralized JSON rules engine (`dq_rules.json`) + Databricks pipeline expectations |
| Change Management | Auto CDC Flows (SCD Type 1 and Type 2) |
| Data Source | Finnhub Stock API (REST + WebSocket) |
| Language | Python (PySpark) and SQL |
