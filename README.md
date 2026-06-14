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

<img width="1553" height="748" alt="image" src="https://github.com/user-attachments/assets/71ac4b55-bf75-4410-86c2-3b0fb372d93b" />


### Architecture Narrative

1. **Ingestion**: Five Finnhub REST datasets (quote, recommendation, financials-reported, profile, metric) are orchestrated by Azure Data Factory on a schedule and landed as files in an ADLS Gen2 "destination" container. A separate `stock_candles` dataset is also landed directly in ADLS as CSV files. In parallel, a Python WebSocket client (`quote_eventhub_stream.py`) subscribes to Finnhub's live trade stream and republishes each trade event to an Azure Event Hub, which exposes a Kafka-compatible endpoint.
2. **Bronze layer**: Databricks Spark Declarative Pipelines use **Auto Loader** (`cloudFiles` format) to incrementally and idempotently load new files from ADLS into Bronze Delta tables, preserving the raw structure and adding ingestion metadata (file name, file size, file path, ingest timestamp). The real-time trade stream is read directly from Event Hubs via the Kafka connector into a Bronze Delta table.
3. **Silver layer**: Each Bronze dataset passes through a validation step driven by a centralized **data quality rules file** (`dq_rules.json`). Records are flattened/normalized (e.g., exploding nested arrays for financials and recommendations, unpivoting metric time series), type-cast, and enriched. Records failing **critical** rules are routed to **Quarantine** tables; records passing validation flow into Silver tables. Several Silver tables use **Auto CDC flows (`create_auto_cdc_flow`)** to manage Slowly Changing Dimensions (SCD Type 1 or Type 2).
4. **Gold layer**: Gold tables are built as **materialized views** (SQL) or batch/streaming tables (PySpark) on top of Silver, applying business logic — financial ratio calculations, technical indicators, analyst consensus scoring, market cap segmentation, and real-time minute-level OHLCV aggregations with anomaly detection.
5. **Quarantine**: A dedicated schema captures records that fail critical data quality expectations, along with the specific reason(s) for failure, enabling data quality monitoring and remediation without blocking the pipeline.
6. **Governance**: **Unity Catalog** governs all tables across Bronze, Silver, Gold, and Quarantine under the `stock_market` catalog, providing centralized access control, lineage, and discovery.
7. **Consumption**: Gold-layer fact and dimension tables are consumed by BI dashboards to present company fundamentals, daily and intraday price action, valuation ratios, analyst sentiment, and live trading activity.

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

The Bronze layer captures source data as close to its raw form as possible, with light enrichment for lineage and auditability. Every Bronze table (except the Event Hub stream) is populated using **Databricks Auto Loader** with `schemaEvolutionMode = "rescue"`, allowing the pipeline to tolerate upstream schema changes without failing, and adds common metadata columns (`file_name`, `file_size`, `file_path`, `ingest_timestamp`). Any column present in the source JSON/CSV that isn't part of the expected schema is captured in `_rescued_data` rather than causing a pipeline failure.

### `bronze.bronze_quote`

Raw daily quote snapshot per symbol, with all source fields landing as strings (cast to numeric types in Silver).

| Column | Type | Description |
|---|---|---|
| `c` | string | Current/close price |
| `d` | string | Change in price |
| `dp` | string | Percent change |
| `h` | string | Day's high price |
| `l` | string | Day's low price |
| `o` | string | Day's open price |
| `pc` | string | Previous close price |
| `symbol` | string | Stock ticker symbol |
| `t` | string | Quote timestamp (Unix epoch seconds, as string) |
| `timestamp` | string | Ingestion-time timestamp from source payload |
| `_rescued_data` | string | Any fields not matching the expected schema, captured for schema-evolution safety |
| `file_name` | string | Source file name |
| `file_size` | long | Source file size in bytes |
| `file_path` | string | Full ADLS source file path |
| `ingest_timestamp` | timestamp | Time the row was ingested into Bronze |

### `bronze.bronze_quote_eh`

Raw Kafka records consumed from the Azure Event Hub carrying live Finnhub trade ticks.

| Column | Type | Description |
|---|---|---|
| `key` | binary | Kafka message key (raw bytes) |
| `value` | binary | Kafka message value (raw bytes) — JSON trade payload |
| `topic` | string | Kafka/Event Hub topic name |
| `partition` | int | Kafka partition number |
| `offset` | long | Kafka message offset |
| `timestamp` | timestamp | Kafka broker-assigned message timestamp |
| `timestampType` | int | Kafka timestamp type code |
| `key_str` | string | `key` cast to string |
| `value_str` | string | `value` cast to string (JSON trade payload parsed in Silver) |

### `bronze.bronze_stock_candles`

Raw historical daily OHLCV candle rows per symbol, loaded from CSV via Auto Loader (all fields land as strings).

| Column | Type | Description |
|---|---|---|
| `Date` | string | Trading date |
| `Open` | string | Opening price |
| `High` | string | High price for the day |
| `Low` | string | Low price for the day |
| `Close` | string | Closing price |
| `Volume` | string | Shares traded |
| `Symbol` | string | Stock ticker symbol |
| `_rescued_data` | string | Unmatched/extra fields captured for schema-evolution safety |
| `file_name` | string | Source file name |
| `file_size` | long | Source file size in bytes |
| `file_path` | string | Full ADLS source file path |
| `ingest_timestamp` | timestamp | Time the row was ingested into Bronze |

### `bronze.bronze_financials_reported`

Raw "as-reported" SEC filing data per symbol, loaded with an explicit nested schema. Each row represents one symbol's full filing history payload; `data` is an array of one element per filing period, each containing nested Balance Sheet (`bs`), Income Statement (`ic`), and Cash Flow Statement (`cf`) line-item arrays.

| Column | Type | Description |
|---|---|---|
| `cik` | string | SEC Central Index Key for the company |
| `data` | array\<struct\> | One element per filing period; each element contains `acceptedDate`, `accessNumber`, `cik`, `endDate`, `filedDate`, `form`, `quarter`, `startDate`, `symbol`, `year`, and a `report` struct holding `bs`, `cf`, `ic` arrays of `{concept, label, unit, value}` line items |
| `symbol` | string | Stock ticker symbol |
| `file_name` | string | Source file name |
| `file_size` | long | Source file size in bytes |
| `file_path` | string | Full ADLS source file path |
| `ingest_timestamp` | timestamp | Time the row was ingested into Bronze |

### `bronze.bronze_metric`

Raw fundamental metrics payload per symbol, loaded with an explicit nested schema. Contains a flat point-in-time `metric` snapshot struct (over 140 valuation/profitability/risk fields) and a `series` struct with `annual` and `quarterly` sub-structs, each holding dozens of named time-series arrays of `{period, v}` pairs (e.g., `eps`, `pe`, `roe`, `grossMargin`, `currentRatio`).

| Column | Type | Description |
|---|---|---|
| `metric` | struct | Flat point-in-time snapshot of ~145 valuation, profitability, growth, leverage, and risk metrics (e.g., `peTTM`, `pb`, `beta`, `roeTTM`, `52WeekHigh`, `marketCapitalization`, `currentDividendYieldTTM`) |
| `metricType` | string | Metric payload type indicator from Finnhub |
| `series.annual` | struct | Annual time series for ~35 metrics (e.g., `bookValue`, `eps`, `pe`, `roe`, `grossMargin`, `totalDebtToEquity`), each an array of `{period, v}` |
| `series.quarterly` | struct | Quarterly time series for ~38 metrics (similar set to annual, plus TTM variants), each an array of `{period, v}` |
| `symbol` | string | Stock ticker symbol |
| `timestamp` | timestamp | Timestamp of the metric snapshot from the source payload |
| `file_name` | string | Source file name |
| `file_size` | long | Source file size in bytes |
| `file_path` | string | Full ADLS source file path |
| `ingest_timestamp` | timestamp | Time the row was ingested into Bronze |

### `bronze.bronze_profile`

Raw company profile/reference attributes per symbol, with numeric fields landing as strings (cast in Silver).

| Column | Type | Description |
|---|---|---|
| `country` | string | Country of company headquarters |
| `currency` | string | Reporting currency |
| `estimateCurrency` | string | Currency used for analyst estimates |
| `exchange` | string | Listing exchange |
| `finnhubIndustry` | string | Industry classification assigned by Finnhub |
| `ipo` | string | IPO date |
| `logo` | string | URL to company logo image |
| `marketCapitalization` | string | Market capitalization (cast to double in Silver) |
| `name` | string | Company name |
| `phone` | string | Company contact phone number |
| `shareOutstanding` | string | Shares outstanding (cast to double in Silver) |
| `symbol` | string | Stock ticker symbol |
| `ticker` | string | Ticker symbol as reported by Finnhub |
| `timestamp` | string | Timestamp of the profile snapshot from the source payload |
| `weburl` | string | Company website URL |
| `_rescued_data` | string | Unmatched/extra fields captured for schema-evolution safety |
| `file_name` | string | Source file name |
| `file_size` | long | Source file size in bytes |
| `file_path` | string | Full ADLS source file path |
| `ingest_timestamp` | timestamp | Time the row was ingested into Bronze |
| `floatingShare` | string | Floating shares outstanding (not currently propagated to Silver) |

### `bronze.bronze_recommendation`

Raw analyst recommendation trend data per symbol; `data` is an array of monthly recommendation count records.

| Column | Type | Description |
|---|---|---|
| `data` | array\<struct\> | One element per monthly period, each containing `period`, `buy`, `hold`, `sell`, `strongBuy`, `strongSell`, and `symbol` |
| `symbol` | string | Stock ticker symbol |
| `timestamp` | string | Timestamp of the recommendation snapshot from the source payload |
| `file_name` | string | Source file name |
| `file_size` | long | Source file size in bytes |
| `file_path` | string | Full ADLS source file path |
| `ingest_timestamp` | timestamp | Time the row was ingested into Bronze |

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

### `silver.silver_quote`

Streaming table, append-only (no SCD). Casts raw Finnhub quote fields to typed columns and converts the quote timestamp to America/New York time. Rows failing critical rules (null symbol, non-positive close price, null event timestamp) are routed to `quarantine.quarantine_quote`.

| Column | Type | Description |
|---|---|---|
| `symbol` | string | Stock ticker symbol |
| `close_price` | double | Current/close price (from Bronze `c`) |
| `change` | double | Price change vs. previous close (from Bronze `d`) |
| `percent_change` | double | Percent change vs. previous close (from Bronze `dp`) |
| `high_price` | double | Day's high price (from Bronze `h`) |
| `low_price` | double | Day's low price (from Bronze `l`) |
| `open_price` | double | Day's open price (from Bronze `o`) |
| `previous_close_price` | double | Previous close price (from Bronze `pc`) |
| `event_timestamp` | timestamp | Quote timestamp converted from Unix epoch to America/New York time |
| `ingest_timestamp` | timestamp | Time the row was ingested into Bronze |

### `silver.silver_quote_eh`

Streaming table, append-only (no SCD), auto-optimized. Parses the Kafka JSON trade payload and enriches each tick with the previous day's closing price via a left join against the latest `silver_quote` snapshot per symbol.

| Column | Type | Description |
|---|---|---|
| `symbol` | string | Stock ticker symbol |
| `price` | double | Trade price |
| `volume` | long | Trade volume (shares) |
| `conditions` | array\<string\> | Finnhub trade condition codes (e.g., regular trade, auction) |
| `timestamp` | timestamp | Trade timestamp, converted from epoch milliseconds |
| `ingest_timestamp` | timestamp | Kafka ingestion timestamp |
| `session_date` | date | Trading session date, derived from `timestamp` |
| `previous_close_price` | double | Previous day's close price for the symbol, sourced from the latest `silver_quote` record |

### `silver.silver_stock_candles`

Streaming table, **SCD Type 1** (`create_auto_cdc_flow`, keys = `symbol, date`). Casts OHLCV columns to numeric types, deduplicates and merges by `symbol + date` keeping the latest version per key based on `ingest_timestamp`. Failed records are routed to `quarantine.quarantine_stock_candles`.

| Column | Type | Description |
|---|---|---|
| `symbol` | string | Stock ticker symbol |
| `date` | date | Trading date |
| `open` | double | Opening price |
| `high` | double | High price for the day |
| `close` | double | Closing price |
| `low` | double | Low price for the day |
| `volume` | int | Shares traded |
| `ingest_timestamp` | timestamp | Time the row was ingested into Bronze |

### `silver.silver_financials_reported`

Streaming table, **SCD Type 1** (keys = `symbol, cik, accessNumber, year, quarter, concept`). Explodes the nested Balance Sheet, Income Statement, and Cash Flow Statement arrays into a long/narrow format — one row per financial line-item `concept` per filing period — then unions the three statement types, casts `value` to double, and deduplicates on the natural key. Failed records are routed to `quarantine.quarantine_financial_reported`.

| Column | Type | Description |
|---|---|---|
| `symbol` | string | Stock ticker symbol |
| `cik` | string | SEC Central Index Key |
| `accessNumber` | string | SEC filing accession number |
| `year` | long | Fiscal year of the filing |
| `quarter` | long | Fiscal quarter (0 = full year/annual filing, 1-4 = quarterly) |
| `startDate` | string | Reporting period start date |
| `endDate` | string | Reporting period end date |
| `filedDate` | string | Date the filing was submitted to the SEC |
| `concept` | string | XBRL financial line-item concept name (e.g., `us-gaap_Revenues`, `us-gaap_NetIncomeLoss`) |
| `unit` | string | Unit of measure for the value (e.g., USD) |
| `label` | string | Human-readable label for the line item |
| `value` | double | Reported value for the line item |
| `ingest_timestamp` | timestamp | Time the row was ingested into Bronze |

### `silver.silver_metric_annual`

Streaming table, **SCD Type 1** (keys = `symbol, period`). Unpivots (via `stack`) the wide `series.annual` struct — dozens of named annual metric series — into a long format with one row per `symbol + metric_type + period`.

| Column | Type | Description |
|---|---|---|
| `symbol` | string | Stock ticker symbol |
| `metric_type` | string | Name of the annual metric series (e.g., `eps`, `pe`, `roe`, `grossMargin`, `currentRatio`) |
| `period` | string | Fiscal period the value corresponds to (e.g., a fiscal year-end date) |
| `v` | double | Value of the metric for that period |
| `ingest_timestamp` | timestamp | Time the row was ingested into Bronze |
| `is_critical` | boolean | Flag indicating whether the row failed one or more critical data quality rules |
| `critical_reason` | string | Description of which critical rule(s) failed, if any |

### `silver.silver_metric_quarterly`

Streaming table, **SCD Type 1** (keys = `symbol, period`). Same unpivot pattern as `silver_metric_annual`, applied to the `series.quarterly` struct.

| Column | Type | Description |
|---|---|---|
| `symbol` | string | Stock ticker symbol |
| `metric_type` | string | Name of the quarterly metric series (e.g., `epsTTM`, `peTTM`, `roeTTM`, `grossMargin`) |
| `period` | string | Fiscal period the value corresponds to (e.g., a fiscal quarter-end date) |
| `v` | double | Value of the metric for that period |
| `ingest_timestamp` | timestamp | Time the row was ingested into Bronze |
| `is_critical` | boolean | Flag indicating whether the row failed one or more critical data quality rules |
| `critical_reason` | string | Description of which critical rule(s) failed, if any |

### `silver.silver_metric_snapshot`

Table, refreshed per run (deduplicated by `symbol + metric_date`). Flattens the `metric` struct (current point-in-time valuation/profitability/risk ratios) into top-level columns and derives `metric_date` from the snapshot `timestamp`. Rows are flagged with `is_critical`/`critical_reason` but are **not** routed to a separate quarantine table — filtering happens downstream in Gold.

| Column | Type | Description |
|---|---|---|
| `10DayAverageTradingVolume` … `yearToDatePriceReturnDaily` | double | The full set of ~145 flattened point-in-time metric fields from the Bronze `metric` struct (valuation ratios such as `peTTM`, `pb`, `psTTM`; profitability ratios such as `roeTTM`, `roaTTM`, `grossMarginTTM`, `netProfitMarginTTM`; risk/volatility measures such as `beta`; price-return metrics such as `52WeekHigh`, `52WeekLow`, `52WeekPriceReturnDaily`, `13WeekPriceReturnDaily`, `26WeekPriceReturnDaily`; size measures such as `marketCapitalization` and `enterpriseValue`; dividend metrics such as `currentDividendYieldTTM`) |
| `symbol` | string | Stock ticker symbol |
| `metric_date` | date | Date of the metric snapshot, derived from the source `timestamp` (converted to America/New York) |
| `ingest_timestamp` | timestamp | Time the row was ingested into Bronze |
| `is_critical` | boolean | Flag indicating whether the row failed one or more critical data quality rules (e.g., non-positive market cap/enterprise value, unrealistic beta) |
| `critical_reason` | string | Description of which critical rule(s) failed, if any |

### `silver.silver_profile`

Streaming table, **SCD Type 2** (keys = `symbol`, full history tracked via `__START_AT` / `__END_AT`, excluding `ingest_timestamp` from change detection). Casts `marketCapitalization` and `shareOutstanding` to double and selects core company attributes.

| Column | Type | Description |
|---|---|---|
| `country` | string | Country of company headquarters |
| `currency` | string | Reporting currency |
| `exchange` | string | Listing exchange |
| `finnhubIndustry` | string | Industry classification assigned by Finnhub |
| `ipo` | string | IPO date |
| `marketCapitalization` | double | Market capitalization |
| `phone` | string | Company contact phone number |
| `shareOutstanding` | double | Shares outstanding |
| `ticker` | string | Ticker symbol as reported by Finnhub |
| `weburl` | string | Company website URL |
| `name` | string | Company name |
| `symbol` | string | Stock ticker symbol |
| `estimateCurrency` | string | Currency used for analyst estimates |
| `ingest_timestamp` | timestamp | Time the row was ingested into Bronze |
| `is_critical` | boolean | Flag indicating whether the row failed one or more critical data quality rules |
| `critical_reason` | string | Description of which critical rule(s) failed, if any |
| `__START_AT` | timestamp | SCD Type 2 validity start timestamp for this version of the record |
| `__END_AT` | timestamp | SCD Type 2 validity end timestamp for this version of the record (null = current version) |

### `silver.silver_recommendation`

Streaming table, **SCD Type 2** (keys = `symbol, period`, full history tracked via `__START_AT` / `__END_AT`, excluding `ingest_timestamp` from change detection). Explodes the array of monthly recommendation records and casts counts to integers.

| Column | Type | Description |
|---|---|---|
| `symbol` | string | Stock ticker symbol |
| `period` | string | Recommendation period (typically a monthly date) |
| `buy` | int | Number of analysts recommending Buy |
| `sell` | int | Number of analysts recommending Sell |
| `hold` | int | Number of analysts recommending Hold |
| `strongBuy` | int | Number of analysts recommending Strong Buy |
| `strongSell` | int | Number of analysts recommending Strong Sell |
| `ingest_timestamp` | timestamp | Time the row was ingested into Bronze |
| `is_critical` | boolean | Flag indicating whether the row failed one or more critical data quality rules |
| `critical_reason` | string | Description of which critical rule(s) failed, if any |
| `__START_AT` | timestamp | SCD Type 2 validity start timestamp for this version of the record |
| `__END_AT` | timestamp | SCD Type 2 validity end timestamp for this version of the record (null = current version) |

### Quarantine Tables

The `quarantine` schema captures rows that fail one or more **critical** data quality rules during Silver processing. Each quarantine table mirrors the structure of its corresponding Silver table, with the addition of a quarantine flag column and a `quarantine_reason` describing which rule(s) failed.

#### `quarantine.quarantine_quote`

Populated from Silver Quote validation: quote rows failing critical rules (e.g., null symbol, non-positive close price, missing event timestamp).

| Column | Type | Description |
|---|---|---|
| `symbol` | string | Stock ticker symbol |
| `close_price` | double | Current/close price |
| `change` | double | Price change vs. previous close |
| `percent_change` | double | Percent change vs. previous close |
| `high_price` | double | Day's high price |
| `low_price` | double | Day's low price |
| `open_price` | double | Day's open price |
| `previous_close_price` | double | Previous close price |
| `event_timestamp` | timestamp | Quote timestamp (America/New York) |
| `ingest_timestamp` | timestamp | Time the row was ingested into Bronze |
| `is_quarantine` | boolean | Flag indicating the row failed one or more critical rules |
| `quarantine_reason` | string | Description of which critical rule(s) failed |

#### `quarantine.quarantine_stock_candles`

Populated from Silver Candles validation: candle rows failing critical rules (e.g., null symbol/date, non-positive open/close).

| Column | Type | Description |
|---|---|---|
| `symbol` | string | Stock ticker symbol |
| `date` | date | Trading date |
| `open` | double | Opening price |
| `high` | double | High price for the day |
| `close` | double | Closing price |
| `low` | double | Low price for the day |
| `volume` | int | Shares traded |
| `ingest_timestamp` | timestamp | Time the row was ingested into Bronze |
| `is_quarantined` | boolean | Flag indicating the row failed one or more critical rules |
| `quarantine_reason` | string | Description of which critical rule(s) failed |

#### `quarantine.quarantine_financial_reported`

Populated from Silver Financials validation: financial line items failing critical rules (e.g., null symbol/cik/accessNumber, invalid year/quarter, null concept/value).

| Column | Type | Description |
|---|---|---|
| `symbol` | string | Stock ticker symbol |
| `cik` | string | SEC Central Index Key |
| `accessNumber` | string | SEC filing accession number |
| `year` | long | Fiscal year of the filing |
| `quarter` | long | Fiscal quarter (0 = annual, 1-4 = quarterly) |
| `startDate` | string | Reporting period start date |
| `endDate` | string | Reporting period end date |
| `filedDate` | string | Date the filing was submitted to the SEC |
| `concept` | string | XBRL financial line-item concept name |
| `unit` | string | Unit of measure for the value |
| `label` | string | Human-readable label for the line item |
| `value` | double | Reported value for the line item |
| `ingest_timestamp` | timestamp | Time the row was ingested into Bronze |
| `is_quarantined` | boolean | Flag indicating the row failed one or more critical rules |
| `quarantine_reason` | string | Description of which critical rule(s) failed |

---

## Gold Layer Design

The Gold layer applies business logic to Silver data to produce **dimension tables**, **fact tables**, and **derived analytical metrics** ready for dashboard consumption. Gold objects are implemented either as SQL **materialized views** (for batch dimensional/fact logic) or as Spark tables (for batch enrichment and real-time streaming aggregation).

### `gold.gold_dim_company`

Materialized view (dimension). Derived from the current (`__END_AT IS NULL`) and valid (`is_critical = False`) records in `silver_profile`. Adds a `market_cap_category` classification based on market capitalization thresholds (Mega cap ≥ 200B, Large cap ≥ 10B, Mid cap ≥ 2B, Small cap ≥ 300M, else Micro cap).

| Column | Type | Description |
|---|---|---|
| `symbol` | string | Stock ticker symbol |
| `name` | string | Company name |
| `exchange` | string | Listing exchange |
| `industry` | string | Industry classification (from `finnhubIndustry`) |
| `market_cap_mm` | double | Market capitalization, in millions |
| `market_cap_category` | string | Size classification: Mega cap, Large cap, Mid cap, Small cap, or Micro cap |
| `shares_outstanding` | double | Total shares outstanding |

### `gold.gold_stock_candles`

Table (batch). Enriches `silver_stock_candles` by computing `previous_close_price` per symbol using a window function (`lag` over `symbol` ordered by `date`), defaulting to 0 for the first available date per symbol. Provides the historical daily price series used both standalone and as an input to the daily quote fact table.

| Column | Type | Description |
|---|---|---|
| `symbol` | string | Stock ticker symbol |
| `date` | date | Trading date |
| `open` | double | Opening price |
| `high` | double | High price for the day |
| `close` | double | Closing price |
| `low` | double | Low price for the day |
| `volume` | int | Shares traded |
| `ingest_timestamp` | timestamp | Time the source row was ingested into Bronze |
| `previous_close_price` | double | Closing price of the prior trading day for the same symbol (0 for the first record per symbol) |

### `gold.gold_fact_daily_quote`

Materialized view (fact). Combines the latest daily snapshot from `silver_quote` (per session date, based on max `ingest_timestamp`) with historical data from `gold_stock_candles` into a unified daily quote fact, deduplicated to one row per `symbol + session_date`.

| Column | Type | Description |
|---|---|---|
| `symbol` | string | Stock ticker symbol |
| `close_price` | double | Closing price for the session |
| `high_price` | double | High price for the session |
| `low_price` | double | Low price for the session |
| `open_price` | double | Opening price for the session |
| `previous_close_price` | double | Previous session's closing price |
| `session_date` | date | Trading session date |
| `percent_change` | double | Percent change vs. previous close |
| `ingest_timestamp` | timestamp | Time the underlying row was ingested |
| `intraday_range` | double | High price minus low price |
| `intraday_pct` | double | Intraday range as a percentage of the opening price |
| `gap_flag` | string | `gap_up`, `gap_down`, or `no_gap`, based on open vs. previous close (±1% threshold) |
| `candle_direction` | string | `bullish` if close ≥ open, otherwise `bearish` |

### `gold.gold_financial_ratio`

Materialized view (fact). Pivots the long-format `silver_financials_reported` line items back into a wide format per `symbol/year/quarter/filing`, extracting key concepts using prioritized concept-name fallbacks via `COALESCE`/`FIRST`, and computes derived financial health ratios.

| Column | Type | Description |
|---|---|---|
| `symbol` | string | Stock ticker symbol |
| `year` | long | Fiscal year |
| `quarter` | long | Fiscal quarter (0 = annual) |
| `startDate` | string | Reporting period start date |
| `endDate` | string | Reporting period end date |
| `filedDate` | string | Date the filing was submitted |
| `cik` | string | SEC Central Index Key |
| `accessNumber` | string | SEC filing accession number |
| `revenue` | double | Total revenue |
| `net_income` | double | Net income (loss) |
| `operating_income` | double | Operating income (loss) |
| `eps_basic` | double | Basic earnings per share |
| `eps_diluted` | double | Diluted earnings per share |
| `assets` | double | Total assets |
| `total_debt` | double | Total debt (long-term and current) |
| `cash_and_equivalents` | double | Cash and cash equivalents |
| `shareholders_equity` | double | Total shareholders' equity |
| `current_assets` | double | Current assets |
| `current_liabilities` | double | Current liabilities |
| `operating_cash_flow` | double | Net cash provided by/used in operating activities |
| `capex` | double | Capital expenditures (payments for property, plant & equipment) |
| `dividends_paid` | double | Cash dividends paid to common shareholders |
| `buybacks` | double | Cash spent on common stock repurchases |
| `free_cash_flow` | double | Operating cash flow minus absolute capex |
| `debt_to_equity` | double | Total debt divided by shareholders' equity |
| `current_ratio` | double | Current assets divided by current liabilities |
| `net_margin` | double | Net income as a percentage of revenue |
| `operating_margin` | double | Operating income as a percentage of revenue |
| `fcf_margin` | double | Free cash flow as a percentage of revenue |
| `period_label` | string | Human-readable period label, e.g. `FY2024` (annual) or `Q2-2024` (quarterly) |

### `gold.gold_financial_annual_yoy`

Year-over-year growth of key financial metrics, derived from `gold_financial_ratio` annual (quarter = 0) records, comparing each fiscal year to the prior fiscal year for the same symbol.

| Column | Type | Description |
|---|---|---|
| `symbol` | string | Stock ticker symbol |
| `year` | long | Fiscal year |
| `quarter` | long | Fiscal quarter (0 = annual) |
| `period_label` | string | Human-readable period label, e.g. `FY2024` |
| `yoy_growth` | double | Year-over-year percentage growth of the underlying financial metric (e.g., revenue) vs. the same period in the prior year |

### `gold.gold_financial_quarterly_qoq`

Quarter-over-quarter growth of key financial metrics, derived from `gold_financial_ratio` quarterly records, comparing each fiscal quarter to the immediately preceding fiscal quarter for the same symbol.

| Column | Type | Description |
|---|---|---|
| `symbol` | string | Stock ticker symbol |
| `year` | long | Fiscal year |
| `quarter` | long | Fiscal quarter (1-4) |
| `period_label` | string | Human-readable period label, e.g. `Q2-2024` |
| `qoq_growth` | double | Quarter-over-quarter percentage growth of the underlying financial metric vs. the immediately prior quarter |

### `gold.gold_fact_stock_metrics`

Table (batch). Filters `silver_metric_snapshot` to valid rows (`is_critical = False`) and selects a curated set of valuation, profitability, and risk metrics. Derives a `risk_category` from Beta thresholds and a composite `profitability_score`.

| Column | Type | Description |
|---|---|---|
| `symbol` | string | Stock ticker symbol |
| `pe_ttm` | double | Price-to-earnings ratio, trailing twelve months |
| `pb` | double | Price-to-book ratio |
| `beta` | double | Stock beta (volatility relative to the market) |
| `risk_category` | string | `High volatility` (beta ≥ 1.5), `Above Market` (beta ≥ 0.5), or `Low Volatility` (beta < 0.5) |
| `profitability_score` | double | Composite score = (ROE TTM × 0.30) + (ROA TTM × 0.20) + (Net Profit Margin TTM × 0.25) + (Gross Margin TTM × 0.25) |
| `ps_ttm` | double | Price-to-sales ratio, trailing twelve months |
| `ev_ebitda_ttm` | double | Enterprise value to EBITDA ratio, trailing twelve months |
| `roe_ttm` | double | Return on equity, trailing twelve months |
| `roa_ttm` | double | Return on assets, trailing twelve months |
| `week52_high` | double | 52-week high price |
| `week52_low` | double | 52-week low price |
| `return_52w` | double | 52-week price return |
| `return_13w` | double | 13-week price return |
| `return_26w` | double | 26-week price return |
| `dividend_yield` | double | Current dividend yield, trailing twelve months |
| `gross_margin_ttm` | double | Gross margin, trailing twelve months |
| `operating_margin_ttm` | double | Operating margin, trailing twelve months |
| `net_profit_margin_ttm` | double | Net profit margin, trailing twelve months |
| `market_capitalization` | double | Market capitalization |
| `metric_date` | date | Date of the metric snapshot |

### `gold.gold_fact_analyst_signal`

Materialized view (fact). Aggregates `silver_recommendation` (filtered to `is_critical = False`) per `symbol/period` into a weighted analyst consensus score.

| Column | Type | Description |
|---|---|---|
| `symbol` | string | Stock ticker symbol |
| `period` | string | Recommendation period (typically a monthly date) |
| `total_analysts` | int | Sum of all analyst counts (`strongBuy + buy + hold + sell + strongSell`) |
| `raw_score` | int | Weighted score: (Strong Buy × 2) + (Buy × 1) − (Sell × 1) − (Strong Sell × 2) |
| `bullish_score` | double | Normalized score on a 1–5 scale: `3.0 + (raw_score / total_analysts)` |
| `pct_strong_buy` | double | Percentage of analysts rating Strong Buy |
| `consensus_label` | string | `Strong buy` (≥4.5), `Buy` (≥3.5), `Hold` (≥2.5), `Sell` (≥1.5), or `Strong sell` (<1.5), based on `bullish_score` |

### `gold.fact_realtime_agg`

Streaming table, auto-optimized. Real-time minute-level OHLCV aggregation from `silver_quote_eh`. Applies a 30-second watermark on trade `timestamp` and groups trades into 1-minute tumbling windows per `symbol`.

| Column | Type | Description |
|---|---|---|
| `symbol` | string | Stock ticker symbol |
| `session_date` | date | Trading session date (derived from window start) |
| `window_start` | timestamp | Start of the 1-minute aggregation window |
| `window_end` | timestamp | End of the 1-minute aggregation window |
| `open_price` | double | First trade price in the window (by time) |
| `high_price` | double | Highest trade price in the window, excluding trades with non-regular trade conditions |
| `low_price` | double | Lowest trade price in the window, excluding trades with non-regular trade conditions |
| `close_price` | double | Last trade price in the window (by time) |
| `volume` | long | Total volume traded in the window |
| `previous_close_price` | double | Previous trading day's close price for the symbol |
| `snapshot_count` | long | Number of trade ticks aggregated into this window |
| `change` | double | `close_price` minus `previous_close_price` |
| `percent_change` | double | `change` as a percentage of `previous_close_price` |
| `intrawindow_spread` | double | `high_price` minus `low_price` within the window |
| `price_drift_pct` | double | Percentage change of `close_price` vs. `previous_close_price`, rounded to 4 decimals |
| `price_anomaly_flag` | boolean | `TRUE` if the absolute value of `price_drift_pct` exceeds 3% |

### `gold.stock_scorecard`

A consolidated, dashboard-ready scorecard that blends company reference data, daily price action, analyst sentiment, and fundamental metrics into a single ranked view per symbol, with composite scoring and tiering.

| Column | Type | Description |
|---|---|---|
| `symbol` | string | Stock ticker symbol |
| `company_name` | string | Company name (from `gold_dim_company`) |
| `exchange` | string | Listing exchange |
| `industry` | string | Industry classification |
| `market_cap_category` | string | Size classification (Mega/Large/Mid/Small/Micro cap) |
| `close_price` | double | Latest closing price (from `gold_fact_daily_quote`) |
| `percent_change` | double | Latest daily percent change |
| `candle_direction` | string | `bullish` or `bearish` for the latest session |
| `total_analysts` | int | Total number of analysts covering the stock (from `gold_fact_analyst_signal`) |
| `bullish_score` | double | Analyst bullish score (1–5 scale) |
| `consensus_label` | string | Analyst consensus label (Strong buy / Buy / Hold / Sell / Strong sell) |
| `pe_ttm` | double | Price-to-earnings ratio, trailing twelve months |
| `roe_ttm` | double | Return on equity, trailing twelve months |
| `beta` | double | Stock beta |
| `return_52w` | double | 52-week price return |
| `market_capitalization` | double | Market capitalization |
| `metric_date` | date | Date of the underlying metric snapshot |
| `momentum_score` | double | Composite score reflecting recent price momentum (derived from price returns/direction) |
| `quality_score` | double | Composite score reflecting fundamental quality (e.g., profitability, returns) |
| `valuation_score` | double | Composite score reflecting relative valuation (e.g., P/E, P/B, EV/EBITDA) |
| `analyst_score` | double | Composite score reflecting analyst sentiment (derived from `bullish_score`) |
| `composite_score` | double | Overall blended score combining momentum, quality, valuation, and analyst scores |
| `sector_rank` | int | Rank of the stock relative to peers within the same industry/sector, based on `composite_score` |
| `overall_rank` | int | Rank of the stock across the entire universe, based on `composite_score` |
| `investment_tier` | string | Categorical tier label derived from `composite_score` (e.g., top-tier / mid-tier / lower-tier) |
| `is_latest` | boolean | Flag indicating whether this row represents the most recent scorecard snapshot for the symbol |

### Gold Layer Relationships

| Fact / Dimension | Grain | Key Joins / Dependencies |
|---|---|---|
| `gold_dim_company` | One row per company (`symbol`) | Source dimension referenced by all fact tables via `symbol` |
| `gold_stock_candles` | One row per `symbol` + `date` | Feeds `gold_fact_daily_quote` |
| `gold_fact_daily_quote` | One row per `symbol` + `session_date` | Combines `silver_quote` (intraday latest snapshot) and `gold_stock_candles` (historical); feeds `stock_scorecard` |
| `gold_financial_ratio` | One row per `symbol` + `year` + `quarter` + filing | Derived from pivoted `silver_financials_reported`; feeds `gold_financial_annual_yoy` and `gold_financial_quarterly_qoq` |
| `gold_financial_annual_yoy` | One row per `symbol` + fiscal year | Derived from annual records in `gold_financial_ratio` |
| `gold_financial_quarterly_qoq` | One row per `symbol` + fiscal quarter | Derived from quarterly records in `gold_financial_ratio` |
| `gold_fact_stock_metrics` | One row per `symbol` + `metric_date` | Derived from `silver_metric_snapshot`; feeds `stock_scorecard` |
| `gold_fact_analyst_signal` | One row per `symbol` + `period` | Derived from `silver_recommendation`; feeds `stock_scorecard` |
| `fact_realtime_agg` | One row per `symbol` + 1-minute window | Derived from `silver_quote_eh`, enriched with `previous_close_price` sourced from `silver_quote` |
| `stock_scorecard` | One row per `symbol` (latest, plus historical snapshots via `is_latest`) | Consolidates `gold_dim_company`, `gold_fact_daily_quote`, `gold_fact_analyst_signal`, and `gold_fact_stock_metrics`, with composite scoring and ranking applied across the universe |

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
