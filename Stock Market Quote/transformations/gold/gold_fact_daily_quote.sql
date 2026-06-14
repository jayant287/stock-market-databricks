-- Please edit the sample below

CREATE MATERIALIZED VIEW stock_market.gold.gold_fact_daily_quote AS
with stock_latest_snapshot as (
   select *,
          max(ingest_timestamp) over(partition by to_date(ingest_timestamp)) as max_timestamp
from stock_market.silver.silver_quote
qualify ingest_timestamp = max_timestamp
),combined_data as (
      select symbol,
       close_price,
       high_price,
       low_price,
       open_price,
       previous_close_price,
       to_date(event_timestamp) as session_date,
       percent_change,
       ingest_timestamp
 from stock_latest_snapshot
 union all
 select symbol,
       close as close_price,
       high as high_price,
       low as low_price,
       open as open_price,
       previous_close_price,
       date as session_date,
       ((close - previous_close_price)/(previous_close_price)) * 100 as percent_change,
       ingest_timestamp
 from stock_market.gold.gold_stock_candles
)

select *,
       (high_price - low_price) as intraday_range,
       ((high_price - low_price) / open_price) * 100 as intraday_pct,
       case when open_price > previous_close_price * 1.01 then 'gap_up'
            when open_price < previous_close_price * 0.99 then 'gap_down'
       else 'no_gap' end as gap_flag,
       case when close_price >= open_price then 'bullish' else 'bearish' end as candle_direction
from combined_data
qualify row_number() over(partition by symbol, session_date order by ingest_timestamp asc) = 1



