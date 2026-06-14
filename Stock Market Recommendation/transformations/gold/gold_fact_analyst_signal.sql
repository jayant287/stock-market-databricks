-- Please edit the sample below

CREATE MATERIALIZED VIEW stock_market.gold.gold_fact_analyst_signal AS
with recommendation_score as (
select symbol,
       period,
       strongBuy + buy + hold + sell + strongSell as total_analysts,
       strongBuy,
       ((strongBuy * 2) + (buy * 1) - (sell * 1) - (strongSell * 2)) as raw_score
from stock_market.silver.silver_recommendation
where is_critical = False
), normalized_bullish_score as (
select symbol,
       period,
       total_analysts,
       raw_score,
       round(3.0 + (cast(raw_score as double)/cast(total_analysts as double)),2) as bullish_score,
       round((cast(strongBuy as double) / cast(total_analysts as double)) * 100,2) as pct_strong_buy

from recommendation_score
)
select *,
       case when bullish_score >=4.5 then 'Strong buy'
            when bullish_score >=3.5 then 'Buy' 
            when bullish_score >=2.5 then 'Hold'
            when bullish_score >=1.5 then 'Sell'
      else 'Strong sell' end as consensus_label
from normalized_bullish_score