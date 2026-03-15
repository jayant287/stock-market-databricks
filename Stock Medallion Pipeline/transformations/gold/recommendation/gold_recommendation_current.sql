-- Please edit the sample below

CREATE MATERIALIZED VIEW stock_market.gold.gold_recommendation_current AS
with recomm_inter as (
    select symbol,
    buy,
    hold,
    sell,
    period,
    strongBuy,
    strongSell,
    buy + hold + sell + strongBuy + strongSell as total_ratings,
    ingest_timestamp,
    `__START_AT` AS eff_dt
         
  from stock_market.silver.silver_recommendation
  where `__END_AT` is null
)
select *,
       (2*strongBuy + buy - sell - 2*strongSell) / total_ratings as bullish_score
from recomm_inter