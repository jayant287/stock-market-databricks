-- Please edit the sample below

CREATE MATERIALIZED VIEW stock_market.gold.gold_recommendation_trends AS
with recommendation_calc(
select symbol,
       period as period_start_date,
       last_day(period) as period_end_date,
       bullish_score,
       (strongBuy + buy) / total_ratings as buy_ratio,
       (sell + strongSell) / total_ratings as sell_ratio,
       lag(bullish_score) over (partition by symbol order by period) as previous_bullish_score
 from stock_market.gold.gold_recommendation_current
)

select *,
       bullish_score - previous_bullish_score as bullish_score_change,
       case when previous_bullish_score is null then 'NEW'
            when bullish_score - previous_bullish_score > 0 then 'IMPROVING'
            when bullish_score - previous_bullish_score < 0 then 'DECLINING'
            else 'STABLE' end as bullish_trend
from recommendation_calc