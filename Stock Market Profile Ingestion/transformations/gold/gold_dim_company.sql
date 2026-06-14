CREATE MATERIALIZED VIEW stock_market.gold.gold_dim_company AS
select symbol,
       name,
       exchange,
       finnhubIndustry as industry,
       marketCapitalization  as market_cap_mm,
       CASE
            WHEN marketCapitalization >= 200000 THEN 'Mega cap'
            WHEN marketCapitalization >= 10000 THEN 'Large cap'
            WHEN marketCapitalization >= 2000 THEN 'Mid cap'
            WHEN marketCapitalization >= 300 THEN 'Small cap'
      ELSE 'Micro cap' end as market_cap_category,
      shareOutstanding  as shares_outstanding
from stock_market.silver.silver_profile
where __END_AT is null and is_critical = False
