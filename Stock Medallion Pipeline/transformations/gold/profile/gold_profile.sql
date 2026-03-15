-- Please edit the sample below

CREATE MATERIALIZED VIEW stock_market.gold.gold_profile AS
select symbol,
       country,
       currency,
       estimateCurrency,
       exchange,
       finnhubIndustry,
       ipo,
       marketCapitalization,
       name,
       phone,
       shareOutstanding,
       ticker,
       weburl,
       `__START_AT` as profile_effective_date
from stock_market.silver.silver_profile
where __END_AT is null