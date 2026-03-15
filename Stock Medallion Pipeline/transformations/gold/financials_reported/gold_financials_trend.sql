-- Please edit the sample below

CREATE MATERIALIZED VIEW stock_market.gold.gold_financials_trend AS
with financial_trends as (
    select symbol,
           year,
           SUM(CASE 
            WHEN ic_concept IN ('us-gaap_Revenues','Revenues') 
            THEN ic_value 
            ELSE 0 
          END) AS revenue,

          SUM(CASE 
              WHEN ic_concept IN ('us-gaap_NetIncomeLoss','NetIncomeLoss') 
              THEN ic_value 
              ELSE 0 
          END) AS net_income,

          SUM(CASE 
              WHEN cf_concept = 'us-gaap_NetCashProvidedByUsedInOperatingActivities'
              THEN cf_value 
              ELSE 0 
          END) AS operating_cash_flow,

          ABS(SUM(CASE 
              WHEN cf_concept = 'us-gaap_PaymentsToAcquirePropertyPlantAndEquipment'
              THEN cf_value 
              ELSE 0 
          END)) AS capex
    from stock_market.silver.silver_financials_reported
    group by symbol,year
),financial_trends_unpacked as (
    select symbol,
           year,
           stack(
               4,
               'revenue',revenue,
               'net_income',net_income,
               'operating_cash_flow',operating_cash_flow,
               'capex',capex
           ) as (metric,metric_value)
    from financial_trends
),financial_trends_calc as (
select *,
       lag(metric_value) over(partition by symbol,metric order by year desc) as previous_value
from financial_trends_unpacked
),growth_pct_calc as (
select symbol,
       year,
       metric,
       metric_value as current_year,
       previous_value,
       TRY_DIVIDE(metric_value - previous_value, ABS(previous_value)) * 100 AS growth_pct
from financial_trends_calc
)
select *,
       CASE WHEN growth_pct > 2 THEN 'GROWING'
            WHEN growth_pct < -2 THEN 'DECLINING'
            ELSE 'STABLE'
          END as trend
from growth_pct_calc