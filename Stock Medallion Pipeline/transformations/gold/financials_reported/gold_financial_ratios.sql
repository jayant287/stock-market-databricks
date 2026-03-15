-- Please edit the sample below

CREATE MATERIALIZED VIEW stock_market.gold.gold_financial_ratios AS
WITH financial_base AS (
    SELECT
        symbol,
        year AS fiscal_year,

        MAX(CASE 
            WHEN bs_concept IN ('us-gaap_Assets','Assets') 
            THEN bs_value 
        END) AS assets,

        MAX(CASE 
            WHEN bs_concept IN ('us-gaap_Liabilities','Liabilities') 
            THEN bs_value 
        END) AS liabilities,

        MAX(CASE 
            WHEN bs_concept IN ('us-gaap_StockholdersEquity','StockholdersEquity') 
            THEN bs_value 
        END) AS equity,

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

    FROM stock_market.silver.silver_financials_reported
    GROUP BY symbol, year
)

SELECT
    symbol,
    fiscal_year,

    assets / liabilities AS current_ratio,
    liabilities / equity AS debt_to_equity,
    net_income / revenue AS net_margin,
    revenue / assets AS asset_turnover,
    (operating_cash_flow - capex) / revenue AS free_cash_flow_margin

FROM financial_base
WHERE assets IS NOT NULL
  AND liabilities IS NOT NULL
  AND equity IS NOT NULL
  AND revenue != 0;