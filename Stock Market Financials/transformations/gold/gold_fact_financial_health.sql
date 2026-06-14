-- Please edit the sample below

CREATE MATERIALIZED VIEW stock_market.gold.gold_financial_ratio AS
with financials_pvt as (
select symbol,
       year,
       quarter,
       startDate,
        endDate,
        filedDate,
        cik,
        accessNumber,
       -- Option A: Separate priority-ordered aggregations (most reliable)
        COALESCE(
        FIRST(CASE WHEN concept = 'us-gaap_Revenues' 
                    THEN value END IGNORE NULLS),
        FIRST(CASE WHEN concept = 'us-gaap_RevenueFromContractWithCustomerExcludingAssessedTax' 
                    THEN value END IGNORE NULLS),
        FIRST(CASE WHEN concept = 'SalesRevenueGoodsNet' 
                    THEN value END IGNORE NULLS),
        0
        ) AS revenue,
        COALESCE(
            FIRST(CASE WHEN concept = 'us-gaap_NetIncomeLoss' 
                    THEN value END IGNORE NULLS),
            FIRST(CASE WHEN concept = 'NetIncomeLoss' 
                        THEN value END IGNORE NULLS),
            FIRST(CASE WHEN concept = 'us-gaap_ProfitLoss' 
                        THEN value END IGNORE NULLS),
            FIRST(CASE WHEN concept = 'ProfitLoss' 
                        THEN value END IGNORE NULLS),
            0
        ) as net_income,
        COALESCE(
            FIRST(CASE WHEN concept = 'us-gaap_OperatingIncomeLoss' 
                    THEN value END IGNORE NULLS),
            FIRST(CASE WHEN concept = 'OperatingIncomeLoss' 
                        THEN value END IGNORE NULLS),
            0
        ) as operating_income,
        COALESCE(
            FIRST(CASE WHEN concept = 'us-gaap_EarningsPerShareBasic' 
                    THEN value END IGNORE NULLS),
            FIRST(CASE WHEN concept = 'EarningsPerShareBasic' 
                        THEN value END IGNORE NULLS),
            0
        ) as eps_basic,
         COALESCE(
            FIRST(CASE WHEN concept = 'us-gaap_EarningsPerShareDiluted' 
                    THEN value END IGNORE NULLS),
            FIRST(CASE WHEN concept = 'EarningsPerShareDiluted' 
                        THEN value END IGNORE NULLS),
            0
        ) as eps_diluted,
        COALESCE(
            FIRST(CASE WHEN concept = 'us-gaap_Assets' 
                    THEN value END IGNORE NULLS),
            FIRST(CASE WHEN concept = 'Assets' 
                    THEN value END IGNORE NULLS),
            0
        ) as assets,
        COALESCE(
            FIRST(CASE WHEN concept = 'us-gaap_LongTermDebt' 
                    THEN value END IGNORE NULLS),
            FIRST(CASE WHEN concept = 'us-gaap_LongTermDebtNoncurrent' 
                        THEN value END IGNORE NULLS),
            FIRST(CASE WHEN concept = 'us-gaap_DebtCurrent' 
                        THEN value END IGNORE NULLS),
            FIRST(CASE WHEN concept = 'LongTermDebtCurrent' 
                        THEN value END IGNORE NULLS),
            0
        ) as total_debt,

        COALESCE(
            FIRST(CASE WHEN concept = 'us-gaap_CashAndCashEquivalentsAtCarryingValue' 
                    THEN value END IGNORE NULLS),
            FIRST(CASE WHEN concept = 'CashAndCashEquivalentsAtCarryingValue' 
                        THEN value END IGNORE NULLS),
            FIRST(CASE WHEN concept = 'us-gaap_CashCashEquivalentsAndShortTermInvestments' 
                        THEN value END IGNORE NULLS),
            0
        ) as cash_and_equivalents,

        COALESCE(
            FIRST(CASE WHEN concept = 'us-gaap_StockholdersEquity' 
                    THEN value END IGNORE NULLS),
            FIRST(CASE WHEN concept = 'StockholdersEquity' 
                        THEN value END IGNORE NULLS),
            0
        ) as shareholders_equity,

        COALESCE(
            FIRST(CASE WHEN concept = 'us-gaap_AssetsCurrent' 
                        THEN value END IGNORE NULLS),
            FIRST(CASE WHEN concept = 'AssetsCurrent' 
                        THEN value END IGNORE NULLS),
            0
        ) as current_assets,

        COALESCE(
            FIRST(CASE WHEN concept = 'us-gaap_LiabilitiesCurrent' 
                        THEN value END IGNORE NULLS),
            FIRST(CASE WHEN concept = 'LiabilitiesCurrent' 
                        THEN value END IGNORE NULLS),
            0
        ) as current_liabilities,

        COALESCE(
            FIRST(CASE WHEN concept = 'us-gaap_NetCashProvidedByUsedInOperatingActivities' 
                        THEN value END IGNORE NULLS),
            FIRST(CASE WHEN concept = 'NetCashProvidedByUsedInOperatingActivities' 
                        THEN value END IGNORE NULLS),
            0
        ) as operating_cash_flow,

        COALESCE(
            FIRST(CASE WHEN concept = 'us-gaap_PaymentsToAcquirePropertyPlantAndEquipment' 
                        THEN value END IGNORE NULLS),
            FIRST(CASE WHEN concept = 'PaymentsToAcquirePropertyPlantAndEquipment' 
                        THEN value END IGNORE NULLS),
            0
        ) as capex,

        COALESCE(
            FIRST(CASE WHEN concept = 'us-gaap_PaymentsOfDividendsCommonStock' 
                        THEN value END IGNORE NULLS),
            FIRST(CASE WHEN concept = 'PaymentsOfDividendsCommonStock' 
                        THEN value END IGNORE NULLS),
            0
        ) as dividends_paid,

        COALESCE(
            FIRST(CASE WHEN concept = 'us-gaap_PaymentsForRepurchaseOfCommonStock' 
                        THEN value END IGNORE NULLS),
            FIRST(CASE WHEN concept = 'PaymentsForRepurchaseOfCommonStock' 
                        THEN value END IGNORE NULLS),
            0
        ) as buybacks 
       
 from stock_market.silver.silver_financials_reported
group by symbol,year,quarter,startDate,
        endDate,
        filedDate,
        cik,
        accessNumber
)
select *,
       operating_cash_flow - ABS(capex) as free_cash_flow,
        ROUND(total_debt / NULLIF(shareholders_equity,0), 4) as debt_to_equity,
        ROUND(current_assets / NULLIF(current_liabilities,0), 4) as current_ratio,
        ROUND(net_income / NULLIF(revenue,0) * 100, 4) as net_margin,
        ROUND(operating_income / NULLIF(revenue,0) * 100, 4) as operating_margin,
        ROUND(free_cash_flow / NULLIF(revenue,0) * 100, 4) as fcf_margin,
        CASE WHEN quarter = 0 THEN CONCAT('FY', CAST(year AS STRING)) ELSE CONCAT('Q', CAST(quarter AS STRING), '-', CAST(year AS STRING)) END as period_label
 from financials_pvt
