-- Please edit the sample below

CREATE MATERIALIZED VIEW stock_market.gold.gold_financials_snapshot AS
WITH financial_snapshot AS (
    SELECT
        symbol,
        accessNumber,
        year,

        /* Balance Sheet */
        MAX(CASE
                WHEN bs_concept IN ('us-gaap_AssetsCurrent', 'AssetsCurrent')
                THEN bs_value ELSE 0
            END) AS assets_current,

        MAX(CASE
                WHEN bs_concept IN ('us-gaap_AssetsNoncurrent', 'AssetsNoncurrent')
                THEN bs_value ELSE 0
            END) AS assets_noncurrent,

        MAX(CASE
                WHEN bs_concept IN ('us-gaap_Assets', 'Assets')
                THEN bs_value ELSE NULL
            END) AS assets_total_reported,

        /* Income Statement */
        SUM(CASE
                WHEN ic_concept IN ('us-gaap_Revenues', 'Revenues')
                THEN ic_value ELSE 0
            END) AS revenue,

        SUM(CASE
                WHEN ic_concept IN ('us-gaap_NetIncomeLoss', 'NetIncomeLoss')
                THEN ic_value ELSE 0
            END) AS net_income,

        /* Cash Flow */
        SUM(CASE
                WHEN cf_concept = 'us-gaap_NetCashProvidedByUsedInOperatingActivities'
                THEN cf_value ELSE 0
            END) AS operating_cash_flow,

        ABS(SUM(CASE
                WHEN cf_concept = 'us-gaap_PaymentsToAcquirePropertyPlantAndEquipment'
                THEN cf_value ELSE 0
            END)) AS capex

    FROM stock_market.silver.silver_financials_reported
    WHERE
        bs_concept IN (
            'us-gaap_AssetsCurrent','AssetsCurrent',
            'us-gaap_AssetsNoncurrent','AssetsNoncurrent',
            'us-gaap_Assets','Assets'
        )
        OR ic_concept IN (
            'us-gaap_Revenues','Revenues',
            'us-gaap_NetIncomeLoss','NetIncomeLoss'
        )
        OR cf_concept IN (
            'us-gaap_NetCashProvidedByUsedInOperatingActivities',
            'us-gaap_PaymentsToAcquirePropertyPlantAndEquipment'
        )
    GROUP BY
        symbol,
        accessNumber,
        year
)

SELECT
    *,
    /* Computed assets */
    (assets_current + assets_noncurrent) AS assets_computed

FROM financial_snapshot;