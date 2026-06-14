from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from pyspark import pipelines as dp

@dp.table(name="stock_market.gold.gold_fact_stock_metrics")
def gold_fact_stock_metrics():

    metric_snapshot_df = spark.read.table("stock_market.silver.silver_metric_snapshot")
    
    metric_snapshot_df = metric_snapshot_df.filter("is_critical = False")

    risk_category_expr = """
    case when beta >= 1.5 then  'High volatility'
        when beta >= 1.0  then 'Above Market'
        when beta >= 0.5  then 'Above Market'
    else 'Low Volatility' end 
    """

    metric_snapshot_calc_df = metric_snapshot_df.select(
        col("symbol"),
        coalesce(col("peTTM"), lit(0)).alias("pe_ttm"),
        coalesce(col("pb"), lit(0)).alias("pb"),
        coalesce(col("beta"), lit(0)).alias("beta"),
        expr(risk_category_expr).alias("risk_category"),
        expr("(coalesce(roeTTM,0) * 0.30) + (coalesce(roaTTM,0) * 0.20) + (coalesce(netProfitMarginTTM,0) * 0.25) + (coalesce(grossMarginTTM,0) * 0.25)").alias("profitability_score"),
        coalesce(col("psTTM"), lit(0)).alias("ps_ttm"),
        coalesce(col("evEbitdaTTM"), lit(0)).alias("ev_ebitda_ttm"),
        coalesce(col("roeTTM"), lit(0)).alias("roe_ttm"),
        coalesce(col("roaTTM"), lit(0)).alias("roa_ttm"),
        coalesce(col("52WeekHigh"), lit(0)).alias("week52_high"),
        coalesce(col("52WeekLow"), lit(0)).alias("week52_low"),
        coalesce(col("52WeekPriceReturnDaily"), lit(0)).alias("return_52w"),
        coalesce(col("13WeekPriceReturnDaily"), lit(0)).alias("return_13w"),
        coalesce(col("26WeekPriceReturnDaily"), lit(0)).alias("return_26w"),
        coalesce(col("currentDividendYieldTTM"), lit(0)).alias("dividend_yield"),
        coalesce(col("grossMarginTTM"), lit(0)).alias("gross_margin_ttm"),
        coalesce(col("operatingMarginTTM"), lit(0)).alias("operating_margin_ttm"),
        coalesce(col("netProfitMarginTTM"), lit(0)).alias("net_profit_margin_ttm"),
        coalesce(col("marketCapitalization"), lit(0)).alias("market_capitalization"),
        col("metric_date")
    )

    return metric_snapshot_calc_df