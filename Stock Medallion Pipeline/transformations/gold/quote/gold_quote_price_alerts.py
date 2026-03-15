from pyspark import pipelines as dp
from pyspark.sql.functions import *
from utilities import utils

@dp.table(name="stock_market.gold.gold_quote_price_alerts")
def gold_quote_price_alerts():

    quote_df = spark.readStream.table("stock_market.silver.silver_quote")

    severity_expr = """
    case when price_jump_pct >= 1 and price_jump_pct < 3 then 'LOW'
         when price_jump_pct >= 3 and price_jump_pct < 7 then 'MEDIUM'
         when price_jump_pct >= 7 then 'HIGH'
         else 'NONE'
    end
    """

    alert_type_query = """
    case when close_price > previous_close_price AND price_jump_pct >= 1 then 'SPIKE'
         when close_price < previous_close_price AND price_jump_pct >= 1 then 'DROP'
         else 'NONE'
    end
    """

    quote_df = quote_df.select(
        col("symbol"),
        col("event_timestamp"),
        col("close_price"),
        col("previous_close_price"),
        abs((col("close_price") - col("previous_close_price"))/(col("previous_close_price"))*100).alias("price_jump_pct")
    )

    quote_df = quote_df.select(
        "*",
        expr(severity_expr).alias("severity"),
        expr(alert_type_query).alias("alert_type")
    )

    return quote_df