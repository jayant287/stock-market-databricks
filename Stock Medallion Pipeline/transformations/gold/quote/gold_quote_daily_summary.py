from pyspark import pipelines as dp
from pyspark.sql.functions import *
from utilities import utils

@dp.table(name="stock_market.gold.gold_quote_daily_summary")
def gold_quote_daily_summary():

    quote_df = spark.read.table("stock_market.silver.silver_quote")

    agg_df = (
           quote_df.groupBy("symbol", window("event_timestamp","1 day"))
                   .agg(
                       min_by("open_price","event_timestamp").alias("day_open_price"),
                       max_by("close_price","event_timestamp").alias("day_close_price"),
                       max("high_price").alias("day_high_price"),
                       min("low_price").alias("day_low_price"),
                       avg("percent_change").alias("avg_percent_change"),
                       count("symbol").alias("quote_count")
                       )
                   
                  
          )

    agg_df = agg_df.select(
                          "*",
                          to_date(col("window.start")).alias("trade_date"),
                          ((col("day_close_price") - col("day_open_price"))/(col("day_open_price")) * 100).alias("daily_return_pct")
    )

    agg_df = agg_df.drop("window")

    return agg_df