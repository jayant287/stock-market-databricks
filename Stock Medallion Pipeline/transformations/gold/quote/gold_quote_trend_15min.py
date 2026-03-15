from pyspark import pipelines as dp
from pyspark.sql.functions import *
from utilities import utils

@dp.table(name="stock_market.gold.gold_quote_trend_15min")
def gold_quote_trend_15min():

    quote_df = spark.readStream.table("stock_market.silver.silver_quote")

    agg_df = (
           quote_df.withWatermark("event_timestamp", "10 minutes")
                   .groupBy("symbol", window("event_timestamp","15 minutes","5 minutes"))
                   .agg(
                       min_by("close_price","event_timestamp").alias("first_close_price"),
                       max_by("close_price","event_timestamp").alias("last_close_price"),
                       max("high_price").alias("max_high_price"),
                       min("low_price").alias("min_low_price"),
                       avg("close_price").alias("avg_close_price"),
                       count("symbol").alias("quote_count")
                       )
                   
                  
          )

    agg_df = agg_df.select(
                          "*",
                          col("window.start").alias("window_start"),
                          col("window.end").alias("window_end"),
                          expr("""
                               case when last_close_price > first_close_price then 'UP'
                                    when last_close_price < first_close_price then 'DOWN'
                                    else 'FLAT' end
                               """).alias("trend_direction"),
                          (((col("max_high_price") - col("min_low_price")) / col("avg_close_price")) * 100.0).alias("volatility_pct")
    )

    agg_df = agg_df.drop("window")

    return agg_df