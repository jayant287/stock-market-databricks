from pyspark import pipelines as dp
from pyspark.sql.functions import *
from utilities import utils

@dp.table(name="stock_market.gold.gold_quote_5min_ohlc")
def gold_quote_5min_ohlc():

    quote_df = spark.readStream.table("stock_market.silver.silver_quote")

    agg_df = (
           quote_df.withWatermark("event_timestamp", "10 minutes")
                   .groupBy("symbol", window("event_timestamp","5 minutes"))
                   .agg(
                       first("open_price").alias("open_price"),
                       last("close_price").alias("close_price"),
                       max("high_price").alias("high_price"),
                       min("low_price").alias("low_price"),
                       avg("close_price").alias("avg_price"),
                       count("symbol").alias("quote_count")
                       )
                  
          )

    agg_df = agg_df.withColumn("price_range", col("high_price") - col("low_price"))

    agg_df = agg_df.selectExpr("*","window.start as window_start","window.end as window_end")

    agg_df = agg_df.drop("window")

    return agg_df