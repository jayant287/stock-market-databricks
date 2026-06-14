from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import *

@dp.table(name="stock_market.gold.gold_stock_candles")
def gold_stock_candles():

    stock_candle_df = spark.read.table("stock_market.silver.silver_stock_candles")

    window_spec = Window.partitionBy("symbol").orderBy("date")

    stock_candle_df = stock_candle_df.withColumn(
        "previous_close_price",
        coalesce(
            lag(col("close")).over(window_spec),
            lit(0)
        )
    )

    return stock_candle_df
