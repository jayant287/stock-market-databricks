from pyspark import pipelines as dp
from pyspark.sql.functions import *
from utilities import utils
from pyspark.sql.window import *

@dp.table(name="stock_market.gold.gold_news_trends")
def gold_news_trends():

    news_df = spark.readStream.table("stock_market.silver.silver_news").withWatermark("datetime", "10 minutes")

    news_df_agg = news_df.groupBy("category",window("datetime","60 minutes")).agg(
    date_format(max("datetime"),"yyyy/MM/dd HH:mm:ss").alias("latest_news_time"),
    count("headline").alias("news_count"),
    approx_count_distinct("source").alias("source_count")
    )

    news_df_agg = news_df_agg.select(
        col("category"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("latest_news_time"),
        col("news_count"),
        col("source_count")
    )

    return news_df_agg