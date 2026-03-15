from pyspark import pipelines as dp
from pyspark.sql.functions import *
from utilities import utils
from pyspark.sql.window import *

@dp.table(name="stock_market.gold.gold_company_news_trends")
def gold_company_news_trends():

    company_news_df = spark.readStream.table("stock_market.silver.silver_company_news").withWatermark("event_timestamp", "10 minutes")
    company_news_df_agg = company_news_df.groupBy("symbol",window("event_timestamp","60 minutes")).agg(
        count("headline").alias("news_count"),
        approx_count_distinct("source").alias("source_count")
        )

    company_news_df_agg = company_news_df_agg.select(
        col("symbol"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("news_count"),
        col("source_count")
    )

    return company_news_df_agg