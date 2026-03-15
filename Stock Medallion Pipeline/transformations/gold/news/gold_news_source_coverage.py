from pyspark import pipelines as dp
from pyspark.sql.functions import *
from utilities import utils
from pyspark.sql.window import *

@dp.table(name="stock_market.gold.gold_news_source_coverage")
def gold_news_source_coverage():

    news_df = spark.readStream.table("stock_market.silver.silver_news")

    news_df = news_df.withColumn("summary_length",length("summary"))

    news_df = news_df.groupBy("category","source").agg(
    min("event_timestamp").alias("first_published"),
    max("event_timestamp").alias("last_published"),
    count("headline").alias("news_count"),
    avg("summary_length").cast("int").alias("avg_summary_length")
    )

    return news_df