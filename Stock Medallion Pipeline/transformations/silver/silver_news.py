from pyspark import pipelines as dp
from pyspark.sql.functions import *
from utilities import utils


@dp.table(name="stock_market.silver.silver_news")
def silver_news():
    df = (
    spark.readStream.format("delta")
    .load("s3://stock-market-fin/bronze/news/")
    )

    news_df = df.select(
    explode(col("news")).alias("news"),
    col("timestamp").alias("event_timestamp"),
    col("ingest_timestamp")
    )

    news_df = news_df.select(
    col("news.category").alias("category"),
    col("news.headline").alias("headline"),
    col("news.summary").alias("summary"),
    from_unixtime(col("news.datetime").cast("bigint")).cast("timestamp").alias("datetime"),
    col("news.id").alias("id"),
    col("news.related").alias("related"),
    col("news.source").alias("source"),
    col("news.url").alias("url"),
    col("event_timestamp"),
    col("ingest_timestamp")
    )
    return news_df