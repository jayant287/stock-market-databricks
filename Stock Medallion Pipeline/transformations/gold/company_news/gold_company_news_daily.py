from pyspark import pipelines as dp
from pyspark.sql.functions import *
from utilities import utils

@dp.table(name="stock_market.gold.gold_company_news_daily")
def gold_company_news_daily():

    df = spark.read.table("stock_market.silver.silver_company_news")
    df = df.withColumn("news_date", to_date(col("event_timestamp")))

    company_news_df = df.groupBy("symbol","news_date").agg(
    min("event_timestamp").alias("first_news_time"),
    max("event_timestamp").alias("last_news_time"),
    count("symbol").alias("article_count"),
    collect_list("headline").alias("combined_headlines"),
    collect_list("summary").alias("combined_summary")
    )

    company_news_df = company_news_df.select(
    col("symbol"),
    col("news_date"),
    date_format("first_news_time","HH:mm").alias("first_news_time"),
    date_format("last_news_time","HH:mm").alias("last_news_time"),
    col("article_count"),
    array_distinct(col("combined_headlines")).alias("combined_headlines"),
    array_distinct(col("combined_summary")).alias("combined_summary")
    )

    return company_news_df