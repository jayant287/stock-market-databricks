from pyspark import pipelines as dp
from pyspark.sql.functions import *
from utilities import utils
from pyspark.sql.window import *

@dp.table(name="stock_market.gold.gold_company_news_latest")
def gold_company_news_latest():

    df = spark.read.table("stock_market.silver.silver_company_news")
    latest_news_window = Window.partitionBy("symbol").orderBy(col("event_timestamp").desc())
    df = df.withColumn("rank", rank().over(latest_news_window))

    df = df.select(
    col("symbol"),
    col("event_timestamp"),
    col("headline"),
    col("summary"),
    col("rank"),
    col("source")
    )
    df = df.dropDuplicates(["symbol", "event_timestamp", "headline","summary"])

    return df