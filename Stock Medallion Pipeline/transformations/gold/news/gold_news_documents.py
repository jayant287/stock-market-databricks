from pyspark import pipelines as dp
from pyspark.sql.functions import *
from utilities import utils


@dp.table(name="stock_market.gold.gold_news_documents")
def gold_news_documents():
    news_df = spark.readStream.table("stock_market.silver.silver_news")
    news_df = news_df.withColumn(
    "doc_id",
    sha2(
        concat_ws(
            "|",
            col("headline"),
            col("source")
        ),
        256
    )
    )
    news_df = news_df.select(
    col("doc_id"),
    to_date(col("event_timestamp")).alias("published_date"),
    col("headline"),
    col("summary"),
    col("source"),
    col("url")
    )
    return news_df
