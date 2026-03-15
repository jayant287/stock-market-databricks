from pyspark import pipelines as dp
from pyspark.sql.functions import *
from utilities import utils


@dp.table(name="stock_market.gold.gold_company_news_documents")
def gold_company_news_documents():
    company_news_df = spark.read.table("stock_market.silver.silver_company_news")
    company_news_df = company_news_df.withColumn(
    "doc_id",
    sha2(
        concat_ws(
            "|",
            col("symbol"),
            col("headline"),
            col("event_timestamp").cast("string"),
            col("source")
        ),
        256
    )
    )
    company_news_df = company_news_df.select(
    col("doc_id"),
    col("symbol"),
    to_date(col("event_timestamp")).alias("published_date"),
    concat_ws(".",col("headline"),col("summary")).alias("document_text"),
    col("source"),
    col("url")
    )
    return company_news_df
