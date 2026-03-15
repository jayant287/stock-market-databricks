from pyspark import pipelines as dp
from pyspark.sql.functions import *
from utilities import utils

company_news_expect_fail = {

    # Identity
    "symbol_not_null":
        "symbol IS NOT NULL AND LENGTH(symbol) > 0",

    "headline_not_null":
        "headline IS NOT NULL",

    "url_not_null":
        "url IS NOT NULL",

    # Temporal integrity
    "datetime_not_null":
        "event_timestamp IS NOT NULL",

    "datetime_parseable":
        "to_timestamp(event_timestamp) IS NOT NULL"
}

company_news_expect_drop = {

    # Content usability
    "headline_min_length":
        "LENGTH(headline) > 10",

    "url_min_length":
        "LENGTH(url) > 10",

    # Temporal sanity
    "datetime_not_future":
        "to_timestamp(event_timestamp) <= current_timestamp()"
}

company_news_expect_warn = {

    # Content completeness
    "summary_present":
        "summary IS NOT NULL",

    "summary_min_length":
        "summary IS NULL OR LENGTH(summary) > 20",

    # Domain hygiene
    "source_present":
        "source IS NOT NULL",

    "related_present":
        "related IS NOT NULL",

    # API behavior tracking
    "id_non_negative":
        "id IS NULL OR id >= 0"
}
@dp.expect_all_or_drop(company_news_expect_drop)
@dp.expect_all_or_fail(company_news_expect_fail)
@dp.expect_all(company_news_expect_warn)
@dp.table(name="stock_market.silver.silver_company_news")
def silver_company_news():
    df = (
    spark.readStream.format("delta")
    .load("s3://stock-market-fin/bronze/company_news/")
    )

    df = df.select(
        col("symbol"),
        explode(col("company_news")).alias("company_news"),
        col("ingest_timestamp")
    )

    company_news_df = (
        df.select(
             col("symbol"),
            col("company_news.summary").alias("summary"),
            from_unixtime(
                col("company_news.datetime").cast("bigint")
            ).cast("timestamp").alias("event_timestamp"),
            col("company_news.headline").alias("headline"),
            col("company_news.id").alias("id"),
            col("company_news.related").alias("related"),
            col("company_news.source").alias("source"),
            col("company_news.url").alias("url")
        )
    )
    
    return company_news_df