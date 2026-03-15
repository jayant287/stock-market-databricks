from pyspark import pipelines as dp
from pyspark.sql.functions import *
from utilities import utils

quote_expect_drop = {
    "symbol_not_null": "symbol IS NOT NULL",
    "close_price_not_null": "close_price IS NOT NULL",
    "event_timestamp_not_null": "event_timestamp IS NOT NULL",
    "close_price_non_negative": "close_price >= 0"
}
quote_expect_fail = {
    "high_greater_than_low": "high_price >= low_price",
    "close_within_day_range": """
        close_price >= low_price
        AND close_price <= high_price
    """,
    "open_price_non_negative": "open_price >= 0",
    "previous_close_non_negative": "previous_close_price >= 0"
}
quote_expect_warn = {
    "percent_change_reasonable": "ABS(percent_change) <= 50",
    "price_jump_warning": """
        ABS(close_price - previous_close_price) 
        / previous_close_price <= 0.2
    """,
    "late_event_warning": """
        event_timestamp >= ingest_timestamp - INTERVAL 10 MINUTES
    """
}
@dp.expect_all_or_drop(quote_expect_drop)
@dp.expect_all_or_fail(quote_expect_fail)
@dp.expect_all(quote_expect_warn)
@dp.table(name="stock_market.silver.silver_quote")
def silver_quote():
    df = (
    spark.readStream.format("delta")
    .load("s3://stock-market-fin/bronze/quote/")
    )

    quote_df = df.select(
    col("symbol"),
    col("quote.c").alias("close_price"),
    col("quote.d").alias("change"),
    col("quote.dp").alias("percent_change"),
    col("quote.h").alias("high_price"),
    col("quote.l").alias("low_price"),
    col("quote.o").alias("open_price"),
    col("quote.pc").alias("previous_close_price"),
    from_unixtime(col("quote.t").cast("bigint")).cast("timestamp").alias("event_timestamp"),
    col("ingest_timestamp")
    )

    quote_df = (
        quote_df.withWatermark("event_timestamp", "15 minutes")
        .dropDuplicatesWithinWatermark(["symbol", "event_timestamp"])
    )
    return quote_df