from pyspark import pipelines as dp
from pyspark.sql.functions import col, explode
from utilities import utils

# ------------------------------
# Expectations (same as before)
# ------------------------------

recommendation_expect_or_drop = {
    "symbol_not_null": "symbol IS NOT NULL",
    "symbol_not_empty": "length(trim(symbol)) > 0",
    "period_not_null": "period IS NOT NULL",
    "period_not_empty": "length(trim(period)) > 0",
    "ingest_timestamp_not_null": "ingest_timestamp IS NOT NULL"
}

recommendation_expect_or_fail = {
    "buy_not_null": "buy IS NOT NULL",
    "hold_not_null": "hold IS NOT NULL",
    "sell_not_null": "sell IS NOT NULL",
    "strong_buy_not_null": "strongBuy IS NOT NULL",
    "strong_sell_not_null": "strongSell IS NOT NULL",
    "buy_non_negative": "buy >= 0",
    "hold_non_negative": "hold >= 0",
    "sell_non_negative": "sell >= 0",
    "strong_buy_non_negative": "strongBuy >= 0",
    "strong_sell_non_negative": "strongSell >= 0",
    "at_least_one_analyst": """
        (buy + hold + sell + strongBuy + strongSell) > 0
    """
}

recommendation_expect_or_warn = {
    "period_not_future": """
        to_date(concat(period, '-01')) <= current_date()
    """,
    "extreme_bullish_signal": """
        (strongBuy + buy) >= 3 * (sell + strongSell)
    """,
    "extreme_bearish_signal": """
        (sell + strongSell) >= 3 * (strongBuy + buy)
    """,
    "low_analyst_coverage": """
        (buy + hold + sell + strongBuy + strongSell) <= 2
    """
}

# ------------------------------
# Bronze Transformation (Batch)
# ------------------------------

@dp.expect_all(recommendation_expect_or_warn)
@dp.expect_all_or_drop(recommendation_expect_or_drop)
@dp.expect_all_or_fail(recommendation_expect_or_fail)
@dp.table(name="bronze_recommendations")
def bronze_recommendation():

    df = (
        spark.readStream.format("delta")
        .load("s3://stock-market-fin/bronze/recommendation/")
    )

    exploded = (
        df
        .select(
            col("symbol"),
            explode(col("recommendation")).alias("recommendation"),
            col("ingest_timestamp")
        )
    )

    return (
        exploded
        .select(
            col("symbol"),
            col("recommendation.buy").alias("buy"),
            col("recommendation.hold").alias("hold"),
            col("recommendation.sell").alias("sell"),
            col("recommendation.period").alias("period"),
            col("recommendation.strongBuy").alias("strongBuy"),
            col("recommendation.strongSell").alias("strongSell"),
            col("ingest_timestamp")
        )
    )

# ------------------------------------
# Create the Silver Streaming Target
# ------------------------------------

dp.create_streaming_table(
    name="stock_market.silver.silver_recommendation",
    comment="Silver recommendations with SCD Type 2 CDC"
)

# ---------------------------------------------------
# Wire the CDC Flow into the Silver Streaming Table
# ---------------------------------------------------

dp.create_auto_cdc_flow(
    target="stock_market.silver.silver_recommendation",
    source="bronze_recommendations",
    keys=["symbol", "period"],
    sequence_by=col("ingest_timestamp"),
    stored_as_scd_type="2",
    track_history_except_column_list=["ingest_timestamp"]
)
