from pyspark import pipelines as dp
from pyspark.sql.functions import col, count, count_if
from utilities import utils

# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.
profile_expect_or_drop = {
    "symbol_not_null": "symbol IS NOT NULL",
    "symbol_not_empty": "length(trim(symbol)) > 0",
    "ticker_not_null": "ticker IS NOT NULL",
    "ingest_timestamp_not_null": "ingest_timestamp IS NOT NULL"
}

profile_expect_or_fail = {
    "symbol_equals_ticker": "symbol = ticker",

    "currency_not_null": "currency IS NOT NULL",
    "currency_length_3": "length(currency) = 3",

    "estimate_currency_valid": "estimateCurrency IS NULL OR length(estimateCurrency) = 3",

    "exchange_not_null": "exchange IS NOT NULL",

    "market_cap_non_negative": "marketCapitalization IS NULL OR marketCapitalization >= 0",

    "share_outstanding_positive": "shareOutstanding IS NULL OR shareOutstanding > 0",

    "ipo_valid_date": "ipo IS NULL OR ipo >= '1900-01-01'"
}

profile_expect_or_warn = {
    "company_name_present": "name IS NOT NULL",

    "industry_present": "finnhubIndustry IS NOT NULL",

    "country_length_reasonable": "country IS NULL OR length(country) >= 2",

    "phone_format_reasonable": "phone IS NULL OR length(phone) >= 7",

    "weburl_format": "weburl IS NULL OR weburl LIKE 'http%'",

    "ipo_reasonable_range": "ipo IS NULL OR ipo <= current_date()"
}

@dp.expect_all(profile_expect_or_warn)
@dp.expect_all_or_drop(profile_expect_or_drop)
@dp.expect_all_or_fail(profile_expect_or_fail)
@dp.view(name="bronze_profile_vw")
def bronze_profile_vw():

    profile_df = (
    spark.readStream.format("delta")
    .load("s3://stock-market-fin/bronze/profile/")
    )
    profile_df.createOrReplaceTempView("profile_df")
    df = (
        spark.readStream
             .format("delta")
             .load("s3://stock-market-fin/bronze/profile/")
    )

    return df.select(
        col("symbol"),
        col("profile.country").alias("country"),
        col("profile.currency").alias("currency"),
        col("profile.estimateCurrency").alias("estimateCurrency"),
        col("profile.exchange").alias("exchange"),
        col("profile.finnhubIndustry").alias("finnhubIndustry"),
        col("profile.ipo").alias("ipo"),
        col("profile.marketCapitalization").alias("marketCapitalization"),
        col("profile.name").alias("name"),
        col("profile.phone").alias("phone"),
        col("profile.shareOutstanding").alias("shareOutstanding"),
        col("profile.ticker").alias("ticker"),
        col("profile.weburl").alias("weburl"),
        col("ingest_timestamp")
    )
    

    return profile_df

dp.create_streaming_table("stock_market.silver.silver_profile")

dp.create_auto_cdc_flow(
    target = "stock_market.silver.silver_profile",
    source = "bronze_profile_vw",
    keys = ["symbol"],
    sequence_by = col("ingest_timestamp"),
    stored_as_scd_type = "2",
    track_history_except_column_list = ["ingest_timestamp"]
    )
    



