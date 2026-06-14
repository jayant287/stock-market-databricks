from pyspark import pipelines as dp
from pyspark.sql.functions import *
import json

dq_volume_path = spark.conf.get('dq_path')

with open(dq_volume_path) as f:
    rules = json.load(f)

critical_rules = rules['quote']['critical']
warn_rules = rules['quote']['warn']
keys_length = len(critical_rules.keys())
quarantine_expr = "NOT({0})".format(" AND ".join(critical_rules.values()))
quarantine_reason_expr = "concat_ws(', ',"
for i, key in enumerate(critical_rules.keys()):
    value = critical_rules[key]
    if i == keys_length - 1:
        quarantine_reason_expr += "case when NOT({0}) then '{1} has failed' end)".format(value,key)
    else:
        quarantine_reason_expr += "case when NOT({0}) then '{1} has failed' end, ".format(value,key)

@dp.table(
    name = "bronze_quote_validation",
    private = True
)
@dp.expect_all(critical_rules)
@dp.expect_all(warn_rules)
def bronze_quote_validation():
    df = spark.readStream.table("stock_market.bronze.bronze_quote")

    quote_df = df.select(
    col("symbol"),
    col("c").cast("double").alias("close_price"),
    col("d").cast("double").alias("change"),
    col("dp").cast("double").alias("percent_change"),
    col("h").cast("double").alias("high_price"),
    col("l").cast("double").alias("low_price"),
    col("o").cast("double").alias("open_price"),
    col("pc").cast("double").alias("previous_close_price"),
    from_utc_timestamp(from_unixtime(col("t").cast("bigint")).cast("timestamp"), 'America/New_York').alias("event_timestamp"),
    col("ingest_timestamp")
    )

    quote_df = quote_df.withColumn("is_quarantine", expr(quarantine_expr))
    quote_df = quote_df.withColumn("quarantine_reason", expr(quarantine_reason_expr))

    return quote_df

@dp.table(name="stock_market.quarantine.quarantine_quote")
def quarantine_quote():
    df = spark.readStream.table("bronze_quote_validation")

    quote_df = df.filter("is_quarantine = true")

    return quote_df

@dp.table(name="stock_market.silver.silver_quote")
def silver_quote():
    df = spark.readStream.table("bronze_quote_validation")

    quote_df = df.filter("is_quarantine = false")
    quote_df = quote_df.drop("is_quarantine","quarantine_reason")

    return quote_df