from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import *
import json

dq_volume_path = spark.conf.get('dq_path')

with open(dq_volume_path) as f:
    rules = json.load(f)

critical_rules = rules['stock_candles']['critical']
warn_rules = rules['stock_candles']['warn']
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
    name = "bronze_stock_candles_validation",
    private = True
)
@dp.expect_all(critical_rules)
@dp.expect_all(warn_rules)
def bronze_stock_candles_validation():

    stock_candle_df = spark.readStream.table("stock_market.bronze.bronze_stock_candles")
    
    stock_candle_df = stock_candle_df.select(
    col("symbol"),
    to_date(col('date')).alias("date"),
    col("open").cast("double").alias("open"),
    col("high").cast("double").alias("high"),
    col("close").cast("double").alias("close"),
    col("low").cast("double").alias("low"),
    col("volume").cast("int").alias("volume"),
    col("ingest_timestamp")
    )

    stock_candle_df = stock_candle_df.withColumn("is_quarantined", expr(quarantine_expr))
    stock_candle_df = stock_candle_df.withColumn("quarantine_reason", expr(quarantine_reason_expr))

    return stock_candle_df


@dp.view(
    name="bronze_stock_candles_vw"
    )
def bronze_stock_candles_vw():

    stock_candle_df = spark.readStream.table("bronze_stock_candles_validation")

    stock_candle_validated_df = stock_candle_df.filter("is_quarantined = false")

    stock_candle_validated_df = stock_candle_validated_df.drop("is_quarantined","quarantine_reason")

    return stock_candle_validated_df

@dp.table(
    name="stock_market.quarantine.quarantine_stock_candles"
    )
def quarantine_stock_candles():

    stock_candle_df = spark.readStream.table("bronze_stock_candles_validation")

    stock_candle_validated_df = stock_candle_df.filter("is_quarantined = true")

    return stock_candle_validated_df

dp.create_streaming_table("stock_market.silver.silver_stock_candles")

dp.create_auto_cdc_flow(
    target = "stock_market.silver.silver_stock_candles",
    source = "bronze_stock_candles_vw",
    keys = ["symbol","date"],
    sequence_by = col("ingest_timestamp"),
    stored_as_scd_type = "1"
    )