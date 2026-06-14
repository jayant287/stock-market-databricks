from pyspark import pipelines as dp
from pyspark.sql.functions import *

import json
dq_volume_path = spark.conf.get('dq_path')

with open(dq_volume_path) as f:
    rules = json.load(f)

critical_rules = rules['profile']['critical']
warn_rules = rules['profile']['warn']
keys_length = len(critical_rules.keys())
critical_expr = "NOT({0})".format(" AND ".join(critical_rules.values()))
critical_reason_expr = "concat_ws(', ',"
for i, key in enumerate(critical_rules.keys()):
    value = critical_rules[key]
    if i == keys_length - 1:
        critical_reason_expr += "case when NOT({0}) then '{1} has failed' end)".format(value,key)
    else:
        critical_reason_expr += "case when NOT({0}) then '{1} has failed' end, ".format(value,key)

@dp.expect_all(critical_rules)
@dp.expect_all(warn_rules)
@dp.view(name="bronze_profile_vw")
def bronze_profile_vw():

    profile_df = spark.readStream.table("stock_market.bronze.bronze_profile")

    profile_df = profile_df.withColumn("marketCapitalization",col("marketCapitalization").cast("double"))

    profile_df = profile_df.withColumn("shareOutstanding",col("shareOutstanding").cast("double"))

    profile_df = profile_df.withColumnRenamed("symbol_name","symbol")

    profile_df = profile_df.select(
        col("country"),
        col("currency"),
        col("exchange"),
        col("finnhubIndustry"),
        col("ipo"),
        col("marketCapitalization"),
        col("phone"),
        col("shareOutstanding"),
        col("ticker"),
        col("weburl"),
        col("name"),
        col("symbol"),
        col("estimateCurrency"),
        col("ingest_timestamp")
    )

    profile_df = profile_df.withColumn("is_critical", expr(critical_expr))
    profile_df = profile_df.withColumn("critical_reason", expr(critical_reason_expr))

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