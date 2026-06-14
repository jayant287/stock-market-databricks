from pyspark import pipelines as dp
from pyspark.sql.functions import col, explode,expr

# ------------------------------
# Expectations (same as before)
# ------------------------------

import json
dq_volume_path = spark.conf.get('dq_path')

with open(dq_volume_path) as f:
    rules = json.load(f)

critical_rules = rules['recommendation']['critical']
warn_rules = rules['recommendation']['warn']
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
@dp.view(name="bronze_recommendation_vw")
def bronze_recommendation_vw():

    df = spark.readStream.table("stock_market.bronze.bronze_recommendation")

    df = df.select(
        col("symbol"),
        explode(col("data")).alias("data"),
        col("ingest_timestamp")
    )

    df = df.select(
        col("symbol"),
        # col("period"),
        col("data.period").alias("period"),
        col("data.buy").cast("int").alias("buy"),
        col("data.sell").cast("int").alias("sell"),
        col("data.hold").cast("int").alias("hold"),
        col("data.strongBuy").cast("int").alias("strongBuy"),
        col("data.strongSell").cast("int").alias("strongSell"),
        col("ingest_timestamp")
    )

    df = df.withColumn("is_critical", expr(critical_expr))
    df = df.withColumn("critical_reason", expr(critical_reason_expr))

    return df

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
    source="bronze_recommendation_vw",
    keys=["symbol", "period"],
    sequence_by=col("ingest_timestamp"),
    stored_as_scd_type="2",
    track_history_except_column_list=["ingest_timestamp"]
)