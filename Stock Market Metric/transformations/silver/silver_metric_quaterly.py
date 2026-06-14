from pyspark import pipelines as dp
from pyspark.sql.functions import *
import json
dq_volume_path = spark.conf.get('dq_path')

with open(dq_volume_path) as f:
    rules = json.load(f)

critical_rules = rules['metric_quarterly']['critical']
warn_rules = rules['metric_quarterly']['warn']
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
@dp.view(name="bronze_metric_quarterly_vw")
def bronze_metric_annual_vw():

    metric_df = spark.readStream.table("stock_market.bronze.bronze_metric")
    
    series_metric_df = metric_df.selectExpr(
    "symbol",
    "series.*",
    "ingest_timestamp"
    )

    quarterly_metric_df = series_metric_df.selectExpr(
    "symbol",
    "quarterly.*",
    "ingest_timestamp"
    )

    quarterly_metric_columns = quarterly_metric_df.columns

    quarterly_metric_columns.remove("symbol")

    quarterly_metric_columns.remove("ingest_timestamp")

    quarterly_metric_length = len(quarterly_metric_columns)

    unpivot_expr = f"stack({quarterly_metric_length},"

    for i in range(0,quarterly_metric_length):

        if i == quarterly_metric_length - 1:

            value = f"'{quarterly_metric_columns[i]}',`{quarterly_metric_columns[i]}`) as (metric_type,value)"

            unpivot_expr = unpivot_expr + value

        else:

            value = f"'{quarterly_metric_columns[i]}',`{quarterly_metric_columns[i]}`,"

            unpivot_expr = unpivot_expr + value

    quarterly_metric_df = quarterly_metric_df.select("symbol","ingest_timestamp", expr(unpivot_expr))

    quarterly_metric_df = quarterly_metric_df.select(
    col("symbol"),
    col("metric_type"),
    explode(col("value")).alias("value"),
    col("ingest_timestamp")
    )

    quarterly_metric_df = quarterly_metric_df.selectExpr(
    "symbol",
    "metric_type",
    "value.*",
    "ingest_timestamp"
    )

    quarterly_metric_df = quarterly_metric_df.withColumn("is_critical", expr(critical_expr))
    quarterly_metric_df = quarterly_metric_df.withColumn("critical_reason", expr(critical_reason_expr))


    return quarterly_metric_df

dp.create_streaming_table("stock_market.silver.silver_metric_quarterly")

dp.create_auto_cdc_flow(
    target = "stock_market.silver.silver_metric_quarterly",
    source = "bronze_metric_quarterly_vw",
    keys = ["symbol","period"],
    sequence_by = col("ingest_timestamp"),
    stored_as_scd_type = "1"
    )