from pyspark import pipelines as dp
from pyspark.sql.functions import *
import json
dq_volume_path = spark.conf.get('dq_path')

with open(dq_volume_path) as f:
    rules = json.load(f)

critical_rules = rules['metric_snapshot']['critical']
warn_rules = rules['metric_snapshot']['warn']
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
@dp.table(name="stock_market.silver.silver_metric_snapshot")
def silver_metric_snapshot():

    metric_df = spark.readStream.table("stock_market.bronze.bronze_metric")

    metric_df = metric_df.withColumn("timestamp",from_utc_timestamp(col("timestamp"), 'America/New_York'))
    
    metric_df = metric_df.selectExpr(
    "metric.*",
    "symbol",
    "to_date(timestamp) as metric_date",
    "ingest_timestamp"
    )
    metric_df = metric_df.dropDuplicates(["symbol","metric_date"])

    metric_df = metric_df.withColumn("is_critical", expr(critical_expr))
    metric_df = metric_df.withColumn("critical_reason", expr(critical_reason_expr))

    return metric_df