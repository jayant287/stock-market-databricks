from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
dq_volume_path = spark.conf.get('dq_path')

with open(dq_volume_path) as f:
    rules = json.load(f)

critical_rules = rules['financials_reported']['critical']
warn_rules = rules['financials_reported']['warn']
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
    name = "bronze_financials_reported_validation",
    private = True
)
@dp.expect_all(critical_rules)
@dp.expect_all(warn_rules)
def bronze_financials_reported_validation():

    df = spark.readStream.table("stock_market.bronze.bronze_financials_reported")
    
    df = df.select(
            col("symbol"),
            col("cik").alias("cik"),
            explode(col("data")).alias("data"),
            col("ingest_timestamp")
        )

    financial_reported_bs_df = (
        df.select(
            col("symbol"),
            col("cik"),
            col("data.accessNumber").alias("accessNumber"),
            col("data.year").alias("year"),
            col("data.quarter").alias("quarter"),
            col("data.startDate").alias("startDate"),
            col("data.endDate").alias("endDate"),
            col("data.filedDate").alias("filedDate"),
            explode(col("data.report.bs")).alias("bs"),
            col("ingest_timestamp")
        )
    )
    financial_reported_ic_df = (
        df.select(
            col("symbol"),
            col("cik"),
            col("data.accessNumber").alias("accessNumber"),
            col("data.year").alias("year"),
            col("data.quarter").alias("quarter"),
            col("data.startDate").alias("startDate"),
            col("data.endDate").alias("endDate"),
            col("data.filedDate").alias("filedDate"),
            explode(col("data.report.ic")).alias("ic"),
            col("ingest_timestamp")
        )
    )

    financial_reported_cf_df = (
        df.select(
            col("symbol"),
            col("cik"),
            col("data.accessNumber").alias("accessNumber"),
            col("data.year").alias("year"),
            col("data.quarter").alias("quarter"),
            col("data.startDate").alias("startDate"),
            col("data.endDate").alias("endDate"),
            col("data.filedDate").alias("filedDate"),
            explode(col("data.report.cf")).alias("cf"),
            col("ingest_timestamp")
        )
    )

    financial_reported_bs_df = financial_reported_bs_df.select(
        col("symbol"),
        col("cik"),
        col("accessNumber"),
        col("year"),
        col("quarter"),
        col("startDate"),
        col("endDate"),
        col("filedDate"),
        col("bs.concept").alias("concept"),
        col("bs.unit").alias("unit"),
        col("bs.label").alias("label"),
        col("bs.value").try_cast("double").alias("value"),
        col("ingest_timestamp")
    )

    financial_reported_ic_df = financial_reported_ic_df.select(
        col("symbol"),
        col("cik"),
        col("accessNumber"),
        col("year"),
        col("quarter"),
        col("startDate"),
        col("endDate"),
        col("filedDate"),
        col("ic.concept").alias("concept"),
        col("ic.unit").alias("unit"),
        col("ic.label").alias("label"),
        col("ic.value").try_cast("double").alias("value"),
        col("ingest_timestamp")
    )

    financial_reported_cf_df = financial_reported_cf_df.select(
        col("symbol"),
        col("cik"),
        col("accessNumber"),
        col("year"),
        col("quarter"),
        col("startDate"),
        col("endDate"),
        col("filedDate"),
        col("cf.concept").alias("concept"),
        col("cf.unit").alias("unit"),
        col("cf.label").alias("label"),
        col("cf.value").try_cast("double").alias("value"),
        col("ingest_timestamp")
    )

    financial_reported_df = (
        financial_reported_bs_df.unionByName(financial_reported_ic_df).unionByName(financial_reported_cf_df)
    )

    financial_reported_df = financial_reported_df.dropDuplicates(["symbol","cik","accessNumber","year","quarter","concept"])

    financial_reported_df = financial_reported_df.withColumn("is_quarantined", expr(quarantine_expr))
    financial_reported_df = financial_reported_df.withColumn("quarantine_reason", expr(quarantine_reason_expr))

    return financial_reported_df

@dp.view(name="bronze_financials_reported_vw")
def bronze_financials_reported_vw():

    financial_reported_df = spark.readStream.table("bronze_financials_reported_validation")

    financial_reported_df = financial_reported_df.filter("is_quarantined = false")

    financial_reported_df = financial_reported_df.drop("is_quarantined","quarantine_reason")

    return financial_reported_df

@dp.table(
    name="stock_market.quarantine.quarantine_financial_reported"
    )
def quarantine_financial_reported():

    financial_reported_df = spark.readStream.table("bronze_financials_reported_validation")

    financial_reported_quarantined_df = financial_reported_df.filter("is_quarantined = true")

    return financial_reported_quarantined_df

dp.create_streaming_table("stock_market.silver.silver_financials_reported")

dp.create_auto_cdc_flow(
    target = "stock_market.silver.silver_financials_reported",
    source = "bronze_financials_reported_vw",
    keys = ["symbol","cik","accessNumber","year","quarter","concept"],
    sequence_by = col("ingest_timestamp"),
    stored_as_scd_type = "1"
    )
