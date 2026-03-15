from pyspark import pipelines as dp
from pyspark.sql.functions import *
from utilities import utils


@dp.table(
    name="stock_market.silver.silver_financials_reported"
)
def silver_financials_reported():
    df = (
        spark.readStream.format("delta")
        .load("s3://stock-market-fin/bronze/financials_reported/")
    )

    df = df.select(
        col("symbol"),
        col("financials_reported.cik").alias("cik"),
        explode(col("financials_reported.data")).alias("data")
    )

    financial_reported_df = (
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
            explode(col("data.report.ic")).alias("ic"),
            explode(col("data.report.cf")).alias("cf")
        )
    )
    financial_reported_df = financial_reported_df.select(
        col("symbol"),
        col("cik"),
        col("accessNumber"),
        col("year"),
        col("quarter"),
        col("startDate"),
        col("endDate"),
        col("filedDate"),
        col("bs.concept").alias("bs_concept"),
        col("bs.unit").alias("bs_unit"),
        col("bs.label").alias("bs_label"),
        col("bs.value").alias("bs_value"),
        col("ic.concept").alias("ic_concept"),
        col("ic.unit").alias("ic_unit"),
        col("ic.label").alias("ic_label"),
        col("ic.value").alias("ic_value"),
        col("cf.concept").alias("cf_concept"),
        col("cf.unit").alias("cf_unit"),
        col("cf.label").alias("cf_label"),
        col("cf.value").alias("cf_value")
    )
    return financial_reported_df