from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dp.table(
    name="stock_market.bronze.bronze_financials_reported",
    spark_conf = {
        "spark.sql.session.timeZone":"America/New_York"
    }
    )
def bronze_financials_reported():

    financials_schema = StructType([StructField('cik', StringType(), True), StructField('data', ArrayType(StructType([StructField('acceptedDate', StringType(), True), StructField('accessNumber', StringType(), True), StructField('cik', StringType(), True), StructField('endDate', StringType(), True), StructField('filedDate', StringType(), True), StructField('form', StringType(), True), StructField('quarter', LongType(), True), StructField('report', StructType([StructField('bs', ArrayType(StructType([StructField('concept', StringType(), True), StructField('label', StringType(), True), StructField('unit', StringType(), True), StructField('value', StringType(), True)]), True), True), StructField('cf', ArrayType(StructType([StructField('concept', StringType(), True), StructField('label', StringType(), True), StructField('unit', StringType(), True), StructField('value', StringType(), True)]), True), True), StructField('ic', ArrayType(StructType([StructField('concept', StringType(), True), StructField('label', StringType(), True), StructField('unit', StringType(), True), StructField('value', StringType(), True)]), True), True)]), True), StructField('startDate', StringType(), True), StructField('symbol', StringType(), True), StructField('year', LongType(), True)]), True), True), StructField('symbol', StringType(), True)])

    df = spark.readStream.format("cloudfiles")\
        .option("cloudFiles.format", "json")\
        .option("cloudFiles.schemaLocation", "abfss://bronze@adfpracticestorage12.dfs.core.windows.net/financials_reported/checkpoint")\
        .option("cloudFiles.useManagedFileEvents", True)\
        .option("schemaEvolutionMode","rescue")\
        .schema(financials_schema)\
        .load("abfss://destination@adfpracticestorage12.dfs.core.windows.net/financials_reported")

    # COMMAND ----------

    df = df.select(
        col("*"),
        col("_metadata.file_name").alias("file_name"),
        col("_metadata.file_size").alias("file_size"),
        col("_metadata.file_path").alias("file_path"),
        current_timestamp().alias("ingest_timestamp")
    )
    

    return df