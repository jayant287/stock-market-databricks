from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table(
    name="stock_market.bronze.bronze_profile",
    spark_conf = {
        "spark.sql.session.timeZone":"America/New_York"
    }
    )
def bronze_profile():

    df = spark.readStream.format("cloudfiles")\
        .option("cloudFiles.format", "json")\
        .option("cloudFiles.schemaLocation", "abfss://bronze@adfpracticestorage12.dfs.core.windows.net/profile/checkpoint")\
        .option("cloudFiles.useManagedFileEvents", True)\
        .load("abfss://destination@adfpracticestorage12.dfs.core.windows.net/profile/")

    # COMMAND ----------

    df = df.select(
        col("*"),
        col("_metadata.file_name").alias("file_name"),
        col("_metadata.file_size").alias("file_size"),
        col("_metadata.file_path").alias("file_path"),
        current_timestamp().alias("ingest_timestamp")
    )
    

    return df