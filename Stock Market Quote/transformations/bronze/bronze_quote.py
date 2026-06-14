from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dp.table(
    name="stock_market.bronze.bronze_quote",
    spark_conf = {
        "spark.sql.session.timeZone":"America/New_York"
    }
    )
def bronze_quote():

    df = spark.readStream.format("cloudfiles")\
        .option("cloudFiles.format", "json")\
        .option("cloudFiles.schemaLocation", "abfss://bronze@adfpracticestorage12.dfs.core.windows.net/quote/checkpoint")\
        .option("cloudFiles.useManagedFileEvents", True)\
        .option("schemaEvolutionMode","rescue")\
        .load("abfss://destination@adfpracticestorage12.dfs.core.windows.net/quote")
    # COMMAND ----------

    df = df.select(
        col("*"),
        col("_metadata.file_name").alias("file_name"),
        col("_metadata.file_size").alias("file_size"),
        col("_metadata.file_path").alias("file_path"),
        current_timestamp().alias("ingest_timestamp")
    )
    

    return df