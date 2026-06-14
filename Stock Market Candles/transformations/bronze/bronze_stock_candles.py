from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dp.table(name="stock_market.bronze.bronze_stock_candles")
def bronze_stock_candles():

    df = spark.readStream.format("cloudfiles")\
        .option("cloudFiles.format", "csv")\
        .option("cloudFiles.schemaLocation", "abfss://bronze@adfpracticestorage12.dfs.core.windows.net/stock_candles/checkpoint")\
        .option("cloudFiles.useManagedFileEvents", True)\
        .option("schemaEvolutionMode","rescue")\
        .load("abfss://destination@adfpracticestorage12.dfs.core.windows.net/stock_candles")

    # COMMAND ----------

    df = df.select(
        col("*"),
        col("_metadata.file_name").alias("file_name"),
        col("_metadata.file_size").alias("file_size"),
        col("_metadata.file_path").alias("file_path"),
        current_timestamp().alias("ingest_timestamp")
    )
    

    return df