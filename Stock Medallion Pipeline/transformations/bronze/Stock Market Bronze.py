# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from datetime import datetime

# COMMAND ----------

file_name = dbutils.widgets.get("file_name")

bucket_name = dbutils.widgets.get("bucket_name")

ddl_string = dbutils.widgets.get("DDL_SCHEMAS")

ddl_string = json.loads(ddl_string)

bronze_path = f"s3://{bucket_name}/bronze/"

source_prefix_path = f"s3://{bucket_name}/source/"

# COMMAND ----------


date_str = datetime.now().strftime("%Y%m%d")

# COMMAND ----------

# file_name = dbutils.widgets.get("file_name")
src_path = f"{source_prefix_path}{file_name}/"
checkpoint_path = f"{bronze_path}{file_name}/checkpoint/"
output_path = f"{bronze_path}{file_name}/"

# COMMAND ----------

print(f"file_name: {file_name}")
print(f"bucket_name: {bucket_name}")
print(f"src_path: {src_path}")
print(f"checkpoint_path: {checkpoint_path}")
print(f"output_path: {output_path}")

# COMMAND ----------

df = spark.readStream.format("cloudfiles")\
    .option("cloudFiles.format", "json")\
    .option("multiline","true")\
    .schema(ddl_string[file_name])\
    .option("cloudFiles.schemaLocation", checkpoint_path)\
    .load(src_path)

# COMMAND ----------

df = df.select(
    col("*"),
    col("_metadata.file_name").alias("file_name"),
    col("_metadata.file_size").alias("file_size"),
    col("_metadata.file_path").alias("file_path"),
    current_timestamp().alias("ingest_timestamp")
)

# COMMAND ----------

df.writeStream.format("delta")\
            .outputMode("append")\
            .trigger(availableNow=True)\
            .option("checkpointLocation", checkpoint_path)\
            .option("path",output_path)\
            .start()

# COMMAND ----------

