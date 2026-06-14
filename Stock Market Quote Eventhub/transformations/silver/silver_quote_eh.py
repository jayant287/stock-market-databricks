from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType
from pyspark.sql.window import Window
from pyspark import pipelines as dp

@dp.table(
    name="stock_market.silver.silver_quote_eh",
    table_properties={
        "pipelines.autoOptimize.managed":"true"
    }
    )
def silver_quote_eh():
    quote_eh_df = spark.readStream.table("stock_market.bronze.bronze_quote_eh")
    finnhub_schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("volume", LongType(), True),
        StructField("conditions", ArrayType(StringType()), True),
        StructField("timestamp", LongType(), True)
    ])

    quote_eh_df_new = quote_eh_df.select(
        from_json(col("value_str"),finnhub_schema).alias("value_str"),
        col("timestamp").alias("ingest_timestamp")
    )
    quote_eh_df_new = quote_eh_df_new.select(
        col("value_str.*"),
        col("ingest_timestamp")
    )
    quote_eh_df_new = quote_eh_df_new.withColumn("timestamp",(col("timestamp") / 1000).cast("timestamp"))
    quote_eh_df_new = quote_eh_df_new.withColumn("session_date",to_date(col("timestamp")))
    silver_quote_static = spark.read.table("stock_market.silver.silver_quote")

    
    latest_row_window = Window.partitionBy("symbol").orderBy(col("event_timestamp").desc())

    latest_pc_df = silver_quote_static.withColumn("row_num", row_number().over(latest_row_window))

    latest_pc_df = latest_pc_df.filter("row_num = 1")

    latest_pc_df = latest_pc_df.select(
        col("symbol").alias("ref_symbol"),
        col("previous_close_price")
        )

    final_df = quote_eh_df_new.alias("oc").join(
        latest_pc_df.alias("lh"),
        (col("oc.symbol") == col("lh.ref_symbol")),
        "left"
    ).select(
        col("oc.*"),
        col("previous_close_price").cast("double")
    )
    return final_df