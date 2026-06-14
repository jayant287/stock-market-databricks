from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dp.table(
    name="stock_market.gold.fact_realtime_agg",
    table_properties={
        "pipelines.autoOptimize.managed":"true"
    }
    )
def fact_realtime_agg():
    silver_quote_eh_df = spark.readStream.table("stock_market.silver.silver_quote_eh")

    exclude_array = array([lit(x) for x in ["4","7","9","11","12","14","22","23","27","29","33","38","52","53"]])


    is_valid_trade = ~arrays_overlap(col("conditions"), exclude_array)
    watermarked_df = silver_quote_eh_df.withWatermark("timestamp", "30 seconds")


    final_df = watermarked_df.groupBy("symbol", 
                                        window(col("timestamp"), "1 minute")).agg(
        min_by("price", "timestamp").alias("open_price"),
        max_by("price", "timestamp").alias("close_price"),
        sum("volume").alias("volume"),
        
        
        min(when(is_valid_trade, col("price"))).alias("low_price"),
        max(when(is_valid_trade, col("price"))).alias("high_price"),
        first("previous_close_price", ignorenulls=True).alias("previous_close_price"),
        count("symbol").alias("snapshot_count")
    )

    
    final_df = final_df.withColumn("change", col("close_price") - col("previous_close_price")) \
                        .withColumn("percent_change", (col("change") / col("previous_close_price")) * 100)

    final_df = final_df.select(
        col("symbol"),
        to_date(col("window.start")).alias("session_date"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("open_price"),
        col("high_price"),
        col("low_price"),
        col("close_price"),
        col("volume"),
        col("previous_close_price"),
        col("snapshot_count"),
        col("change"),
        col("percent_change"),
        (col("high_price") - col("low_price")).alias("intrawindow_spread"),
        round(((col("close_price") - col("previous_close_price")) / col("previous_close_price")) * 100, 4).alias("price_drift_pct")
    )

    price_anomaly_flag_expr = """
    CASE WHEN ABS(price_drift_pct) > 3.0 THEN TRUE ELSE FALSE END
    """

    final_df = final_df.withColumn("price_anomaly_flag", expr(price_anomaly_flag_expr))

    return final_df
