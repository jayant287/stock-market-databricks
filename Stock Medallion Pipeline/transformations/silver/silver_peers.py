from pyspark import pipelines as dp
from pyspark.sql.functions import *
from utilities import utils

stock_peers_expect_drop = { "symbol_not_null": "symbol IS NOT NULL", "peer_symbol_not_null": "peer IS NOT NULL", "symbol_not_equal_peer": "symbol != peer", "ingest_timestamp_not_null": "ingest_timestamp IS NOT NULL" }
@dp.view(name="bronze_peers")
@dp.expect_all_or_drop(stock_peers_expect_drop)
def bronze_peers():

    return (
        spark.readStream.format("delta")
        .load("s3://stock-market-fin/bronze/peers/")
        .select(
            col("symbol"),
            explode(col("peers")).alias("peer"),
            col("ingest_timestamp")
        )
        .filter(col("symbol") != col("peer"))
    )


dp.create_streaming_table("stock_market.silver.silver_peers")

dp.create_auto_cdc_flow(
    target="stock_market.silver.silver_peers",
    source="bronze_peers",
    keys=["symbol", "peer"],
    sequence_by=struct("ingest_timestamp", "peer"),
    stored_as_scd_type="2",
    track_history_except_column_list=["ingest_timestamp"]
)