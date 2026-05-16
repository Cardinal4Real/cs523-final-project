import json
import happybase
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, min, max,
    sum, when, lit, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    LongType, DoubleType, BooleanType
)

# ── Spark Session ────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("CryptoTradeToHBase") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✓ Spark session started")

# ── Schema ───────────────────────────────────────────────────
trade_schema = StructType([
    StructField("event_type",  StringType(),  True),
    StructField("event_time",  LongType(),    True),
    StructField("symbol",      StringType(),  True),
    StructField("trade_id",    LongType(),    True),
    StructField("price",       DoubleType(),  True),
    StructField("quantity",    DoubleType(),  True),
    StructField("buyer_maker", BooleanType(), True),
    StructField("trade_time",  LongType(),    True),
])

# ── Read from Kafka ──────────────────────────────────────────
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto-trades") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

trades = raw_stream \
    .select(from_json(col("value").cast("string"), trade_schema).alias("d")) \
    .select("d.*") \
    .withColumn("timestamp", current_timestamp())

# ── Transformation 1: Windowed Aggregation ───────────────────
windowed_agg = trades \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(window(col("timestamp"), "30 seconds"), col("symbol")) \
    .agg(
        avg("price").alias("avg_price"),
        min("price").alias("min_price"),
        max("price").alias("max_price"),
        sum("quantity").alias("total_volume"),
    )

# ── Transformation 2: Moving Average ────────────────────────
moving_avg = trades \
    .withWatermark("timestamp", "3 minutes") \
    .groupBy(window(col("timestamp"), "2 minutes", "30 seconds"), col("symbol")) \
    .agg(
        avg("price").alias("moving_avg_price"),
        sum("quantity").alias("total_volume"),
    )

# ── Transformation 3: Anomaly Detection ─────────────────────
anomalies = trades \
    .withWatermark("timestamp", "2 minutes") \
    .groupBy(window(col("timestamp"), "1 minute"), col("symbol")) \
    .agg(
        avg("price").alias("avg_price"),
        max("price").alias("max_price"),
        min("price").alias("min_price"),
    ) \
    .withColumn(
        "price_range_pct",
        (col("max_price") - col("min_price")) / col("avg_price") * 100
    ) \
    .withColumn(
        "is_anomaly",
        when(col("price_range_pct") > 0.5, lit(True)).otherwise(lit(False))
    )

# ── HBase write function ─────────────────────────────────────
def write_windowed_to_hbase(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return
    try:
        conn = happybase.Connection("localhost")
        table = conn.table("crypto_windowed")
        for row in rows:
            # Row key: SYMBOL_windowstart (e.g. BTCUSD_2026-05-09T14:20:00)
            row_key = f"{row['symbol']}_{row['window']['start']}".encode()
            table.put(row_key, {
                b"price:avg":    str(round(row["avg_price"], 6)).encode(),
                b"price:min":    str(round(row["min_price"], 6)).encode(),
                b"price:max":    str(round(row["max_price"], 6)).encode(),
                b"volume:total": str(round(row["total_volume"], 6)).encode(),
            })
        conn.close()
        print(f"[HBase] windowed_agg batch {batch_id} → {len(rows)} rows written")
    except Exception as e:
        print(f"[HBase ERROR] windowed_agg: {e}")

def write_moving_avg_to_hbase(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return
    try:
        conn = happybase.Connection("localhost")
        table = conn.table("crypto_moving_avg")
        for row in rows:
            row_key = f"{row['symbol']}_{row['window']['start']}_{row['window']['end']}".encode()
            table.put(row_key, {
                b"price:moving_avg": str(round(row["moving_avg_price"], 6)).encode(),
                b"volume:total":     str(round(row["total_volume"], 6)).encode(),
            })
        conn.close()
        print(f"[HBase] moving_avg batch {batch_id} → {len(rows)} rows written")
    except Exception as e:
        print(f"[HBase ERROR] moving_avg: {e}")

def write_anomalies_to_hbase(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return
    try:
        conn = happybase.Connection("localhost")
        table = conn.table("crypto_anomalies")
        for row in rows:
            row_key = f"{row['symbol']}_{row['window']['start']}".encode()
            table.put(row_key, {
                b"price:avg":           str(round(row["avg_price"], 6)).encode(),
                b"price:max":           str(round(row["max_price"], 6)).encode(),
                b"price:min":           str(round(row["min_price"], 6)).encode(),
                b"alert:price_range_pct": str(round(row["price_range_pct"], 6)).encode(),
                b"alert:is_anomaly":    str(row["is_anomaly"]).encode(),
            })
        conn.close()
        print(f"[HBase] anomalies batch {batch_id} → {len(rows)} rows written")
    except Exception as e:
        print(f"[HBase ERROR] anomalies: {e}")

# ── Sink to HBase using foreachBatch ────────────────────────
q1 = windowed_agg.writeStream \
    .outputMode("update") \
    .foreachBatch(write_windowed_to_hbase) \
    .queryName("windowed_to_hbase") \
    .option("checkpointLocation", "/tmp/checkpoint/windowed") \
    .start()

q2 = moving_avg.writeStream \
    .outputMode("update") \
    .foreachBatch(write_moving_avg_to_hbase) \
    .queryName("moving_avg_to_hbase") \
    .option("checkpointLocation", "/tmp/checkpoint/moving_avg") \
    .start()

q3 = anomalies.writeStream \
    .outputMode("update") \
    .foreachBatch(write_anomalies_to_hbase) \
    .queryName("anomalies_to_hbase") \
    .option("checkpointLocation", "/tmp/checkpoint/anomalies") \
    .start()

print("✓ All 3 queries streaming to HBase...")
print("  → crypto_windowed:   avg/min/max price per 30s window")
print("  → crypto_moving_avg: 2-minute rolling average")
print("  → crypto_anomalies:  anomaly flags per 1-minute window")

spark.streams.awaitAnyTermination()
