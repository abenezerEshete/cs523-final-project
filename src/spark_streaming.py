from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, min, max, sum,
    when, abs, lit, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    LongType, DoubleType, BooleanType
)

# ── Spark Session ────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("CryptoTradeStreaming") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "3") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✓ Spark session started")

# ── Schema matching our Kafka producer messages ──────────────
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

print("✓ Connected to Kafka topic: crypto-trades")

# ── Parse JSON payload ───────────────────────────────────────
trades = raw_stream \
    .select(from_json(col("value").cast("string"), trade_schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", current_timestamp())

# ════════════════════════════════════════════════════════════
# TRANSFORMATION 1: Windowed Aggregation (30-second windows)
# Avg / Min / Max price + total volume per symbol
# ════════════════════════════════════════════════════════════
windowed_agg = trades \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(
        window(col("timestamp"), "30 seconds"),
        col("symbol")
    ) \
    .agg(
        avg("price").alias("avg_price"),
        min("price").alias("min_price"),
        max("price").alias("max_price"),
        sum("quantity").alias("total_volume"),
    ) \
    .select(
        col("symbol"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_price"),
        col("min_price"),
        col("max_price"),
        col("total_volume"),
    )

# ════════════════════════════════════════════════════════════
# TRANSFORMATION 2: Moving Average (2-minute sliding window)
# Slides every 30 seconds
# ════════════════════════════════════════════════════════════
moving_avg = trades \
    .withWatermark("timestamp", "3 minutes") \
    .groupBy(
        window(col("timestamp"), "2 minutes", "30 seconds"),
        col("symbol")
    ) \
    .agg(
        avg("price").alias("moving_avg_price"),
        sum("quantity").alias("total_volume"),
    ) \
    .select(
        col("symbol"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("moving_avg_price"),
        col("total_volume"),
    )

# ════════════════════════════════════════════════════════════
# TRANSFORMATION 3: Anomaly Detection
# Flag trades where price deviates > 0.5% from a reference
# Uses a 1-minute window avg as the baseline
# ════════════════════════════════════════════════════════════
baseline = trades \
    .withWatermark("timestamp", "2 minutes") \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("symbol")
    ) \
    .agg(avg("price").alias("baseline_price"))

anomalies = trades.alias("t") \
    .withWatermark("timestamp", "2 minutes") \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("symbol")
    ) \
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
    ) \
    .select(
        col("symbol"),
        col("window.start").alias("window_start"),
        col("avg_price"),
        col("max_price"),
        col("min_price"),
        col("price_range_pct"),
        col("is_anomaly"),
    )

# ── Output queries → console ─────────────────────────────────
q1 = windowed_agg.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 20) \
    .queryName("windowed_aggregation") \
    .start()

q2 = moving_avg.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 20) \
    .queryName("moving_average") \
    .start()

q3 = anomalies.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 20) \
    .queryName("anomaly_detection") \
    .start()

print("✓ All 3 streaming queries running. Waiting for data...")
print("  → windowed_aggregation: avg/min/max price per 30s window")
print("  → moving_average:       2-minute rolling mean per symbol")
print("  → anomaly_detection:    flags price range > 0.5% per minute")

spark.streams.awaitAnyTermination()
