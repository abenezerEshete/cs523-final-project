from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, min, max,
    sum, when, lit, current_timestamp, round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    LongType, DoubleType, BooleanType
)
import happybase

# ── Spark Session ────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("CryptoEnrichedStreaming") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✓ Spark session started")

# ════════════════════════════════════════════════════════════
# PART 5: Load static dataset from HDFS
# Broadcast it so every executor has a local copy for fast joins
# ════════════════════════════════════════════════════════════
print("Loading static coin metadata from HDFS...")

static_metadata = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("hdfs://localhost:9000/cs523/static/coin_metadata.csv") \
    .cache()   # cache in memory — it never changes

print("✓ Static metadata loaded from HDFS:")
static_metadata.show()

# ── Trade stream schema ───────────────────────────────────────
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

# ── Read from Kafka ───────────────────────────────────────────
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

# ── Windowed aggregation ──────────────────────────────────────
windowed_agg = trades \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(window(col("timestamp"), "30 seconds"), col("symbol")) \
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
# foreachBatch: join streaming batch with static HDFS data
# using the DataFrame API (no temp view registration needed)
# ════════════════════════════════════════════════════════════
def enrich_and_write(batch_df, batch_id):
    if batch_df.count() == 0:
        print(f"[Batch {batch_id}] Empty, skipping.")
        return

    # ── Spark SQL join via DataFrame API ─────────────────────
    # Join streaming batch with static HDFS metadata DataFrame
    enriched = batch_df.join(
            static_metadata,
            on="symbol",
            how="inner"
        ) \
        .withColumn("price_range_pct",
            spark_round(
                (col("max_price") - col("min_price")) / col("avg_price") * 100,
                6
            )
        ) \
        .withColumn("status",
            when(col("price_range_pct") > 0.5, lit("ANOMALY"))
            .otherwise(lit("NORMAL"))
        ) \
        .withColumn("avg_price",    spark_round(col("avg_price"), 4)) \
        .withColumn("min_price",    spark_round(col("min_price"), 4)) \
        .withColumn("max_price",    spark_round(col("max_price"), 4)) \
        .withColumn("total_volume", spark_round(col("total_volume"), 4)) \
        .select(
            "symbol",
            "full_name",
            "market_cap_tier",
            "sector",
            "launched_year",
            "window_start",
            "window_end",
            "avg_price",
            "min_price",
            "max_price",
            "total_volume",
            "price_range_pct",
            "status",
        ) \
        .orderBy("symbol", "window_start")

    print(f"\n{'='*80}")
    print(f"[Batch {batch_id}] Enriched — streaming join with HDFS static dataset:")
    print(f"{'='*80}")
    enriched.show(truncate=False)

    # ── Write enriched rows to HBase ─────────────────────────
    rows = enriched.collect()
    if not rows:
        return

    try:
        conn = happybase.Connection("localhost")
        table = conn.table("crypto_windowed")
        for row in rows:
            row_key = f"{row['symbol']}_{row['window_start']}".encode()
            table.put(row_key, {
                b"price:avg":       str(row["avg_price"]).encode(),
                b"price:min":       str(row["min_price"]).encode(),
                b"price:max":       str(row["max_price"]).encode(),
                b"price:range_pct": str(row["price_range_pct"]).encode(),
                b"price:status":    row["status"].encode(),
                b"volume:total":    str(row["total_volume"]).encode(),
                # Enriched columns from static HDFS dataset
                b"price:full_name": row["full_name"].encode(),
                b"price:tier":      row["market_cap_tier"].encode(),
                b"price:sector":    row["sector"].encode(),
                b"price:launched":  str(row["launched_year"]).encode(),
            })
        conn.close()
        print(f"[HBase] Wrote {len(rows)} enriched rows to crypto_windowed")
    except Exception as e:
        print(f"[HBase ERROR] {e}")

# ── Start streaming query ─────────────────────────────────────
query = windowed_agg.writeStream \
    .outputMode("update") \
    .foreachBatch(enrich_and_write) \
    .queryName("enriched_sql_join") \
    .option("checkpointLocation", "/tmp/checkpoint/enriched3") \
    .start()

print("✓ Enriched streaming query running...")
print("  → Kafka → Spark → DataFrame join with HDFS metadata → HBase")

spark.streams.awaitAnyTermination()
