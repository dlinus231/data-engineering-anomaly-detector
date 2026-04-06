from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, current_timestamp
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType,
    DoubleType, TimestampType
)
from pyspark.sql.streaming import GroupState, GroupStateTimeout

import math
from datetime import timedelta

# -----------------------------------
# 1. Spark session
# -----------------------------------
spark = SparkSession.builder \
    .appName("3hr-anomaly-detector") \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", "200")

# -----------------------------------
# 2. Schema
# -----------------------------------

# if cols are nested JSON
# schema = StructType([
#     StructField("id", IntegerType(), True),
#     StructField("name", StringType(), True),
#     StructField("symbol", StringType(), True),

#     StructField("quote", StructType([
#         StructField("USD", StructType([
#             StructField("price", DoubleType(), True),
#             StructField("last_updated", TimestampType(), True),
#             StructField("volume_24h", DoubleType(), True),
#             StructField("volume_change_24h", DoubleType(), True),
#             StructField("percent_change_1h", DoubleType(), True),
#             StructField("percent_change_24h", DoubleType(), True),
#         ]), True)
#     ]), True)
# ])

# if cols are flattened
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("quote.USD.price", DoubleType(), True),
    StructField("quote.USD.last_updated", TimestampType(), True),
    StructField("quote.USD.volume_24h", DoubleType(), True),
    StructField("quote.USD.volume_change_24h", DoubleType(), True),
    StructField("quote.USD.percent_change_1h", DoubleType(), True),
    StructField("quote.USD.percent_change_24h", DoubleType(), True),
])

# -----------------------------------
# 3. Read from Kafka
# -----------------------------------
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "input-topic") \
    .option("startingOffsets", "latest") \
    .load()


parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select(
    col("data.id"),
    col("data.name"),
    col("data.symbol"),

    col("data.price").alias("price"),
    col("data.last_updated").alias("event_ts"),
    col("data.volume_24h"),
    col("data.volume_change_24h"),
    col("data.percent_change_1h"),
    col("data.percent_change_24h"),

    current_timestamp().alias("processing_time")
)

# -----------------------------------
# 4. Add watermark (for state cleanup)
# -----------------------------------
df = parsed_df.withWatermark("event_ts", "3 hours")

# -----------------------------------
# 5. Stateful anomaly detection
# -----------------------------------

def update_state(user_id, rows, state: GroupState):
    """
    Maintain last 3 hours of values per user_id.
    Compute mean/std and emit anomalies.
    """

    # state = list of (timestamp, value)
    history = state.get() if state.exists else []

    outputs = []

    THREE_HOURS = 3 * 60 * 60  # seconds

    for row in rows:
        current_ts = row.event_ts.timestamp()
        val = row.value

        # 1. Add new event
        history.append((current_ts, val))

        # 2. Evict old events
        history = [
            (ts, v) for (ts, v) in history
            if current_ts - ts <= THREE_HOURS
        ]

        # 3. Compute stats
        values = [v for (_, v) in history]

        if len(values) > 1:
            mean = sum(values) / len(values)
            variance = sum((v - mean) ** 2 for v in values) / len(values)
            std = math.sqrt(variance)

            # 4. Detect anomaly
            if std > 0 and abs(val - mean) > 3 * std:
                outputs.append((
                    user_id,
                    row.event_ts,
                    val,
                    mean,
                    std,
                    True  # is_anomaly
                ))
            else:
                outputs.append((
                    user_id,
                    row.event_ts,
                    val,
                    mean,
                    std,
                    False
                ))
        else:
            outputs.append((
                user_id,
                row.event_ts,
                val,
                val,
                0.0,
                False
            ))

    # update state
    state.update(history)

    # set timeout for cleanup
    state.setTimeoutDuration("3 hours")

    return outputs


# Apply stateful processing
result_df = df.groupBy("user_id").flatMapGroupsWithState(
    outputMode="append",
    updateFunction=update_state,
    timeoutConf=GroupStateTimeout.EventTimeTimeout
)

# Define schema for result
from pyspark.sql.types import StructType, StructField, BooleanType

result_schema = StructType([
    StructField("user_id", StringType()),
    StructField("event_ts", TimestampType()),
    StructField("value", DoubleType()),
    StructField("mean", DoubleType()),
    StructField("std", DoubleType()),
    StructField("is_anomaly", BooleanType())
])

result_df = spark.createDataFrame(result_df, result_schema)

# -----------------------------------
# 6. Split outputs
# -----------------------------------
raw_df = result_df
alerts_df = result_df.filter(col("is_anomaly") == True)

# Add partition column
from pyspark.sql.functions import to_date

raw_df = raw_df.withColumn("event_dt", to_date("event_ts"))
alerts_df = alerts_df.withColumn("event_dt", to_date("event_ts"))

# -----------------------------------
# 7. Write raw data to CSV
# -----------------------------------
raw_query = raw_df.writeStream \
    .format("csv") \
    .option("path", "/tmp/raw_output") \
    .option("checkpointLocation", "/tmp/checkpoints/raw") \
    .partitionBy("event_dt") \
    .outputMode("append") \
    .start()

# -----------------------------------
# 8. Write alerts to CSV
# -----------------------------------
alerts_query = alerts_df.writeStream \
    .format("csv") \
    .option("path", "/tmp/alerts_output") \
    .option("checkpointLocation", "/tmp/checkpoints/alerts") \
    .partitionBy("event_dt") \
    .outputMode("append") \
    .start()

# -----------------------------------
# 9. Await termination
# -----------------------------------
spark.streams.awaitAnyTermination()