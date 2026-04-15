from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, current_timestamp, to_date
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType,
    DoubleType, TimestampType,
    BooleanType
)
# from pyspark.sql.streaming import GroupState, GroupStateTimeout

import math
from datetime import timedelta
from dotenv import load_dotenv
import os


# -----------------------------------
# 1. Spark session
# -----------------------------------
def create_spark_session(app_name="anomaly-detector"):

    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    # spark.conf.set("spark.sql.shuffle.partitions", "200") # maybe set better default?
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark

def update_state(user_id, rows, state):
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



def build_stream(spark, out_path, ckpt_path):
    # load necessary dotenv vars
    load_dotenv()
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    KAFKA_API_KEY = os.getenv("KAFKA_API_KEY")
    KAFKA_API_SECRET = os.getenv("KAFKA_API_SECRET")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")


    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("last_updated", TimestampType(), True),
        StructField("volume_24h", DoubleType(), True),
        StructField("volume_change_24h", DoubleType(), True),
        StructField("percent_change_1h", DoubleType(), True),
        StructField("percent_change_24h", DoubleType(), True),
    ])

    # -----------------------------------
    # 3. Read from Kafka
    # -----------------------------------

    # Source - https://stackoverflow.com/a/63948372
    # Posted by YoongKang Lim
    # Retrieved 2026-04-07, License - CC BY-SA 4.0
    confluent_connection_string = f"""org.apache.kafka.common.security.plain.PlainLoginModule required username="{KAFKA_API_KEY}" password="{KAFKA_API_SECRET}";"""
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("kafka.security.protocol","SASL_SSL") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", confluent_connection_string) \
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

    # console_write_stream = parsed_df.writeStream \
    #     .format("console") \
    #     .outputMode("update") \
    #     .start()

    # try:
    #     console_write_stream.awaitTermination()
    # except KeyboardInterrupt:
    #     console_write_stream.stop()
    # finally:
    #     spark.stop()

    # -----------------------------------
    # 4. Add watermark (for state cleanup)
    # -----------------------------------
    watermarked_df = parsed_df.withWatermark("event_ts", "3 hours")

    # -----------------------------------
    # 5. Stateful anomaly detection
    # -----------------------------------

    # Apply stateful processing
    result_df = watermarked_df.groupByKey(lambda row: row.id) \
        .flatMapGroupsWithState(
            outputMode="append",
            updateFunction=update_state,
            timeoutConf=GroupStateTimeout.EventTimeTimeout
        )

    # Define schema for result

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
    raw_df = raw_df.withColumn("event_dt", to_date("event_ts"))
    alerts_df = alerts_df.withColumn("event_dt", to_date("event_ts"))

    # -----------------------------------
    # 7. Write raw data to CSV
    # -----------------------------------
    raw_output_path = out_path + "/raw_output"
    raw_output_checkpoint_path = ckpt_path + "/raw_output"

    alert_path = out_path + "/raw_alert"
    alert_checkpoint_path = ckpt_path + "/raw_alert"

    raw_query = raw_df.writeStream \
        .format("csv") \
        .option("path", raw_output_path) \
        .option("checkpointLocation", raw_output_checkpoint_path) \
        .partitionBy("event_dt") \
        .outputMode("append") \
        .start()

    # -----------------------------------
    # 8. Write alerts to CSV
    # -----------------------------------
    alerts_query = alerts_df.writeStream \
        .format("csv") \
        .option("path", alert_path) \
        .option("checkpointLocation", alert_checkpoint_path) \
        .partitionBy("event_dt") \
        .outputMode("append") \
        .start()

    return [raw_query, alerts_query]
    # -----------------------------------
    # 9. Await termination
    # -----------------------------------
    spark.streams.awaitAnyTermination()

def main():
    spark_session = create_spark_session()
    print("spark_session_created")
    try:
        print("Trying build_stream()")
        queries = build_stream(spark_session, "/home/compute/d.linus/data-engineering-anomaly-detector/output", "/home/compute/d.linus/data-engineering-anomaly-detector/checkpoints")
        for q in queries:
            q.awaitTermination()
        print("Finished build_stream()")
    except KeyboardInterrupt:
        for q in queries:
            q.stop()
        print("Exiting gracefully from keyboard interrupt")
    except Exception as e:
        print("Exception encountered")
        print(e)

if __name__ == "__main__":
    # trying to submit using: "spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 spark_code.py"
    main()