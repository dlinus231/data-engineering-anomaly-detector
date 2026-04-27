from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, from_json, 
    to_timestamp, window, 
    current_timestamp, to_date, 
    avg, variance, 
    when, count, 
    struct, sha2,
    concat_ws
)
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType,
    DoubleType, TimestampType,
    BooleanType, ArrayType
)

import math
from datetime import timedelta
from dotenv import load_dotenv
import os
import traceback
import pandas as pd

# -----------------------------------
# 1. Spark session
# -----------------------------------
def create_spark_session(app_name="anomaly-detector"):

    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    # spark.conf.set("spark.sql.shuffle.partitions", "200") # maybe set better default?
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark

def make_update_state(min_z_score, min_data_points, window_size_hours, metric_column, timeout_hours):
    """Factory that closes over config so applyInPandasWithState signature is unchanged."""
    WINDOW_SIZE_IN_SECS = window_size_hours * 60 * 60
    TIMEOUT_MS = int(timeout_hours * 60 * 60 * 1000)

    def update_state(key, pdf_iter, state):
        if state.hasTimedOut:
            state.remove()
            return

        user_id = key[0]

        if state.exists:
            history = [(h[0], h[1]) for h in state.get[0]]
        else:
            history = []

        latest_ts = None

        for pdf in pdf_iter:
            outputs = []

            if pdf.empty:
                continue

            pdf = pdf.sort_values("event_ts")

            for _, row in pdf.iterrows():
                current_ts = row["event_ts"]
                val = row[metric_column]
                event_id = row["event_id"]
                latest_ts = current_ts

                history = [
                    h for h in history
                    if (current_ts - h[0]).total_seconds() <= WINDOW_SIZE_IN_SECS
                ]

                historical_timestamps, historical_values = zip(*history) if history else ([], [])
                history.append((current_ts, float(val)))

                if len(historical_values) > min_data_points:
                    mean = sum(historical_values) / len(historical_values)
                    variance = sum((v - mean) ** 2 for v in historical_values) / len(historical_values)
                    std = math.sqrt(variance)
                    z_score = abs(val - mean) / std if std > 0 else 0.0
                    is_anomaly = z_score > min_z_score

                    if is_anomaly:
                        outputs.append({
                            "id": user_id,
                            "event_id": event_id,
                            "event_ts": current_ts,
                            "curr_val": val,
                            "past_vals": historical_values,
                            "column_name_for_val_tracked": metric_column,
                            "past_timestamps": historical_timestamps,
                            "mean": mean,
                            "std": std,
                            "z_score": z_score
                        })

            if outputs:
                yield pd.DataFrame(outputs)

        state.update((history,))

        if latest_ts is not None:
            state.setTimeoutTimestamp(
                int(latest_ts.timestamp() * 1000) + TIMEOUT_MS
            )

    return update_state


    
def build_stream(spark, config):
    # load necessary dotenv vars
    load_dotenv()
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    KAFKA_API_KEY = os.getenv("KAFKA_API_KEY")
    KAFKA_API_SECRET = os.getenv("KAFKA_API_SECRET")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

    out_path = config["out_path"]
    ckpt_path = config["ckpt_path"]
    metric_column = config["metric_column"]
    watermark_duration = config["watermark_duration"]

    update_state = make_update_state(
        min_z_score=config["min_z_score"],
        min_data_points=config["min_data_points"],
        window_size_hours=config["window_size_hours"],
        metric_column=metric_column,
        timeout_hours=config["timeout_hours"]
    )

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
        col("data.id").alias("id"),
        col("data.name").alias("name"),
        col("data.symbol").alias("symbol"),

        col("data.price").alias("price"),
        col("data.last_updated").alias("event_ts"),
        col("data.volume_24h").alias("volume_24h"),
        col("data.volume_change_24h").alias("volume_change_24h"),
        col("data.percent_change_1h").alias("percent_change_1h"),
        col("data.percent_change_24h").alias("percent_change_24h"),
        current_timestamp().alias("processing_time")
    )

    # add a unique identifier for alert records to reference
    parsed_df = parsed_df.withColumn(
        "event_id", 
        sha2(
            concat_ws("||", col("id"), col("price"), col("event_ts")), 
            256
        )
    )

    # -----------------------------------
    # 4. Add watermark (for state cleanup)
    # -----------------------------------
    watermarked_df = parsed_df.withWatermark("event_ts", watermark_duration)


    # TODO: Define schema for result
    result_schema = StructType([
        StructField("id", IntegerType()),
        StructField("event_id", StringType()),
        StructField("event_ts", TimestampType()),
        StructField("curr_val", DoubleType()),
        StructField("past_vals", ArrayType(DoubleType())),
        StructField("column_name_for_val_tracked", StringType()),
        StructField("past_timestamps", ArrayType(TimestampType())),
        StructField("mean", DoubleType()),
        StructField("std", DoubleType()),
        StructField("z_score", DoubleType())
    ])
    state_schema = StructType([
        StructField(
            "history",
            ArrayType(
                StructType([
                    StructField("ts", TimestampType()),
                    StructField("value", DoubleType())
                ])
            )
        )
    ])

    result_df = watermarked_df \
        .withWatermark("event_ts", watermark_duration) \
        .groupBy("id") \
        .applyInPandasWithState(
            update_state,
            outputStructType=result_schema,
            stateStructType=state_schema,
            outputMode="append",
            timeoutConf="EventTimeTimeout"
        ) \
        .withColumn("past_vals", col("past_vals").cast("string")) \
        .withColumn("past_timestamps", col("past_timestamps").cast("string")) \
        .withColumn("alert_id", col("event_id"))

    # -----------------------------------
    # 6. Split outputs
    # -----------------------------------
    raw_df = parsed_df
    raw_df = raw_df.withColumn("event_dt", to_date("event_ts"))

    alerts_df = result_df
    alerts_df = alerts_df.withColumn("event_dt", to_date("event_ts"))

    # -----------------------------------
    # 7. Write raw events and alerts to CSV
    #
    # forEachBatch is bound to a single streaming DataFrame, so raw_df and
    # alerts_df each need their own writeStream query with separate checkpoints.
    # -----------------------------------
    raw_output_path = out_path + "/raw_output"
    raw_output_checkpoint_path = ckpt_path + "/raw_output"

    alert_path = out_path + "/alerts"
    alert_checkpoint_path = ckpt_path + "/alerts"

    def write_raw(batch_df, batch_id):
        batch_df.coalesce(1).write \
            .format("csv") \
            .mode("append") \
            .option("header", "true") \
            .partitionBy("event_dt") \
            .save(raw_output_path)

    def write_alerts(batch_df, batch_id):
        batch_df \
            .orderBy(col("id").asc(), col("z_score").desc()) \
            .coalesce(1) \
            .write \
            .format("csv") \
            .mode("append") \
            .option("header", "true") \
            .partitionBy("event_dt") \
            .save(alert_path)

    raw_query = raw_df.writeStream \
        .foreachBatch(write_raw) \
        .option("checkpointLocation", raw_output_checkpoint_path) \
        .trigger(processingTime="1 minute") \
        .start()

    # -----------------------------------
    # 8. Write alerts_df (anomaly detection output) to CSV
    # -----------------------------------
    alerts_query = alerts_df.writeStream \
        .foreachBatch(write_alerts) \
        .option("checkpointLocation", alert_checkpoint_path) \
        .trigger(processingTime="1 minute") \
        .start()

    return [raw_query, alerts_query]

def main():
    load_dotenv()
    config = {
        "out_path":           os.getenv("OUT_PATH"),
        "ckpt_path":          os.getenv("CKPT_PATH"),
        "min_z_score":        float(os.getenv("MIN_Z_SCORE_FOR_ANOMALY", 2.0)),
        "min_data_points":    int(os.getenv("MIN_DATA_POINTS", 10)),
        "window_size_hours":  float(os.getenv("WINDOW_SIZE_HOURS", 1)),
        "metric_column":      os.getenv("METRIC_COLUMN", "percent_change_1h"),
        "timeout_hours":      float(os.getenv("TIMEOUT_HOURS", 1)),
        "watermark_duration": os.getenv("WATERMARK_DURATION", "3 hours"),
    }
    
    spark_session = create_spark_session()
    print("spark_session_created")
    try:
        print("Trying build_stream()")
        queries = build_stream(spark_session, config)
        for q in queries:
            q.awaitTermination()
    except KeyboardInterrupt:
        for q in queries:
            q.stop()
        print("Exited gracefully from keyboard interrupt")
    except Exception as e:
        print("Exception encountered")
        print(e)
        traceback.print_exc()

if __name__ == "__main__":
    # trying to submit using: "spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 spark_code.py"
    main()