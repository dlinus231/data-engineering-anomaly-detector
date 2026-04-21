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
# from pyspark.sql.streaming import GroupState, GroupStateTimeout

import math
from datetime import timedelta
from dotenv import load_dotenv
import os
import traceback
import pandas as pd


# continue from this chat: https://chatgpt.com/c/69e106f7-67f0-8332-a427-2fa286905ad1

# -----------------------------------
# 1. Spark session
# -----------------------------------
def create_spark_session(app_name="anomaly-detector"):

    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    # spark.conf.set("spark.sql.shuffle.partitions", "200") # maybe set better default?
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark

# called once per key, where key here is a unique id for a crypto-coin
def update_state(key, pdf_iter, state):
    # -----------------------------------
    # 0. Handle timeout safely
    # -----------------------------------
    if state.hasTimedOut:
        state.remove()
        return

    Z_SCORE_THRESHOLD = 2.5
    user_id = key[0]

    # -----------------------------------
    # 1. Load state
    # -----------------------------------
    # applyInPandasWithState uses tuple-based state conventions:
    #   - state.get() returns a Row (tuple subclass); access fields by position
    #   - nested struct elements in an ArrayType are also Rows (tuples)
    # Store history entries as plain tuples (ts, value) matching schema
    # field order so state.update() serializes them without ambiguity.
    if state.exists:
        history = [(h[0], h[1]) for h in state.get[0]]  # [0] = first schema field (history)
    else:
        history = []

    THREE_HOURS = 30 * 60  # 30 minutes for testing

    # Track last timestamp safely
    latest_ts = None

    # -----------------------------------
    # 2. Process incoming data
    # -----------------------------------
    for pdf in pdf_iter: # each pandas_df is a different batch with rows for a given crypto-coin
        outputs = []

        if pdf.empty:
            continue  # avoid edge-case crashes

        pdf = pdf.sort_values("event_ts")

        for _, row in pdf.iterrows():
            current_ts = row["event_ts"]
            val = row["price"]
            event_id = row["event_id"]
            latest_ts = current_ts  # track safely

            # -----------------------------------
            # 4. Evict old events
            # -----------------------------------
            history = [
                h for h in history
                if (current_ts - h[0]).total_seconds() <= THREE_HOURS  # h[0] = ts
            ]
            
            historical_timestamps, historical_prices = zip(*history) if history else ([], [])

            history.append((current_ts, float(val)))  # (ts, value) matching nested StructType field order

            # -----------------------------------
            # 5. Compute stats on historical metrics (excluding current data point)
            # -----------------------------------
            if len(historical_prices) > 5: # TODO: this should depend on the window size, e.g. should be smth like: historical_prices >= (window_length_mins // 5 - 1)
                mean = sum(historical_prices) / len(historical_prices)
                variance = sum((v - mean) ** 2 for v in historical_prices) / len(historical_prices)
                std = math.sqrt(variance)
                z_score = abs(val - mean) / std if std > 0 else 0.0
                is_anomaly = z_score > Z_SCORE_THRESHOLD

                # -----------------------------------
                # 6. Append output and history if anomalous
                # -----------------------------------
                if is_anomaly:
                    outputs.append({
                        "id": user_id,
                        "event_id": event_id,
                        "event_ts": current_ts,
                        "curr_price": val,
                        "past_prices": historical_prices,
                        "past_timestamps": historical_timestamps,
                        "mean": mean,
                        "std": std,
                        "is_anomaly": is_anomaly,
                        "z_score": z_score
                    })
            # else:
            #     mean = val
            #     std = 0.0
            #     is_anomaly = False
            #     z_score = 0.0


        if outputs:
            yield pd.DataFrame(outputs)

    # -----------------------------------
    # 8. Update state
    # -----------------------------------
    state.update((history,))  # tuple: one element per state schema field, in order

    # -----------------------------------
    # 9. Set timeout safely
    # -----------------------------------
    if latest_ts is not None:
        state.setTimeoutTimestamp(
            int(latest_ts.timestamp() * 1000) + 3 * 60 * 60 * 1000
        )
        


    
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
    watermarked_df = parsed_df.withWatermark("event_ts", "3 hours")


    # TODO: Define schema for result
    result_schema = StructType([
        StructField("id", IntegerType()),
        StructField("event_id", StringType()),
        StructField("event_ts", TimestampType()),
        StructField("curr_price", DoubleType()),
        StructField("past_prices", ArrayType(DoubleType())),
        StructField("past_timestamps", ArrayType(TimestampType())),
        StructField("mean", DoubleType()),
        StructField("std", DoubleType()),
        StructField("is_anomaly", BooleanType()),
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

    result_df = watermarked_df.groupBy("id").applyInPandasWithState(
        update_state,
        outputStructType=result_schema,
        stateStructType=state_schema,
        outputMode="append",
        timeoutConf="EventTimeTimeout"
    ).withColumn("alert_id", col("event_id")) # link anomalies to the event which generated it

    # -----------------------------------
    # 6. Split outputs
    # -----------------------------------
    raw_df = parsed_df
    raw_df = raw_df.withColumn("event_dt", to_date("event_ts"))

    alerts_df = result_df.filter(col("is_anomaly") == True)
    # alerts_df = result_df # for now, push all records through
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
        batch_df.write \
            .format("csv") \
            .mode("append") \
            .option("header", "true") \
            .partitionBy("event_dt") \
            .save(raw_output_path)

    def write_alerts(batch_df, batch_id):
        batch_df.write \
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
        traceback.print_exc()

if __name__ == "__main__":
    # trying to submit using: "spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 spark_code.py"
    main()