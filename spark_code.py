from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, from_json, to_timestamp, window, current_timestamp, to_date, avg, variance, when, count, struct
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

def update_state(key, pdf_iter, state):
    # -----------------------------------
    # 0. Handle timeout safely
    # -----------------------------------
    if state.hasTimedOut:
        state.remove()
        return

    Z_SCORE_THRESHOLD = 1.5
    user_id = key[0]

    # -----------------------------------
    # 1. Load state (keep as tuples)
    # -----------------------------------
    if state.exists:
        raw_state = state.get()
        print("STATE RAW:", raw_state)
        print("STATE TYPE:", type(raw_state))

        raw_history = raw_state["history"]
        print("HISTORY TYPE:", type(raw_history))
        print("HISTORY SAMPLE:", raw_history[:3] if raw_history else "EMPTY")

        for i, h in enumerate(raw_history[:3]):
            print(f"h[{i}] TYPE:", type(h))
            try:
                print(f"h[{i}] VALUE:", h)
            except Exception as e:
                print(f"h[{i}] ERROR PRINTING:", e)
        history = list(state.get()["history"])  # already tuple/Row-like
    else:
        history = []

    THREE_HOURS = 30 * 60  # 30 minutes for testing

    # Track last timestamp safely
    latest_ts = None

    # -----------------------------------
    # 2. Process incoming data
    # -----------------------------------
    for pdf in pdf_iter:
        outputs = []

        if pdf.empty:
            continue  # avoid edge-case crashes

        pdf = pdf.sort_values("event_ts")

        for _, row in pdf.iterrows():
            current_ts = row["event_ts"]
            val = row["price"]
            latest_ts = current_ts  # track safely

            # -----------------------------------
            # 3. Add new event (tuple!)
            # -----------------------------------
            history.append((current_ts, val))

            # -----------------------------------
            # 4. Evict old events
            # -----------------------------------
            history = [
                h for h in history
                if (current_ts - h[0]).total_seconds() <= THREE_HOURS
            ]

            # -----------------------------------
            # 5. Compute stats
            # -----------------------------------
            values = [h[1] for h in history]

            if len(values) > 1:
                mean = sum(values) / len(values)
                variance = sum((v - mean) ** 2 for v in values) / len(values)
                std = math.sqrt(variance)

                is_anomaly = std > 0 and abs(val - mean) > Z_SCORE_THRESHOLD * std
            else:
                mean = val
                std = 0.0
                is_anomaly = False

            # -----------------------------------
            # 6. Append output
            # -----------------------------------
            outputs.append({
                "id": user_id,
                "event_ts": current_ts,
                "value": val,
                "mean": mean,
                "std": std,
                "is_anomaly": is_anomaly
            })

        # -----------------------------------
        # 7. Emit batch output
        # -----------------------------------
        if outputs:
            yield pd.DataFrame(outputs)

    # -----------------------------------
    # 8. Update state
    # -----------------------------------
    print("UPDATING STATE WITH:")
    print("history type:", type(history))
    print("history sample:", history[:3] if history else "EMPTY")

    for i, h in enumerate(history[:3]):
        print(f"OUT h[{i}] TYPE:", type(h), "VALUE:", h)
        print("TS TYPE:", type(h[0]), "VALUE TYPE:", type(h[1]))
    state.update({"history": history})

    # -----------------------------------
    # 9. Set timeout safely
    # -----------------------------------
    if latest_ts is not None:
        state.setTimeoutTimestamp(
            int(latest_ts.timestamp() * 1000) + 3 * 60 * 60 * 1000
        )
        
# def update_state(key, pdf_iter, state):
#     if state.hasTimedOut:
#         state.remove()
#         return

#     Z_SCORE_THRESHOLD = 1.5 # the threshold above which an alert is sent
#     user_id = key[0]

#     # TODO: unexpected tuple h with StructType

#     if state.exists:
#         raw_history = state.get()["history"]
#         history = [
#             {"ts": h[0], "value": h[1]}
#             for h in raw_history
#         ]
#     else:
#         history = []

#     THREE_HOURS = 30 * 60  # 30 minutes for testing
#     # THREE_HOURS = 3 * 60 * 60  # seconds

#     for pdf in pdf_iter:
#         outputs = []

#         # Ensure sorted by event time
#         pdf = pdf.sort_values("event_ts")

#         for _, row in pdf.iterrows():
#             current_ts = row["event_ts"]
#             val = row["price"]   # IMPORTANT: your column is "price"

#             # 1. Add new event
#             history.append({"ts": current_ts, "value": val})

#             # 2. Evict old events
#             history = [
#                 h for h in history
#                 if (current_ts - h["ts"]).total_seconds() <= THREE_HOURS
#             ]

#             values = [h["value"] for h in history]

#             if len(values) > 1:
#                 mean = sum(values) / len(values)
#                 variance = sum((v - mean) ** 2 for v in values) / len(values)
#                 std = math.sqrt(variance)

#                 is_anomaly = std > 0 and abs(val - mean) > Z_SCORE_THRESHOLD * std
#             else:
#                 mean = val
#                 std = 0.0
#                 is_anomaly = False

#             outputs.append({
#                 "id": user_id,
#                 "event_ts": current_ts,
#                 "value": val,
#                 "mean": mean,
#                 "std": std,
#                 "is_anomaly": is_anomaly
#             })

#         # Emit batch
#         yield pd.DataFrame(outputs)

#     # Update state
#     state.update({"history": history})

#     # set timeout based on latest event time seen
#     state.setTimeoutTimestamp(int(current_ts.timestamp() * 1000) + 3 * 60 * 60 * 1000)


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


    # TODO: Define schema for result
    result_schema = StructType([
        StructField("id", IntegerType()),
        StructField("event_ts", TimestampType()),
        StructField("value", DoubleType()),
        StructField("mean", DoubleType()),
        StructField("std", DoubleType()),
        StructField("is_anomaly", BooleanType())
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

    # result_df = watermarked_df.groupBy("id").applyInPandasWithState(
    #     update_state,
    #     outputStructType=result_schema,
    #     stateStructType=state_schema,
    #     outputMode="append",
    #     timeoutConf="EventTimeTimeout"
    # )

    # -----------------------------------
    # 6. Split outputs
    # -----------------------------------
    # raw_df = parsed_df
    # raw_df = raw_df.withColumn("event_dt", to_date("event_ts"))

    # alerts_df = result_df.filter(col("is_anomaly") == True)
    alerts_df = parsed_df # for now, push all records through
    alerts_df = alerts_df.withColumn("event_dt", to_date("event_ts"))

    # -----------------------------------
    # 7. Write raw data to CSV
    # -----------------------------------
    raw_output_path = out_path + "/raw_output"
    raw_output_checkpoint_path = ckpt_path + "/raw_output"

    alert_path = out_path + "/raw_alert"
    alert_checkpoint_path = ckpt_path + "/raw_alert"

    # raw_query = raw_df.writeStream \
    #     .format("csv") \
    #     .option("path", raw_output_path) \
    #     .option("checkpointLocation", raw_output_checkpoint_path) \
    #     .partitionBy("event_dt") \
    #     .outputMode("append") \
    #     .start()

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

    return [alerts_query]
    # return [raw_query, alerts_query]

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