
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType
)

# 1. Create Spark Session
spark = (
    SparkSession.builder
    .appName("CampusEventsStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 2. Define the JSON schema
event_schema = StructType([
    StructField("studentId", IntegerType(), True),
    StructField("eventType", StringType(), True),
    StructField("eventTime", StringType(), True)
])

# 3. Read the Kafka topic as a streaming DataFrame
kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "campus-events")
    .option("startingOffsets", "latest")
    .load()
)

# 4. Extract and parse JSON messages
events = (
    kafka_stream
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(from_json(col("json_value"), event_schema).alias("event"))
    .select("event.*")
    .withColumn("eventTime", to_timestamp(col("eventTime")))
    .filter(
        col("studentId").isNotNull()
        & col("eventType").isNotNull()
        & col("eventTime").isNotNull()
    )
)

# 5. Count events by eventType in one-minute windows
event_counts = (
    events
    .groupBy(
        window(col("eventTime"), "1 minute"),
        col("eventType")
    )
    .count()
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("eventType"),
        col("count")
    )
)

# 6. Display streaming results in the console
query = (
    event_counts.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .option("numRows", 50)
    .option("checkpointLocation", "/tmp/spark-campus-events-checkpoint")
    .start()
)

print("Spark Structured Streaming started...")
print("Reading events from Kafka topic: campus-events")

query.awaitTermination()
