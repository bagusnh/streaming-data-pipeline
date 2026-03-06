from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("StreamingTransactions") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("user_id", StringType()),
    StructField("amount", IntegerType()),
    StructField("timestamp", StringType()),
    StructField("source", StringType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING) as json")

parsed = json_df.select(from_json(col("json"), schema).alias("data")).select("data.*")

events = parsed.withColumn(
    "event_time",
    to_timestamp("timestamp")
)

# -------------------
# VALIDATION RULES
# -------------------

validated = events.withColumn(
    "error_reason",
    when(col("user_id").isNull(), "missing_user")
    .when(col("amount").isNull(), "missing_amount")
    .when(col("timestamp").isNull(), "missing_timestamp")
    .when(col("event_time").isNull(), "invalid_timestamp")
    .when(~col("source").isin("mobile", "web", "pos"), "invalid_source")
    .when(col("amount") < 1, "amount_negative")
    .when(col("amount") > 10000000, "amount_too_large")
)

validated = validated.withColumn(
    "is_valid",
    when(col("error_reason").isNull(), True).otherwise(False)
)

# -------------------
# WATERMARK
# -------------------

watermarked = validated \
    .withWatermark("event_time", "3 minutes")

# -------------------
# DUPLICATE DETECTION
# -------------------

dedup = watermarked.dropDuplicates(["user_id", "timestamp"])

# -------------------
# SPLIT VALID / INVALID
# -------------------

valid = dedup.filter(col("is_valid") == True)
invalid = dedup.filter(col("is_valid") == False)

# -------------------
# WRITE VALID TO KAFKA
# -------------------

valid.selectExpr(
    "CAST(user_id AS STRING) as key",
    "to_json(struct(*)) AS value"
).writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "transactions_valid") \
    .option("checkpointLocation", "/tmp/checkpoint_valid") \
    .start()

# -------------------
# WRITE INVALID TO DLQ
# -------------------

invalid.selectExpr(
    "CAST(user_id AS STRING) as key",
    "to_json(struct(*)) AS value"
).writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "transactions_dlq") \
    .option("checkpointLocation", "/tmp/checkpoint_dlq") \
    .start()

# -------------------
# WINDOW MONITORING
# -------------------

windowed = valid.groupBy(
    window(col("event_time"), "1 minute")
).agg(
    sum("amount").alias("running_total")
)

query = windowed.select(
    current_timestamp().alias("timestamp"),
    col("running_total")
).writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()