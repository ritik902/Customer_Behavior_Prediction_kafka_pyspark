from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, from_unixtime, length, when

spark = SparkSession.builder \
    .appName("KafkaToMySQL") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,mysql:mysql-connector-java:8.0.27") \
    .getOrCreate()

# Read data from Kafka
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "reviews") \
    .load()

# Convert the binary Kafka message value to string
kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Split CSV into columns
columns = ["Id", "ProductId", "UserId", "ProfileName", "HelpfulnessNumerator",
           "HelpfulnessDenominator", "Score", "Time", "Summary", "Text"]

df = kafka_df.withColumn("csv", split(col("value"), ","))

for i, column in enumerate(columns):
    df = df.withColumn(column, col("csv").getItem(i))

df = df.drop("csv")

# Preprocess Data
df = df.na.drop(subset=["ProductId", "UserId", "Score", "Text"])
df = df.withColumn("Date", from_unixtime(col("Time")).cast("timestamp"))
df = df.withColumn("HelpfulnessRatio",
                   when(col("HelpfulnessDenominator") > 0,
                        col("HelpfulnessNumerator") / col("HelpfulnessDenominator"))
                   .otherwise(None))
df = df.withColumn("TextLength", length(col("Text")))
df = df.withColumn("SummaryLength", length(col("Summary")))
df = df.filter(col("HelpfulnessDenominator") > 0)
df = df.dropDuplicates(["ProductId", "UserId", "Time"])


# Write to a file system as CSV
df.coalesce(1).writeStream \
    .format("csv") \
    .option("path", "/opt/bitnami/spark/output/final") \
    .option("checkpointLocation", "/opt/bitnami/spark/output/checkpoint") \
    .option("header", "true") \
    .start() \
    .awaitTermination()

# Stop the Spark session
spark.stop()
