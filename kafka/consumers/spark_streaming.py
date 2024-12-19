from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, TimestampType

# Define the schema for incoming data
schema = StructType() \
    .add("id", StringType()) \
    .add("title", StringType()) \
    .add("author", StringType()) \
    .add("score", IntegerType()) \
    .add("url", StringType()) \
    .add("num_comments", IntegerType()) \
    .add("created_utc", DoubleType())

def process_stream(df, batch_id):
    # Clean the data
    df = df.na.fill("unknown")
    df = df.withColumn("timestamp", (df["created_utc"]).cast(TimestampType()))
    df.drop("created_utc")
    
    # Write to a storage layer (CSV for now)
    df.write.mode("append").csv("output/cleaned_data")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Kafka-Spark-Streaming") \
        .getOrCreate()

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "reddit_worldnews,reddit_politics") \
        .load() \
        .selectExpr("CAST(value AS STRING) as json")

    structured_df = spark.read.json(kafka_df.rdd, schema=schema)
    
    query = structured_df.writeStream \
        .foreachBatch(process_stream) \
        .start()
    
    query.awaitTermination()
