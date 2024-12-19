from pyspark.sql import SparkSession
from sparknlp.pretrained import PretrainedPipeline

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Sentiment-Analysis") \
        .getOrCreate()

    data = spark.read.csv("output/nlp_processed", header=True)

    # Pretrained sentiment pipeline
    sentiment_pipeline = PretrainedPipeline("analyze_sentiment", lang="en")
    
    # Apply sentiment analysis
    data = data.withColumn("sentiment", sentiment_pipeline.annotate("lemmas"))
    
    data.write.mode("overwrite").csv("output/sentiment_analysis")
