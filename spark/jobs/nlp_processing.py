from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from sparknlp.base import DocumentAssembler, Finisher
from sparknlp.annotator import LemmatizerModel

def nlp_pipeline(df):
    # Tokenize text
    tokenizer = Tokenizer(inputCol="title", outputCol="words")
    tokenized_df = tokenizer.transform(df)
    
    # Remove stopwords
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    clean_df = remover.transform(tokenized_df)
    
    # Lemmatization
    lemmatizer = LemmatizerModel.pretrained("lemma_antbnc", "en") \
        .setInputCols(["words"]) \
        .setOutputCol("lemmas")
    lemmatized_df = lemmatizer.transform(clean_df)
    
    return lemmatized_df

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("NLP-Processing") \
        .getOrCreate()
    
    data = spark.read.csv("output/cleaned_data", header=True)
    processed_data = nlp_pipeline(data)
    
    processed_data.write.mode("overwrite").csv("output/nlp_processed")
