from lib2to3.pgen2 import token
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, regexp_replace, count, explode
from pyspark.ml.feature import RegexTokenizer



def filter_characters(dataframe: DataFrame) -> DataFrame:
    """
    Remove special characters.
    
    Keep alpha numeric characters and ("," & "'"). Remove "-", "+" ,".", etc 
    """
    dataframe = dataframe.select(
        regexp_replace(col("value"),"[^A-Za-z']"," ").alias("value")
        )

    return dataframe


def tokenized_counts(dataframe: DataFrame) -> DataFrame:
    """Tokenize corpos and aggregate word counts"""

    # Tokenizer
    regex_tokenizer = RegexTokenizer(inputCol="value", outputCol="words")
    dataframe = regex_tokenizer.transform(dataframe)

    # GroupBy Aggregration
    dataframe = dataframe.withColumn("word", explode(col("words"))) \
        .groupBy("word") \
        .agg(count("*").alias('count')) \
        .sort('word', ascending=True)

    return dataframe


def run(spark: SparkSession, input_path: str, output_path: str) -> None:
    logging.info("Reading text file from: %s", input_path)
    input_df = spark.read.text(input_path)

    # Preprocessing
    input_df = filter_characters(input_df)
    input_df = tokenized_counts(input_df)

    logging.info("Writing csv to directory: %s", output_path)

    input_df.coalesce(1).write.csv(output_path, header=True)
