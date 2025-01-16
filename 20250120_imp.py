# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, substr

# Initialize SparkSession
spark = SparkSession.builder.appName("CardMasking").getOrCreate()

# Sample data
data = [
    (1234567812345678,),
    (2345678923456789,),
    (3456789034567890,)
]

# Create DataFrame
columns = ['card_number']
cards_df = spark.createDataFrame(data, columns)

# Masking card number: Show only the last 4 digits
masked_df = cards_df.select(
    concat(lit('*' * 12), substr(col('card_number').cast('string'), -4, 4)).alias('masked_card_number')
)

# Show the result
masked_df.show(truncate=False)

