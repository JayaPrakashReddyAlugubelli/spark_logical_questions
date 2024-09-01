# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max as _max

# Initialize Spark Session
spark = SparkSession.builder.appName("Convert SQL to PySpark").getOrCreate()

# Create the data
data = [(2,), (5,), (6,), (6,), (7,), (8,), (8,)]

# Define the schema
columns = ["id"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Group by 'id' and count the occurrences
df_counts = df.groupBy("id").agg(count("*").alias("counts"))

# Filter rows where the count is 1
df_filtered = df_counts.filter(col("counts") == 1)

# Find the maximum 'id' from the filtered rows
result_df = df_filtered.agg(_max("id").alias("max_id"))

# Show the result
result_df.show()

