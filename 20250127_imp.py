# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, substring_index

# Initialize Spark session
spark = SparkSession.builder.master("local").appName("CustomerTable").getOrCreate()

# Data
data = [
    (1, 'abc@gmail.com'),
    (2, 'xyz@hotmail.com'),
    (3, 'pqr@outlook.com')
]

# Create DataFrame
columns = ['id', 'email']
df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show(truncate=False)
df_with_domain = df.withColumn("domain", split(df['email'], '@')[1])

# Show the result
df_with_domain.show(truncate=False)

