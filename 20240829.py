# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min as _min

# Initialize Spark Session
spark = SparkSession.builder.appName("Convert SQL to PySpark").getOrCreate()

# Create the data for tablea and tableb
data_a = [(1, 'AA', 1000), (2, 'BB', 300)]
data_b = [(2, 'BB', 400), (3, 'CC', 100)]

# Define the schema
columns = ["empid", "empname", "salary"]

# Create DataFrames
df_a = spark.createDataFrame(data_a, columns)
df_b = spark.createDataFrame(data_b, columns)

# Union the two DataFrames
df_union = df_a.union(df_b)

# Group by 'empid' and 'empname' and calculate the minimum salary
result_df = df_union.groupBy("empid", "empname").agg(_min("salary").alias("salary"))

# Show the result
result_df.show()

