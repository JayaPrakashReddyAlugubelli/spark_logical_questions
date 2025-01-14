# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Create a Spark session
spark = SparkSession.builder.appName("Product Aggregated Cost").getOrCreate()

# Sample data
data = [
    ('2023-12-01', 'A', 'A1', 1000),
    ('2023-12-01', 'A', 'A2', 1300),
    ('2023-12-01', 'B', 'B1', 800),
    ('2023-12-02', 'A', 'A1', 1800),
    ('2023-12-02', 'B', 'B1', 900),
    ('2023-12-10', 'A', 'A1', 1400),
    ('2023-12-10', 'A', 'A1', 1200),
    ('2023-12-10', 'C', 'C1', 2500)
]

# Create a DataFrame from the sample data
columns = ['dt', 'brand', 'model', 'production_cost']
df = spark.createDataFrame(data, columns)
df.show()

window=Window.partitionBy("dt","brand")

df=df.withColumn("agg_cost", F.sum("production_cost").over(window))
df.show()
