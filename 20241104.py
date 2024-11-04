# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

# Initialize Spark session
spark = SparkSession.builder.appName("Worldometer").getOrCreate()

# Define data
data = [
    ('AUS', 123456),
    ('PAK', 9876543),
    ('JPN', 34532),
    ('USA', 567899),
    ('NZ', 2345),
    ('IND', 12345678),
    ('CHN', 12345677)
]

# Define schema
columns = ["country_name", "population"]

# Create DataFrame
worldometer_df = spark.createDataFrame(data, schema=columns)

# Sort DataFrame
sorted_df = worldometer_df.orderBy(
    when(col("country_name") == "IND", 1)
    .when(col("country_name") == "USA", 2)
    .otherwise(3),
    col("country_name")
)

# Show result
sorted_df.show()

