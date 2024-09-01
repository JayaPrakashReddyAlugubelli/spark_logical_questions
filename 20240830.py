# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag

# Initialize Spark Session
spark = SparkSession.builder.appName("Convert SQL to PySpark").getOrCreate()

# Create the data
data = [('jan', 15, 1), ('feb', 22, 2), ('mar', 35, 3), ('apr', 45, 4), ('may', 60, 5)]

# Define the schema
columns = ["month", "ytd_sales", "monthnum"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Define window specification for the lag function
windowSpec = Window.orderBy("monthnum")

# Calculate sales difference using lag function
df_with_diff = df.withColumn("sales_diff", col("ytd_sales") - lag("ytd_sales", 1, 0).over(windowSpec))

# Show the result
df_with_diff.show()

