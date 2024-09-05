# Databricks notebook source
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Convert SQL to PySpark").getOrCreate()

# Create sample data
data = [
    ("2024-01-13 09:25:00", 10), ("2024-01-13 19:35:00", 10), ("2024-01-16 09:10:00", 10),
    ("2024-01-16 18:10:00", 10), ("2024-02-11 09:07:00", 10), ("2024-02-11 19:20:00", 10),
    ("2024-02-17 08:40:00", 17), ("2024-02-17 18:04:00", 17), ("2024-03-23 09:20:00", 10),
    ("2024-03-23 18:30:00", 10)
]

# Create DataFrame
schema = ["id", "empid"]
emp_df = spark.createDataFrame(data, schema=schema)
emp_df.show()
emp_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, date_format, dayofweek, max as pyspark_max, min as pyspark_min, sum as pyspark_sum
from pyspark.sql.types import IntegerType, TimestampType

# Convert the 'id' column to TimestampType
emp_df = emp_df.withColumn("id", col("id").cast(TimestampType()))

# Filter records where the day of the week is Saturday (7) or Sunday (1)
filtered_df = emp_df.filter(dayofweek("id").isin(1, 7))

# Group by employee ID and day, and find the maximum and minimum timestamps for each employee
cte_df = filtered_df.groupBy("empid", date_format(col("id"), "yyyy-MM-dd").alias("day")) \
    .agg(
        pyspark_max("id").alias("max"),
        pyspark_min("id").alias("min")
    )

# Calculate the difference in hours and sum them up for each employee
result_df = cte_df.withColumn("hours_diff", (col("max").cast("long") - col("min").cast("long")) / 3600.0) \
    .groupBy("empid") \
    .agg(pyspark_sum("hours_diff").alias("hours_week"))

result_df.show()

