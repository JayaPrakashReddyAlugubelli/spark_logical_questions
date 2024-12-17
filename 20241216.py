# Databricks notebook source
from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder.appName("CreateDataFrames").getOrCreate()

# Define the data for employees and bonuses
employees_data = [
    (1, "a", 8),
    (2, "b", 16),
    (3, "c", 6)
]

bonuses_data = [
    (2022, 3000000.00),
    (2023, 3000000.00),
    (2024, 6000000.00)
]

# Define the schema for the data frames
employees_schema = ["id", "name", "yearsofexp"]
bonuses_schema = ["year", "bonus"]

# Create data frames from the data
employees_df = spark.createDataFrame(employees_data, schema=employees_schema)
bonuses_df = spark.createDataFrame(bonuses_data, schema=bonuses_schema)

# Show the data frames
employees_df.show()
bonuses_df.show()


# COMMAND ----------

from pyspark.sql.functions import col, sum as spark_sum, lit
total_exp = employees_df.agg(spark_sum("yearsofexp").alias("total_experience")).collect()[0]["total_experience"]
bonus_sum = bonuses_df.agg(spark_sum("bonus").alias("b_s")).collect()[0]["b_s"]
result_df = employees_df.withColumn("b_salary", (col("yearsofexp") / lit(total_exp)) * lit(bonus_sum))
result_df.show()
