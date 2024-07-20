# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, count

# Initialize Spark session
spark = SparkSession.builder.appName("UserDetails").getOrCreate()

# Sample data with explicit data types
data = [
    (1, 24, "M", "Engineer", 85711), 
    (2, 53, "F", "other", 94043), 
    (3, 23, "M", "writer", 32067), 
    (4, 24, "M", "technician", 43537), 
    (5, 33, "F", "doctor", 15213), 
    (6, 35, "M", "Cricketer", 32067), 
    (7, 44, "M", "technician", 43537), 
    (8, 33, "F", "Engineer", 15213), 
    (9, 24, "M", "Engineer", 85711), 
    (10, 35, "F", "Cricketer", 32067), 
    (11, 24, "M", "Engineer", 85711), 
    (12, 23, "F", "writer", 32067), 
    (13, 23, "F", "writer", 32067)
]
columns = ["user_id", "age", "gender", "occupation", "zip_code"]

# Create DataFrame and explicitly cast column types
df = spark.createDataFrame(data, columns) \
    .withColumn("user_id", col("user_id").cast("int")) \
    .withColumn("age", col("age").cast("int")) \
    .withColumn("gender", col("gender").cast("string")) \
    .withColumn("occupation", col("occupation").cast("string")) \
    .withColumn("zip_code", col("zip_code").cast("int"))

# Create `male` and `female` columns
df_with_gender_count = df.withColumn("male", when(col("gender") == "M", 1).otherwise(0)) \
                         .withColumn("female", when(col("gender") == "F", 1).otherwise(0))

# Group by `occupation` and calculate total counts, male counts, and female counts
occupation_count_df = df_with_gender_count.groupBy("occupation").agg(
    count("gender").alias("total"),
    sum("male").alias("total_male"),
    sum("female").alias("total_female")
)
# Calculate ratios
result_df = occupation_count_df.select(
    col("occupation"),
    (col("total_male") / col("total") * 100).alias("male_ratio"),
    (col("total_female") / col("total") * 100).alias("female_ratio")
)
# Show the result
result_df.show(truncate=False)

