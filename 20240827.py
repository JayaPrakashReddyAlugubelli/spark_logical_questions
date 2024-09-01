# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Initialize Spark Session
spark = SparkSession.builder.appName("Convert SQL to PySpark").getOrCreate()

# Create the data
data = [('A', 'X', 75), ('A', 'Y', 75), ('A', 'Z', 80), 
        ('B', 'X', 90), ('B', 'Y', 91), ('B', 'Z', 75)]

# Define the schema
columns = ["sname", "sid", "marks"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Define window for row_number calculation
windowSpec = Window.partitionBy("sname").orderBy(col("marks").desc())

# Apply row_number() to create a new column
df_with_rn = df.withColumn("RN", row_number().over(windowSpec))

# Filter rows with RN < 3 and group by sname to calculate the sum of marks
result_df = df_with_rn.filter(col("RN") < 3).groupBy("sname").agg(_sum("marks").alias("Total Marks"))

# Show the result
result_df.show()
