# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import when, max, avg

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Insert into fact_machine") \
    .getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
    StructField("MachineId", IntegerType(), True),
    StructField("processingID", IntegerType(), True),
    StructField("activityType", StringType(), True),
    StructField("timestamp", DoubleType(), True)
])

# Define the data as a list of tuples
data = [
    (0, 0, 'Start', 0.712),
    (0, 0, 'End', 1.520),
    (0, 1, 'Start', 3.140),
    (0, 1, 'End', 4.120),
    (1, 0, 'Start', 0.550),
    (1, 0, 'End', 1.550),
    (1, 1, 'Start', 0.430),
    (1, 1, 'End', 1.420),
    (2, 0, 'Start', 4.100),
    (2, 0, 'End', 4.512),
    (2, 1, 'Start', 2.500),
    (2, 1, 'End', 5.000)
]

# Create DataFrame from the data and schema
df = spark.createDataFrame(data, schema)

# Define the CTE
cte_df = df.groupBy("MachineId", "processingID") \
    .agg(max(when(df.activityType == "Start", df.timestamp)).alias("starttime"),
         max(when(df.activityType == "End", df.timestamp)).alias("endtime"))

# Calculate average processing time
result_df = cte_df.groupBy("MachineId") \
    .agg(avg(cte_df.endtime - cte_df.starttime).alias("avg_processingtime"))

# Show the result
result_df.show()

