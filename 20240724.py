# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
schema = StructType([
    StructField("stockid", StringType(), True),
    StructField("prediction_1", IntegerType(), True),
    StructField("prediction_2", IntegerType(), True),
    StructField("prediction_3", IntegerType(), True),
    StructField("prediction_4", IntegerType(), True),
    StructField("prediction_5", IntegerType(), True),
    StructField("prediction_6", IntegerType(), True),
    StructField("prediction_7", IntegerType(), True)
])
data = [
    ("RIL", 1000, 1005, 1090, 1200, 1000, 900, 890),
    ("HDFC", 890, 940, 810, 730, 735, 960, 980),
    ("INFY", 1001, 902, 1000, 990, 1230, 1100, 1200)
]
df = spark.createDataFrame(data, schema=schema)

# Show DataFrame
df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import greatest, least, col
# Create a DataFrame with maxv, minv, and profit
df_with_max_min = df.withColumn("maxv", greatest(col("prediction_1"),col("prediction_2"),
col("prediction_3"), col("prediction_4"),col("prediction_5"),col("prediction_6"),col("prediction_7"))).withColumn( "minv", least(col("prediction_1"),col("prediction_2"),col("prediction_3"),col("prediction_4"),col("prediction_5"),col("prediction_6"),col("prediction_7"))).withColumn("profit", col("maxv") - col("minv"))
# Select required columns
result_df = df_with_max_min.select("stockid","maxv","minv","profit"
)
# Show the result
result_df.show(truncate=False)

