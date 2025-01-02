# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import current_date, datediff, unix_timestamp,col,count
spark = SparkSession.builder \
    .appName("Example") \
    .getOrCreate()
# Define schema
schema = StructType([
    StructField("log_id", IntegerType(), True),StructField("user_id", IntegerType(), True),StructField("action", StringType(), True),
    StructField("timestamp", StringType(), True)
])
# Create DataFrame with correct timestamp format
data = [(1, 101, 'login', '2023-09-05 08:30:00'),(9, 101, 'login', '2024-03-16 08:30:00'),(10, 101, 'login', '2024-03-14 08:30:00'),
     (2, 102, 'click', '2023-09-06 12:45:00'),(3, 101, 'click', '2024-03-10 14:15:00'),(4, 103, 'login', '2023-09-08 09:00:00'),
    (5, 102, 'logout', '2024-03-10 17:30:00'),(11, 102, 'login', '2024-02-21 08:30:00'),(6, 101, 'click', '2024-03-14 11:20:00'),
    (7, 103, 'click', '2024-03-15 10:15:00'), (13, 103, 'click', '2024-03-10 10:15:00'),(12, 102, 'login', '2024-03-12 08:30:00'),
    (8, 102, 'click', '2023-03-13 13:10:00')
]
df = spark.createDataFrame(data, schema)
df=df.filter(datediff(current_date(), col("timestamp")) <= 7)
##df.groupBy('user_id','action').count().show()
df.groupBy('user_id', 'action').agg(count('*').alias('count')).show()
