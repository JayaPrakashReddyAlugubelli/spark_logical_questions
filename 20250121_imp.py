# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Initialize a Spark session
spark = SparkSession.builder.appName("Running Total").getOrCreate()

# Sample data (same as your input data)
data = [
    (53151, 'deposit', 178, '2022-07-08'),
    (29776, 'withdrawal', 25, '2022-07-08'),
    (16461, 'withdrawal', 45, '2022-07-08'),
    (19153, 'deposit', 65, '2022-07-10'),
    (77134, 'deposit', 32, '2022-07-10')
]
# Create a DataFrame
columns = ['transaction_id', 'type', 'amount', 'transaction_date']
df = spark.createDataFrame(data, columns)

df=df.withColumn("rn", monotonically_increasing_id()).withColumn("running_total",when(col("type") == 'withdrawal', -col("amount")).otherwise(col("amount")))
window=Window.orderBy("rn")
df=df.withColumn("running_total",sum(col("running_total")).over(window)).drop("rn")
df.show()

