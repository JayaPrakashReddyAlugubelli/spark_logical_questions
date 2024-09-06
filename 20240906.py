# Databricks notebook source
from pyspark.sql import SparkSession


# Initialize Spark session
spark = SparkSession.builder.appName("EmpDept Update").getOrCreate()

# Create the DataFrame
data = [
    (1, 'd1', 1.0), (2, 'd1', 5.28), (3, 'd1', 4.0), (4, 'd2', 8.0), 
    (5, 'd1', 2.5), (6, 'd2', 7.0), (7, 'd3', 9.0), (8, 'd4', 10.2)
]

columns = ['eid', 'dept', 'scores']
df = spark.createDataFrame(data, columns)

df.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, first

# Define the window specification with partitioning by dept and ordering by scores in descending order
window_spec = Window.partitionBy("dept").orderBy(col("scores").desc())

# Calculate the maximum score per department
df_with_max_score = df.withColumn('max_score', first('scores').over(window_spec))

# Update the scores column with the department-wise maximum score
df_updated = df_with_max_score.withColumn('scores', col('max_score')).drop('max_score')

# Show the updated DataFrame
df_updated.show()
