# Databricks notebook source
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Sales Variance").getOrCreate()

# Create DataFrame with sample data
data = [('2023-10-03', 10), ('2023-10-04', 20), ('2023-10-05', 60), 
        ('2023-10-06', 50), ('2023-10-07', 10)]
columns = ['dt', 'sales']

salesvar_df = spark.createDataFrame(data, columns)
salesvar_df.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions  import lag,col,expr
# Define the window specification for lag calculation
window_spec = Window.orderBy("dt")
# Calculate the lag and profit percentage
cte = salesvar_df.withColumn("lg", lag("sales", 1, "sales").over(window_spec))
cte2 = cte.withColumn("profit", expr("(sales - lg) * 100 / lg"))
# Filter the results where profit is non-negative
result_df = cte2.filter(col("profit") >= 0)
# Show the results
result_df.show()
