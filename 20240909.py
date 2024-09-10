# Databricks notebook source
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("Employee Management").getOrCreate()

# Create DataFrame
data = [(1, 'John', 50000, None), (2, 'Alice', 40000, 1), (3, 'Bob', 70000, 1),
        (4, 'Emily', 55000, None), (5, 'Charlie', 65000, 4), (6, 'David', 50000, 4)]

columns = ["empid", "ename", "salary", "managerid"]

employees_df = spark.createDataFrame(data, columns)
employees_df.show()

result_df =employees_df.alias('t1').join(employees_df.alias('t2'))


# COMMAND ----------

from pyspark.sql.functions import col
result_df =employees_df.alias('t1').join(employees_df.alias('t2'),(col('t1.managerid')==col('t2.empid')) & (col('t1.salary') > col('t2.salary')))
result_df.select('t1.ename').show()
