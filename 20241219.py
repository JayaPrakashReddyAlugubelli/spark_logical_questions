# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("SalaryComparison").getOrCreate()

# Sample data
data = [
    (1, 'Joe', 70000, 3),
    (2, 'Henry', 80000, 4),
    (3, 'Sam', 60000, None),
    (4, 'Max', 90000, None)
]
columns = ["id", "name", "salary", "manager_id"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

df_joined = df.alias('e').join(df.alias('m'), col('e.manager_id') == col('m.id'), 'inner') \
    .select(col('e.id').alias('employee_id'), col('e.name').alias('employee_name'),
            col('e.salary').alias('employee_salary'), col('m.name').alias('manager_name'),
            col('m.salary').alias('manager_salary'))

# Filter the DataFrame to find employees earning more than their managers
df_result = df_joined.filter(col('employee_salary') > col('manager_salary'))

# Show the result
df_result.show()




