# Databricks notebook source
from pyspark.sql import Row
from pyspark.sql.functions import col

# Sample Data
data = [
    (1, 'John', 50000, None),
    (2, 'Alice', 40000, 1),
    (3, 'Bob', 70000, 1),
    (4, 'Emily', 55000, None),
    (5, 'Charlie', 65000, 4),
    (6, 'David', 50000, 4)
]

# Create a DataFrame from the sample data
df = spark.createDataFrame(data, ['empid', 'ename', 'salary', 'managerid'])

# Show the DataFrame to verify
df.show()


df_final = df.alias('e1').join(
    df.alias('e2'),
    col('e1.managerid') == col('e2.empid'),
    'inner'
).filter(col('e1.salary') > col('e2.salary')).select('e1.ename')
df_final.show()

# COMMAND ----------

df_final = df.alias('e1').join(
    df.alias('e2'),
    col('e1.managerid') == col('e2.empid'),
    'inner'
).filter(col('e1.salary') > col('e2.salary')).select('e1.ename')
df_final.show()

