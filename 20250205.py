# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("EmployeeSalary").getOrCreate()

# Create the DataFrame
data = [
    (1, 'Tildie', 'Female', 'Support', 1676),
    (2, 'Averil', 'Female', 'Engineering', 1572),
    (3, 'Matthiew', 'Male', 'Engineering', 1477),
    (4, 'Reinald', 'Male', 'Services', 1877),
    (5, 'Karola', 'Genderqueer', 'Marketing', 1029),
    (6, 'Manon', 'Genderfluid', 'Research and Development', 1729),
    (7, 'Tabbie', 'Female', 'Research and Development', 1000),
    (8, 'Corette', 'Female', 'Marketing', 1624),
    (9, 'Edward', 'Male', 'Accounting', 1157),
    (10, 'Philipa', 'Female', 'Human Resources', 1992),
    (11, 'Ingeberg', 'Female', 'Services', 1515),
    (12, 'Kort', 'Male', 'Support', 1005),
    (13, 'Shelby', 'Male', 'Product Management', 1020),
    (14, 'Shelden', 'Male', 'Legal', 1354),
    (15, 'Sonya', 'Female', 'Marketing', 1321)
]

# Create the DataFrame with column names
columns = ["id", "emp_name", "gender", "dept", "salary"]
df = spark.createDataFrame(data, columns)

# Define window specification for ranking
windowSpec = Window.partitionBy("dept").orderBy("salary")

# Add rank columns for highest and lowest salaries
df_with_rank = df.withColumn("hs_rank", F.dense_rank().over(windowSpec.orderBy(F.col("salary").desc()))) \
                 .withColumn("ls_rank", F.dense_rank().over(windowSpec.orderBy("salary")))

# Filter for highest and lowest salary employees per department
result_df = df_with_rank.filter((df_with_rank.hs_rank == 1) | (df_with_rank.ls_rank == 1)) \
                        .select("dept", 
                                F.when(df_with_rank.hs_rank == 1, df_with_rank.emp_name).alias("hs"),
                                F.when(df_with_rank.ls_rank == 1, df_with_rank.emp_name).alias("ls"))

# Group by dept and show the result
result_df.groupBy("dept").agg(
    F.max("hs").alias("hs"),
    F.max("ls").alias("ls")
).show(truncate=False)

