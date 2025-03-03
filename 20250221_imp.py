# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# Start Spark session
spark = SparkSession.builder.master("local").appName("Tourist Data").getOrCreate()

# Create DataFrame with the tourist data
data = [
    (1, '2024-01-01', 700),
    (2, '2024-02-01', 460),
    (3, '2024-03-01', 550),
    (4, '2024-04-01', 510),
    (5, '2024-05-01', 550),
    (6, '2024-06-01', 540),
    (7, '2024-07-01', 90),
    (8, '2024-08-01', 650),
    (9, '2024-09-01', 580),
    (10, '2024-10-01', 590)
]

columns = ['id', 'date_id', 'visited_people']

df = spark.createDataFrame(data, columns)

# Filter out rows where visited_people < 500
df_filtered = df.filter(df.visited_people >= 500)

# Window specification to order by date_id
window_spec = Window.orderBy("date_id")

# Calculate the row number and 'row_num' based on date format difference
df_with_rn = df_filtered.withColumn("row_num", F.month('date_id') - F.row_number().over(window_spec))

# Group by 'row_num' and count the number of occurrences
grouped_df = df_with_rn.groupBy("row_num").agg(F.count("*").alias("group_count"))

# Filter groups where count is >= 3
filtered_groups = grouped_df.filter(grouped_df.group_count >= 3)

# Collect the 'row_num' values from filtered_groups
rn_values = [row['row_num'] for row in filtered_groups.select("row_num").collect()]

# Filter df_with_rn based on those 'row_num' values
filtered_df = df_with_rn.filter(df_with_rn.row_num.isin(rn_values))

# Select and show the required columns
filtered_df.select('id', 'date_id', 'visited_people').show()

