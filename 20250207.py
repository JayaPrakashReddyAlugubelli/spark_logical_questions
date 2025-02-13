# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, dense_rank
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("CallDuration").getOrCreate()

# Sample Data for call_start and call_end
call_start_data = [
    ('contact_1', '2024-05-01 10:20:00'),
    ('contact_1', '2024-05-01 16:25:00'),
    ('contact_2', '2024-05-01 12:30:00'),
    ('contact_3', '2024-05-02 10:00:00'),
    ('contact_3', '2024-05-02 12:30:00'),
    ('contact_3', '2024-05-03 09:20:00')
]

call_end_data = [
    ('contact_1', '2024-05-01 10:45:00'),
    ('contact_1', '2024-05-01 17:05:00'),
    ('contact_2', '2024-05-01 12:55:00'),
    ('contact_3', '2024-05-02 10:20:00'),
    ('contact_3', '2024-05-02 12:50:00'),
    ('contact_3', '2024-05-03 09:40:00')
]

# Create DataFrames
call_start_df = spark.createDataFrame(call_start_data, ['ph_no', 'start_time'])
call_end_df = spark.createDataFrame(call_end_data, ['ph_no', 'end_time'])

# Convert start_time and end_time to timestamp
call_start_df = call_start_df.withColumn('start_time', unix_timestamp('start_time', 'yyyy-MM-dd HH:mm:ss').cast('timestamp'))
call_end_df = call_end_df.withColumn('end_time', unix_timestamp('end_time', 'yyyy-MM-dd HH:mm:ss').cast('timestamp'))

# Create dense rank columns based on start_time and end_time for each ph_no
window_spec = Window.partitionBy('ph_no').orderBy('start_time')
call_start_df = call_start_df.withColumn('dn', dense_rank().over(window_spec))

window_spec_end = Window.partitionBy('ph_no').orderBy('end_time')
call_end_df = call_end_df.withColumn('dn', dense_rank().over(window_spec_end))

# Join the two DataFrames on ph_no and dense rank
joined_df = call_start_df.join(call_end_df, ['ph_no', 'dn'])

# Calculate the time difference in minutes
result_df = joined_df.withColumn('time_diff_minutes', 
                                 (unix_timestamp('end_time') - unix_timestamp('start_time')) / 60)

# Show the result
result_df.select('ph_no', 'start_time', 'end_time', 'time_diff_minutes').show(truncate=False)

