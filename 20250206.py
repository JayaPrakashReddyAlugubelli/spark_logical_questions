# Databricks notebook source
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType
from pyspark.sql.functions import col  # Import col function

# Define schema for ticket table
ticket_schema = StructType([
    StructField("ticket_id", IntegerType(), False),
    StructField("issue_date", DateType(), False),
    StructField("resolve_date", DateType(), False)
])

# Define schema for holiday table
holiday_schema = StructType([
    StructField("holiday_date", DateType(), False),
    StructField("occasion", StringType(), False)
])

# Create DataFrames
ticket_data = [
    Row(1, '2024-12-18', '2025-01-07'),
    Row(2, '2024-12-20', '2025-01-10'),
    Row(3, '2024-12-22', '2025-01-11'),
    Row(4, '2025-01-02', '2025-01-13')
]

holiday_data = [
    Row('2024-12-25', 'christmas'),
    Row('2025-01-01', 'new_year')
]

# Create DataFrames from the data
ticket_df = spark.createDataFrame(ticket_data, ticket_schema)
holiday_df = spark.createDataFrame(holiday_data, holiday_schema)

# Convert date strings to proper DateType
ticket_df = ticket_df.withColumn("issue_date", col("issue_date").cast(DateType()))\
                     .withColumn("resolve_date", col("resolve_date").cast(DateType()))
holiday_df = holiday_df.withColumn("holiday_date", col("holiday_date").cast(DateType()))

ticket_df.show()
holiday_df.show()


# COMMAND ----------

from pyspark.sql.functions import expr

# Calculate the number of days between issue_date and resolve_date
ticket_df = ticket_df.withColumn("days_diff", datediff(col("resolve_date"), col("issue_date")))

# Calculate the weekends (Saturday and Sunday) in the date range
ticket_df = ticket_df.withColumn("weekends", expr("floor(days_diff / 7) * 2"))

# Now, let's calculate the holidays between issue_date and resolve_date
ticket_with_holidays = ticket_df.join(holiday_df,
                                      (holiday_df.holiday_date >= ticket_df.issue_date) &
                                      (holiday_df.holiday_date <= ticket_df.resolve_date),
                                      "left").groupBy("ticket_id", "issue_date", "resolve_date", "days_diff", "weekends").agg(count("occasion").alias("holiday_count"))

# Calculate the actual working days
ticket_result = ticket_with_holidays.withColumn(
    "actual_working_days",
    (col("days_diff") - col("weekends") - col("holiday_count"))
)

# Show the result
ticket_result.select("ticket_id", "issue_date", "resolve_date", "actual_working_days").show()

