# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("MissingWeekDays").getOrCreate()
weeknames_data = [('Monday'), ('Tuesday'), ('Wednesday'),('Thursday'), ('Friday')]
account_info_data = [(1, 'GCGV21', '2024-11-01', 'Friday'),(2, 'GCGV22', '2024-11-04', 'Monday'),(3, 'GCGV24', '2024-11-05', 'Tuesday'),(4, 'GCGV21', '2024-11-07', 'Thursday'),
                     (5, 'GCGV23', '2024-11-11', 'Monday'),(6, 'GCGV21', '2024-11-14', 'Thursday'),(7, 'GCGV21', '2024-11-15', 'Friday')]

weeknames_df = spark.createDataFrame([(name,) for name in weeknames_data], ["day_name"])
account_info_df = spark.createDataFrame(account_info_data, ["account_id", "RPG", "date", "dayofweek"])

distinct_dayofweek_df = account_info_df.select("dayofweek").distinct()

missing_week_df = weeknames_df.join(distinct_dayofweek_df, weeknames_df.day_name == distinct_dayofweek_df.dayofweek, "left") \
    .filter(distinct_dayofweek_df.dayofweek.isNull()).select(weeknames_df.day_name.alias("missing_week"))

missing_week_df.show()
