# Databricks notebook source
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("CinemaSeats").getOrCreate()

# Create a DataFrame with the provided data
data = [(1, 1), (2, 0), (3, 1), (4, 0), (5, 1), (6, 1), (7, 1), (8, 0), (9, 1), (10, 1)]
columns = ["seat_id", "free"]
cinema_df = spark.createDataFrame(data, columns)

# COMMAND ----------

from pyspark.sql import functions as F


# Perform self-join to find the required seat_ids
result_df = (
    cinema_df.alias("a")
    .join(cinema_df.alias("b"), (F.abs(F.col("a.seat_id") - F.col("b.seat_id")) == 1) & (F.col("a.free") == 1) & (F.col("b.free") == 1))
    .select("a.seat_id")
    .distinct()
)

# Show the result
result_df.show()


# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Define the window specification for lag and lead operations
window_spec = Window.orderBy("seat_id")

# Apply the lag and lead window functions
cinema_df = cinema_df.withColumn("lags", F.col("free") * F.lag("free", 1, 0).over(window_spec))
cinema_df = cinema_df.withColumn("leads", F.col("free") * F.lead("free", 1, 0).over(window_spec))

# Filter the DataFrame based on the condition where lags or leads is 1
result_df = cinema_df.filter((F.col("lags") == 1) | (F.col("leads") == 1)).select("seat_id")

# Show the result
result_df.show()
