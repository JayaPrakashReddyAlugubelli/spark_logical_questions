from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.master("local").appName("CrossJoinExample").getOrCreate()

# Sample data
data = [("1",), ("2",), ("3",), ("4",)]

# Create DataFrame
df = spark.createDataFrame(data, ["cola"])

# Perform CROSS JOIN using the DataFrame API
cross_joined_df = df.alias("a").crossJoin(df.alias("b"))

# Filter rows where a.cola > b.cola
result_df = cross_joined_df.filter(col("a.cola") > col("b.cola"))

# Select and order the results
result_df = result_df.select(col("a.cola").alias("cola"), col("b.cola").alias("colb")).orderBy("cola")

# Show the result
result_df.show()
