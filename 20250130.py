from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, dense_rank, collect_list
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.master("local").appName("LiftCapacity").getOrCreate()

# Data for Lifts and Passengers
lifts_data = [
    (1, 300),
    (2, 350)
]

passengers_data = [
    ('Rahul', 85, 1),
    ('Adarsh', 73, 1),
    ('Riti', 95, 1),
    ('Dheeraj', 80, 1),
    ('Vimal', 83, 2),
    ('Neha', 77, 2),
    ('Priti', 73, 2),
    ('Himanshi', 85, 2)
]

# Define the schema
lifts_columns = ['LIFT_ID', 'CAPACITY_KG']
passengers_columns = ['PASSENGER_NAME', 'WEIGHT_KG', 'LIFT_ID']

# Create DataFrames
lifts_df = spark.createDataFrame(lifts_data, lifts_columns)
passengers_df = spark.createDataFrame(passengers_data, passengers_columns)

# Step 1: Create a window specification to calculate cumulative sum of passenger weights per lift
window_spec = Window.partitionBy("LIFT_ID").orderBy("WEIGHT_KG")

# Step 2: Add cumulative sum to the Passengers DataFrame
passengers_with_cumsum_df = passengers_df.withColumn(
    "cum_sum", sum("WEIGHT_KG").over(window_spec)
)

# Step 3: Join the Passengers DataFrame with the Lifts DataFrame
joined_df = passengers_with_cumsum_df.join(lifts_df, "LIFT_ID")

# Step 4: Filter the rows where the cumulative sum is less than the lift's capacity
filtered_df = joined_df.filter(col("CAPACITY_KG") > col("cum_sum"))

# Step 5: Group by LIFT_ID and concatenate passenger names
result_df = filtered_df.groupBy("LIFT_ID").agg(
    collect_list("PASSENGER_NAME").alias("PASSENGERS_IN_LIFT")
)

# Step 6: Show the result
result_df.show(truncate=False)
