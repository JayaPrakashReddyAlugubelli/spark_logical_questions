from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum
from pyspark.sql.window import Window

# Create a Spark session
spark = SparkSession.builder \
    .appName("Bus Weight Calculation") \
    .getOrCreate()

# Sample data
data = [
    (5, 'john', 120, 2),
    (4, 'tom', 100, 1),
    (3, 'rahul', 95, 4),
    (6, 'bhavna', 100, 5),
    (1, 'ankita', 79, 6),
    (2, 'Alex', 80, 3)
]

# Define the schema
columns = ["personId", "personName", "personWeight", "turn"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Create a Window specification to calculate cumulative sum ordered by turn
window_spec = Window.orderBy("turn")

# Add a column for the cumulative sum of the weights
df_with_sum = df.withColumn("cumulativeWeight", _sum("personWeight").over(window_spec))

# Filter out rows where the cumulative weight exceeds 400
df_filtered = df_with_sum.filter(df_with_sum.cumulativeWeight <= 400)

# Get the last person who fits within the weight limit (ordering by turn)
result = df_filtered.orderBy("turn", ascending=False).limit(1)

# Show the result
result.select("personName").show()
