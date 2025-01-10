from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, max

# Initialize Spark session
spark = SparkSession.builder.master("local").appName("Swap Students").getOrCreate()

# Define the data
data = [
    (1, 'Abbot'),
    (2, 'Doris'),
    (3, 'Emerson'),
    (4, 'Green'),
    (5, 'Jeames')
]

# Create DataFrame
df = spark.createDataFrame(data, ['id', 'student'])

# Calculate the maximum 'id'
max_id = df.agg(max('id')).collect()[0][0]

# Apply the CASE logic using 'when'
df_with_swap = df.withColumn(
    'swap', 
    when((col('id') % 2 == 1) & (col('id') != max_id), col('id') + 1)  # Odd id and not max_id, increment by 1
    .when(col('id') % 2 == 0, col('id') - 1)  # Even id, decrement by 1
    .when((col('id') % 2 == 1) & (col('id') == max_id), col('id'))  # Odd id and is the max_id, keep same
    .otherwise(col('id'))  # Default case, keep the id
)

# Order the DataFrame by 'swap' column
df_with_swap = df_with_swap.orderBy('swap')

# Show the result
df_with_swap.show()
