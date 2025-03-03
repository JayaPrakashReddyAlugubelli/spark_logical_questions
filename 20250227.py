from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.master("local").appName("Flight Source and Destination").getOrCreate()

# Sample data
data = [
    (1, 'Delhi', 'Kolkata'),
    (1, 'Kolkata', 'Bangalore'),
    (2, 'Mumbai', 'Pune'),
    (2, 'Pune', 'Goa'),
    (3, 'Kolkata', 'Delhi'),
    (3, 'Delhi', 'Srinagar')
]

# Define schema
columns = ['id', 'source', 'destination']

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Perform self-join to get the source and destination as per the requirement
result = df.alias('f1').join(
    df.alias('f2'),
    (col('f1.id') == col('f2.id')) & (col('f1.destination') == col('f2.source')),
    'inner'
).select(
    col('f1.id').alias('flight_id'),
    col('f1.source'),
    col('f2.destination')
)

# Show the result
result.show()
