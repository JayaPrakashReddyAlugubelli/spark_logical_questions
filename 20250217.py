from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize Spark session
spark = SparkSession.builder.appName("ProductPivot").getOrCreate()

# Sample data
data = [
    ('banana', 1000, 'usa'),
    ('carrots', 1500, 'usa'),
    ('beans', 1600, 'usa'),
    ('orange', 2000, 'usa'),
    ('banana', 400, 'china'),
    ('carrots', 1200, 'china'),
    ('beans', 1500, 'china'),
    ('orange', 4000, 'china'),
    ('banana', 2000, 'canada'),
    ('carrots', 2000, 'canada'),
    ('beans', 2000, 'mexico')
]

# Define schema
columns = ['product', 'amount', 'country']

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Pivot the DataFrame
pivoted_df = df.groupBy('country') \
    .pivot('product') \
    .agg({'amount': 'max'}) \
    .na.fill(0)  # Replace nulls with 0

# Show the result
pivoted_df.show()
