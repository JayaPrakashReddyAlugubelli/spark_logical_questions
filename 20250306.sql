from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName('ContinuousSequence').getOrCreate()

# Sample data based on the problem statement
data = [
    ('A', 1),
    ('A', 2),
    ('A', 3),
    ('A', 5),
    ('A', 6),
    ('A', 8),
    ('A', 9),
    ('B', 11),
    ('C', 1),
    ('C', 2),
    ('C', 3)
]

# Define schema
columns = ['Grou', 'Sequence']

# Create a DataFrame
df = spark.createDataFrame(data, columns)

# Define the window specification for partitioning by Grou
windowSpec = Window.partitionBy('Grou').orderBy('Sequence')

# Add a row number column to identify continuous segments
df_with_rn = df.withColumn(
    'rn', F.col('Sequence') - F.row_number().over(windowSpec)
)

# Group by Grou and rn to get min and max values of continuous Sequence
result = df_with_rn.groupBy('Grou', 'rn').agg(
    F.min('Sequence').alias('min_Sequence'),
    F.max('Sequence').alias('MAX_Sequence')
).orderBy('Grou', 'min_Sequence')

# Show the result
result.show(truncate=False)
