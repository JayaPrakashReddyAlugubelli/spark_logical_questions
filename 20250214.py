from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, row_number, count, sum
from pyspark.sql.window import Window

# Initialize the Spark session
spark = SparkSession.builder.master("local").appName("ImmediateOrder").getOrCreate()

# Sample data
data = [
    (1, 1, '2024-12-01', '2024-12-02'),
    (2, 2, '2024-12-02', '2024-12-02'),
    (3, 1, '2024-12-10', '2024-12-11'),
    (4, 3, '2024-12-01', '2024-12-01'),
    (5, 3, '2024-12-02', '2024-12-03'),
    (6, 2, '2024-12-10', '2024-12-11'),
    (7, 4, '2024-12-02', '2024-12-02')
]

# Define the schema
columns = ['id', 'customer_id', 'order_date', 'delivery_date']

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Define window specification
window_spec = Window.partitionBy('customer_id').orderBy('order_date')

# Add ROW_NUMBER and flag columns
df_with_flag = df.withColumn('rn', row_number().over(window_spec)) \
                 .withColumn('flag', when(col('order_date') == col('delivery_date'), 1).otherwise(0))

# Filter first orders and calculate total and immediate order percentages
result = df_with_flag.filter(col('rn') == 1) \
                     .agg(
                         count('*').alias('total_user'),
                         sum('flag').alias('immediate_order'),
                         (sum('flag') / count('*') * 100).alias('immediate_order_percentage')
                     )

# Show the result
result.show(truncate=False)
