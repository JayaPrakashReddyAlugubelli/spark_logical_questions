from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.master("local").appName("InventoryRollingSum").getOrCreate()

# Sample data
data = [
    ('Keyboard', 'K1001', 20, '2020-03-01'),
    ('Keyboard', 'K1001', 30, '2020-03-02'),
    ('Keyboard', 'K1001', 10, '2020-03-03'),
    ('Keyboard', 'K1001', 40, '2020-03-04'),
    ('Laptop', 'L1002', 100, '2020-03-01'),
    ('Laptop', 'L1002', 60, '2020-03-02'),
    ('Laptop', 'L1002', 40, '2020-03-03'),
    ('Monitor', 'M5005', 30, '2020-03-01'),
    ('Monitor', 'M5005', 20, '2020-03-02')
]

# Define schema
columns = ['ProdName', 'ProductCode', 'Quantity', 'InventoryDate']

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Convert InventoryDate to date type
df = df.withColumn('InventoryDate', F.to_date(F.col('InventoryDate')))

# Define window specification
window_spec = Window.partitionBy('ProdName').orderBy('InventoryDate')

# Calculate cumulative sum
df_with_cumsum = df.withColumn('rolling_sum', F.sum('Quantity').over(window_spec))

# Show the result
df_with_cumsum.show()
3