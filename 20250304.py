from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lead, row_number
from pyspark.sql.window import Window

# Initialize the Spark session
spark = SparkSession.builder.appName("Orders").getOrCreate()

# Create the DataFrame
data = [
    (1, 101, '2024-01-10', 150.00),
    (2, 101, '2024-02-15', 200.00),
    (3, 101, '2024-03-20', 180.00),
    (4, 102, '2024-01-12', 200.00),
    (5, 102, '2024-02-25', 250.00),
    (6, 102, '2024-03-10', 320.00),
    (7, 103, '2024-01-25', 400.00),
    (8, 103, '2024-02-15', 420.00)
]

columns = ["order_id", "customer_id", "order_date", "order_amount"]

df = spark.createDataFrame(data, columns)

# Define the window specification
window_spec = Window.partitionBy("customer_id").orderBy(col("order_date").desc())

# Apply LEAD and ROW_NUMBER
df_with_lead = df.withColumn(
    "second_highest_order", lead("order_amount", 1).over(window_spec)
).withColumn(
    "rn", row_number().over(window_spec)
)

# Filter the rows where rn = 1 to get the latest order for each customer
final_result = df_with_lead.filter(col("rn") == 1).select(
    "customer_id", "order_amount", "second_highest_order"
).withColumnRenamed("order_amount", "latest_order_amount")

# Show the result
final_result.show()

# Stop the Spark session
spark.stop()
