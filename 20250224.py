from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, when
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("ProductPrices").getOrCreate()

# Create a DataFrame
data = [
    (1, 20, '2024-12-14'),
    (2, 50, '2024-12-14'),
    (1, 30, '2024-12-15'),
    (1, 35, '2024-12-16'),
    (2, 65, '2024-12-17'),
    (3, 20, '2024-12-18')
]

columns = ["product_id", "new_price", "change_date"]

df = spark.createDataFrame(data, columns)

# Define the window specification
window_spec = Window.partitionBy("product_id").orderBy("change_date")

# Add a column with lag for new price
df_with_price = df.withColumn(
    "price",
    when(col("change_date") == '2024-12-16', col("new_price"))
    .when((col("change_date") > '2024-12-16') & (col("product_id") == lag("product_id", 1).over(window_spec)),
          lag("new_price", 1).over(window_spec))
    .when(col("change_date") > '2024-12-16', 10)
    .otherwise(None)
)

# Filter and select the final result
result = df_with_price.filter(col("price").isNotNull()).select("product_id", "price")

# Show the result
result.show()

# Stop Spark session
spark.stop()
