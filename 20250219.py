from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, when
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("StockPriceChange").getOrCreate()

# Sample data
data = [
    ("𝘈", "2024-01-01", 100),
    ("𝘈", "2024-01-02", 105),
    ("𝘈", "2024-01-03", 104),
    ("𝘉", "2024-01-01", 200),
    ("𝘉", "2024-01-02", 200),
    ("𝘉", "2024-01-03", 201)
]

# Define schema
columns = ["stock_id", "date", "price"]

# Create the DataFrame
df = spark.createDataFrame(data, columns)

# Define a window spec to partition by stock_id and order by date
window_spec = Window.partitionBy("stock_id").orderBy("date")

# Add the previous day's price using lag function
df_with_lag = df.withColumn("prv_price", lag("price", 1).over(window_spec))

# Add price_change column based on comparison with previous day's price
df_with_change = df_with_lag.withColumn(
    "price_change",
    when(col("price") == col("prv_price"), "SAME")
    .when(col("price") > col("prv_price"), "UP")
    .when(col("price") < col("prv_price"), "DOWN")
)

# Show the final result
df_with_change.select("stock_id", "date", "price", "price_change").show()
