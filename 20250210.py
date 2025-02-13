from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("SalesAnalysis").getOrCreate()

# Sample Data for 'products' table
products_data = [(1, 'mobile'), (2, 'computer'), (3, 'tv')]
products_columns = ["product_id", "product_name"]

# Sample Data for 'sales' table
sales_data = [
    (1, 2022, 10, 1000),
    (1, 2023, 12, 1000),
    (1, 2024, 11, 1000),
    (2, 2022, 15, 1300),
    (2, 2023, 18, 1300),
    (3, 2022, 13, 1500)
]
sales_columns = ["product_id", "year", "quantity", "price"]

# Create DataFrames
products_df = spark.createDataFrame(products_data, products_columns)
sales_df = spark.createDataFrame(sales_data, sales_columns)

# Define a window specification for DENSE_RANK
window_spec = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())

# Create CTE (Common Table Expression) equivalent with DENSE_RANK
cte_df = sales_df.withColumn("dn", F.dense_rank().over(window_spec))

# Join products and sales based on product_id and filter for rank = 1
result_df = cte_df.join(products_df, cte_df.product_id == products_df.product_id) \
    .filter(cte_df.dn == 1) \
    .select(products_df.product_name, cte_df.year, cte_df.quantity, cte_df.price)

# Show the result
result_df.show(truncate=False)
