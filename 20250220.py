from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Initialize Spark session
spark = SparkSession.builder.appName("CustomerProducts").getOrCreate()

# Create sample data for products and customers
products_data = [(1,), (2,), (3,)]
customers_data = [('c1', 1), ('c2', 1), ('c2', 2), ('c2', 3), ('c3', 1), ('c3', 2)]

# Create DataFrames for products and customers
products_df = spark.createDataFrame(products_data, ['product_id'])
customers_df = spark.createDataFrame(customers_data, ['customer_id', 'product_id'])

# Count the total number of products
total_products = products_df.count()

# Group customers by customer_id and count the number of unique products they bought
customer_counts = customers_df.groupBy('customer_id').agg(count('product_id').alias('product_count'))

# Find customers who bought all products
customers_who_bought_all = customer_counts.filter(col('product_count') == total_products)

# Show the result
customers_who_bought_all.show()
