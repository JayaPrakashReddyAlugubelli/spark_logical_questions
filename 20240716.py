# Databricks notebook source
data = [
    ('Delhi', 101, 1, '2021-09-05'),
    ('Bangalore', 102, 12, '2021-09-08'),
    ('Bangalore', 102, 13, '2021-09-08'),
    ('Bangalore', 102, 14, '2021-09-08'),
    ('Mumbai', 103, 3, '2021-09-10'),
    ('Mumbai', 103, 30, '2021-09-10'),
    ('Chennai', 104, 4, '2021-09-15'),
    ('Delhi', 105, 5, '2021-09-20'),
    ('Bangalore', 106, 6, '2021-09-25'),
    ('Mumbai', 107, 7, '2021-09-28'),
    ('Chennai', 108, 8, '2021-09-30'),
    ('Delhi', 109, 9, '2021-10-05'),
    ('Bangalore', 110, 10, '2021-10-08'),
    ('Mumbai', 111, 11, '2021-10-10'),
    ('Chennai', 112, 12, '2021-10-15'),
    ('Kolkata', 113, 13, '2021-10-20'),
    ('Hyderabad', 114, 14, '2021-10-25'),
    ('Pune', 115, 15, '2021-10-28'),
    ('Jaipur', 116, 16, '2021-10-30')
]

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
schema = StructType([
    StructField("city", StringType(), True),
    StructField("restaurant_id", IntegerType(), True),
    StructField("order_id", IntegerType(), True),
    StructField("order_date", StringType(), True)
])
restaurant_orders= spark.createDataFrame(data, schema)
restaurant_orders.show()

# COMMAND ----------

from pyspark.sql.functions import col, count
# Filtering data
filtered_orders = restaurant_orders.filter((col("city").isin(['Delhi', 'Mumbai', 'Bangalore', 'Hyderabad'])) &
                                           (col("order_date").between('2021-09-01', '2021-09-30')))

# Aggregating and counting orders
city_orders = filtered_orders.groupBy("city").agg(count("order_id").alias("total_orders"))

# Finding city with maximum orders
max_orders_city = city_orders.orderBy(col("total_orders").desc()).limit(1)

# Show or collect results
max_orders_city.show()
