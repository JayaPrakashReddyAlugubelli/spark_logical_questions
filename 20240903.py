# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Initialize Spark session
spark = SparkSession.builder.appName("CreateDataFrame").getOrCreate()

# Define the schema
schema = StructType([
    StructField("category", StringType(), True),
    StructField("brand_name", StringType(), True)
])

# Define the data
data = [
    ('chocolates', '5-star'),
    (None, 'dairy milk'),
    (None, 'perk'),
    (None, 'eclair'),
    ('Biscuits', 'Britania'),
    (None, 'good day'),
    (None, 'boost')
]

# Create DataFrame
brands_df = spark.createDataFrame(data, schema)

# Show the DataFrame
brands_df.show()


# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, row_number, count, first, col
from pyspark.sql.window import Window

# Add a unique column for ordering
brands_df = brands_df.withColumn("dc", monotonically_increasing_id())

# Define the window spec for row_number
window_spec_rn = Window.orderBy("dc")

# Add row number
brands_df_rn = brands_df.withColumn("rn", row_number().over(window_spec_rn))

# Drop the unique column used for ordering
brands_df_rn = brands_df_rn.drop("dc")

# Define the window spec for counting categories
window_spec_count = Window.orderBy("rn").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Add count of categories
brands_df_ct = brands_df_rn.withColumn("ct", count("category").over(window_spec_count))

# Define the window spec for the first value of category
window_spec_first_value = Window.partitionBy("ct").orderBy("rn")

# Select the first value of category and brand_name
result_df = brands_df_ct.select(
    first("category").over(window_spec_first_value).alias("category"),
    col("brand_name")
)

# Show the result
result_df.show()
