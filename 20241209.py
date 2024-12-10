# Databricks notebook source
# How many delayed orders does each delivery partner have, considering the predicted delivery time and the actual delivery time?

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("del_partner", StringType(), True),
    StructField("predicted_time", StringType(), True),
    StructField("delivery_time", StringType(), True)
])

data = [
    (11, 'Partner C', '2024-02-29 11:30:00', '2024-02-29 12:00:00'),
    (12, 'Partner A', '2024-02-29 10:45:00', '2024-02-29 11:30:00'),
    (13, 'Partner B', '2024-02-29 09:00:00', '2024-02-29 09:45:00'),
    (14, 'Partner A', '2024-02-29 12:15:00', '2024-02-29 13:00:00'),
    (15, 'Partner C', '2024-02-29 13:30:00', '2024-02-29 14:15:00'),
    (16, 'Partner B', '2024-02-29 14:45:00', '2024-02-29 15:30:00'),
    (17, 'Partner A', '2024-02-29 16:00:00', '2024-02-29 16:45:00'),
    (18, 'Partner B', '2024-02-29 17:15:00', '2024-02-29 18:00:00'),
    (19, 'Partner C', '2024-02-29 18:30:00', '2024-02-29 19:15:00')
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)
df.show()

# COMMAND ----------

from pyspark.sql.functions import col, count
result_df=df.filter(col("predicted_time") < col("delivery_time")).groupBy(col('del_partner')).agg(count("*").alias("count")).orderBy("del_partner")
result_df.show()
