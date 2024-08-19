# Databricks notebook source
# Find the popularity percentage for each user on Meta/Facebook. The populrity percentage is defined as the no of friends the user has divided by the total no of users in the platform multiply with 100. Output each user with popular percentage. Order records in ascending order by user id.

# COMMAND ----------

from pyspark.sql import SparkSession

# Step 1: Initialize a Spark session
spark = SparkSession.builder.appName("UserRelationships").getOrCreate()

# Step 2: Create the DataFrame
data = [(1, 5), (1, 3), (1, 6), (2, 1), (2, 6), (3, 9), (4, 1), (7, 2), (8, 3)]
schema = ["user1", "user2"]

df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

from pyspark.sql.functions import col, count, round

df_union = df.select("user1", "user2").union(df.select(col("user2").alias("user1"), col("user1").alias("user2")))

df_counts = df_union.groupBy("user1").agg(count("user2").alias("user_counts"))

total_users = df_counts.count()

df_friends_ratio = df_counts.withColumn("friends_ratio", round((col("user_counts") / total_users) * 100, 2)).show()


