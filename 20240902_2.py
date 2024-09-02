# Databricks notebook source
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("HappinessRanking").getOrCreate()

# Create a DataFrame with the provided data
data = [
    (1, 'Finland'), (2, 'Denmark'), (3, 'Iceland'), (4, 'Israel'), (5, 'Netherlands'),
    (6, 'Sweden'), (7, 'Norway'), (8, 'Switzerland'), (9, 'Luxembourg'),
    (128, 'Srilanka'), (126, 'India')
]
columns = ["ranking", "country"]
happiness_df = spark.createDataFrame(data, columns)

happiness_df.show()

# COMMAND ----------

from pyspark.sql.functions import when,col
result_df=happiness_df.withColumn("ranks", when(col("country")=="India",1).when(col("country")=="Srilanka",2).otherwise(3)).orderBy("ranks")
result_df.show()
