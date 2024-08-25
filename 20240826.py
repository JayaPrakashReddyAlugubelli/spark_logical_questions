# Databricks notebook source
from pyspark.sql.functions import explode
data = [(1,['mobile','PC','Tab']),(2,['mobile','PC']),(3,['Tab','Pen'])]
schema=['customer_id','product_purchase']
df = spark.createDataFrame(data, schema)
df.show()

resultDf=df.withColumn("product",explode("product_purchase"))
resultDf.show()
