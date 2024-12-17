# Databricks notebook source
from pyspark.sql.functions import col
data=[("A","AA"),("B","BB"),("C","CC"),("AA","AAA"),("BB","BBB"),("CC","CCC")]
columns=["child","parent"]
df=spark.createDataFrame(data,columns)
df.show()
df.alias("df1").join(df.alias("df2"), col("df1.parent")==col("df2.child"),"inner").select(col("df1.child"),col("df1.parent"),col("df2.parent").alias("Grandparent")).show()

