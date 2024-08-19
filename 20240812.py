# Databricks notebook source
# findout total marks for each student add new col as Total_Sum
from pyspark.sql.functions import col
data=[(203040,"Rajesh",10,20,30,40,50)]
columns=["RollNo","name","tamil","eng","math","sci","social"]
df=spark.createDataFrame(data,columns)
df.withColumn("Total_Sum", col("tamil")+col("eng")+col("math")+col("sci")+col("social")).show()

