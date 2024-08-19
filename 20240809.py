# Databricks notebook source
# find out toatl employes in each dep ,how many male emps and female emps
# output:-
# +-----+---+----+------+
# |  Dep|Dep|Male|Female|
# +-----+---+----+------+
# |   IT|  4|   3|     1|
# |   HR|  5|null|     5|
# |Sales|  6|   4|     2|
# +-----+---+----+------+

# COMMAND ----------

data=[('IT','M'),('IT','F'),('IT','M'),('IT','M'),('HR','F'),('HR','F'),('HR','F'),('HR','F'),('HR','F'),('Sales','M'),('Sales','M'),('Sales','F'),('Sales','M'),('Sales','M'),('Sales','F')]
schema=["Dep","Gender"]
df1=spark.createDataFrame(data,schema)
df1.show()

# COMMAND ----------

from pyspark.sql.functions import when,count,sum,col

df2=df1.select(df1.Dep ,when(df1.Gender == "M",1).alias("Male"),
               when(df1.Gender == "F",1).alias("Female"))

result=df2.groupBy(df2.Dep).agg(count("Dep").alias("total_count"),sum("Male").alias("Male_count"),sum("Female").alias("feMale_count")).show()
