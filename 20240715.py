# Databricks notebook source
data = [("ramu","a.b.c"), ("hari","x.y.z")]
columns=["Col1","Col2"]
df=spark.createDataFrame(data,columns)
df.show()

# COMMAND ----------

#output
# +----+-----+----+----+----+
# |Col1|Col2 |Col3|Col4|Col5|
# +----+-----+----+----+----+
# |ramu|a.b.c|a   |b   |c   |
# |hari|x.y.z|x   |y   |z   |
# +----+-----+----+----+----+

# COMMAND ----------

from pyspark.sql.functions import *
df1 = df.withColumn('Col3', split(df['Col2'], '\.').getItem(0)) \
        .withColumn('Col4', split(df['Col2'], '\.').getItem(1)) \
        .withColumn('Col5', split(df['Col2'], '\.').getItem(2))
df1.show(truncate=False)
