# Databricks notebook source
data=[(1,'John','ADF'),(1,'John','ADB'),(1,'John','PowerBI'),(2,'Joanne','ADF'),(2,'Joanne','SQL'),(2,'Joanne','Crystal Report'),(3,'Vikas','ADF'),(3,'Vikas','SQL'),(3,'Vikas','SSIS'),(4,'Monu','SQL'),(4,'Monu','SSIS'),(4,'Monu','SSAS'),(4,'Monu','ADF')]
schema=["EmpId","EmpName","Skill"]
df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

# output:
#     +-------+--------------------------+
# |EmpName|                     Skill|
# +-------+--------------------------+
# |   John|       [ADF, ADB, PowerBI]|
# | Joanne|[ADF, SQL, Crystal Report]|
# |  Vikas|          [ADF, SQL, SSIS]|
# |   Monu|    [SQL, SSIS, SSAS, ADF]|
# +-------+--------------------------+

# COMMAND ----------

from pyspark.sql.functions import collect_list,concat_ws,collect_set
#using collection_list
df1=df.groupBy('EmpName').agg(collect_list('Skill').alias('Skill'))
df1.show()

#using collect_set
df2=df.groupBy('EmpName').agg(collect_set('Skill').alias('Skill'))
df2.show()


