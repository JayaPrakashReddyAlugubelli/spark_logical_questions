# Databricks notebook source
# schedule a match between each teams
data=[(1,"India"),(2,"Australia"),(3,"England"),(4,"NewZealand")]
columns=["Id","TeamName"]
df=spark.createDataFrame(data,columns)
df.show()

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws

joined_df = df.alias("a").join(df.alias("b"),col("a.Id") < col("b.Id"))

result_df = joined_df.select(
    concat_ws(" VS ", col("a.TeamName"), col("b.TeamName")).alias("schedule_match")
).orderBy(col("a.Id"), col("b.Id"))

result_df.show(truncate=False)
