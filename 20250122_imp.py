from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Sample Data
data = [
    (1, 2019),
    (1, 2020),
    (1, 2021),
    (2, 2022),
    (2, 2021),
    (3, 2019),
    (3, 2021),
    (3, 2022)
]

# Create DataFrame
df = spark.createDataFrame(data, ["pid", "year"])

# Define Window Specification
window_spec = Window.partitionBy("pid").orderBy("year")

# Add row number and calculate consecutive years' grouping number
df_with_rn = df.withColumn("rn", F.row_number().over(window_spec)) \
    .withColumn("group_id", F.col("year") - F.col("rn"))

# Group by pid and group_id, and filter for those with 3 or more events
result = df_with_rn.groupBy("pid", "group_id").count() \
    .filter("count >= 3") \
    .select("pid")

# Show the result
result.show()
