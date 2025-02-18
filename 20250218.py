from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.master("local").appName("Consecutive Free Seats").getOrCreate()

# Sample data
data = [(1, 1), (2, 0), (3, 1), (4, 1), (5, 1), (6, 0), (7, 1), 
        (8, 1), (9, 0), (10, 1), (11, 0), (12, 1), (13, 0), (14, 1),
        (15, 1), (16, 0), (17, 1), (18, 1), (19, 1), (20, 1)]

# Create DataFrame
df = spark.createDataFrame(data, ["id", "seat_id"])

# Create a window specification for row numbering
windowSpec = Window.orderBy("id")

# Add row_number to identify consecutive free seats
df_with_rn = df.filter(df.seat_id == 1).withColumn("rn", row_number().over(windowSpec))

# Create a new column 'rn_group' to identify consecutive seat groups
df_with_group = df_with_rn.withColumn("rn_group", df_with_rn["id"] - df_with_rn["rn"])

# Group by the 'rn_group' column to find the consecutive free seat groups
result = df_with_group.groupBy("rn_group").count().filter("count >= 3").join(df_with_group, "rn_group").select("id").orderBy("id")

# Show the result
result.show()
