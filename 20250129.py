from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dense_rank
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.master("local").appName("PerformanceRanking").getOrCreate()

# Create the DataFrame
data = [
    ('Virat', 'Bat', 89),
    ('Axar', 'Ball', 3),
    ('Gill', 'Bat', 47),
    ('Buvi', 'Ball', 1),
    ('Shami', 'Ball', 5),
    ('Shardul', 'Ball', 2),
    ('Sreyas', 'Bat', 54),
    ('Rohit', 'Bat', 38)
]

columns = ['Name', 'Role', 'Performance']

df = spark.createDataFrame(data, columns)

# Create window specifications for batsmen and bowlers
window_spec_bat = Window.partitionBy("Role").orderBy(col("Performance").desc())
window_spec_ball = Window.partitionBy("Role").orderBy(col("Performance").desc())

# Apply dense_rank for batsmen and bowlers
df_bat = df.filter(col("Role") == "Bat") \
           .withColumn("dr", dense_rank().over(window_spec_bat)) \
           .select(col("Name").alias("batsmen"), "dr")

df_ball = df.filter(col("Role") == "Ball") \
            .withColumn("dr", dense_rank().over(window_spec_ball)) \
            .select(col("Name").alias("bowler"), "dr")

# Join the DataFrames on the dense rank column 'dr'
result = df_bat.join(df_ball, "dr").select("batsmen", "bowler", "dr")

# Show final result
result.show()
