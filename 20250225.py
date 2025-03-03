from pyspark.sql import SparkSession
from pyspark.sql.functions import col, first
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("CityPopulation").getOrCreate()

# Create a DataFrame with the city information
data = [
    (1, 'Maharashtra', 'Mumbai', 1000),
    (2, 'Maharashtra', 'Pune', 500),
    (3, 'Maharashtra', 'Nagpur', 400),
    (4, 'Punjab', 'Amritsar', 800),
    (5, 'Punjab', 'Ludhiana', 350),
    (6, 'Punjab', 'Patiala', 200),
    (7, 'TamilNadu', 'Chennai', 700),
    (8, 'TamilNadu', 'Vellore', 400)
]

columns = ["id", "state", "city", "population"]

df = spark.createDataFrame(data, columns)

# Define the window specification to partition by state and order by population
window_spec_asc = Window.partitionBy("state").orderBy("population")
window_spec_desc = Window.partitionBy("state").orderBy(col("population").desc())

# Use first() function to get the first city in ascending and descending population order
result = df.withColumn("min_population", first("city").over(window_spec_asc)) \
           .withColumn("max_population", first("city").over(window_spec_desc)) \
           .select("state", "min_population", "max_population") \
           .distinct()

# Show the result
result.show()

# Stop Spark session
spark.stop()
