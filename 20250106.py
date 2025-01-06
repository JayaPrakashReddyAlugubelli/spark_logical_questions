from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark session
spark = SparkSession.builder.appName("Grade Classification").getOrCreate()

# Create the DataFrame with the given data
data = [
    ('A', 75),
    ('B', 30),
    ('C', 55),
    ('A', 60),
    ('D', 91),
    ('B', 19),
    ('G', 36),
    ('S', 65),
    ('K', 49)
]

columns = ["sname", "marks"]

# Creating the DataFrame
df = spark.createDataFrame(data, columns)

# Add a new column 'grad' based on the marks
df = df.withColumn(
    "grad",
    F.when(df["marks"] >= 80, "ex")
     .when((df["marks"] >= 60) & (df["marks"] < 80), "vg")
     .when((df["marks"] >= 35) & (df["marks"] < 60), "av")
     .otherwise("po")
)

# Show the result
df.show()
