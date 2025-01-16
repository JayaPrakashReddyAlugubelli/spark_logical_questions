from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count,sum

# Initialize Spark session
spark = SparkSession.builder.master("local").appName("GenderPercentage").getOrCreate()

# Sample data (you can replace this with actual data)
data = [
    (1, 'John Doe', 'Male'),
    (2, 'Jane Smith', 'Female'),
    (3, 'Michael Johnson', 'Male'),
    (4, 'Emily Davis', 'Female'),
    (5, 'Robert Brown', 'Male'),
    (6, 'Sophia Wilson', 'Female'),
    (7, 'David Lee', 'Male'),
    (8, 'Emma White', 'Female'),
    (9, 'James Taylor', 'Male'),
    (10, 'William Clark', 'Male')
]

# Define schema
columns = ["eid", "ename", "gender"]

# Create DataFrame
df = spark.createDataFrame(data, columns)


# Total count of rows
total_count = df.count()


df=df.withColumn("Male_pre",when(col("gender") == "Male", 1).otherwise(0)).withColumn("Female_pre",when(col("gender") == "Female", 1).otherwise(0)).select((sum("Male_pre") * 100)/total_count ,(sum("Female_pre")* 100)/total_count)

df.show()