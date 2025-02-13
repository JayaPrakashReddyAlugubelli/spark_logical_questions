from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, max

# Initialize Spark session
spark = SparkSession.builder.appName("Phone Directory").getOrCreate()

# Create the PhoneDirectory DataFrame
data = [
    (1001, 'Cellular', '555-897-5421'),
    (1001, 'Work', '555-897-6542'),
    (1001, 'Home', '555-698-9874'),
    (2002, 'Cellular', '555-963-6544'),
    (2002, 'Work', '555-812-9856'),
    (3003, 'Cellular', '555-987-6541')
]

columns = ['CustomerID', 'Type', 'PhoneNumber']

phone_df = spark.createDataFrame(data, columns)

# Pivoting the data to get phone numbers by type
pivoted_df = phone_df.groupBy("CustomerID").agg(
    max(when(col("Type") == 'Cellular', col("PhoneNumber"))).alias("Cellular"),
    max(when(col("Type") == 'Work', col("PhoneNumber"))).alias("Work"),
    max(when(col("Type") == 'Home', col("PhoneNumber"))).alias("Home")
)

# Show the result ordered by CustomerID
pivoted_df.orderBy("CustomerID").show(truncate=False)



---------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max

# Initialize Spark session
spark = SparkSession.builder.appName("Phone Directory").getOrCreate()

# Create the PhoneDirectory DataFrame
data = [
    (1001, 'Cellular', '555-897-5421'),
    (1001, 'Work', '555-897-6542'),
    (1001, 'Home', '555-698-9874'),
    (2002, 'Cellular', '555-963-6544'),
    (2002, 'Work', '555-812-9856'),
    (3003, 'Cellular', '555-987-6541')
]

columns = ['CustomerID', 'Type', 'PhoneNumber']

phone_df = spark.createDataFrame(data, columns)

# Pivoting the data to get phone numbers by type
pivoted_df = phone_df.groupBy("CustomerID") \
    .pivot("Type", ["Cellular", "Work", "Home"]) \
    .agg(max("PhoneNumber"))

# Show the result ordered by CustomerID
pivoted_df.orderBy("CustomerID").show(truncate=False)
