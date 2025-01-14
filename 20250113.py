from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.master("local").appName("DENSE_RANK Example").getOrCreate()

# Create the DataFrame (emulating the SQL 'emp' table)
data = [
    (101, 'John Doe', 75000.00, 'HR'),
    (102, 'Jane Smith', 82000.00, 'Finance'),
    (103, 'Alice Johnson', 95000.00, 'IT'),
    (104, 'Bob Brown', 67000.00, 'Marketing'),
    (105, 'Carol Davis', 72000.00, 'Sales'),
    (106, 'David Wilson', 58000.00, 'IT'),
    (107, 'Emma Garcia', 70000.00, 'Finance'),
    (108, 'Frank Miller', 88000.00, 'Marketing'),
    (109, 'Grace Clark', 68000.00, 'Sales'),
    (110, 'Henry Lee', 62000.00, 'HR')
]

columns = ["id", "name", "salary", "dept"]

df = spark.createDataFrame(data, columns)

# Define the window specification for ranking based on salary in descending order
windowSpec = Window.orderBy(F.col("salary").desc())

# Apply DENSE_RANK to the DataFrame
ranked_df = df.withColumn("rank", F.dense_rank().over(windowSpec))

# Filter the 2nd and 3rd highest salaries and calculate their sum
result = ranked_df.filter(F.col("rank").isin([2, 3])).agg(F.sum("salary").alias("sum_2nd_3rd_highest"))

# Show the result
result.show()
