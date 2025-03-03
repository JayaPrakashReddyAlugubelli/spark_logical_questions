from pyspark.sql import SparkSession
from pyspark.sql.functions import max, min

# Initialize Spark session
spark = SparkSession.builder.master("local").appName("Salary Analysis").getOrCreate()

# Sample data
data = [
    ('emp1', 'X', 1000, 'Sales'),
    ('emp2', 'X', 1020, 'IT'),
    ('emp3', 'X', 870, 'Sales'),
    ('emp4', 'Y', 1200, 'Marketing'),
    ('emp5', 'Y', 1500, 'IT'),
    ('emp6', 'Y', 1100, 'Sales'),
    ('emp7', 'Z', 1050, 'IT'),
    ('emp8', 'Z', 1350, 'Marketing'),
    ('emp9', 'Z', 1700, 'Sales')
]

# Define schema
columns = ['emp_id', 'company', 'salary', 'dept']

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Perform aggregation to find max and min salary by company
result = df.groupBy("company").agg(
    max("salary").alias("highest_salary"),
    min("salary").alias("lowest_salary")
)

# Show the result
result.show()
