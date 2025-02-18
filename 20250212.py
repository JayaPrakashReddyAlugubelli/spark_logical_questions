from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("EmployeeSalaryAnalysis").getOrCreate()

# Sample Data for 'emp' table
emp_data = [
    (1, 'salary', 10000),
    (1, 'bonus', 5000),
    (1, 'hike_percent', 10),
    (2, 'salary', 15000),
    (2, 'bonus', 7000),
    (2, 'hike_percent', 8),
    (3, 'salary', 12000),
    (3, 'bonus', 6000),
    (3, 'hike_percent', 7)
]
emp_columns = ["emp_id", "salary_component_type", "val"]

# Create DataFrame for emp table
emp_df = spark.createDataFrame(emp_data, emp_columns)

# Pivoting the data to get separate columns for salary, bonus, and hike_percent
pivot_df = emp_df.groupBy("emp_id") \
    .pivot("salary_component_type", ["salary", "bonus", "hike_percent"]) \
    .agg(F.max("val"))

# Show the result
pivot_df.show(truncate=False)
