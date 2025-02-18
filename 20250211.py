from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("EmployeeAnalysis").getOrCreate()

# Sample Data for 'emp_info' table
emp_info_data = [
    (1, 'Akash', 'Sales', 100),
    (2, 'John', 'Sales', 110),
    (3, 'Rohit', 'Sales', 100),
    (4, 'Tom', 'IT', 200),
    (5, 'Subham', 'IT', 205),
    (6, 'Vabna', 'IT', 200),
    (7, 'Prativa', 'Marketing', 150),
    (8, 'Rahul', 'Marketing', 155),
    (9, 'Yash', 'Marketing', 160)
]
emp_info_columns = ["id", "name", "dept", "salary"]

# Create DataFrame for emp_info table
emp_info_df = spark.createDataFrame(emp_info_data, emp_info_columns)

# Group by salary and count occurrences
salary_count_df = emp_info_df.groupBy("salary").count()

# Filter salaries that appear more than once
salaries_to_filter = salary_count_df.filter(salary_count_df["count"] > 1).select("salary")

# Join the filtered salaries with the original DataFrame
result_df = emp_info_df.join(salaries_to_filter, "salary").select("id", "name", "dept", "salary")

# Show the result
result_df.show(truncate=False)
