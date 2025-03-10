from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.master("local").appName("Employees Above Average Salary").getOrCreate()

# Create the Employee DataFrame
data = [
    (1001, 'Mark', 60000, 2),
    (1002, 'Antony', 40000, 2),
    (1003, 'Andrew', 15000, 1),
    (1004, 'Peter', 35000, 1),
    (1005, 'John', 55000, 1),
    (1006, 'Albert', 25000, 3),
    (1007, 'Donald', 35000, 3)
]

columns = ["EmpID", "EmpName", "Salary", "DeptID"]

emp_df = spark.createDataFrame(data, columns)

# Define the window spec to partition by DeptID and calculate average salary
window_spec = Window.partitionBy("DeptID")

# Add the average salary column for each department
emp_with_avg_salary = emp_df.withColumn("avg_salary", F.avg("Salary").over(window_spec))

# Filter the employees who earn more than the average salary in their department
result_df = emp_with_avg_salary.filter(emp_with_avg_salary.Salary > emp_with_avg_salary.avg_salary)

# Select the relevant columns and show the result
result_df.select("EmpID", "EmpName", "Salary", "DeptID").show(truncate=False)
