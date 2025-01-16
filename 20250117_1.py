from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Initialize SparkSession
spark = SparkSession.builder.appName("EmployeeSalaries").getOrCreate()

# Sample data
data = [
    (3, 'Bob', 60000),
    (4, 'Diana', 70000),
    (5, 'Eve', 60000),
    (6, 'Frank', 80000),
    (7, 'Grace', 70000),
    (8, 'Henry', 90000)
]

# Create DataFrame
columns = ['employee_id', 'ename', 'salary']
employee_df = spark.createDataFrame(data, columns)

# Step 1: Group by salary and filter salaries that appear more than once
salaries_with_multiple_employees = employee_df.groupBy('salary') \
    .agg(count('employee_id').alias('count')) \
    .filter(col('count') > 1) \
    .select('salary')

# Step 2: Join the original employee DataFrame with the filtered salaries
result_df = employee_df.join(salaries_with_multiple_employees, on='salary', how='inner') \
    .select('ename')

# Show the result
result_df.show()
