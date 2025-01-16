from pyspark.sql import SparkSession
from pyspark.sql.functions import first
from pyspark.sql.window import Window

# Initialize SparkSession
spark = SparkSession.builder.appName("EmployeeData").getOrCreate()

# Sample data
data = [
    ('Siva', 1, 30000),
    ('Ravi', 2, 40000),
    ('Prasad', 1, 50000),
    ('Sai', 2, 20000),
    ('Anna', 2, 10000)
]

# Define schema
columns = ['emp_name', 'dept_id', 'salary']

# Create DataFrame
emps_tbl = spark.createDataFrame(data, columns)

# Define the window specification
window_spec = Window.partitionBy('dept_id').orderBy('salary')

# Define the window for max salary (ordering descending)
window_spec_desc = Window.partitionBy('dept_id').orderBy(emps_tbl.salary.desc())

# Perform the query to get max and min salary employees by dept_id
result_df = emps_tbl.select(
    'dept_id',
    first('emp_name').over(window_spec_desc).alias('max_sal_empname'),
    first('emp_name').over(window_spec).alias('min_sal_empname')
).distinct()

# Show the result
result_df.show()
