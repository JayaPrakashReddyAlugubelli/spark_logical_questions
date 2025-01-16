from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dense_rank
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.master("local").appName("DeptwiseSecondHighestSalary").getOrCreate()

# Create the 'department' DataFrame
department_data = [
    (101, 'HR'),
    (102, 'Finance'),
    (103, 'Marketing')
]
department_columns = ['deptid', 'deptname']
department_df = spark.createDataFrame(department_data, department_columns)

# Create the 'employee' DataFrame
employee_data = [
    (1, 70000, 101),
    (2, 50000, 101),
    (3, 60000, 101),
    (4, 65000, 102),
    (5, 65000, 102),
    (6, 55000, 102),
    (7, 60000, 103),
    (8, 70000, 103),
    (9, 80000, 103)
]
employee_columns = ['empid', 'salary', 'deptid']
employee_df = spark.createDataFrame(employee_data, employee_columns)

# Join employee and department DataFrames on deptid
joined_df = employee_df.join(department_df, on='deptid', how='inner')

# Define the window specification for DENSE_RANK
window_spec = Window.partitionBy("deptid").orderBy(col("salary").desc())

# Add DENSE_RANK() column to the DataFrame
ranked_df = joined_df.withColumn("dn", dense_rank().over(window_spec))

# Filter for the second-highest salary (dn == 2)
second_highest_df = ranked_df.filter(col("dn") == 2).select("empid", "deptname", "salary", "dn")

# Show the result
second_highest_df.show()
