from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank

# Initialize Spark session
spark = SparkSession.builder.appName("EmployeeManager").getOrCreate()

# Sample employee data
data = [
    ('John', 'Smith'),
    ('Erica', 'Meyers'),
    ('Charles', 'Brown'),
    ('Michael', 'Brown'),
    ('HARSH', 'PARESH'),
    ('NARESH', 'PARESH'),
    ('SURESH', 'PARESH')
]

# Create a DataFrame
columns = ["emp_name", "mgr_name"]
df = spark.createDataFrame(data, columns)

# Step 1: Group by manager and count the number of employees
mgr_employee_count = df.groupBy("mgr_name").agg(count("emp_name").alias("emp_counts"))

# Step 2: Add dense rank for highest and lowest employee counts
window_desc = Window.orderBy(col("emp_counts").desc())
window_asc = Window.orderBy(col("emp_counts").asc())

ranked_df = mgr_employee_count.withColumn("HC", dense_rank().over(window_desc)) \
                              .withColumn("LC", dense_rank().over(window_asc))

# Step 3: Filter to get the manager with the highest and lowest employee counts
result = ranked_df.filter((col("HC") == 1) | (col("LC") == 1))

# Show the result
result.select("mgr_name", "emp_counts").show()
