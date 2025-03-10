from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws

# Initialize Spark session
spark = SparkSession.builder.master("local").appName("Email Concatenation").getOrCreate()

# Create the Emp_Details DataFrame
data = [
    (1001, 'M', 'YYYYY@gmaix.com', 104),
    (1002, 'M', 'ZZZ@gmaix.com', 103),
    (1003, 'F', 'AAAAA@gmaix.com', 102),
    (1004, 'F', 'PP@gmaix.com', 104),
    (1005, 'M', 'CCCC@yahu.com', 101),
    (1006, 'M', 'DDDDD@yahu.com', 100),
    (1007, 'F', 'E@yahu.com', 102),
    (1008, 'M', 'M@yahu.com', 102),
    (1009, 'F', 'SS@yahu.com', 100)
]

columns = ["EMPID", "Gender", "EmailID", "DeptID"]

emp_df = spark.createDataFrame(data, columns)

# Group by DeptID and concatenate EmailID values with semicolon separator
result_df = emp_df.groupBy("DeptID").agg(concat_ws(";", col("EmailID")).alias("Email_List"))

# Show the result
result_df.show(truncate=False)
