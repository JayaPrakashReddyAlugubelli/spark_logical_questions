from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

# Assuming spark is already initialized
# spark = SparkSession.builder.appName("RemoveDuplicates").getOrCreate()

# Create DataFrame from SQL query or directly from the data
df = spark.sql("""
    SELECT ID, EmpName, Department, Age, Gender, Salary 
    FROM EmpDetail
""")

# Define window for partitioning and ordering
window_spec = Window.partitionBy("EmpName", "Department", "Age", "Gender", "Salary").orderBy("ID")

# Add row number to identify duplicates
df_with_row_num = df.withColumn("row_num", row_number().over(window_spec))

# Keep only rows where row_num == 1
df_no_duplicates = df_with_row_num.filter("row_num == 1")

# If you want to replace the existing table with this new data
df_no_duplicates.write.mode("overwrite").saveAsTable("EmpDetail")

# Alternatively, if you're dealing with a view or temp table:
# df_no_duplicates.createOrReplaceTempView("EmpDetail_no_duplicates")
# Then you might want to run SQL to delete from the original table based on this view