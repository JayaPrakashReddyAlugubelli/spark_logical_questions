from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lead, datediff, date_add
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.master("local").appName("MissingWeeks").getOrCreate()

# Sample data (same as provided in the SQL query)
data = [
    (201, '2024-07-11', 140),
    (201, '2024-07-18', 160),
    (201, '2024-07-25', 150),
    (201, '2024-08-01', 180),
    (201, '2024-08-15', 170),
    (201, '2024-08-29', 130)
]

# Create DataFrame
columns = ['pid', 'sls_dt', 'sls_amt']
df = spark.createDataFrame(data, columns)

# Define the window specification
window_spec = Window.partitionBy('pid').orderBy('sls_dt')

# Add the next sales date and calculate the date difference
df_with_diff = df.withColumn('next_sls_dt', lead('sls_dt').over(window_spec)) \
                 .withColumn('diff', datediff('next_sls_dt', 'sls_dt'))

# Filter where the difference is greater than 7 days and add 7 days to the current sales date
missing_weeks_df = df_with_diff.filter(col('diff') > 7) \
                               .withColumn('missing_date', date_add('sls_dt', 7)) \
                               .select('missing_date')

# Show the result
missing_weeks_df.show()
