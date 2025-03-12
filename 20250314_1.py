from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("MostRecentTransaction").getOrCreate()

# Create data for the Transaction_Table
data = [
    (550, '2020-05-12 05:29:44.120', 1001, 2000),
    (550, '2020-05-15 10:29:25.630', 1002, 8000),
    (460, '2020-03-15 11:29:23.620', 1003, 9000),
    (460, '2020-04-30 11:29:57.320', 1004, 7000),
    (460, '2020-04-30 12:32:44.233', 1005, 5000),
    (640, '2020-02-18 06:29:34.420', 1006, 5000),
    (640, '2020-02-18 06:29:37.120', 1007, 9000)
]

# Define columns
columns = ['AccountNumber', 'TransactionTime', 'TransactionID', 'Balance']

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Define the window specification to partition by AccountNumber and order by TransactionTime descending
windowSpec = Window.partitionBy('AccountNumber').orderBy(F.col('TransactionTime').desc())

# Add a row number based on the partition and order
df_with_rownum = df.withColumn('row_num', F.row_number().over(windowSpec))

# Filter the rows to get only the most recent transaction for each AccountNumber
result_df = df_with_rownum.filter(F.col('row_num') == 1).select('AccountNumber', 'TransactionTime', 'TransactionID', 'Balance')

# Show the result
result_df.show(truncate=False)
