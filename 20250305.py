from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName('TransactionAnalysis').getOrCreate()

# Sample data based on the problem statement
data = [
    (1001, 20001, 10000, '2020-04-25'),
    (1001, 20002, 15000, '2020-04-25'),
    (1001, 20003, 80000, '2020-04-25'),
    (1001, 20004, 20000, '2020-04-25'),
    (1002, 30001, 7000, '2020-04-25'),
    (1002, 30002, 15000, '2020-04-25'),
    (1002, 30003, 22000, '2020-04-25')
]

# Define schema
columns = ['CustID', 'TranID', 'TranAmt', 'TranDate']

# Create a DataFrame
df = spark.createDataFrame(data, columns)

# Define the window specification for partitioning by CustID
windowSpec = Window.partitionBy('CustID')

# Calculate the MaxTranAmt and ratio
df_with_max_and_ratio = df.withColumn(
    'MaxTranAmt', F.max('TranAmt').over(windowSpec)
).withColumn(
    'ratio', F.round(F.col('TranAmt') / F.col('MaxTranAmt'), 3)
)

# Show the result
df_with_max_and_ratio.select('CustID', 'TranID', 'TranAmt', 'MaxTranAmt', 'ratio', 'TranDate').show(truncate=False)
