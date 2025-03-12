from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when

# Initialize Spark session
spark = SparkSession.builder.appName("TotalSales").getOrCreate()

# Create data for the SalesTable
data = [
    ('ProductA', 1998, 1000),
    ('ProductA', 1999, 1200),
    ('ProductA', 2000, 1500),
    ('ProductB', 1998, 800),
    ('ProductB', 1999, 900),
    ('ProductB', 2000, 1100),
    ('ProductC', 1998, 600),
    ('ProductC', 1999, 700),
    ('ProductC', 2000, 750)
]

# Define columns
columns = ['Product', 'SalesYear', 'QuantitySold']

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Group by SalesYear and calculate total sales for each year
sales_by_year = df.groupBy('SalesYear').agg(
    sum('QuantitySold').alias('TotalSales')
)

# Pivot the result to get total sales for each year as separate columns
pivot_df = sales_by_year.groupBy().pivot('SalesYear', [1998, 1999, 2000]).agg(
    sum('TotalSales')
)

# Add the 'totalsales' row to match the desired output format
pivot_df = pivot_df.withColumn('totalsales', lit('totalsales'))

# Re-arrange the columns and display the result
final_df = pivot_df.select(
    lit('totalsales').alias('totalsales'),
    col('1998').alias('1998'),
    col('1999').alias('1999'),
    col('2000').alias('2000')
)

# Show the result
final_df.show(truncate=False)
