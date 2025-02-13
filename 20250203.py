from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag
from pyspark.sql.window import Window

# Initialize the Spark session
spark = SparkSession.builder.appName("RevenueIncreasingCompany").getOrCreate()

# Create the DataFrame
data = [
    ('x', 2020, 100),
    ('x', 2021, 90),
    ('x', 2022, 120),
    ('y', 2020, 100),
    ('y', 2021, 100),
    ('z', 2020, 100),
    ('z', 2021, 120),
    ('z', 2022, 130),
]

columns = ['company_name', 'year_cal', 'revenue']

df = spark.createDataFrame(data, columns)

# Define the window specification for partitioning by company_name and ordering by year_cal
windowSpec = Window.partitionBy("company_name").orderBy("year_cal")

# Calculate the revenue difference with the previous year (lag)
df_with_diff = df.withColumn("yr_rn", col("revenue") - lag("revenue", 1).over(windowSpec))

# Filter companies where the minimum revenue difference is greater than or equal to 0
filtered_companies = df_with_diff.groupBy("company_name").agg(
    {"yr_rn": "min"}).filter(col("min(yr_rn)") >= 0)

# Join the original DataFrame with the filtered companies to get the result
result = df.join(filtered_companies, "company_name", "inner")

# Show the result
result.select("company_name", "year_cal", "revenue").show()
