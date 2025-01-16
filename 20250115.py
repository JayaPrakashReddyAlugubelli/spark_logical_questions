from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, date_format

# Initialize Spark session
spark = SparkSession.builder.master("local").appName("CurrencyExchange").getOrCreate()

# Create the DataFrame with your data
data = [
    ('USD', '2024-06-01', 1.20),
    ('USD', '2024-06-02', 1.21),
    ('USD', '2024-06-03', 1.22),
    ('USD', '2024-06-04', 1.23),
    ('USD', '2024-07-01', 1.25),
    ('USD', '2024-07-02', 1.26),
    ('USD', '2024-07-03', 1.27),
    ('EUR', '2024-06-01', 1.40),
    ('EUR', '2024-06-02', 1.41),
    ('EUR', '2024-06-03', 1.42),
    ('EUR', '2024-06-04', 1.43),
    ('EUR', '2024-07-01', 1.45),
    ('EUR', '2024-07-02', 1.46),
    ('EUR', '2024-07-03', 1.47)
]

# Define the schema for the DataFrame
columns = ['currency_code', 'date', 'currency_exchange_rate']

# Create a DataFrame from the list of data
df = spark.createDataFrame(data, columns)

# Convert the `date` column to DateType
df = df.withColumn("date", df["date"].cast("date"))

# Add a new column for the currency_code_year_month (formatted as yyyy_MM)
df = df.withColumn("currency_code_year_month", 
                   col("currency_code") + "_" + date_format(col("date"), "yyyy_MM"))

# Group by `currency_code_year_month` and get the min and max exchange rates
result = df.groupBy("currency_code_year_month") \
    .agg(
        min("currency_exchange_rate").alias("currency_rate_beginofmonth"),
        max("currency_exchange_rate").alias("currency_rate_endofmonth")
    )

# Show the results
result.show(truncate=False)
