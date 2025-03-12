from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark session
spark = SparkSession.builder \
    .appName("ProductSoldOnBothDays") \
    .getOrCreate()

# Create DataFrame (assuming you already have a DataFrame created with the provided data)
data = [
    ('2015-05-01', 'ODR1', 'PROD1', 5, 5),
    ('2015-05-01', 'ODR2', 'PROD2', 2, 10),
    ('2015-05-01', 'ODR3', 'PROD3', 10, 25),
    ('2015-05-01', 'ODR4', 'PROD1', 20, 5),
    ('2015-05-02', 'ODR5', 'PROD3', 5, 25),
    ('2015-05-02', 'ODR6', 'PROD4', 6, 20),
    ('2015-05-02', 'ODR7', 'PROD1', 2, 5),
    ('2015-05-02', 'ODR8', 'PROD5', 1, 50),
    ('2015-05-02', 'ODR9', 'PROD6', 2, 50),
    ('2015-05-02', 'ODR10', 'PROD2', 4, 10)
]

# Define schema
columns = ['ORDER_DAY', 'ORDER_ID', 'PRODUCT_ID', 'QUANTITY', 'PRICE']

# Create the DataFrame
df = spark.createDataFrame(data, columns)

# Group by PRODUCT_ID and count the occurrences
product_counts = df.groupBy('PRODUCT_ID').count()

# Filter to get products sold on both days (count > 1)
result = product_counts.filter(col('count') > 1)

# Show the result
result.show()


# Filter data for May 2nd
df_may2 = df.filter(df['ORDER_DAY'] == '2015-05-02')

# Get the products ordered on May 1st
df_may1 = df.filter(df['ORDER_DAY'] == '2015-05-01')

# Perform a left anti join to get products from May 2nd but not in May 1st
result = df_may2.join(df_may1, 'PRODUCT_ID', 'left_anti')

# Select distinct product IDs
result = result.select('PRODUCT_ID').distinct()

# Show the result
result.show()
