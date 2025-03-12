from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, sum

# Initialize Spark session
spark = SparkSession.builder.appName("MatchResults").getOrCreate()

# Create the data for the Match_Result table
data = [
    ('India', 'Australia', 'India'),
    ('India', 'England', 'England'),
    ('SouthAfrica', 'India', 'India'),
    ('Australia', 'England', None),
    ('England', 'SouthAfrica', 'SouthAfrica'),
    ('Australia', 'India', 'Australia')
]

# Define columns
columns = ['Team_1', 'Team_2', 'Result']

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Create a CTE-like DataFrame with the results of both Team_1 and Team_2
cte_df = df.select('Team_1', when(col('Result').isNull(), 'tie').otherwise(col('Result')).alias('Result'))\
    .unionAll(df.select('Team_2', when(col('Result').isNull(), 'tie').otherwise(col('Result')).alias('Result')))

# Group the data to calculate the total matches played, won, lost, and tied
result_df = cte_df.groupBy('Team_1') \
    .agg(
        count('Team_1').alias('played'),
        sum(when(col('Team_1') == col('Result'), 1).otherwise(0)).alias('win'),
        sum(when(col('Team_1') != col('Result'), 1).otherwise(0)).alias('loss'),
        sum(when(col('Result') == 'tie', 1).otherwise(0)).alias('tie')
    )

# Show the result
result_df.show(truncate=False)
