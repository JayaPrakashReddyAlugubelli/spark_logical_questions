from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, sum

# Initialize Spark session
spark = SparkSession.builder.appName("CricketMatchAnalysis").getOrCreate()

# Create the DataFrame for cricket matches
data = [
    (1, 'ENG', 'NZ', 'NZ'),
    (2, 'PAK', 'NED', 'PAK'),
    (3, 'AFG', 'BAN', 'BAN'),
    (4, 'SA', 'SL', 'SA'),
    (5, 'AUS', 'IND', 'AUS'),
    (6, 'NZ', 'NED', 'NZ'),
    (7, 'ENG', 'BAN', 'ENG'),
    (8, 'SL', 'PAK', 'PAK'),
    (9, 'AFG', 'IND', 'IND'),
    (10, 'SA', 'AUS', 'SA'),
    (11, 'BAN', 'NZ', 'BAN'),
    (12, 'PAK', 'IND', 'IND'),
    (13, 'SA', 'IND', 'DRAW')
]

columns = ['match_id', 'team1', 'team2', 'result']

df = spark.createDataFrame(data, columns)

# Create a DataFrame with team names and results from both team1 and team2
team_results = df.select('team1', 'result').union(
    df.select('team2', 'result').withColumnRenamed('team2', 'team1')
)

# Calculate the counts: number of matches, wins, losses, and draws
result = team_results.groupBy('team1').agg(
    count('*').alias('noofmatch'),
    sum(when(col('team1') == col('result'), 1).otherwise(0)).alias('noofwin'),
    sum(when((col('team1') != col('result')) & (col('result') != 'DRAW'), 1).otherwise(0)).alias('nooflost'),
    sum(when(col('result') == 'DRAW', 1).otherwise(0)).alias('DRAW')
)

# Show the result
result.show(truncate=False)
