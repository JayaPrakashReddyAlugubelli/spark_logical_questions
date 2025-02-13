from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum

# Initialize Spark session
spark = SparkSession.builder.appName("Team Matches").getOrCreate()

# Create the Matches DataFrame
data = [
    ('India', 'Pakistan', 'India'),
    ('India', 'Srilanka', 'India'),
    ('Srilanka', 'India', 'India'),
    ('Pakistan', 'Srilanka', 'Srilanka'),
    ('Pakistan', 'England', 'Pakistan'),
    ('Srilanka', 'England', 'Srilanka')
]

columns = ['Team1', 'Team2', 'Winner']

matches_df = spark.createDataFrame(data, columns)

# Create a union of both Team1 and Team2 columns with their respective winners
team_results_df = matches_df.select(col("Team1").alias("TeamName"), col("Winner")) \
    .union(matches_df.select(col("Team2").alias("TeamName"), col("Winner")))

# Calculate Won and Lost for each Team
team_stats_df = team_results_df.groupBy("TeamName") \
    .agg(
        sum(when(col("TeamName") == col("Winner"), 1).otherwise(0)).alias("Won"),
        sum(when(col("TeamName") != col("Winner"), 1).otherwise(0)).alias("Lost")
    )

# Calculate TotalMatches and order by TeamName
final_df = team_stats_df.withColumn("TotalMatches", col("Won") + col("Lost")) \
    .select("TeamName", "TotalMatches", "Won", "Lost") \
    .orderBy("TeamName")

# Show the result
final_df.show(truncate=False)
