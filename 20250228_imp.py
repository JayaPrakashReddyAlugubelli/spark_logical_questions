from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, when

# Initialize Spark session
spark = SparkSession.builder.master("local").appName("SQL to PySpark").getOrCreate()

# Create the source DataFrame
source_data = [(1, 'A'), (2, 'B'), (3, 'C'), (4, 'D')]
target_data = [(1, 'A'), (2, 'B'), (4, 'X'), (5, 'F')]

source_columns = ['id', 'name']
target_columns = ['id', 'name']

source_df = spark.createDataFrame(source_data, source_columns)
target_df = spark.createDataFrame(target_data, target_columns)

# Perform the left join
left_join = source_df.alias('a').join(target_df.alias('b'), source_df.id == target_df.id, 'left') \
    .select(coalesce(source_df.id, target_df.id).alias('id'),
            when(source_df.name == target_df.name, 'Matched')
            .when(source_df.name != target_df.name, 'Mismatched')
            .when(target_df.name.isNull(), 'New in Source')
            .when(source_df.name.isNull(), 'New in target')
            .alias('output'))

# Perform the right join
right_join = source_df.alias('a').join(target_df.alias('b'), source_df.id == target_df.id, 'right') \
    .select(coalesce(source_df.id, target_df.id).alias('id'),
            when(source_df.name == target_df.name, 'Matched')
            .when(source_df.name != target_df.name, 'Mismatched')
            .when(target_df.name.isNull(), 'New in Source')
            .when(source_df.name.isNull(), 'New in target')
            .alias('output'))

# Union the results
result_df = left_join.union(right_join)

# Filter out 'Matched' rows
filtered_result_df = result_df.filter(result_df.output != 'Matched')

# Show the result
filtered_result_df.show()
