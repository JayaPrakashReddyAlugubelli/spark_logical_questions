
from pyspark.sql.functions import col,coalesce
# Data and Schema
data = [(1, 'yes', None, None),
        (2, None, 'yes', None),
        (3, 'No', None, 'yes')]

schema = ['customer_id', 'device_using1', 'device_using2', 'device_using3']

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show DataFrame
df.show()

resultDf=df.withColumn("result",coalesce(col("device_using1"),col("device_using2"),col("device_using2")))
resultDf.show()

