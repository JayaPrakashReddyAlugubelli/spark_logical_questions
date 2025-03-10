from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Create Spark session
spark = SparkSession.builder.master("local").appName("Student Marks").getOrCreate()

# Create the DataFrame with the provided data
data = [
    ('Rahul', 90, 2010),
    ('Sanjay', 80, 2010),
    ('Mohan', 70, 2010),
    ('Rahul', 90, 2011),
    ('Sanjay', 85, 2011),
    ('Mohan', 65, 2011),
    ('Rahul', 80, 2012),
    ('Sanjay', 80, 2012),
    ('Mohan', 90, 2012)
]

columns = ['Student_Name', 'Total_Marks', 'Year']

df = spark.createDataFrame(data, columns)

# Define window spec for lag function
window_spec = Window.partitionBy('Student_Name').orderBy('Year')

# Add the prev_ye_marks using the lag function
df_with_prev_marks = df.withColumn('prev_ye_marks', F.lag('Total_Marks').over(window_spec))

# Filter the rows where Total_Marks is greater than or equal to prev_ye_marks and prev_ye_marks is not null
result = df_with_prev_marks.filter((F.col('prev_ye_marks').isNotNull()) & (F.col('Total_Marks') >= F.col('prev_ye_marks')))

# Show the result
result.select('Student_Name', 'Total_Marks', 'Year', 'prev_ye_marks').show()
