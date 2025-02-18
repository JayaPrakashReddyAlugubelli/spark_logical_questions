from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("ProductsAnalysis").getOrCreate()

# Sample data for the Products table
products_data = [
    (1, 'Laptop', 'Electronics'),
    (2, 'Smartphone', 'Electronics'),
    (3, 'Tablet', 'Electronics'),
    (4, 'Headphones', 'Accessories'),
    (5, 'Smartwatch', 'Accessories'),
    (6, 'Keyboard', 'Accessories'),
    (7, 'Mouse', 'Accessories'),
    (8, 'Monitor', 'Accessories'),
    (9, 'Printer', 'Electronics')
]

# Creating DataFrame for the Products table
columns = ["ProductID", "Product", "Category"]
products_df = spark.createDataFrame(products_data, columns)

# Define window specifications for row numbering
window_spec_asc = Window.partitionBy("Category").orderBy("ProductID")
window_spec_desc = Window.partitionBy("Category").orderBy(F.desc("ProductID"))

# Adding row numbers to the DataFrame
products_with_rownum = products_df.withColumn("rn1", F.row_number().over(window_spec_asc)) \
                                  .withColumn("rn2", F.row_number().over(window_spec_desc))

# Perform the join on rn1 and rn2
result_df = products_with_rownum.alias("c1") \
    .join(products_with_rownum.alias("c2"), (F.col("c1.Category") == F.col("c2.Category")) & (F.col("c1.rn1") == F.col("c2.rn2"))) \
    .select("c1.ProductID", "c2.Product", "c2.Category")

# Show the result
result_df.show(truncate=False)
