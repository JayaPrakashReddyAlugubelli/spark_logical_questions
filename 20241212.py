# Databricks notebook source
from pyspark.sql.window import Window

employees_Salary = [("James", "Sales", 2000),("sofy", "Sales", 3000),("Laren", "Sales", 4000),("Kiku", "Sales", 5000),("Sam", "Finance", 6000),("Samuel", "Finance", 6000),("Yash", "Finance", 8000),("Rabin", "Finance", 9000),("Lukasz", "Marketing", 10000),("Jolly", "Marketing", 11000),("Mausam", "Marketing", 12000),("Lamba", "Marketing", 13000),("Jogesh", "HR", 14000),("Mannu", "HR", 15000),("Sylvia", "HR", 16000),("Sama", "HR", 17000),]
employeesDF = spark.createDataFrame(employees_Salary,schema="employee_name STRING, dept_name STRING, salary INTEGER")
employeesDF.show()

# COMMAND ----------

from pyspark.sql.functions import dense_rank,col,asc,desc
windowPartition = Window.partitionBy("dept_name").orderBy(col("salary").asc())
employeeDF = employeesDF.withColumn("rank", dense_rank().over(windowPartition))
employeeDF.filter("rank==1").show()
