# Databricks notebook source
#Query to calculate Average of Employee Salaries working under manager in SQL
from pyspark.sql.functions import col
data=[(10,"Anil",50000,18),(11,"Vika",75000,16),(12,"Nisha",40000,18),(13,"Nidhi",60000,17),(14,"Priya",80000,18),(15,"Mohit",45000,18),(16,"Rajesh",90000,16),(17,"Raman",55000,16),(18,"Santosh",65000,17)]
columns=["Emp_Id","Emp_name","Salary","Manager_Id"]
df=spark.createDataFrame(data,columns)
df.show()
df.createOrReplaceTempView("Employee1")
df2=spark.sql("select e1.Emp_Id,e1.Emp_name,e1.Salary,e1.Manager_Id,e2.Emp_name as Manager_Name  from Employee1 e1,Employee1 e2 where e1.Manager_Id=e2.Emp_Id order by e1.Emp_Id ")
df2.groupby("Manager_Id","Manager_Name").avg("Salary").show()



