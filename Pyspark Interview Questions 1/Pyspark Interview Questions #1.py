# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

emp_df = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/Employee_sample_data1-1.csv")
emp_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Q1 For a given department find which employee got the highest salary**

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Divide the employees deparment wise - use parititonBy in these scenarios
# MAGIC 2. Order the employees using salary in each partition i.e department
# MAGIC 3. Decide which window function is most suitable - rank, row_number, dense_rank
# MAGIC 4. Based on the fact that if you need highest or lowest, your ordering should be in ascending or descending.
# MAGIC 5. In both the cases the value that you have to filter = 1

# COMMAND ----------

win = Window.partitionBy(col("department"))\
    .orderBy(col("Salary").desc())
resultDf = emp_df.withColumn("rn", row_number().over(win))

colslist = ["employee_id", "full_name", "job", "department", "salary", "rn"]
resultDf.select(colslist).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Q2 For a given department list the employees whose salaries are higher than department average**

# COMMAND ----------

emp_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Create dataframe with department and average salary of each department
# MAGIC 2. Join emp df and dept avg salary df based on department name and salary condition

# COMMAND ----------

dept_avg = emp_df.groupBy(col("department")).agg(round(avg("salary"), 2).alias("avg_dept_salary"))
dept_avg.display()

# COMMAND ----------

colslist = ["employee_id", "full_name", "job", "department", "salary", "avg_dept_salary"]
result_df = emp_df.join(dept_avg, (emp_df["department"]==dept_avg["department"]) & 
                        (emp_df["salary"]>=dept_avg["avg_dept_salary"]), "inner")\
                    .select(emp_df["*"], dept_avg["avg_dept_salary"])
result_df.select(colslist).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Q3 For a given department list the employees whose salaries are lower than department average**

# COMMAND ----------

colslist = ["employee_id", "full_name", "job", "department", "salary", "avg_dept_salary"]
result_df = emp_df.join(dept_avg, (emp_df["department"]==dept_avg["department"]) & 
                        (emp_df["salary"]<=dept_avg["avg_dept_salary"]), "inner")\
                    .select(emp_df["*"], dept_avg["avg_dept_salary"])
result_df.select(colslist).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Q4 For a given department list the employees who have been working for more than 2 years**

# COMMAND ----------

emp_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Convert the hire date to date type
# MAGIC 2. Find no of years of service. Use months_between to calculate no of months between two dates
# MAGIC 3. Divide the total no of months value by 12 to get total years

# COMMAND ----------

resultDf = emp_df\
    .withColumn("hire_date", to_date(col("hire date"), "dd-MM-yyy"))\
    .withColumn("noOfYears", round(months_between(current_date(), col("hire_date"))/lit(12), 1))



resultDf.filter(col("noOfYears")>=2).display()


# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC **Q5 What is the total count of employee in each department that are not managers**

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Filter the non manager employees
# MAGIC 2. Group by using department 
# MAGIC 3. Find the count 

# COMMAND ----------

emp_df.display()

# COMMAND ----------

non_manager = emp_df.filter(col("job")!="Manager")
non_manager.display()


# COMMAND ----------

resultDf = non_manager.groupBy(col("department")).count()]
resultDf.display()

# COMMAND ----------


