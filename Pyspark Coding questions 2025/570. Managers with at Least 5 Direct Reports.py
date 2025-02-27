# Databricks notebook source
# MAGIC %md
# MAGIC Write a solution to find managers with at least five direct reports.
# MAGIC ------------

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# Create the Employee DataFrame
data = [
    (101, "John", "A", None),
    (102, "Dan", "A", 101),
    (103, "James", "A", 101),
    (104, "Amy", "A", 101),
    (105, "Anne", "A", 101),
    (106, "Ron", "B", 101),
]
columns = ["id", "name", "department", "managerId"]

employee_df = spark.createDataFrame(data, columns)
employee_df.createOrReplaceTempView("v_employee_df")
employee_df.display()


# COMMAND ----------

Output: 
+------+
| name |
+------+
| John |
+------+

# COMMAND ----------

# MAGIC %sql
# MAGIC select id, name from v_employee_df where id in (
# MAGIC select managerid from v_employee_df group by 1 having count(*) >=5
# MAGIC )

# COMMAND ----------

# Count the number of direct reports for each manager
manager_counts = employee_df.groupBy("managerId").agg(count("*").alias("direct_reports"))
manager_counts.display()

# COMMAND ----------

# Filter managers with at least 5 direct reports
managers_with_5_or_more_reports = manager_counts.filter(col("direct_reports") >= 5)
managers_with_5_or_more_reports.display()

# COMMAND ----------

# Join to get manager details
result_df = employee_df.join(managers_with_5_or_more_reports, 
                             employee_df["id"]==managers_with_5_or_more_reports["managerId"], 
                             "inner")\
    .select("id", "name", "department", "direct_reports")

result_df.display()

# COMMAND ----------


