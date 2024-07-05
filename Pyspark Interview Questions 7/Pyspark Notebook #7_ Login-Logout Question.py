# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window


# COMMAND ----------

# MAGIC %md
# MAGIC Write pyspark code to display following data from below tables
# MAGIC -------
# MAGIC 1. employeeid
# MAGIC 2. default_number,
# MAGIC 3. total_entry
# MAGIC 4. total_login
# MAGIC 5. total_logout
# MAGIC 6. first_login
# MAGIC 7. first_logout
# MAGIC 8. last_login
# MAGIC 9. last_logout
# MAGIC

# COMMAND ----------

df_emp_logs = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/employee_logs.csv")
df_emp_logs.display()

# COMMAND ----------

df_emp_ph = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/employee_phone.csv")
df_emp_ph.display()

# COMMAND ----------

df_emp_logs.createOrReplaceTempView("df_emp_logs_tbl")
df_emp_ph.createOrReplaceTempView("df_emp_ph_tbl")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df_emp_logs_tbl

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df_emp_ph_tbl

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC SQL Solution
# MAGIC ---------

# COMMAND ----------

# MAGIC %md
# MAGIC Display the following data for each employee- 
# MAGIC
# MAGIC employeeid, default_number, total_entry, total_login, total_logout, last_login, last_logout, first_login, first_logout,

# COMMAND ----------

# MAGIC %md
# MAGIC Order of SQL Execution:
# MAGIC
# MAGIC FROM, JOIN, WHERE, GROUP BY, HAVING, SELECT, DISTINCT, ORDER BY, and finally, LIMIT/OFFSET

# COMMAND ----------

# MAGIC %md
# MAGIC Certain words in sql questions and what they convey -
# MAGIC
# MAGIC total, sum, no of, how many times -> count() or sum()
# MAGIC
# MAGIC first, last, most, least, highest, lowest -> max() or min()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- employeeid, default_number, total_entry, total_login, total_logout, last_login, last_logout, first_login, first_logout,
# MAGIC
# MAGIC -- FROM, JOIN, WHERE, GROUP BY, HAVING, SELECT, DISTINCT, ORDER BY, and finally, LIMIT/OFFSET
# MAGIC select 
# MAGIC       e.employee_id,
# MAGIC       e.phone,
# MAGIC       count(*) as totalEntires,
# MAGIC       sum(case when entry_detail = 'login' then 1 else 0 end) as total_login,
# MAGIC       sum(case when entry_detail = 'logout' then 1 else 0 end) as total_logout,
# MAGIC       max(case when entry_detail = 'login' then timestamp_detail else 0 end) as last_login,
# MAGIC       max(case when entry_detail = 'logout' then timestamp_detail else 0 end) as last_logout,
# MAGIC       min(case when entry_detail = 'login' then timestamp_detail else null end) as first_login,
# MAGIC       min(case when entry_detail = 'logout' then timestamp_detail else null end) as first_logout
# MAGIC from df_emp_logs_tbl ec join df_emp_ph_tbl e on ec.employee_id = e.employee_id
# MAGIC where e.default = true
# MAGIC group by 1,2

# COMMAND ----------

df_result = spark.sql("""
    select 
      e.employee_id,
      e.phone,
      count(*) as totalEntires,
      sum(case when entry_detail = 'login' then 1 else 0 end) as total_login,
      sum(case when entry_detail = 'logout' then 1 else 0 end) as total_logout,
      max(case when entry_detail = 'login' then timestamp_detail else 0 end) as last_login,
      max(case when entry_detail = 'logout' then timestamp_detail else 0 end) as last_logout,
      min(case when entry_detail = 'login' then timestamp_detail else null end) as first_login,
      min(case when entry_detail = 'logout' then timestamp_detail else null end) as first_logout
from df_emp_logs_tbl ec join df_emp_ph_tbl e on ec.employee_id = e.employee_id
where e.default = true
group by 1,2""")

df_result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC DataFrame Solutions
# MAGIC ------

# COMMAND ----------

# MAGIC %md
# MAGIC Display the following data for each employee- 
# MAGIC
# MAGIC employeeid, default_number, total_entry, total_login, total_logout, last_login, last_logout, first_login, first_logout,

# COMMAND ----------

df_log = df_emp_logs.withColumn("total_login_num", when(col("entry_detail") == "login", lit(1)).otherwise(lit(0)))\
    .withColumn("total_logout_num", when(col("entry_detail")== "logout", lit(1)).otherwise(lit(0)))\
    .withColumn("login_time", when(col("entry_detail") == "login", col("timestamp_detail")).otherwise(None))\
    .withColumn("logout_time", when(col("entry_detail") == "logout", col("timestamp_detail")).otherwise(None))
df_log.display()

# COMMAND ----------

df_log_transform = df_log.groupBy("employee_id").agg(
    count("*").alias("total_entries"),
    sum(col('total_login_num')).alias('total_login'),\
    sum(col('total_logout_num')).alias('total_logout'),\
    max(col('login_time')).alias('last_login'),\
    max(col('logout_time')).alias('last_logout'),\
    min(col('login_time')).alias('first_login'),\
    min(col('logout_time')).alias('first_logout'))

df_log_transform.display()

# COMMAND ----------

df_join = df_log_transform.join(df_emp_ph, df_log_transform["employee_id"]==df_emp_ph["employee_id"], "inner")\
    .select(df_log_transform["*"], "phone", "default")
df_join.display()

# COMMAND ----------

df_result = df_join.filter("default is true")
df_result.display()

# COMMAND ----------


