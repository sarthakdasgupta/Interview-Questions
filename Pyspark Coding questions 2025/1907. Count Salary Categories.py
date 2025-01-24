# Databricks notebook source
# MAGIC %md
# MAGIC Write a solution to calculate the number of bank accounts for each salary category. The salary categories are:
# MAGIC ------------------------------
# MAGIC 1. "Low Salary": All the salaries strictly less than $20000.
# MAGIC 2. "Average Salary": All the salaries in the inclusive range [$20000, $50000].
# MAGIC 3. "High Salary": All the salaries strictly greater than $50000.
# MAGIC
# MAGIC The result table must contain all three categories. If there are no accounts in a category, return 0.

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# Create the Accounts DataFrame
data = [
    (3, 108939),
    (2, 12747),
    (8, 87709),
    (6, 91796),
]
columns = ["account_id", "income"]

accounts_df = spark.createDataFrame(data, columns)

accounts_df.display()

# COMMAND ----------

# Create the categories and compute the counts
low_salary_count = accounts_df.filter(col("income") < 20000).count()
average_salary_count = accounts_df.filter((col("income") >= 20000) & (col("income") <= 50000)).count()
high_salary_count = accounts_df.filter(col("income") > 50000).count()

print(f"Count for all three categoriers => low_salary_count:{low_salary_count}, average_salary_count:{average_salary_count} high_salary_count:{high_salary_count}, ")

# COMMAND ----------

# Combine results into a single DataFrame
result_data = [
    ("Low Salary", low_salary_count),
    ("Average Salary", average_salary_count),
    ("High Salary", high_salary_count),
]

result_df = spark.createDataFrame(result_data, ["category", "accounts_count"])

# Show the result
result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Alternate Approach
# MAGIC -------------

# COMMAND ----------

accounts_df.display()

# COMMAND ----------

df_low_salary = accounts_df.withColumn("income_count", when(col("income")<20000,1).otherwise(0))\
    .withColumn("category", lit("Low_salary"))\
    .select("income_count", "category")\

df_low_salary.display()

# COMMAND ----------

df_avg_salary = accounts_df.withColumn("income_count", when((col("income")>=20000) & (col("income")<=50000), 1).otherwise(0))\
    .withColumn("category", lit("Average Salary"))\
    .select("income_count", "category")\

df_avg_salary.display()

# COMMAND ----------

df_high_salary = accounts_df.withColumn("income_count", when(col("income")>50000, 1).otherwise(0))\
    .withColumn("category", lit("High Salary"))\
    .select("income_count", "category")\
\]
df_high_salary.display()

# COMMAND ----------

df_salary_union = df_low_salary.union(df_avg_salary).union(df_high_salary)
df_salary_union.display()

# COMMAND ----------

df_final = df_salary_union.groupBy("category").agg(sum(col("income_count")).alias("income_count"))
df_final.display()
