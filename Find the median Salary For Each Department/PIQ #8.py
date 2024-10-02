# Databricks notebook source
# MAGIC %md
# MAGIC Q. Find the median salary for each department
# MAGIC ----------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC The median salary is a statistical measure that represents the middle value of a set of salaries when they are sorted in order, either ascending or descending.<br>
# MAGIC
# MAGIC If there are an odd number of salary values, the median is simply the middle value.<br>
# MAGIC Example: If the salaries are [5000, 6000, 7000], the median is 6000 because it is the middle value.<br>
# MAGIC
# MAGIC If there are an even number of salary values, the median is the average of the two middle values.<br>
# MAGIC Example: If the salaries are [5000, 6000, 7000, 8000], the median is (6000 + 7000) / 2 = 6500.<br>

# COMMAND ----------

from pyspark.sql.functions import *  
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC Create DataFrame
# MAGIC ----------------

# COMMAND ----------

df_src = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/emp_data.csv")

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Define Window Function
# MAGIC ------------------------

# COMMAND ----------

# Define a window partitioned by department and ordered by salary
window_spec = Window.partitionBy("Department").orderBy("Salary")

# Add a row number and count of salaries within each department
df_with_row = df_src.withColumn("row_num", row_number().over(window_spec))\
                .withColumn("count", count("Salary").over(Window.partitionBy("Department")))

df_with_row.display()


# COMMAND ----------

# MAGIC %md
# MAGIC Result using rank and dense_rank
# MAGIC -----------------------------------

# COMMAND ----------

# Define a window partitioned by department and ordered by salary
window_spec = Window.partitionBy("Department").orderBy("Salary")

# Add a row number and count of salaries within each department
df_combined = df_src.withColumn("row_num", row_number().over(window_spec))\
    .withColumn("rank", rank().over(window_spec))\
    .withColumn("dense_rank", dense_rank().over(window_spec))\
    .withColumn("count", count("Salary").over(Window.partitionBy("Department")))

df_combined.display()


# COMMAND ----------

# MAGIC %md
# MAGIC How to handle odd and even counts
# MAGIC ---------------------------------------------
# MAGIC If count is odd -> middle value<br>
# MAGIC If count is even -> avg(middle + next to middle)

# COMMAND ----------

# Find the median position
# middle = middle index of the window. 3/2= 1.5 and ceil(1.5) ->
# next_to_middle = if the no of elements in window is even then middle index + 1

df_with_median = df_with_row.withColumn("middle", ceil(col("count") / 2))\
        .withColumn("next_to_middle", when(col("count")%2==0, col("middle")+1).otherwise(col("middle")))

df_with_median.display()                                    

# COMMAND ----------

# Filter to get the median salary per department
median_pos_salary_df = df_with_median.filter((col("row_num") == col("middle")) | (col("row_num") == col("next_to_middle")))
median_pos_salary_df.display()

# COMMAND ----------

final_result = median_pos_salary_df.groupBy("department").agg(ceil(avg("salary"))
                                                              .alias("median_salary"))
final_result.display()                                                              

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Why rank and dense_rank might not work in this case
# MAGIC -----------------------------------------------------

# COMMAND ----------

df_with_median = df_combined.withColumn("middle", ceil(col("count") / 2))\
        .withColumn("next_to_middle", when(col("count")%2==0, col("middle")+1).otherwise(col("middle")))

df_with_median.display()   
# df_with_median = df_combined.withColumn("median_pos", (df_combined["count"] / 2 + 0.5).cast("int"))
# df_with_median.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Rank**

# COMMAND ----------

# Filter to get the median salary per department
# median_pos_salary_df = df_with_median.filter((col("row_num") == col("middle")) | (col("row_num") == col("next_to_middle")))
# median_pos_salary_df.display()

median_salary_rank_df= df_with_median.filter((col("rank") == col("middle")) | (col("rank") == col("next_to_middle")))
median_salary_rank_df.display()                                  
                                

# COMMAND ----------

# MAGIC %md
# MAGIC **Dense Rank**

# COMMAND ----------

median_salary_drank_df= df_with_median.filter((col("dense_rank") == col("middle")) | (col("dense_rank") == col("next_to_middle")))
median_salary_drank_df.display()

# COMMAND ----------

final_result = median_salary_drank_df.groupBy("department").agg(ceil(avg("salary"))
                                                              .alias("median_salary"))
final_result.display()  

# COMMAND ----------

median_salary_df.display()

# COMMAND ----------

# Filter to get the median salary per department
median_salary_dense_rank_df= df_with_median.filter(col("dense_rank") == col("median_pos"))\
                                  .select("Department", "Salary")

median_salary_dense_rank_df.display()                                  
                                

# COMMAND ----------


