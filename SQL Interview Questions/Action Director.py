# Databricks notebook source
# MAGIC %md
# MAGIC Actors and Directors Who Cooperated At Least Three Times
# MAGIC -----

# COMMAND ----------

# MAGIC %md
# MAGIC **Write a SQL query for a report that provides the pairs (actor_id, director_id) <br> where the actor has cooperated with the director at least three times.**

# COMMAND ----------

df = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/action_dir_data-2.csv")
df.createOrReplaceTempView("v_action_dir")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_action_dir

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct actor_id, director_id from v_action_dir

# COMMAND ----------

# MAGIC %md
# MAGIC **Count of distinct actor_id and director_id combination**

# COMMAND ----------

# MAGIC %sql
# MAGIC select actor_id, director_id, COUNT(*) from v_action_dir GROUP BY 1,2

# COMMAND ----------

# MAGIC %md
# MAGIC **Selecting the record with count = 3**

# COMMAND ----------

# MAGIC %sql
# MAGIC select actor_id, director_id from (
# MAGIC select actor_id, director_id, COUNT(*) as c from v_action_dir GROUP BY 1,2
# MAGIC ) where c >= 3

# COMMAND ----------

# MAGIC %md
# MAGIC **Solution without subquery**

# COMMAND ----------

# MAGIC %sql
# MAGIC select actor_id, director_id from v_action_dir group by actor_id, director_id having count(*) >= 3

# COMMAND ----------


