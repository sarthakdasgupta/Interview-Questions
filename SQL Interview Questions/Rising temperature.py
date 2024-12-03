# Databricks notebook source
# MAGIC %md
# MAGIC <h1> RISING TEMPERATURE <h1>

# COMMAND ----------

# MAGIC %md
# MAGIC <h3>Write SQL query to find all records with higher temperatures compared to its previous dates (yesterday).
# MAGIC Return the result table in any order.<h3>

# COMMAND ----------

df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/rise_temp_data.csv")
df1.createOrReplaceTempView("v_rise_temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_rise_temp

# COMMAND ----------

# MAGIC %md
# MAGIC **How to get the previous records**

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   *,
# MAGIC   lag(temperature) over(order by recordDate) as prev_temp,
# MAGIC   lag(recordDate) over(order by 1) as prev_date
# MAGIC from v_rise_temp

# COMMAND ----------

# MAGIC %md
# MAGIC **Solution using lag**

# COMMAND ----------

# MAGIC %sql
# MAGIC with prevT as (
# MAGIC     select 
# MAGIC         *,
# MAGIC         lag(temperature) over(order by recordDate) as prev_temp,
# MAGIC         lag(recordDate) over(order by 1) as prev_date
# MAGIC     from v_rise_temp
# MAGIC )
# MAGIC select
# MAGIC     *
# MAGIC from prevT
# MAGIC where prev_temp is not null 
# MAGIC     and temperature>prev_temp 
# MAGIC     and date_sub(recordDate, 1) = prev_date

# COMMAND ----------

# MAGIC %md
# MAGIC **Solution using join**

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC     w1.id
# MAGIC from v_rise_temp w1 join v_rise_temp w2
# MAGIC on date_sub(w1.recordDate, 1) = w2.recordDate
# MAGIC where w1.temperature > w2.temperature

# COMMAND ----------


