# Databricks notebook source
# MAGIC %md
# MAGIC SALES ANALYSIS 1
# MAGIC -------------------

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC <h3>Write an SQL query that reports the best seller by total sales price, If there is a tie, report them all.<h3>
# MAGIC

# COMMAND ----------

df_products = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/product_sales_analysis_data.csv")
df_products.createOrReplaceTempView("v_products")

df_sales = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/sales_sales_analysis_data.csv")
df_sales.createOrReplaceTempView("v_sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_products

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_sales

# COMMAND ----------

# MAGIC %md
# MAGIC **SQL SOLUTION**

# COMMAND ----------

# MAGIC %sql
# MAGIC Select a.seller_id
# MAGIC from (
# MAGIC   select 
# MAGIC     seller_id, 
# MAGIC     rank() over(order by sum(price) desc) as rk
# MAGIC   from v_sales
# MAGIC   group by 1
# MAGIC   ) a
# MAGIC where a.rk=1

# COMMAND ----------

# MAGIC %md
# MAGIC **How much each seller sold**

# COMMAND ----------

# Step 1: Aggregate by seller_id and calculate the total price for each seller
aggregated_df = df_sales.groupBy("seller_id").agg(F.sum("price").alias("total_price"))
aggregated_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Adding rank order by sum of price**

# COMMAND ----------

# Step 2 a window specification to rank sellers by the sum of their prices in descending order
window_spec = Window.orderBy(desc("total_price"))

# Step 3: Add a rank column based on the window specification
ranked_df = aggregated_df.withColumn("rk", rank().over(window_spec))
ranked_df.display()

# COMMAND ----------

# Step 4: Filter for the seller with rank 1
top_seller_df = ranked_df.filter(col("rk") == 1).select("seller_id")
top_seller_df.display()
