# Databricks notebook source
# MAGIC %md
# MAGIC SALES ANALYSIS 2
# MAGIC -------------

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC <h3>Write a pyspark code that reports the buyers who have bought S8 but not iPhone. Note that S8 and iPhone are products present in the Product table.

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
# MAGIC select s.buyer_id
# MAGIC from 
# MAGIC v_sales s
# MAGIC where 
# MAGIC     s.buyer_id in (
# MAGIC         select s.buyer_id from v_sales s join v_products p on s.product_id = p.product_id where p.product_name = "S8"
# MAGIC     )
# MAGIC     and 
# MAGIC     s.buyer_id not in (
# MAGIC         select s.buyer_id from v_sales s join v_products p on s.product_id = p.product_id where p.product_name = "iPhone"
# MAGIC     )

# COMMAND ----------

# Filter for buyers who purchased "S8"
buyers_with_s8 = df_sales.join(df_products, df_sales.product_id == df_products.product_id) \
    .filter(col("product_name") == "S8") \
    .select("buyer_id").distinct()

buyers_with_s8.display()

# COMMAND ----------

# Filter for buyers who purchased "iPhone"
buyers_with_iphone = df_sales.join(df_products, df_sales.product_id == df_products.product_id) \
    .filter(col("product_name") == "iPhone") \
    .select("buyer_id").distinct()

buyers_with_iphone.display()

# COMMAND ----------

# Find buyers who purchased "S8" but not "iPhone"
# LEFT-ANTI join return non matching records from left
result = buyers_with_s8.join(buyers_with_iphone, "buyer_id", "left_anti") \
    .select("buyer_id").distinct()

result.display()

# COMMAND ----------


