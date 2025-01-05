# Databricks notebook source
# MAGIC %md
# MAGIC SALES ANALYSIS 3
# MAGIC ------------------

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC <h3>Write pyspark code that reports the products that were only sold in the first quarter of 2019. That is, between 2019-01-01 and 2019-03-31 inclusive.

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
# MAGIC select product_id, product_name from(
# MAGIC   select 
# MAGIC     p.product_id, p.product_name, max(s.sale_date) last_sold_date, min(s.sale_date) first_sold_date 
# MAGIC   from v_products p join v_sales s on 
# MAGIC     p.product_id = s.product_id 
# MAGIC   group by 1,2
# MAGIC ) x
# MAGIC where x.first_sold_date >= "2019-01-01" and x.last_sold_date<= "2019-03-31"
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Pyspark Solution
# MAGIC ----------------

# COMMAND ----------

# Step 1: Join `products` and `sales` on `product_id`
joined_df = df_products.join(df_sales, "product_id")
joined_df.display()

# COMMAND ----------

# Step 2: Group by `product_id` and `product_name` and calculate `max` and `min` sale_date
aggregated_df = joined_df.groupBy("product_id", "product_name") \
    .agg(
        max(col("sale_date")).alias("last_sold_date"),
        min(col("sale_date")).alias("first_sold_date")
    )
aggregated_df.display()


# COMMAND ----------

# Step 3: Filter based on `first_sold_date` and `last_sold_date`
result = aggregated_df.filter(
    (col("first_sold_date") >= "2019-01-01") & (col("last_sold_date") <= "2019-03-31")
).select("product_id", "product_name")

result.display()

# COMMAND ----------


