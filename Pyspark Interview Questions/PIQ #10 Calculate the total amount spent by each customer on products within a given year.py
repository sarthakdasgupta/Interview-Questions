# Databricks notebook source
# MAGIC %md
# MAGIC Calculate the total amount spent by each customer on products within a given year
# MAGIC ---------------

# COMMAND ----------

# MAGIC %md
# MAGIC You are given a dataset containing information about customer transactions. Each transaction has the following fields: `customer_id`, `transaction_date`, `product_id`, and `transaction_amount`.
# MAGIC
# MAGIC
# MAGIC You need to calculate <br>
# MAGIC 1. the total amount spent by each customer on products within a given year
# MAGIC 2. identify the top 2 products they spent the most money on. Additionally
# MAGIC 3. return the overall total spending of each customer across all year.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# Sample data
data = [
    (1, "2023-01-15", "A", 100.0),
    (1, "2023-03-10", "B", 150.0),
    (2, "2023-02-05", "A", 50.0),
    (1, "2023-02-20", "A", 200.0),
    (2, "2023-04-30", "C", 300.0),
    (1, "2022-05-12", "B", 120.0),
    (2, "2022-09-22", "A", 200.0),
    (1, "2023-05-15", "C", 250.0),
]

# Create DataFrame
columns = ["customer_id", "transaction_date", "product_id", "transaction_amount"]
df = spark.createDataFrame(data, columns)
df.display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# Step 1: Filter transactions for the year 2023
df_2023 = df.filter(year(col("transaction_date")) == 2023)

# Step 2: Calculate total spending by each customer for each product in 2023
customer_product_spending = df_2023.groupBy("customer_id", "product_id") \
    .agg(sum("transaction_amount").alias("total_spent"))

customer_product_spending.display()

# COMMAND ----------

# Step 3: Find top 2 products by spending for each customer in 2023
# Using a window function to rank products by spending
window_spec = Window.partitionBy("customer_id").orderBy(desc("total_spent"))

# Add rank column and filter top 2 products
ranked_products = customer_product_spending.withColumn("rank", rank().over(window_spec)) \
    .filter(col("rank") <= 2) \
    .select("customer_id", "product_id", "total_spent")

print("Top 2 products by spending for each customer in 2023:")
ranked_products.display()

# COMMAND ----------


# Step 4: Calculate total spending for each customer across all years
total_spending = df.groupBy("customer_id") \
    .agg(_sum("transaction_amount").alias("total_spent"))

print("Total spending by each customer across all years:")
total_spending.display()


# COMMAND ----------


