# Databricks notebook source
# MAGIC %md
# MAGIC Find customers who have placed orders on consecutive days
# MAGIC ---------------------------------------------------------------

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# Sample data
data = [
    (1, '2024-10-01'),
    (1, '2024-10-02'),
    (1, '2024-10-04'),
    (2, '2024-10-03'),
    (2, '2024-10-05'),
    (3, '2024-10-01'),
    (3, '2024-10-02'),
    (3, '2024-10-03'),
]

# Create DataFrame
df = spark.createDataFrame(data, ["customer_id", "order_date"])
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **OUTPUT**

# COMMAND ----------

#   |-------------|
#   | customer_id |
#   |-------------|
#   |      1	  |
#   |      3	  |
#   |-------------| 

# COMMAND ----------

# Define a window partitioned by customer and ordered by order_date
window_spec = Window.partitionBy("customer_id").orderBy("order_date")

# Use lag to get the previous order_date per customer
df_lag = df.withColumn("previous_order_date", lag("order_date").over(window_spec))
df_lag.display()

# COMMAND ----------

# Calculate the difference between the current and previous order_date
df_diff = df_lag.withColumn("date_diff", datediff("order_date", "previous_order_date"))
df_diff.display()

# COMMAND ----------



# Filter for rows where the date difference is 1 (indicating consecutive days)
consecutive_orders = df_diff.filter(col("date_diff") == 1)

# Show the result
consecutive_orders.display()


# COMMAND ----------

consecutive_orders.select("customer_id").distinct().display()

# COMMAND ----------


