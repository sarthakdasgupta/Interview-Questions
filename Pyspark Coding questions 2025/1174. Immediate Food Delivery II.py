# Databricks notebook source
# MAGIC %md
# MAGIC Write a solution to find the percentage of immediate orders in the first order of all customers, rounded to 2 decimal places.
# MAGIC --------------------

# COMMAND ----------

# MAGIC %md
# MAGIC If the customer's preferred delivery date is the same as the order date, then the order is called immediate; otherwise, it is called scheduled.
# MAGIC
# MAGIC The first order of a customer is the order with the earliest order date that the customer made. It is guaranteed that a customer has precisely one first order.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Define the schema
schema = StructType([
    StructField("delivery_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("customer_pref_delivery_date", StringType(), True)
])

# Define the data
data = [
    (1, 1, "2019-08-01", "2019-08-02"),
    (2, 2, "2019-08-02", "2019-08-02"),
    (3, 1, "2019-08-11", "2019-08-12"),
    (4, 3, "2019-08-24", "2019-08-24"),
    (5, 3, "2019-08-21", "2019-08-22"),
    (6, 2, "2019-08-11", "2019-08-13"),
    (7, 4, "2019-08-09", "2019-08-09")
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
df.display()

# COMMAND ----------

# Find the first order for all the customers
first_order = df.groupBy(col("customer_id")).agg(min(col("order_date")).alias("first_order_date"))
first_order.display()

# COMMAND ----------

# Join the first_order date with the original customer order data
first_order_joined = df.join(first_order, df["customer_id"]==first_order["customer_id"], "inner").select(df["*"], first_order["first_order_date"])
first_order_joined.display()

# COMMAND ----------


# Filter immediate deliveries
immediate_deliveries = first_order_joined.filter(col("first_order_date") == col("customer_pref_delivery_date"))
immediate_deliveries.display()

# COMMAND ----------

# Calculate total orders and immediate orders
total_orders = df.count()
immediate_orders = immediate_deliveries.count()
print("immediate_orders: ", immediate_orders, ", total_orders: ", total_orders)

# COMMAND ----------

# Calculate percentage
percentage = (immediate_orders / total_orders) * 100 if total_orders > 0 else 0

# Print the result
print(f"Percentage of immediate orders: {percentage:.2f}%")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Using DF API
# MAGIC ----------------

# COMMAND ----------

# Calculate immediate deliveries
df_with_immediate = first_order_joined\
    .withColumn("is_immediate", when(col("first_order_date") == col("customer_pref_delivery_date"), 1).otherwise(0))

# Aggregate to find the percentage of immediate deliveries
result_df = df_with_immediate.agg(round((sum(col("is_immediate")) / count(lit(1)) * 100), 2).alias("immediate_order_percentage"))

# Show the result
result_df.display()


# COMMAND ----------


