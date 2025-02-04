# Databricks notebook source
# MAGIC %md
# MAGIC Write a solution to find the person_name of the last person that can fit on the bus without exceeding the weight limit.
# MAGIC ------------

# COMMAND ----------

# MAGIC %md
# MAGIC There is a queue of people waiting to board a bus. However, the bus has a weight limit of 1000 kilograms, so there may be some people who cannot board. Note that only one person can board the bus at any given turn.

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *

# COMMAND ----------


# Create the Queue DataFrame
data = [
    (5, "Alice", 250, 1),
    (4, "Bob", 175, 5),
    (3, "Alex", 350, 2),
    (6, "John Cena", 400, 3),
    (1, "Winston", 500, 6),
    (2, "Marie", 200, 4),
]
columns = ["person_id", "person_name", "weight", "turn"]

queue_df = spark.createDataFrame(data, columns)

queue_df.display()

# COMMAND ----------

# Define the window specification
window_spec = Window.orderBy("turn")

# Calculate accumulated weight
accumulated_df = queue_df.withColumn("accumulated_weight", sum("weight").over(window_spec))

accumulated_df.display()

# COMMAND ----------

# Filter to include only those who fit within the weight limit
filtered_df = accumulated_df.filter(col("accumulated_weight") <= 1000)

filtered_df.display()

# COMMAND ----------

# Get the last person by accumulated weight
result_df = filtered_df.orderBy(col("turn").desc()).limit(1)

# Show the result
result_df.display()

# COMMAND ----------


