# Databricks notebook source
# MAGIC %md
# MAGIC Write a solution to report the fraction of players that logged in again on the day after the day they first logged in, rounded to 2 decimal places.
# MAGIC --------------

# COMMAND ----------

# MAGIC %md
# MAGIC **In other words, you need to count the number of players that logged in for at least two consecutive days starting from their first login date, then divide that number by the total number of players.**

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import math

# COMMAND ----------

# Define the data
data = [
    (1, 2, "2016-03-01", 5),
    (1, 2, "2016-03-02", 6),
    (2, 3, "2017-06-25", 1),
    (3, 1, "2016-03-02", 0),
    (3, 4, "2018-07-03", 5),
]
columns = ["player_id", "device_id", "event_date", "games_played"]

# Create DataFrame
df = spark.createDataFrame(data, schema=columns)
df.display()

# COMMAND ----------

# Step 1: Calculate the first login date for each player
first_login_df = df.groupBy("player_id").agg(min("event_date").alias("first_login_date"))
first_login_df.display()

# COMMAND ----------

# Step 2: Add a column for the day after the first login date
login_after_first = first_login_df.withColumn("day_after_first_login", date_add(col("first_login_date"), 1))\
    .withColumnRenamed("player_id", "login_player_id")
login_after_first.display()

# COMMAND ----------

# Step 3: Filter the original DataFrame where (player_id, event_date) matches
filtered_df = df.join(
    login_after_first,
    (df["player_id"] == login_after_first["login_player_id"]) &
    (df["event_date"] == login_after_first["day_after_first_login"]),
    "inner")

filtered_df.display()

# COMMAND ----------

filtered_player_df = filtered_df.select("player_id").distinct().display()

# COMMAND ----------

# Step 4: Calculate the fraction
total_players = df.select("player_id").distinct().count()
matching_players = filtered_df.count()

fraction = matching_players / total_players * 100 if total_players > 0 else 0

# Step 5: Show the result
print(f"Fraction: {fraction:.2f}%")


# COMMAND ----------

# MAGIC %md
# MAGIC Using DF API
# MAGIC ------------------

# COMMAND ----------

# Step 3: Filter the original DataFrame where (player_id, event_date) matches
filtered_df = df.join(
    login_after_first,
    (df["player_id"] == login_after_first["login_player_id"]) &
    (df["event_date"] == login_after_first["day_after_first_login"]),
    "left")\

filtered_df.display()

# COMMAND ----------

final_df = filtered_df.agg(round(countDistinct(col("login_player_id")) / countDistinct(col("player_id")) * 100, 2).alias("fraction"))
final_df.display()
# withColumn("fraction", when(col("player_id").isNotNull(), 1).otherwise(0))

# COMMAND ----------


