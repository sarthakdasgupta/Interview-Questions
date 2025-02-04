# Databricks notebook source
# MAGIC %md
# MAGIC Write a solution to find the daily active user count for a period of 30 days ending 2019-07-27 inclusively.
# MAGIC ----
# MAGIC 30 days prior to 27th July 2019 is 27th June 2019. A user was active on someday if they made at least one activity on that day.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import  *

# COMMAND ----------

# Create the Activity DataFrame
activity_data = [
    (1, 1, "2019-07-20", "open_session"),
    (1, 1, "2019-07-20", "scroll_down"),
    (1, 1, "2019-07-20", "end_session"),
    (2, 4, "2019-07-20", "open_session"),
    (2, 4, "2019-07-21", "send_message"),
    (2, 4, "2019-07-21", "end_session"),
    (3, 2, "2019-07-21", "open_session"),
    (3, 2, "2019-07-21", "send_message"),
    (3, 2, "2019-07-21", "end_session"),
    (4, 3, "2019-06-25", "open_session"),
    (4, 3, "2019-06-25", "end_session"),
]
activity_columns = ["user_id", "session_id", "activity_date", "activity_type"]

activity_df = spark.createDataFrame(activity_data, activity_columns)
activity_df.display()

# COMMAND ----------

# Filter the data for the date range
filtered_df = activity_df.filter(
    (col("activity_date") > date_sub(lit("2019-07-27"), 30)) &
    (col("activity_date") <= lit("2019-07-27"))
)
filtered_df.display()

# COMMAND ----------

# Group by activity_date and count distinct users
result_df = filtered_df.groupBy("activity_date")\
    .agg(countDistinct("user_id").alias("active_users"))\
    .withColumnRenamed("activity_date", "day")

# Show the result
result_df.display()


# COMMAND ----------


