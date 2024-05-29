# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------



# COMMAND ----------

df_sales_src = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/sales_short.csv")
df_sales_src.display()


# COMMAND ----------

df_order = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/sales_order_price.csv")
df_order.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **1. How to find out and format current date or timestamp**
# MAGIC ------------------------------------------------
# MAGIC current_date() – function return current system date without time in PySpark DateType which is in format yyyy-MM-dd.
# MAGIC
# MAGIC current_timestamp() – function returns current system date time in PySpark TimestampType which is in format yyyy-MM-dd HH:mm:ss.SSS.
# MAGIC

# COMMAND ----------

df_sales_src.withColumn("current_date",current_date()) \
    .withColumn("current_timestamp", current_timestamp()) \
    .select(col("current_date"), col("current_timestamp"))\
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC Format returned by default
# MAGIC
# MAGIC current_date: yyyy-MM-dd
# MAGIC  
# MAGIC current_timestamp: yyyy-MM-dd HH:mm:ss.SSSS

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------

# COMMAND ----------

# MAGIC %md
# MAGIC **date_format():**	
# MAGIC
# MAGIC It is used to format a date or timestamp column in a DataFrame to a specified date or time format pattern. It requires the first argument to be in 'yyyy-MM-dd' or 'yyyy-MM-dd HH:mm:ss.SSSS' format, else it populates the new column with null.
# MAGIC

# COMMAND ----------

# How to change the format of date and timestamp - getting null issue
df_sales = df_sales_src.withColumn("new_order_date_str", date_format(col("order_date"),"yyyy-MM-dd"))
df_sales\
    .select("*",
            current_date().alias("current_date"), 
            current_timestamp().alias("current_timestamp")).display()
    




# COMMAND ----------



# COMMAND ----------

df_sales.withColumn("other_format1", date_format(col("order_date"), "MM-dd-yyyy"))\
    .select("other_format1",
            date_format(col("other_format1"), "MM-dd").alias("other_format2"),
            current_date().alias("current_date"), 
            current_timestamp().alias("current_timestamp")).display()
    

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Acceptable formats -
# MAGIC
# MAGIC for date : yyyy-MM-dd
# MAGIC  
# MAGIC for timestamp: yyyy-MM-dd HH:mm:ss.SSSS

# COMMAND ----------

# MAGIC %md
# MAGIC **to_date()	It converts a string column representing a date or timestamp into a date type column in a DataFrame**
# MAGIC
# MAGIC **to_timestamp() It converts a string column representing a date or timestamp into a timestamp type column in a DataFrame**

# COMMAND ----------

df_sales = df_sales\
    .withColumn("new_order_date", to_date(col("new_order_date_str"))) \
    .withColumn("order_timestamp", to_timestamp(col("order_date")))

df_sales.select(col("order_date"),col("new_order_date_str"), col("new_order_date"), col("order_timestamp")).display()





# COMMAND ----------

# What happens when you apply to_date to invalid date strings
df_sales\
    .select(col("order_date"),
            col("new_order_date_str"),
            date_format(col("new_order_date_str"), "MM-dd-yyyy").alias("new_order_date_str2"),
            to_date(col("new_order_date_str2")).alias("new_order_date2"),
            col("new_order_date"), 
            col("order_timestamp")).display()





# COMMAND ----------

# MAGIC %md
# MAGIC **Use case of to_date and date_format**
# MAGIC
# MAGIC date_format: Used when someone wants to return a particular format for visualization only and it wont be used further as an input to any of the date function
# MAGIC
# MAGIC to_date: When you want to convert a date string which is acceptable date format to date type and the resulting format will be yyyy-MM-dd. Same is for to_timestamp just it will return timestamp type

# COMMAND ----------

# MAGIC %md
# MAGIC **2. How to add or substract from a date field**
# MAGIC ------------------------------------------------
# MAGIC add_months()	It is used to add or subtract a specified number of months to a date or timestamp column in a DataFrame. It takes two arguments: the column representing the date or timestamp, and the number of months to add or subtract.
# MAGIC
# MAGIC date_add() is used to add a specified number of days to a date column
# MAGIC
# MAGIC date_sub() is used to subtract a specified number of days from a date column

# COMMAND ----------

df_sales.withColumn("add_2_months", add_months(col("new_order_date"), 2))\
    .withColumn("sub_3_months", add_months(col("new_order_date"), -3))\
    .withColumn("add_2_days", date_add(col("new_order_date"), 2))\
    .withColumn("sub_4_days", date_sub(col("new_order_date"), 4))\
    .select("new_order_date", "add_2_months", "sub_3_months", "add_2_days", "sub_4_days")\]
    .display()

    

# COMMAND ----------

# MAGIC %md
# MAGIC **3. How to find the difference between two dates**
# MAGIC ------------------------------------------------
# MAGIC datediff(end, start)	It is used to calculate the difference in days between two date columns in a DataFrame. It takes two arguments: the two date columns to calculate the difference between.

# COMMAND ----------

df_sales.withColumn("current_date_diff", datediff(current_date(), col("new_order_date")))\
    .select("current_date_diff", current_date().alias("current_date"), "new_order_date")\
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC **4. How to find the months beteen two dates**
# MAGIC -----------------------------------------------
# MAGIC months_between(end, start)	It is used to calculate the difference in months between two date or timestamp columns in a DataFrame. 
# MAGIC
# MAGIC It takes two arguments: the two date or timestamp columns to calculate the difference between them.
# MAGIC

# COMMAND ----------

df_sales.withColumn("current_months_diff", months_between(current_date(), col("new_order_date")))\
    .select(round("current_months_diff").alias("current_months_diff"), current_date().alias("current_date"), "new_order_date")\
    .display()


# COMMAND ----------

# MAGIC %md
# MAGIC **5. How to find out specific part of a date**
# MAGIC -------------------------------------------------------
# MAGIC year(column)	Returns the year from a given date or timestamp.
# MAGIC
# MAGIC quarter(column)	Returns the quarter as an integer from a given date or timestamp.
# MAGIC
# MAGIC month(column)	Returns the month as an integer from a given date or timestamp
# MAGIC
# MAGIC day(column)	Returns the quarter as an integer from a given date or timestamp.
# MAGIC

# COMMAND ----------

df_sales.select(col("new_order_date"),
                # day(col("new_order_date")).alias("day"),
                year(col("new_order_date")).alias("year"),
                quarter(col("new_order_date")).alias("quarter"),
                month(col("new_order_date")).alias("month")
                )\
    .display()

# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.day.html



# COMMAND ----------

# MAGIC %md
# MAGIC **6. How to find out specific day and week of a period**
# MAGIC -------------------------------------------------------
# MAGIC dayofweek(column)	Extract the day of the week from a date or timestamp column in a DataFrame. Monday is represented by 1, Tuesday by 2, and so on until Sunday, which is represented by 7.
# MAGIC
# MAGIC dayofmonth(column)	Extracts the day of the month from a given date or timestamp.
# MAGIC
# MAGIC dayofyear(column)	Extracts the day of the year from a given date or timestamp.
# MAGIC
# MAGIC weekofyear(column)	Extract the week number from a date or timestamp column in a DataFrame.
# MAGIC
# MAGIC last_day(column)	Return the last day of the month for a given date or timestamp column.The result is a date column where each date corresponds to the last day of the month for the original dates in the specified column.

# COMMAND ----------

df_sales.withColumn("dayofweek", dayofweek(col("new_order_date")))\
    .withColumn("dayofmonth", dayofmonth(col("new_order_date")))\
    .withColumn("dayofyear", dayofyear(col("new_order_date")))\
    .withColumn("weekofyear", weekofyear(col("new_order_date")))\
    .withColumn("last_day", last_day(col("new_order_date")))\
    .select("new_order_date", "dayofweek", "dayofmonth", "dayofyear", "weekofyear", "last_day")\
    .display()

    

# COMMAND ----------

# MAGIC %md
# MAGIC **7. Working with unix time**
# MAGIC ----------------------------------------------------
# MAGIC The unix time is the time in seconds from January 1st, 1970 (1970-01-01 00:00:00) to the very moment you call for.
# MAGIC
# MAGIC unix_timestamp()	It is used to convert a date or timestamp to a Unix timestamp value (i.e., the number of seconds since the Unix epoch). 
# MAGIC
# MAGIC from_unixtime(column)	Convert a Unix timestamp (represented as the number of seconds since the Unix epoch) to a string column. 
# MAGIC

# COMMAND ----------

df_sales = df_sales\
    .withColumn("unix_time_from_date", unix_timestamp(col("new_order_date")))\
    .withColumn("unix_time_from_timestamp", unix_timestamp(col("order_timestamp")))\
    .withColumn("from_unix", from_unixtime(col("unix_time")))\
    .withColumn("from_unix_format", from_unixtime(col("unix_time"), "yyyy-MM-dd"))
df_sales.select("new_order_date", "unix_time_from_date", "order_timestamp", "unix_time_from_timestamp", "from_unix", "from_unix_format")\
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC
