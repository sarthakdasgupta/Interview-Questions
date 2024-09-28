# Databricks notebook source
# MAGIC %md
# MAGIC Date Functions in SQL
# MAGIC ----------------------

# COMMAND ----------

# MAGIC %md
# MAGIC **DEFAULT FORMATS:** 
# MAGIC Dates are typically represented and manipulated using this default format.<br>
# MAGIC Date Format - 'yyyy-MM-dd' 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales_data

# COMMAND ----------

# MAGIC %md
# MAGIC `current_date()`	Returns the current date as a date column.

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, current_date() as today from sales_data

# COMMAND ----------

# MAGIC %md
# MAGIC **Date Formatting**

# COMMAND ----------

# MAGIC %md
# MAGIC `date_format(dateExpr,format)`	Converts a date/timestamp/string to a value of string in the format specified.

# COMMAND ----------

# MAGIC %sql
# MAGIC select order_date, date_format(order_date, "dd-MM-yyyy") as order_date_formatted, item from sales_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select order_date, date_format(order_date, "MM-yyyy") as order_date_formatted, item from sales_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select order_date, 
# MAGIC date_format(order_date, "dd-MM-yyyy") as order_date_formatted, 
# MAGIC date_format(now(), "dd-yyyy-MM") as current_date_formatted,  
# MAGIC item from sales_data

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC `to_date(column)`	Converts StringType Date column into DateType. Use this syntax when date inside this column is in default date format i.e. 'yyyy-MM-dd' or 'yyyy-MM-dd hh:mm:ss'<br>
# MAGIC
# MAGIC `to_date(column, format)`	Converts the column into a DateType with a specified format. Use this syntax when the date or timestamp is in a different format. Specify that format in the 2nd parameter of the to_date function

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_timestamp(), 
# MAGIC to_date(current_timestamp()) as to_date_result

# COMMAND ----------

# MAGIC %sql
# MAGIC select to_date('22-09-2024', 'dd-MM-yyyy') as to_date_res

# COMMAND ----------

# MAGIC %sql
# MAGIC select to_date('22-09-2024', 'MM-dd-yyyy') as to_date_res

# COMMAND ----------

# MAGIC %sql
# MAGIC select to_date('2024-09-22 16:59:36', "yyyy-MM-dd HH:mm:ss") as to_date_res

# COMMAND ----------

# MAGIC %sql
# MAGIC select order_date, 
# MAGIC date_format(order_date, "dd-MM-yyyy") as date_format_result, 
# MAGIC to_date(order_date, "dd-MM-yyyy") as to_date_result,
# MAGIC item from sales_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select order_date, date_format_result, to_date(date_format_result, "dd-MM-yyyy") as to_date_result from (
# MAGIC   select order_date, 
# MAGIC   date_format(order_date, "dd-MM-yyyy") as date_format_result, 
# MAGIC   item from sales_data
# MAGIC )

# COMMAND ----------

# The difference between date_format and to_date is:

# date_format changes the format of a date column (e.g., from "yyyy-mm-dd" to a different format).
# to_date converts a string into a date type by specifying the format of the input date.

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **Date Addition Substraction**

# COMMAND ----------

# MAGIC %md
# MAGIC `date_add(column, days)`	Add days to a date column<br>
# MAGIC `date_sub(column, days)`	Substract days from a date column<br>
# MAGIC `datediff(end, start)`	Returns the number of days from start to end<br>

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC order_date, 
# MAGIC date_add(order_date, 1) added_1_day,
# MAGIC date_sub(order_date, 1) substracted_1_day,
# MAGIC date_diff(current_date(), order_date) day_diff_with_current
# MAGIC from sales_data

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC `add_months(column, numMonths)`	Returns the date that is `numMonths` after `startDate`.

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC order_date, 
# MAGIC add_months(order_date, 1) added_1_month,
# MAGIC add_months(order_date, -1) substracted_1_momth
# MAGIC from sales_data

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC `months_between(end, start)`	Returns number of months between dates `start` and `end`. A whole number is returned if both inputs have the same day of month or both are the last day of their respective months. Otherwise, the difference is calculated assuming 31 days per month. <br>
# MAGIC
# MAGIC `months_between(end, start, roundOff)`	Returns number of months between dates `end` and `start`. If `roundOff` is set to true, the result is rounded off to 8 digits; it is not rounded otherwise.

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC order_date, 
# MAGIC months_between(order_date, current_date) months_between_current,
# MAGIC months_between(order_date, current_date, false) months_between_current_rounded_off
# MAGIC from sales_data

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **Elements of Date 1**

# COMMAND ----------

# MAGIC %md
# MAGIC `year(column)`	Extracts the year as an integer from a given date/timestamp/string <br>
# MAGIC `quarter(column)`	Extracts the quarter as an integer from a given date/timestamp/string<br>
# MAGIC `month(column)`	Extracts the month as an integer from a given date/timestamp/string<br>

# COMMAND ----------

# MAGIC %sql
# MAGIC select order_date,
# MAGIC year(order_date) as year,
# MAGIC quarter(order_date) as quarter,
# MAGIC month(order_date) as month,
# MAGIC day(order_date) as day
# MAGIC from sales_data

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC **Elements of Date 2**

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC `dayofweek(column)`	Extracts the day of the week as an integer from a given date/timestamp/string. Ranges from 1 for a Sunday through to 7 for a Saturday<br>
# MAGIC `dayofmonth(column)`	Extracts the day of the month as an integer from a given date/timestamp/string.<br>
# MAGIC `dayofyear(column)`	Extracts the day of the year as an integer from a given date/timestamp/string.<br>
# MAGIC `weekofyear(column)`	Extracts the week number as an integer from a given date/timestamp/string. A week is considered to start on a Monday and week 1 is the first week with more than 3 days, as defined by ISO 8601<br>

# COMMAND ----------

# MAGIC %sql
# MAGIC select order_date,
# MAGIC dayofweek(order_date) as dayofweek,
# MAGIC dayofmonth(order_date) as dayofmonth,
# MAGIC dayofyear(order_date) as dayofyear,
# MAGIC weekofyear(order_date) as weekofyear
# MAGIC from sales_data

# COMMAND ----------

# MAGIC %md
# MAGIC **Next Day and Last Day**

# COMMAND ----------

# MAGIC %md
# MAGIC `next_day(column, dayOfWeek)`	Returns the first date which is later than the value of the `date` column that is on the specified day of the week. For example, next_day(‘2015-07-27’, “Sunday”) returns 2015-08-02 because that is the first Sunday after 2015-07-27.<br>
# MAGIC
# MAGIC `last_day(column)`	Returns the last day of the month which the given date belongs to. For example, input “2015-07-27” returns “2015-07-31” since July 31 is the last day of the month in July 2015.<br>

# COMMAND ----------

# MAGIC %sql
# MAGIC select order_date,
# MAGIC next_day(order_date, "monday") next_monday,
# MAGIC last_day(order_date) last_day_of_month
# MAGIC from sales_data

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **Date Truncate**

# COMMAND ----------

# MAGIC %md
# MAGIC `trunc(column, format)`	Returns date truncated to the unit specified by the format.
# MAGIC 	For example, `trunc(“2018-11-19 12:01:19”, “year”)` returns 2018-01-01
# MAGIC 	format: ‘year’, ‘yyyy’, ‘yy’ to truncate by year,
# MAGIC 	‘month’, ‘mon’, ‘mm’ to truncate by month
# MAGIC  
# MAGIC `date_trunc(format, timestamp)`	Returns timestamp truncated to the unit specified by the format.
# MAGIC 	For example, `date_trunc(“year”, “2018-11-19 12:01:19”)` returns 2018-01-01 00:00:00
# MAGIC 	format: ‘year’, ‘yyyy’, ‘yy’ to truncate by year,
# MAGIC 	‘month’, ‘mon’, ‘mm’ to truncate by month,
# MAGIC 	‘day’, ‘dd’ to truncate by day,
# MAGIC 	Other options are: ‘second’, ‘minute’, ‘hour’, ‘week’, ‘month’, ‘quarter’

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC "2024-04-22 03:24:44" as sample_time,
# MAGIC trunc("2024-04-22 03:24:44", "yy") as truncated_yy,
# MAGIC trunc("2024-04-22 03:24:44", "mm") as truncated_mm,
# MAGIC date_trunc("year", "2024-04-22 03:24:44") as date_trunc_year,
# MAGIC date_trunc("day", "2024-04-22 03:24:44") as date_trunc_day,
# MAGIC date_trunc("hour", "2024-04-22 03:24:44") as date_trunc_hour,
# MAGIC date_trunc("minute", "2024-04-22 03:24:44") as date_trunc_minute,
# MAGIC date_trunc("second", "2024-04-22 03:24:44") as date_trunc_seconds

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC TIMESTAMP FUNCTIONS
# MAGIC -----------------------

# COMMAND ----------

# MAGIC %md
# MAGIC **DEFAULT FORMAT:** 
# MAGIC Dates are typically represented and manipulated using this default format.<br>
# MAGIC Timestamp Format - 'yyyy-MM-dd HH:mm:ss'

# COMMAND ----------

# MAGIC %md
# MAGIC `current_timestamp()`	Returns the current timestamp as a timestamp column

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_timestamp()

# COMMAND ----------

# MAGIC %md
# MAGIC **Timestamp Formatting**

# COMMAND ----------

# MAGIC %md
# MAGIC `to_timestamp(column`)	Converts to a timestamp by casting rules to TimestampType.<br>
# MAGIC `to_timestamp(column, format)`	Converts time string with the given pattern to timestamp.

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC current_date,
# MAGIC to_timestamp(current_date, "yyyy-MM-dd HH:mm:ss") as current_date_to_timestamp,
# MAGIC to_timestamp("24-12-2023", "dd-MM-yyyy") as order_date_to_timestamp,
# MAGIC to_timestamp("24-12-2023") as order_date_to_timestamp,
# MAGIC to_timestamp("12-2023", "MM-yyyy") as order_date_to_timestamp
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **Elements of Time**

# COMMAND ----------

# MAGIC %md
# MAGIC `hour(column)`	Extracts the hours as an integer from a given date/timestamp/string.<br>
# MAGIC `minute(column)`	Extracts the minutes as an integer from a given date/timestamp/string.<br>
# MAGIC `second(column)`	Extracts the seconds as an integer from a given date/timestamp/string.<br>

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC current_timestamp(),
# MAGIC hour(current_timestamp()),
# MAGIC minute(current_timestamp()),
# MAGIC second(current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC **Unix Timestamp in Long Datatype**<br>
# MAGIC a way to represent time and date as the number of seconds that have passed since the Unix epoch, which is January 1, 1970

# COMMAND ----------

# MAGIC %md
# MAGIC `unix_timestamp(column)`	Converts time string in format yyyy-MM-dd HH:mm:ss to Unix timestamp (in seconds), using the default timezone and the default locale.<br>
# MAGIC `to_unix_timestamp(column, input time format)` Converts input column into unix timestamp. Input time format has to be provided 

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC order_date, 
# MAGIC unix_timestamp(order_date) unix_timestamp,
# MAGIC to_unix_timestamp("2024-01-02 25:01:44", 'yyyy-MM-dd mm:HH:ss') to_unix_timestamp
# MAGIC from sales_data

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **Convert Unix Long to Timestamp**

# COMMAND ----------

# MAGIC %md
# MAGIC `from_unixtime(column)`	Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string representing the timestamp of that moment in the current system time zone in the yyyy-MM-dd HH:mm:ss format.<br>
# MAGIC `from_unixtime(column, f)`	Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string representing the timestamp of that moment in the current system time zone in the given format.

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC "2024-01-02 25:01:44" sample_timestamp,
# MAGIC to_unix_timestamp("2024-01-02 25:01:44", 'yyyy-MM-dd mm:HH:ss') to_unix_timestamp,
# MAGIC from_unixtime(1704158744) from_unix_timestamp_1,
# MAGIC from_unixtime(1704158744, 'dd-MM-yyyy mm:HH:ss') from_unix_timestamp_2

# COMMAND ----------


