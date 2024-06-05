# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

df_sales = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/sales_short.csv")
df_sales.display()

# COMMAND ----------

df_order = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/sales_order_price.csv")
df_order.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Q1. Display the total price of each product ordered in 1st and 5th month of 2020
# MAGIC --------------------------------------------------------------------------------------

# COMMAND ----------

# 1. 1st DF (sales) will contain all product and order_id that were purchased in 1st and 5th month of 2020. You can keep order date also for reference
# 2. 2nd DF (order) will contain order_id and total_price (total_price = qty*price_each)]
# 3. Now join the 1st and 2nd df on order id.
# 4. Then group by on product and sum total price

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

df_product = df_sales.select("Order_date", "Order_id", "Product").filter("month(order_date) in (1, 5) and year(order_date)=2020")
df_product.display()

# COMMAND ----------

df_price = df_order.select(col("order_id"), lit(col("qty")*col("price_each")).alias("total_price"))
df_price.display()

# COMMAND ----------

df_result = df_product.join(df_price, df_product["order_id"]==df_price["order_id"]).select(df_product["*"], "total_price")

df_result.groupBy(col("product")).agg(expr("sum(total_price) as total_price")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Q2. How many no of days between 1st USB-C Charging Cable and 2nd USB-C Charging Cable sales
# MAGIC -------

# COMMAND ----------

# 1. Create a DF containing details of USB-C Charging Cable from sales 
# 2. Order by this DF and keep 1st two rows
# 3. Use lag to pick the prev date from order date and create a col using it
# 4. Subtract order date and new date column to get no of days

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

df_usb_all = df_sales.filter("Product = 'USB-C Charging Cable'")
df_usb_all.display()

# COMMAND ----------

df_usb_2 = df_usb_all.orderBy("order_date").limit(2)
df_usb_2.display()
df_usb_2.createOrReplaceTempView("df_usb_2_v")

# COMMAND ----------

win = Window.partitionBy("product").orderBy("product")
# df_usb_prev_date = spark.sql("select *, lag(order_date) over(order by product) as prev_date from df_usb_2_v")
df_usb_prev_date = df_usb_2.withColumn("prev_date", lag("order_date").over(win))
df_usb_prev_date.display()

# COMMAND ----------

df_result = df_usb_prev_date.withColumn("days_between", datediff(col("order_date"), col("prev_date")))
df_result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Q3. Which month of 2020 has the most no of total sales
# MAGIC --

# COMMAND ----------

# 1. Create a DF from sales for year 2020
# 2. In df_2020 create a column to store month of the order_date
# 3. Join df_2020 and df_price on order_id
# 4. Group by month and sum total_price
# 5. Order the result DF by total price in desc order

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

df_2020 = df_sales.filter("year(order_date) == 2020").withColumn("month", month("order_date"))
df_2020.display()

# COMMAND ----------

df_price.display()

# COMMAND ----------

df_order_2020 = df_2020.join(df_price, df_2020["order_id"]==df_price["order_id"]).select(df_2020["*"], "total_price")
df_order_2020.display()

# COMMAND ----------

df_result = df_order_2020.groupBy("month").agg(sum("total_price").alias("sum_price"))
df_result.display()

# COMMAND ----------

df_result.orderBy(col("sum_price").desc()).limit(1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Q4. Which hour of the day is most favourable for sales in all years of purchase
# MAGIC --

# COMMAND ----------

# 1. Create a DF using sales and add a new column containing hour from order_date
# 2. Join with df_price on order id
# 3. Group by hour column and sum the total price 
# 4. Order by the sum total price col in desc

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

df_hour = df_sales.withColumn("hour", hour("order_date")).select("order_id", "hour")
df_hour.display()

# COMMAND ----------

df_order_hour = df_hour.join(df_price, df_hour["order_id"]==df_price["order_id"]).select(df_hour["*"], "total_price")
df_order_hour.display()

# COMMAND ----------

df_result = df_order_hour.groupBy("hour").agg(sum("total_price").alias("total_price"))
df_result.display()

# COMMAND ----------

df_result.orderBy(col("total_price").desc()).limit(1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Q5. How many months between the purchase of 1st and 3rd 27in FHD Monitor
# MAGIC --

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

df_fhd = df_sales.filter("Product = '27in FHD Monitor'")
df_fhd.display()

# COMMAND ----------

win = Window.partitionBy("product").orderBy("product")
# df_usb_prev_date = spark.sql("select *, lag(order_date) over(order by product) as prev_date from df_usb_2_v")
df_fhd_prev_date = df_fhd.withColumn("prev_date", lag("order_date", 2).over(win))
df_fhd_prev_date.display()

# COMMAND ----------

df_result = df_fhd_prev_date.withColumn("months_between", months_between(col("order_date"), col("prev_date")))
df_result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Q6. List down all the weeks of each year with most sales
# MAGIC --

# COMMAND ----------

# 1. Create a DF df_week from sales containing week of each year and also value of each year based on order date
# 2. Join df_week with df_price on order id
# 3. In the resultant DF group on year and week and get the total price 
# 4. Finally find out in each year which week no has the highest sales

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

df_week = df_sales.withColumn("week_of_year", weekofyear(col("order_date")))\
    .withColumn("year", year(col("order_date")))
df_week.display()

# COMMAND ----------

df_order_week = df_week.join(df_price, df_week["order_id"]==df_price["order_id"]).select("week_of_year", "year", "total_price")
df_order_week.display()

# 17 2019 389.99
# 17 2019 150

# COMMAND ----------

df_total_week_price = df_order_week.groupBy("week_of_year", "year").agg(sum("total_price").alias("total_price_per_week"))
df_total_week_price.display()
# 17 2019 539.99

# COMMAND ----------

win = Window.partitionBy("year").orderBy(col("total_price_per_week").desc())
df_total_week_price.withColumn("rn", row_number().over(win))\
    # ].filter("rn==1")\
    .display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Q7. Among all the days in a week which one generated the least revenue and what products were impacted
# MAGIC --

# COMMAND ----------

# 1. Create a dataframe (df_week_day) from sales and add a column day_of_week consisting day of the week from order date
# 2. Join with df_price on order id
# 3. Now on the joined DF, find out the sum of total price for each day of the week
# 4. In this data filter out the min sum value and its day of week
# 5. Now join min sum DF back to df_week_day on day of the week]

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

df_week_day = df_sales.withColumn("day_of_week", dayofweek("order_date"))
df_week_day.display()

# COMMAND ----------

df_price_week_day = df_week_day.join(df_price, df_week_day["order_id"]==df_price["order_id"]).select("day_of_week", "total_price")
df_price_week_day.display()


# COMMAND ----------

df_price_group = df_price_week_day.groupBy("day_of_week").agg(sum("total_price").alias("sum_price"))
df_price_group.display()

# COMMAND ----------

min_price = df_price_group.select(min("sum_price")).collect()[0][0]
# min_price = (lambda x: x for x in min_price)
print(min_price)

# COMMAND ----------

df_min_price = df_price_group.filter(f"sum_price=={min_price}".format(min_price))
df_min_price.display()

# COMMAND ----------

df_join = df_week_day.join(df_min_price, df_week_day["day_of_week"]==df_min_price["day_of_week"]).select(df_week_day["*"], "sum_price")
df_join.display()

# COMMAND ----------


