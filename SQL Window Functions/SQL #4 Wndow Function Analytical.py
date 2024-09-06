# Databricks notebook source
# MAGIC %md
# MAGIC Analytical or Value Window Functions
# MAGIC ----------------------------------------
# MAGIC **The value window functions in SQL are used to assign a value to a row from other rows.**
# MAGIC 1. First Value
# MAGIC 2. Last Value
# MAGIC 3. Nth Value

# COMMAND ----------

# MAGIC %md
# MAGIC Sample Data
# MAGIC -------------

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales_data

# COMMAND ----------

# MAGIC %md
# MAGIC First Value
# MAGIC ---------------
# MAGIC It is used to find the first value of a given column or expression for a group of rows. It can be within a partition as well.

# COMMAND ----------

# Syntax:
#     FIRST_VALUE(expression, keep nulls or remove nulls) or
#     FIRST(expression, keep nulls or remove nulls)

#     1. You can use either column name or an expression
#     2. If 2nd parameter is set true first() will return only non null values and if false then null values are also included
    

# COMMAND ----------

# MAGIC %md
# MAGIC **How to add expression in first_value**

# COMMAND ----------

# MAGIC %sql
# MAGIC select first(upper(item)) first_item, first(lower(sales_man)) first_man from sales_data

# COMMAND ----------

# MAGIC %md
# MAGIC **How first_value works with null**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- first_value will return null as ignoreNull = false
# MAGIC SELECT first_value(col, false) FROM VALUES (NULL), (10), (5) AS tab(col);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- first_value will skip null as ignoreNull = true is set
# MAGIC SELECT first_value(col, true) FROM VALUES AS tab(col);

# COMMAND ----------

# MAGIC %md
# MAGIC **Q. Which is the most expensive item sold by each salesman in central region**

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,
# MAGIC units*unit_price as amount,
# MAGIC first_value(item) over(partition by sales_man order by units*unit_price desc) expensive_item
# MAGIC from sales_data 
# MAGIC where region = "Central"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (
# MAGIC select *,
# MAGIC units*unit_price as amount,
# MAGIC first_value(item) over(partition by sales_man order by units*unit_price desc) expensive_item
# MAGIC from sales_data 
# MAGIC where region = "Central"
# MAGIC )
# MAGIC where item = expensive_item

# COMMAND ----------

# MAGIC %md
# MAGIC **Q Which salesman who sold the Home Theatre has the highest sale for it**

# COMMAND ----------

# MAGIC %sql
# MAGIC select sales_man, item, sum(units*unit_price) as sum_amount from sales_data where item = "Home Theater" group by 1,2 

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,
# MAGIC first(sales_man) over(order by sum_amount desc) first_sales_man from (
# MAGIC select sales_man, item, sum(units*unit_price) sum_amount from sales_data where item = "Home Theater" group by 1,2 )

# COMMAND ----------

# MAGIC %md
# MAGIC Last Value
# MAGIC ------------
# MAGIC It is used to find the last value of a given column or expression for a group of rows. It is opposite to first value

# COMMAND ----------

# Syntax:
#     last_value(expression, [keep null or ignore null])
#     last(expression, [keep null or ignore null])

#     1. You can use either column name or an expression
#     2. If 2nd parameter is set true last() will return only non-null values and if false then null values are also included


# COMMAND ----------

# MAGIC %md
# MAGIC **How to add expression in last_value**
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select last(concat(region, " India")) last_region from sales_data

# COMMAND ----------

# MAGIC %md
# MAGIC **How last_value works with null**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- last_value will return null as false is set
# MAGIC SELECT last_value(col, false) FROM VALUES (10), (5), (NULL) AS tab(col);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- last_value will ignore null as true is set
# MAGIC SELECT last_value(col, true) FROM VALUES (10), (5), (NULL) AS tab(col);

# COMMAND ----------

# MAGIC %md
# MAGIC **Q. Which item has the lowest units sold for each salesman in East region**

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,
# MAGIC last_value(item) over(partition by sales_man order by units desc) as least_sold,
# MAGIC first_value(item) over(partition by sales_man order by units desc) as first_sold
# MAGIC from sales_data
# MAGIC where region = "East"

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,
# MAGIC last_value(item) over(partition by sales_man order by units desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as least_sold,
# MAGIC first_value(item) over(partition by sales_man order by units desc) as first_sold
# MAGIC from sales_data
# MAGIC where region = "East"

# COMMAND ----------

# MAGIC %md
# MAGIC Frame Clause
# MAGIC ---------------
# MAGIC A frame is a subset of records with a window. The above query was not returning correct last value because of the frame clause.

# COMMAND ----------

# MAGIC %md
# MAGIC **RANGE BETWEEN UNBOUNDED PRECEEDING AND CURRENT ROW**

# COMMAND ----------

## This is the default frame clause. Also called Rows between unbounded preceeding and current row ##
# --------------------------------------------------------------------------------------------------#
# eg: while calculating last value for first record-
#     The unbounded preceeding of the first row will be the first row as there are no rows prior to it and current row is anyway the first row.
#     So last_value() will check and compare first row with itself so it return the item in first row
# eg: while calculating last value for 2nd record:
#     The unbounded preceeding of the 2nd row will be the 1st row and 2nd row and current row is 2nd row.
#     So last_value() will check and compare 2nd rows with 1st row, and as 2nd row consists of the least units so it returns the item in 2nd row
# eg: while calculating last value for 2nd record:
#     The unbounded preceeding of the 2nd row will be the 1st row and 2nd row and current row is 2nd row.
#     So last_value() will check and compare 2nd rows with 1st row, and as 2nd row consists of the least units so it returns the item in 2nd row
# ---------------------------------------------------------------------------------------------------------------------------------------------------#

# Conclusion: by default last value compares all the records from first row till the current row and as we have ordered the records in descending so current rows value is returned through last value


# COMMAND ----------

# MAGIC %sql
# MAGIC select *,
# MAGIC last_value(item) over(partition by sales_man order by units desc
# MAGIC rows between unbounded preceding and current row
# MAGIC ) as least_sold
# MAGIC from sales_data
# MAGIC where region = "East" and sales_man = "Alexander"

# COMMAND ----------

# MAGIC %md
# MAGIC **ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING**

# COMMAND ----------

## This is also called Range between unbounded preceding and unbounded following ##
# --------------------------------------------------------------------------------#
# Unbound Preceeding - All records prior to current row
# Unbounded Following - All records next to current row
# When we use this then last value compare all the rows togethor and then finds out which value is least or at the last position
# eg: Units in all rows are compared and then the last value is return

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,
# MAGIC last_value(item) over(partition by sales_man order by units desc
# MAGIC rows between unbounded preceding and unbounded following
# MAGIC ) as least_sold
# MAGIC from sales_data
# MAGIC where region = "East" and sales_man = "Alexander"

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Nth Value
# MAGIC ------------
# MAGIC The NTH_VALUE() is a window function that allows you to get a value from the Nth row in an ordered set of rows.

# COMMAND ----------

# Syntax:
    
#     NTH_VALUE(expression, N)
#       expression: column name or expression as used in first and last function
#       N: a positive integer signifying which row to return
#       Note. The NTH_VALUE() function returns the value of expression from the Nth row of the window frame. If that Nth row does not exist, the function returns NULL. N must be a    positive integer e.g., 1, 2, and 3.

# COMMAND ----------

# MAGIC %md
# MAGIC **NTH_VALUE USING DEFAULT FRAME CLAUSE**

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,
# MAGIC nth_value(item, 2) over(order by units) as nth_value from sales_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,
# MAGIC nth_value(item, 4) over(order by units) as nth_value from sales_data

# COMMAND ----------

# MAGIC %md
# MAGIC **NTH_VALUE USING UNBOUNDED FOLLOWING FRAME CLAUSE**

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,
# MAGIC nth_value(item, 2) over(order by units rows between unbounded preceding and unbounded following) as nth_value from sales_data

# COMMAND ----------

# MAGIC %md
# MAGIC **NTH_VALUE USING PARTITION BY**

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,
# MAGIC nth_value(item, 2) over(partition by sales_man order by units rows between unbounded preceding and unbounded following) as nth_value from sales_data

# COMMAND ----------

# MAGIC %md
# MAGIC **Q: Which month in 2018 has the 4th and 6th highest sales**

# COMMAND ----------

# MAGIC %sql
# MAGIC select order_date,  year(order_date) as year_of, item, sales_man, units*unit_price amount from sales_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select order_date, month(order_date) as month, item, sales_man, units*unit_price amount from sales_data
# MAGIC where year(order_date) = "2018"
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select month(order_date) as month, sum(units*unit_price) total_amount from sales_data
# MAGIC where year(order_date) = "2018"
# MAGIC group by 1
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, 
# MAGIC nth_value(month, 4) over(order by total_amount desc rows between unbounded preceding and unbounded following) as 4th_highest_sale,
# MAGIC nth_value(month, 6) over(order by total_amount desc rows between unbounded preceding and unbounded following) as 6th_highest_sale
# MAGIC from (
# MAGIC   select month(order_date) as month, sum(units*unit_price) total_amount from sales_data
# MAGIC   where year(order_date) = "2018"
# MAGIC   group by 1
# MAGIC   order by total_amount desc
# MAGIC )

# COMMAND ----------


