# Databricks notebook source
# MAGIC %md
# MAGIC Analytical or Value Window Functions
# MAGIC ----------------------------------------
# MAGIC **The value window functions in SQL are used to assign a value to a row from other rows.**
# MAGIC 1. Lag
# MAGIC 2. Lead
# MAGIC 3. Nth Value
# MAGIC 4. First Value
# MAGIC 5. Last Value

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales_data

# COMMAND ----------

# MAGIC %md
# MAGIC LAG()
# MAGIC ---------
# MAGIC Lag function assigns value to each row that belongs to the previous row or whatever specified offset is before current row.

# COMMAND ----------

# Lag Syntax: 

#   select *, lag(col_name, offset, default) over(partitioned by col1 order by col2) from tbl

#       col_name: name of the column or an expression
#         eg: lag(col1-1) => here whatever value the expression (col1-1) will generate that will be assigned in the lag column.
      
#       offset: an integer number that tells how many no of rows behind current row the value will be fetched. Default offset is 1
#         eg: offset = 1 => 1 row behind; offset = 2 => 2 rows behind

#       default: a default value to assign if the offset goes beyond first row. Null is assign if we dont define default value. 
#         eg: default = "hi" offset = 1 so for the first row lag will be "hi" as there is no row previous to first row.

# COMMAND ----------

# MAGIC %md
# MAGIC **Example 1: SQL Lag function without a default value**

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, lag(order_date) over(order by order_date) as lag from sales_data
# MAGIC
# MAGIC -- The first row shows NULL value for the lag column because it does not have any previous rows
# MAGIC -- The second row contains previous row value in the lag column. It takes value from the previous row due to default offset value 1

# COMMAND ----------

# MAGIC %md
# MAGIC **Example 2: SQL Lag function with a default value**

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, lag(order_date, 1, "2015-01-01") over(order by order_date) as lag from sales_data
# MAGIC
# MAGIC -- The first row has default value instead of null
# MAGIC -- The second row contains previous row value in the lag column. It takes value from the previous row due to offset value 1

# COMMAND ----------

# MAGIC %md
# MAGIC **Example 3: SQL Lag function with OFFSET value 2**

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, lag(order_date, 2, "2015-01-01") over(order by order_date) as lag from sales_data 
# MAGIC
# MAGIC -- The first and second row has default value instead of null because offset is 2 and there is no 2nd row prior to it.
# MAGIC -- The third row contains 1st row's value in the lag column. It takes value from 2 row priod to it thats why 1st row's value was picked

# COMMAND ----------

# MAGIC %md
# MAGIC **Example 4: SQL Lag function with PARTITION BY clause**

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, lag(order_date, 3, "2015-01-01") over(partition by region order by order_date) as lag from sales_data 
# MAGIC
# MAGIC -- Using patitionBy clause, lag will pick values from previous row within a window or a partition. Rest functionality remains same.
# MAGIC -- The first, second and third row has default value instead of null because offset is 3 and there are no 3rd row prior to them
# MAGIC -- The fourth row contains 1st row's value in the lag column. It takes value from 3 rows prio to it thats why 1st row's value was picked

# COMMAND ----------

# MAGIC %md
# MAGIC **Example 5: Using expression instead of column name**

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, lag(date_add(order_date, 1), 3, "2015-01-01") over(partition by region order by order_date) as lag from sales_data
# MAGIC
# MAGIC -- The first, second and third row has default value instead of null because offset is 3 and there are no 3rd row prior to them
# MAGIC -- All rows starting from fourth row contains outcome of the expression orderdate + 1  
# MAGIC -- eg: fourth row => 2018-01-23 + 1 day = 2018-01-24: fifth_row => 2018-02-09 + 1 day = 2018-02-10. 
# MAGIC
# MAGIC -- conclusion:  orderdate values are picked from 3 rows prior to current row due to offset 3 and within a window due to the parition by clause and finally added 1 day to it due to the expression
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Q. What is the percentage increase/decrease in current sale when compared with previous sale amount for each salesman in central region. Percentage should be to 2 decimal place**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Current sale and Previous sale value for each salesman in central region
# MAGIC select *, 
# MAGIC   units*unit_price as curr_sale, 
# MAGIC   lag(units*unit_price, 1, 0) over(partition by sales_man order by order_date) prev_sale 
# MAGIC from sales_data
# MAGIC where region = "Central"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Difference between current and previous sale
# MAGIC with sale_calc(
# MAGIC   select *, 
# MAGIC   units*unit_price as curr_sale, 
# MAGIC   lag(units*unit_price, 1, 0) over(partition by sales_man order by order_date) prev_sale 
# MAGIC from sales_data
# MAGIC where region = "Central"
# MAGIC )
# MAGIC
# MAGIC select order_date, region, sales_man, item, units, unit_price, curr_sale, prev_sale, 
# MAGIC coalesce(round((curr_sale-prev_sale)/prev_sale * 100, 2), curr_sale) as per_diff from sale_calc
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Lead()
# MAGIC --------
# MAGIC The LEAD() window function is the exact opposite of the LAG() function because while LAG() returns for each row the value of the previous row, the LEAD() function will return the value of the following row.

# COMMAND ----------

# Lead Syntax: 
#   select *, lead(col_name, offset, default) over(partitioned by col1 order by col2) from tbl
#       col_name: name of the column or an expression
#         eg: lead(col1-1) => here whatever value the expression (col1-1) will generate that will be assigned in the lead column.
      
#       offset: an integer number that tells how many no of rows after current row the value will be fetched. Default offset is 1
#         eg: offset = 1 => 1 row after; offset = 2 => 2 rows after

#       default: a default value to assign if the offset goes beyond last row. Null is assign if we dont define default value. 
#         eg: default = "hi" offset = 1 so for the last row lead will be "hi" as there is no row next to last row.

# COMMAND ----------

# MAGIC %md
# MAGIC **Example 1: SQL Lead function without a default value**

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, lead(order_date) over(order by order_date) as lead from sales_data
# MAGIC
# MAGIC -- The last row shows NULL value for the lead column because it does not have any next rows
# MAGIC -- The first row contains next row value in the lead column. It takes value from the next row due to default offset value 1

# COMMAND ----------

# MAGIC %md
# MAGIC **Example 2: SQL Lead function with a default value**

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, lead(order_date, 1, "2000-01-01") over(order by order_date) as lead from sales_data
# MAGIC
# MAGIC -- The last row has default value instead of null
# MAGIC -- The second row contains next row value in the lead column. It takes value from the next row due to offset value 1

# COMMAND ----------

# MAGIC %md
# MAGIC **Example 3: SQL Lead function with OFFSET value 2**

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, lead(order_date, 2, "2000-01-01") over(order by order_date) as lead from sales_data
# MAGIC
# MAGIC -- The last and second-last row has default value instead of null because offset is 2 and there is no 2nd row prior to it.
# MAGIC -- The first row contains 3rd row's value in the lead column. It takes value from 2 row next to it thats why 3rd row's value was picked

# COMMAND ----------

# MAGIC %md
# MAGIC **Example 4: SQL Lead function with PARTITION BY clause**

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, lead(order_date, 3, "2015-01-01") over(partition by region order by order_date) as lead from sales_data 
# MAGIC
# MAGIC -- Using partitionBy clause, lead will pick values from next row within a window or a partition. Rest functionality remains same.
# MAGIC -- The last, second-last and third-last row has default value instead of null because offset is 3 and there is no 3rd row prior to them
# MAGIC -- The first row contains 4th row's value in the lead column. It takes value from 3 rows prior to it thats why 4th row's value was picked

# COMMAND ----------

# MAGIC %md
# MAGIC **Example 5: Using expression instead of column name**

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, lead(date_add(order_date, 1), 2, "2015-01-01") over(partition by region order by order_date) as lead from sales_data
# MAGIC
# MAGIC -- The last and second-last row has default value instead of null because offset is 2 and there is no 2nd row next to them
# MAGIC -- All rows starting except the last 2 rows contains outcome of the expression orderdate + 1  
# MAGIC -- eg: 1st row => 2018-02-26 + 1 day = 2018-02-27: fifth_row => 2018-04-18 + 1 day = 2018-04-19. 
# MAGIC
# MAGIC -- conclusion:  order_date values are picked from 2 rows next to current row due to offset 2 and within a window due to the partition by clause and finally added 1 day to it due to the expression
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Q. List down all the items whose units sold in current sale was more than next sale**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Displayed next_units using lead
# MAGIC select order_date, sales_man, item, units, lead(units) over(partition by item order by order_date) as next_units from sales_data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- compared current sale units with next_units to find out which one is higher
# MAGIC select * from (
# MAGIC   select order_date, sales_man, item, units, lead(units) over(partition by item order by order_date) as next_units from sales_data
# MAGIC ) x where x.units>x.next_units

# COMMAND ----------


