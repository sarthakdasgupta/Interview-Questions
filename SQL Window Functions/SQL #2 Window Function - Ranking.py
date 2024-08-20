# Databricks notebook source
# MAGIC %md
# MAGIC Spark Window functions
# MAGIC --------------------
# MAGIC Spark SQL Window Function operate on a group of rows (like frame, partition) and return a single value for every input row. A single partition is referred as a window

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Ranking Function
# MAGIC 2. Analytical Function
# MAGIC 3. Aggregate Function

# COMMAND ----------

# MAGIC %md
# MAGIC Ranking Function
# MAGIC ------------------

# COMMAND ----------

# MAGIC %md
# MAGIC 1. row_number()
# MAGIC 2. rank()
# MAGIC 3. dense_rank()
# MAGIC 4. percent_rank()
# MAGIC 5. n_tile()

# COMMAND ----------

# MAGIC %md
# MAGIC Part 1: 
# MAGIC ------
# MAGIC 1. row_number
# MAGIC 2. rank()
# MAGIC 3. dense_rank()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_data

# COMMAND ----------

# MAGIC %md
# MAGIC ROW_NUMBER()
# MAGIC ------------
# MAGIC row_number() creates a column containing sequence of numbers starting from 1 within a partition

# COMMAND ----------

# MAGIC %md
# MAGIC **Q1. Display the employee who get the highest and lowest salary in each department**

# COMMAND ----------

# MAGIC %sql
# MAGIC --Highest salary
# MAGIC select employee_id, full_name, job, salary, department, row_num from(
# MAGIC select *, 
# MAGIC row_number() over(partition by department order by salary desc) as row_num 
# MAGIC from employee_data 
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --Lowest Salary
# MAGIC select employee_id, full_name, job, salary, department, row_num from(
# MAGIC select *, 
# MAGIC row_number() over(partition by department order by salary) as row_num 
# MAGIC from employee_data 
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC with c as (
# MAGIC select full_name, department, salary,
# MAGIC row_number() over(partition by department order by salary desc) as row_high,
# MAGIC row_number() over(partition by department order by salary) as row_low 
# MAGIC from employee_data)
# MAGIC select * from c

# COMMAND ----------

# MAGIC %sql
# MAGIC with c as (
# MAGIC select full_name, department, salary,
# MAGIC row_number() over(partition by department order by salary desc) as row_high,
# MAGIC row_number() over(partition by department order by salary) as row_low 
# MAGIC from employee_data)
# MAGIC select * from c where row_high = 1 or row_low = 1

# COMMAND ----------

# MAGIC %md
# MAGIC **Q2. From employee table display the columns: department, highest_salary_emp which contains name of employee getting highest salary and lowest_salary_emp which is opposite of the previous column**

# COMMAND ----------

# MAGIC %sql
# MAGIC with c as (
# MAGIC select full_name, department, salary,
# MAGIC row_number() over(partition by department order by salary desc) as row_high,
# MAGIC row_number() over(partition by department order by salary) as row_low 
# MAGIC from employee_data)
# MAGIC select 
# MAGIC department,
# MAGIC full_name,
# MAGIC case when row_high = 1 then full_name else null end as highest_salary_emp,
# MAGIC row_high,
# MAGIC case when row_low = 1 then full_name else null end as lowest_salary_emp,
# MAGIC row_low
# MAGIC from c
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC with c as (
# MAGIC select full_name, department, salary,
# MAGIC row_number() over(partition by department order by salary desc) as row_high,
# MAGIC row_number() over(partition by department order by salary) as row_low 
# MAGIC from employee_data)
# MAGIC select 
# MAGIC department, 
# MAGIC max(case when row_high = 1 then full_name else null end) as highest_salary_emp,
# MAGIC max(case when row_low = 1 then full_name else null end) as lowest_salary_emp
# MAGIC from c
# MAGIC group by 1

# COMMAND ----------

# MAGIC %md
# MAGIC Get all the highest salary employee from each department

# COMMAND ----------

# MAGIC %sql
# MAGIC select employee_id, full_name, job, salary, department, row_num from(
# MAGIC select *, 
# MAGIC row_number() over(partition by department order by salary desc) as row_num 
# MAGIC from employee_data 
# MAGIC )
# MAGIC where department = "Finance"

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC RANK()
# MAGIC ------------
# MAGIC rank() creates a column containing sequence of numbers starting from 1 with gaps within a window. Within a window if there are two or more rows with same set of values based on the order then rank of same number will be created and it picks up where the row number left off for the next record

# COMMAND ----------

# MAGIC %sql
# MAGIC select employee_id, full_name, job, salary, department, rank_num from(
# MAGIC select *, 
# MAGIC rank() over(partition by department order by salary desc) as rank_num 
# MAGIC from employee_data 
# MAGIC )
# MAGIC where department = "Finance"

# COMMAND ----------

# MAGIC %md
# MAGIC **Q1 For each department list all the employees who get highest salary and lowest salary**

# COMMAND ----------

# MAGIC %sql
# MAGIC --highest salary
# MAGIC select employee_id, full_name, job, salary, department, rank_num from(
# MAGIC select *, 
# MAGIC rank() over(partition by department order by salary desc) as rank_num 
# MAGIC from employee_data 
# MAGIC )
# MAGIC where rank_num = 1
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --lowest salary
# MAGIC select employee_id, full_name, job, salary, department, rank_num from(
# MAGIC select *, 
# MAGIC rank() over(partition by department order by salary) as rank_num 
# MAGIC from employee_data 
# MAGIC )
# MAGIC where rank_num = 1
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --highest salary
# MAGIC select employee_id, full_name, job, salary, department, rank_num from(
# MAGIC select *, 
# MAGIC rank() over(partition by department order by salary desc) as rank_num 
# MAGIC from employee_data 
# MAGIC )
# MAGIC where rank_num = 1
# MAGIC union
# MAGIC --lowest salary
# MAGIC select employee_id, full_name, job, salary, department, rank_num from(
# MAGIC select *, 
# MAGIC rank() over(partition by department order by salary) as rank_num 
# MAGIC from employee_data 
# MAGIC )
# MAGIC where rank_num = 1

# COMMAND ----------

# MAGIC %md
# MAGIC **Q2.For each department list all the employees who get highest salary and lowest salary along with a column indicating if that employee has highest or lowest salary**

# COMMAND ----------

# MAGIC %sql
# MAGIC --highest salary
# MAGIC select employee_id, full_name, job, salary, department, "highest_salary_emp" as salary_status from(
# MAGIC select *, 
# MAGIC rank() over(partition by department order by salary desc) as rank_num 
# MAGIC from employee_data 
# MAGIC )
# MAGIC where rank_num = 1
# MAGIC union
# MAGIC --lowest salary
# MAGIC select employee_id, full_name, job, salary, department, "lowest_salary_emp" as salary_status from(
# MAGIC select *, 
# MAGIC rank() over(partition by department order by salary) as rank_num 
# MAGIC from employee_data 
# MAGIC )
# MAGIC where rank_num = 1

# COMMAND ----------

# MAGIC %md
# MAGIC **Q. Who are the youngest and oldest employee in each department**

# COMMAND ----------

# MAGIC %sql
# MAGIC --youngest
# MAGIC select full_name, department, age, rank_num from(
# MAGIC select *, 
# MAGIC rank() over(partition by department order by age) as rank_num 
# MAGIC from employee_data )
# MAGIC where rank_num = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC --oldest
# MAGIC select full_name, department, age, rank_num from(
# MAGIC select *, 
# MAGIC rank() over(partition by department order by age desc) as rank_num 
# MAGIC from employee_data )
# MAGIC where rank_num = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC --youngest
# MAGIC select full_name, department, age, rank_num from(
# MAGIC select *, 
# MAGIC rank() over(partition by department order by age) as rank_num 
# MAGIC from employee_data )
# MAGIC where rank_num = 1
# MAGIC union
# MAGIC --oldest
# MAGIC select full_name, department, age, rank_num from(
# MAGIC select *, 
# MAGIC rank() over(partition by department order by age desc) as rank_num 
# MAGIC from employee_data )
# MAGIC where rank_num = 1

# COMMAND ----------

# MAGIC %md
# MAGIC Getting nth highest or lowest from a partition

# COMMAND ----------

# MAGIC %sql
# MAGIC select full_name, department, age, rank_num from(
# MAGIC select *, 
# MAGIC rank() over(partition by department order by age) as rank_num 
# MAGIC from employee_data )
# MAGIC where department = "Finance"

# COMMAND ----------

# MAGIC %md
# MAGIC DENSE_RANK()
# MAGIC ------------
# MAGIC dense_rank() creates a column containing sequence of numbers starting from 1. Within a window if there are two or more rows with same set of values based on the order then rank of same number will be created but unlike rank() it will pick up from next number and not leave any gap

# COMMAND ----------

# MAGIC %sql
# MAGIC select full_name, department, age, rank_num from(
# MAGIC select *, 
# MAGIC dense_rank() over(partition by department order by age) as rank_num 
# MAGIC from employee_data )
# MAGIC where department = "Finance"

# COMMAND ----------

# MAGIC %md
# MAGIC **Q1. Display 2nd highest salary of all employees in a department**

# COMMAND ----------

# MAGIC %sql
# MAGIC select full_name, department, salary, drank_num from(
# MAGIC select *, 
# MAGIC dense_rank() over(partition by department order by salary desc) as drank_num 
# MAGIC from employee_data )
# MAGIC where drank_num = 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select full_name, department, salary, drank_num from(
# MAGIC select *, 
# MAGIC dense_rank() over(partition by department order by salary desc) as drank_num 
# MAGIC from employee_data )
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Dense_rank() is best suited for situation with nth highest or lowest values**

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Row_number() | Rank() | Dense_rank() Comparison
# MAGIC ------------------------------------------------

# COMMAND ----------

# MAGIC %sql
# MAGIC select full_name, department, salary, row_num, rank_num, dense_rank_num from(
# MAGIC select *, 
# MAGIC row_number() over(partition by department order by salary desc) as row_num,
# MAGIC rank() over(partition by department order by salary desc) as rank_num,
# MAGIC dense_rank() over(partition by department order by salary desc) as dense_rank_num 
# MAGIC from employee_data )
# MAGIC

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Part 2: 
# MAGIC ------
# MAGIC These functions are particularly useful when you want to compute relative rankings or perform statistical analyses on your data.
# MAGIC 1. percent_rank()
# MAGIC 2. cume_dist()
# MAGIC 3. n_tile()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_data

# COMMAND ----------

# MAGIC %md
# MAGIC PERCENT_RANK()
# MAGIC ------------
# MAGIC Returns the percentile ranking number which ranges from zero to one for rows within a window or partition.

# COMMAND ----------

# MAGIC %md
# MAGIC Formula:
# MAGIC (rank - 1) / (total_rows - 1)
# MAGIC 1. rank: rank of the rowin that window
# MAGIC 2. total_rows: total no of rows in that window

# COMMAND ----------

# MAGIC %sql
# MAGIC select employee_id, department, salary, 
# MAGIC round(percent_rank() over(partition by department order by salary), 2) as percent_rank,
# MAGIC rank() over(partition by department order by salary) as rank
# MAGIC from employee_data
# MAGIC
# MAGIC --Result
# MAGIC -- In window Finance
# MAGIC -- 4000 is lowest salary so pr = 0 -> (1-1)/(7-1) = 0
# MAGIC -- 10000 is highest but more than 1 employee gets 10000 so pr = 0.83 -> (6-1)/(7-1) = 5/6 = 0.83
# MAGIC
# MAGIC -- In window IT
# MAGIC -- 3000 is lowest salary so pr = 0
# MAGIC -- 10000 is highedst salary compared to all so 1 -> (7-1)/(7-1) = 6/6 = 1
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Q1 What is the percentage of how much younger or older an employee is compared to other employees belonging to Research & Development**

# COMMAND ----------

# MAGIC %sql
# MAGIC select employee_id, full_name, business_unit, age,
# MAGIC round(percent_rank() over(partition by business_unit order by age), 2) * 100 as age_rank
# MAGIC from employee_data
# MAGIC where business_unit = "Research & Development"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC **Q2. From employee_data find out the average of high salaries and average of low salaries compared among all the employees. Salaries of employee whose percentage is greater than or equal to 50 is high salary and rest is low salary**

# COMMAND ----------

# MAGIC %sql
# MAGIC --percentage of salary among all employee
# MAGIC select employee_id, full_name, salary,
# MAGIC round(percent_rank() over(order by salary), 1) * 100 as salary_percent
# MAGIC from employee_data

# COMMAND ----------

# MAGIC %sql
# MAGIC with sal as (
# MAGIC   select employee_id, full_name, salary,
# MAGIC   round(percent_rank() over(order by salary), 1) * 100 as salary_percent
# MAGIC   from employee_data
# MAGIC )
# MAGIC select 'average_salary',
# MAGIC round(avg(case when salary_percent>=50 then salary end)) as high,
# MAGIC round(avg(case when salary_percent<50 then salary end)
# MAGIC ) as low
# MAGIC from sal
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC CUME_DIST
# MAGIC ----------
# MAGIC CUME_DIST returns the cumulative distribution of a value compared to all other values in a window

# COMMAND ----------

# MAGIC %md
# MAGIC Formula: (no of rows with a value <= the current row's value) / total rows in a window

# COMMAND ----------

# MAGIC %sql
# MAGIC select full_name, department, salary, 
# MAGIC round(cume_dist() over(partition by department order by salary), 3) as cum_dist,
# MAGIC round(cume_dist() over(partition by department order by salary) * 100, 2) as cum_dist_percent
# MAGIC from employee_data

# COMMAND ----------

# MAGIC %md
# MAGIC **Q1. List all the employees who distribution percentage IT based on their age is more than 30%**

# COMMAND ----------

# MAGIC %sql
# MAGIC --distribution percentage based on age
# MAGIC select full_name, department, age, 
# MAGIC round(cume_dist() over(partition by department order by age) * 100, 2) as age_percent
# MAGIC from employee_data
# MAGIC where department="IT"
# MAGIC
# MAGIC --age_percent: It returns the distribution percentage of age compared to all other age

# COMMAND ----------

# MAGIC %md
# MAGIC **Q2. List down the employee whose salary constitutes for first 40% distribution compared to all the salaries accross the organization**

# COMMAND ----------

# MAGIC %sql
# MAGIC select full_name, department, salary, salary_dist_percent from(
# MAGIC select *, 
# MAGIC round(cume_dist() over(partition by department order by salary) * 100, 2) as salary_dist_percent
# MAGIC from employee_data)
# MAGIC where salary_dist_percent < 40
# MAGIC order by salary_dist_percent

# COMMAND ----------

# MAGIC %md
# MAGIC **Percent_rank() vs Cume_dist()**
# MAGIC 1. Percent_rank: percentile of a rank within a window
# MAGIC 2. Cume_dist: distribution of a value compared to all values in a window

# COMMAND ----------

# MAGIC %sql
# MAGIC select department, salary,
# MAGIC rank() over(partition by department order by salary) as rank,
# MAGIC round(percent_rank() over(partition by department order by salary), 2) as percent_rank,
# MAGIC round(cume_dist() over(partition by department order by salary), 2) as cume_dist
# MAGIC from employee_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select department, salary,
# MAGIC rank() over(partition by department order by salary) as rank,
# MAGIC round(percent_rank() over(partition by department order by salary)*100, 2) as percent_rank_percentage,
# MAGIC round(cume_dist() over(partition by department order by salary)*100, 2) as cume_dist_percentage
# MAGIC from employee_data

# COMMAND ----------

# MAGIC %md
# MAGIC NTILE()
# MAGIC ------------
# MAGIC It groups togethor a set of data within a partition forming buckets of equal size and assigns a number to each bucket.

# COMMAND ----------

# MAGIC %sql
# MAGIC select employee_id, department, salary, 
# MAGIC ntile(3) over(partition by department order by salary) as ntile_bucket
# MAGIC from employee_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select employee_id, department, salary,
# MAGIC ntile() over(partition by department order by salary) as ntile_bucket_1,
# MAGIC ntile(2) over(partition by department order by salary) as ntile_bucket_2,
# MAGIC ntile(3) over(partition by department order by salary) as ntile_bucket_3
# MAGIC from employee_data
# MAGIC where department = "Finance"

# COMMAND ----------

# MAGIC %md
# MAGIC **Q. From employee_data segregate all the employees with older, mid and younger age compared to each other where oldest comes at top and so on**

# COMMAND ----------

# MAGIC %sql
# MAGIC select employee_id, age,
# MAGIC ntile(3) over(order by age desc) as age_ntile
# MAGIC from employee_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select full_name, age, 
# MAGIC case when age_ntile = 1 then "Older"
# MAGIC       when age_ntile = 2 then "Mid"
# MAGIC       else "Younger" end as age_status
# MAGIC from (
# MAGIC select *,
# MAGIC ntile(3) over(order by age desc) as age_ntile
# MAGIC from employee_data)

# COMMAND ----------


