# Databricks notebook source
# MAGIC %md
# MAGIC Part 1
# MAGIC --------

# COMMAND ----------

# MAGIC %md
# MAGIC Display the output when joining tbl1 and tbl2 using inner, left, right, full
# MAGIC ------------
# MAGIC **Also find the total records after each join**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE tbl1 (
# MAGIC     id int
# MAGIC );
# MAGIC insert into tbl1 values (1),(1),(1),(2),(3),(3),(3);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE tbl2 (
# MAGIC     id int
# MAGIC ) ;
# MAGIC insert into tbl2 values (1),(1),(2),(2),(4),(null);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tbl1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tbl2

# COMMAND ----------

# MAGIC %md
# MAGIC Inner Join
# MAGIC ------
# MAGIC Inner join will combine only matching records from both the join keys

# COMMAND ----------

# -- #--Tbl1-#--#--Tbl2-#--
# ------id-------|----id------|
# ---|-----------|------------|
# -- |    1      |      1     |
# -- |    1      |      1     |
# -- |    1      |      2     |
# -- |    2      |      2     |
# -- |    3      |      4     |
# -- |    3      |      null  |
# -- |    3      |------------|
# -- |-----------|


# COMMAND ----------

# Rules for inner join
# ---------------------
# 1. Pick a record from id of tbl1 lets say id = 1, then check how times it occurs in tbl2
# 2. Create a result table with column tbl1.id, tbl2.id
# 3. The total no of times id=1 occurs in tbl1 multiplied by total no of times id=1 occurs in tbl2 is the total records for id=1
# 4. tbl1.id(1) and tbl2.id(1) will have 6 rows

# COMMAND ----------

# Eg: 
#     total no of time id=1 occurs in tbl1 -> 3
#     total no of time id=1 occurs in tbl2 -> 2
#     total no of rows for tbl1.id=tbl2.id=1 -> 3*2=6
#     tbl1.id | tbl2.id
#     -----------------
#         1   |   1
#         1   |   1
#         1   |   1
#         1   |   1
#         1   |   1
#         1   |   1
#     -----------------

# COMMAND ----------

# Total no of records for each id (1,2 are matching records in tbl1 and tbl2)
# id = 1 -> 3*2=6
# id = 2 -> 1*2=2
# total = 6+2 = 8


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tbl1 join tbl2 on tbl1.id=tbl2.id

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE tbl3 (
# MAGIC     id int
# MAGIC );
# MAGIC insert into tbl3 values (1),(1),(1),(2),(null),(null),(3);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tbl3

# COMMAND ----------

# -- #--Tbl3-#--#--Tbl2-#--
# ------id-------|----id------|
# ---|-----------|------------|
# -- |    1      |      1     |
# -- |    1      |      1     |
# -- |    1      |      2     |
# -- |    2      |      2     |
# -- |    null   |      4     |
# -- |    null   |      null  |
# -- |    3      |------------|
# -- |-----------|


# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tbl3 join tbl2 on tbl3.id=tbl2.id

# COMMAND ----------

# MAGIC %md
# MAGIC Left Join
# MAGIC -----------
# MAGIC Left join combines all the matching records for each table and for the non matching records it adds null to the right table columns

# COMMAND ----------

# -- #--Tbl1-#--#--Tbl2-#--
# ------id-------|----id------|
# ---|-----------|------------|
# -- |    1      |      1     |
# -- |    1      |      1     |
# -- |    1      |      2     |
# -- |    2      |      2     |
# -- |    3      |      4     |
# -- |    3      |      null  |
# -- |    3      |------------|
# -- |-----------|

# COMMAND ----------

# Rules for Left Join
# ---------------------
# 1. First perform inner join
# 2. For non matching records add null to right table columns
# 3. total record = inner join records + non matching records from left -> 8+3=11

# COMMAND ----------

# Eg:
#     3 is a non matching record as it is present in left table and not in right table
#     First we count no of times 3 occurs in left table (tbl1) = 3
#     Then we create three rows in result like this -> tbl1.id = 3 | tbl2.id = null
#     tbl1.id | tbl2.id
#     -----------------
#         3   |   null
#         3   |   null
#         3   |   null
#     -----------------

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tbl1 left join tbl2 on tbl1.id=tbl2.id

# COMMAND ----------

# -- #--Tbl3-#--#--Tbl2-#--
# ------id-------|----id------|
# ---|-----------|------------|
# -- |    1      |      1     |
# -- |    1      |      1     |
# -- |    1      |      2     |
# -- |    2      |      2     |
# -- |    null   |      4     |
# -- |    null   |      null  |
# -- |    3      |------------|
# -- |-----------|


# COMMAND ----------

#     tbl1.id | tbl2.id
#     -----------------
#         null |   null
#         null |   null
#         3    |   null
#     -----------------

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tbl3 left join tbl2 on tbl3.id=tbl2.id

# COMMAND ----------

# MAGIC %md
# MAGIC Right Join
# MAGIC -----------
# MAGIC Right join combines all the matching records for each table and for the non matching records it adds null to the left table columns

# COMMAND ----------

# -- #--Tbl1-#--#--Tbl2-#--
# ------id-------|----id------|
# ---|-----------|------------|
# -- |    1      |      1     |
# -- |    1      |      1     |
# -- |    1      |      2     |
# -- |    2      |      2     |
# -- |    3      |      4     |
# -- |    3      |      null  |
# -- |    3      |------------|
# -- |-----------|

# COMMAND ----------

# Rules for Right Join
# ---------------------
# 1. First perform inner join
# 2. For non matching records add null to left table columns
# 3. total record = inner join records + non matching records from right -> 8+2=10

# COMMAND ----------

# Eg:
#     4 is a non matching record as it is present in right table and not in left table
#     First we count no of times 4 occurs in right table (tbl2) = 1
#     Then we create 1 row in result like this -> tbl1.id = null | tbl2.id = 4
#     tbl1.id | tbl2.id
#     -----------------
#       null  |   4
#     -----------------

# Note: null will always be a non matching record or in other words every null is a unique record.
#     Just like 4 we count the no of times null occurs in right table and display the result similar to above eg.
#     Here null occurs once so the result would have 1 row containing null in both left and right table column
#     tbl1.id | tbl2.id
#     -----------------
#       null  |   null
#     -----------------

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tbl1 right join tbl2 on tbl1.id=tbl2.id

# COMMAND ----------

# -- #--Tbl3-#--#--Tbl2-#--
# ------id-------|----id------|
# ---|-----------|------------|
# -- |    1      |      1     |
# -- |    1      |      1     |
# -- |    1      |      2     |
# -- |    2      |      2     |
# -- |    null   |      4     |
# -- |    null   |      null  |
# -- |    3      |------------|
# -- |-----------|


# COMMAND ----------

#     tbl1.id | tbl2.id
#     -----------------
#         null |   4
#         null |   null
#     -----------------

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tbl3 right join tbl2 on tbl3.id=tbl2.id

# COMMAND ----------

# MAGIC %md
# MAGIC Full or Fullouter Join
# MAGIC -----------
# MAGIC Full join returns all the matching records along with each non matching record from left and right table

# COMMAND ----------

# Rules for Full Join
# ---------------------]
# 1. First perform inner join
# 2. Fetch additional records from left table which is not present in right table
# 3. Fetch additional records from right table which is not present in left table 

# COMMAND ----------

# -- #--Tbl1---#-#--Tbl2-#---#--fulljoin--#
# ----|-----------|------------|tbl1_id-|-tbl2_id|
# --0 |    1      |      1     |    1   |  1    |
# --1 |    1      |      1     |    1   |  1    |
# --2 |    1      |      2     |    1   |  1    |
# --3 |    2      |      2     |    1   |  1    |
# --4 |    3      |      4     |    1   |  1    |
# --5 |    3      |      null  |    1   |  1    |
# --6 |    3      |------------|    2   |  2    |
# --7 |-----------|            |    2   |  2    |<---till here inner join results
# --                           |----------------|
# --8                          |    3   |  null |<-|
# --9                          |    3   |  null |  |
# --10                         |    3   |  null |<-|--[All additional records in 
# --11                         |----------------|    left not present in right] x1
# --12                         | null   |  4    |<-|
# --13                         | null   |  null |<-|--[All additional records in
# --                           |----------------|    right not present in left] x2

# COMMAND ----------

# Total no of records
# ----------------------
# total inner join records + non matching records from left + non matching reco]nullrds from right
# 8 + 3 + 2 = 13

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tbl1 full join tbl2 on tbl1.id=tbl2.id

# COMMAND ----------

# -- #--Tbl3-#--#--Tbl2-#--
# ------id-------|----id------|
# ---|-----------|------------|
# -- |    1      |      1     |
# -- |    1      |      1     |
# -- |    1      |      2     |
# -- |    2      |      2     |
# -- |    null   |      4     |
# -- |    null   |      null  |
# -- |    3      |------------|
# -- |-----------|


# COMMAND ----------

# -- #--Tbl1---#-#--Tbl2-#---#--leftjoin--#
# ----|-----------|------------|tbl1_id-|-tbl2_id|
# --0 |    1      |      1     |    1   |  1    |
# --1 |    1      |      1     |    1   |  1    |
# --2 |    1      |      2     |    1   |  1    |
# --3 |    2      |      2     |    1   |  1    |
# --4 |    null   |      4     |    1   |  1    |
# --5 |    null   |      null  |    1   |  1    |
# --6 |    3      |------------|    2   |  2    |
# --7 |-----------|            |    2   |  2    |<---till here inner join results
# --                           |----------------|
# --8                          |    null|  null |<-|
# --9                          |    null|  null |  |
# --10                         |    3   |  null |<-|--[All additional records in 
# --11                         |----------------|    left not present in right] x1
# --12                         | null   |  4    |<-|
# --13                         | null   |  null |<-|--[All additional records in
# --                           |----------------|    right not present in left] x2

# COMMAND ----------

# Total no of records
# ----------------------
# total inner join records + non matching records from left + non matching records from right
# 8 + 3 + 2 = 13

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tbl3 full join tbl2 on tbl3.id=tbl2.id order by tbl3.id asc

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Part 2
# MAGIC -----------

# COMMAND ----------

# MAGIC %md
# MAGIC Display the out when joining tbl1 and tbl2 using cross, semi, anti join
# MAGIC ------------
# MAGIC **Also find the total records after each join**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE tbl1 (
# MAGIC     id int
# MAGIC );
# MAGIC insert into tbl1 values (1),(1),(1),(2),(3),(3),(3);
# MAGIC
# MAGIC %sql
# MAGIC CREATE TABLE tbl2 (
# MAGIC     id int
# MAGIC ) ;
# MAGIC insert into tbl2 values (1),(1),(2),(2),(4),(null);
# MAGIC
# MAGIC %sql
# MAGIC CREATE TABLE tbl3 (
# MAGIC     id int
# MAGIC );
# MAGIC insert into tbl3 values (1),(1),(1),(2),(null),(null),(3);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tbl1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tbl2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tbl3

# COMMAND ----------

# MAGIC %md
# MAGIC Cross Join
# MAGIC --------------
# MAGIC CROSS JOIN returns a combination of each row in the left table paired with each row in the right table.

# COMMAND ----------

# Rules of Cross Join
# -----------------------
# 1. Every record from left will try to match with every record from right. 
# 2. Even if it doesnt match it will be populated in the result

# COMMAND ----------

# -- #--Tbl1-#--#--Tbl2-#--
# ------id-------|----id------|
# ---|-----------|------------|
# -- |    1      |      1     |
# -- |    1      |      1     |
# -- |    1      |      2     |
# -- |    2      |      2     |
# -- |    3      |      4     |
# -- |    3      |      null  |
# -- |    3      |------------|
# -- |-----------|

# COMMAND ----------

# Eg - Result for first row from left table -> tbl1.id =1
# -- #--Tbl1-#--#--Tbl2-#--
# ------id-------|----id------|
# ---|-----------|------------|
# -- |    ]     |      1     |
# -- |    1      |      1     |
# -- |    1      |      2     |
# -- |    1      |      2     |
# -- |    1      |      4     |
# -- |    1      |      null  |
# -- |-----------|------------|


# COMMAND ----------

# Total no of records
# Total no of records in left table multiplied by total no of records in right table
# 7 * 6 = 42

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tbl1 cross join tbl2

# COMMAND ----------

# What happens if you perform cross join with join condition
# Result will be inner join

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tbl1 cross join tbl2 on tbl1.id=tbl2.id

# COMMAND ----------

# -- #--Tbl3-#--#--Tbl2-#--
# ------id-------|----id------|
# ---|-----------|------------|
# -- |    1      |      1     |
# -- |    1      |      1     |
# -- |    1      |      2     |
# -- |    2      |      2     |
# -- |    null   |      4     |
# -- |    null   |      null  |
# -- |    3      |------------|
# -- |-----------|


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tbl3 cross join tbl2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tbl3 cross join tbl2 on tbl3.id=tbl2.id

# COMMAND ----------

# MAGIC %md
# MAGIC Semi of Left Semi Join
# MAGIC --------------
# MAGIC A Semi-join returns rows from the left table which are matching in the right table.

# COMMAND ----------

# -- #--Tbl1-#--#--Tbl2-#--
# ------id-------|----id------|
# ---|-----------|------------|
# -- |    1      |      1     |
# -- |    1      |      1     |
# -- |    1      |      2     |
# -- |    2      |      2     |
# -- |    3      |      4     |]
# -- |    3      |      null  |
# -- |    3      |------------|
# -- |-----------|


# COMMAND ----------

# Rules of Semi Join
# ---------------------
# 1. Find all the records in left table which are matching with right table
# 2. Return all these matching records from left table.
# 3. If a record in left table matches with multiple records in right table then return only 1 isinstance
# 4. Null is avoided as it is unique.

# COMMAND ----------

# Total no of records
# --------------------
# total no of matching records from left table -> 4

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tbl1 left semi join tbl2 on tbl1.id=tbl2.id

# COMMAND ----------

# Join tbl2 with tbl1
# -- #--Tbl1-#--#--Tbl2-#--
# ------id-------|----id------|
# ---|-----------|------------|
# -- |    1      |      1     |
# -- |    1      |      1     |
# -- |    1      |      2     |
# -- |    2      |      2     |
# -- |    3      |      4     |
# -- |    3      |      null  |
# -- |    3      |------------|
# -- |-----------|


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tbl2 left semi join tbl1 on tbl2.id=tbl1.id

# COMMAND ----------

# -- #--Tbl3-#--#--Tbl2-#--
# ------id-------|----id------|
# ---|-----------|------------|
# -- |    1      |      1     |
# -- |    1      |      1     |
# -- |    1      |      2     |
# -- |    2      |      2     |
# -- |    null   |      4     |
# -- |    null   |      null  |
# -- |    3      |------------|
# -- |-----------|


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tbl3 left semi join tbl2 on tbl3.id=tbl2.id

# COMMAND ----------

# MAGIC %md
# MAGIC Anti or Left Anti Join
# MAGIC --------------
# MAGIC Anti join return rows from left table which are not matching with right table

# COMMAND ----------

# Rules of Anti Join
# ---------------------
# 1. Find all the records in left table which are not matching with right table
# 2. Return all these non matching records from left table.
# 4. Null is not avoided in this join

# COMMAND ----------

# -- #--Tbl1-#--#--Tbl2-#--
# ------id-------|----id------|
# ---|-----------|------------|
# -- |    1      |      1     |
# -- |    1      |      1     |
# -- |    1      |      2     |
# -- |    2      |      2     |
# -- |    3      |      4     |
# -- |    3      |      null  |
# -- |    3      |------------|
# -- |-----------|
 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tbl1 left anti join tbl2 on tbl1.id=tbl2.id

# COMMAND ----------

# Join tbl2 with tbl1
# -- #--Tbl1-#--#--Tbl2-#--
# ------id-------|----id------|
# ---|-----------|------------|
# -- |    1      |      1     |
# -- |    1      |      1     |
# -- |    1      |      2     |
# -- |    2      |      2     |
# -- |    3      |      4     |
# -- |    3      |      null  |
# -- |    3      |------------|
# -- |-----------|


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tbl2 anti join tbl1 on tbl2.id=tbl1.id

# COMMAND ----------

# -- #--Tbl3-#--#--Tbl2-#--
# ------id-------|----id------|
# ---|-----------|------------|
# -- |    1      |      1     |
# -- |    1      |      1     |
# -- |    1      |      2     |
# -- |    2      |      2     |
# -- |    null   |      4     |
# -- |    null   |      null  |
# -- |    3      |------------|
# -- |-----------|


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tbl3 anti join tbl2 on tbl3.id=tbl2.id

# COMMAND ----------

# MAGIC %md
# MAGIC Bonus Question
# MAGIC --------------
# MAGIC **What will be the result if you perform semi join between tbl1 and tbl2 and then perform anti join with tbl3**

# COMMAND ----------

# -- #--Tbl1-#--#--Tbl2-#--
# ------id-------|----id------|
# ---|-----------|------------|
# -- |    1      |      1     |
# -- |    1      |      1     |
# -- |    1      |      2     |
# -- |    2      |      2     |
# -- |    3      |      4     |
# -- |    3      |      null  |
# -- |    3      |------------|
# -- |-----------|

# -- #--Tbl3-#---
# ------id-------|
# ---|-----------|
# -- |    1      |
# -- |    1      |
# -- |    1      |
# -- |    2      |
# -- |    null   |
# -- |    null   |
# -- |    3      |
# -- |-----------|

 

# COMMAND ----------

# Solution
# -----------
# 1. Find the records in tbl1 that are matching with tbl2
# 2. Avoid the nulls
# 3. Find the records for result of semi join which are not matching with tbl3

# COMMAND ----------

# -- #--Tbl1-#--#--Tbl2-#--
# ------id-------|----id------|
# ---|-----------|------------|
# -- |    1      |      1     |
# -- |    1      |      1     |
# -- |    1      |      2     |
# -- |    2      |      2     |
# -- |    3      |      4     |
# -- |    3      |      null  |
# -- |    3      |------------|
# -- |-----------|

# COMMAND ----------

# Records in tbl1 matching with tbl2 (result of semi join)
# ------------------------------------------------------

# tbl4
# -------------------
# |  1  |
# |  1  |
# |  1  |
# |  2  |

# COMMAND ----------

# -- #--Tbl4-#--#--Tbl3-#--
# ------id-------|----id------|
# ---|-----------|------------|
# -- |    1      |      1     |
# -- |    1      |      1     |
# -- |    1      |      1     |
# -- |    2      |      2     |
# -- |-----------|      null  |
# --             |      null  |
# --             |      3     |    
# --             |------------|



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tbl1 
# MAGIC left semi join tbl2 on tbl1.id=tbl2.id
# MAGIC
