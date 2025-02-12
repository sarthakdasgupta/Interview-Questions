# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC How to handle corrupt records
# MAGIC --------------------

# COMMAND ----------

CSV File
==========

employee_id	full_name	        department	age	salary	
E02006	    Harper Dominguez	Finance	    42	10000	
E02009	    Miles Chang	        IT	        30	6000	txt
E02002	    Kai Le	            Finance	    30	4000	
E02032	    Ruby Alexander	    IT	        50	10000	pol
E02046	    Ruby Kaur	        Finance	    26	4000	
E02011	    Jameson Thomas	    Finance	    50	8000	
E02013	    Bella Wu	        Finance	    32	6000	gfg
E02020	    Jordan Kumar	    IT	        24	3000	
E02007	    Ezra Vu	            IT	        yes	5500	


# COMMAND ----------

expected_schema = StructType([
    StructField("employee_id", StringType(), True),
    StructField("full_name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", IntegerType(), True),
])


# COMMAND ----------

# MAGIC %md
# MAGIC Different Types of Read Modes
# MAGIC -----------

# COMMAND ----------

# MAGIC %md
# MAGIC **1. Permisive**<br>
# MAGIC This is the default read mode in spark. Below are the main use case for permisive mode <br>
# MAGIC 1. <b>Handling of Corrupted Records:</b> When a corrupted record is encountered then Spark sets these to null. Moreover if _corrupt_record is mentioned in schema, then corrupted records get mapped to this column.
# MAGIC 2. <b>Advantage:</b> Permisive mode allows processing without any interruption, even if there are few corrupted records encounterd. It comes in handy when '<b>continues data ingestion takes more prioity over data integrity</b>'. 

# COMMAND ----------

# MAGIC %md
# MAGIC Without Schema

# COMMAND ----------

df_perm_no_schema = spark.read.format("csv")\
  .option("inferSchema", "true")\
  .option("header", "true")\
  .load("dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/emp_corrupt.csv")

df_perm_no_schema.display()

# COMMAND ----------

# MAGIC %md
# MAGIC With Schema

# COMMAND ----------

df_perm = spark.read.format("csv")\
  .schema(expected_schema)\
  .option("header", "true")\
  .load("dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/emp_corrupt.csv")

df_perm.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Giving a random name to last column

# COMMAND ----------

expected_schema_cc = StructType([
    StructField("employee_id", StringType(), True),
    StructField("full_name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", IntegerType(), True),
    StructField("cc", StringType(), True),
])


df_perm_cc = spark.read.format("csv")\
  .schema(expected_schema_cc)\
  .option("header", "true")\
  .load("dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/emp_corrupt.csv")

df_perm_cc.display()

# COMMAND ----------

# MAGIC %md
# MAGIC With _corrupt_record

# COMMAND ----------

expected_schema_cr = StructType([
    StructField("employee_id", StringType(), True),
    StructField("full_name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", IntegerType(), True),
    StructField("_corrupt_record", StringType(),True)
])


df_perm_cr = spark.read.format("csv")\
  .schema(expected_schema_cr)\
  .option("mode", "PERMISSIVE") \
  .option("header", "true")\
  .load("dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/emp_corrupt.csv")


df_perm_cr.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **2. DropMalformed**<br>
# MAGIC It is not as forgiving as permisive
# MAGIC 1. <b>Handling of Corrupted Records:</b> Spark directly drops rows that contain corrupted or malformed records, ensuring only clean records remain.
# MAGIC 2. <b>Advantage:</b> This mode is useful if our aim is to strictly keep the records that align with the schema. If you aim for a clean data and are okay with potential data loss, this mode is your go-to.

# COMMAND ----------

df_dm = spark.read.format("csv")\
  .schema(expected_schema)\
  .option("mode", "dropMalformed")\
  .option("header", "true")\
  .load("dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/emp_corrupt.csv")

df_dm.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **3. FailFast**<br>
# MAGIC This is the strictest among the three modes.
# MAGIC 1. <b>Handling of Corrupted Records:</b> Spark fails the job in this mode and throws an exception when a corrupted record is encountered.
# MAGIC 2. <b>Advantage:</b> This strict approach ensures maximum data quality. This mode is ideal if the dataset strictly follows the expected schema without any discrepancies.

# COMMAND ----------

df_ff = spark.read.format("csv")\
  .schema(expected_schema)\
  .option("mode", "failFast")\
  .option("header", "true")\
  .load("dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/emp_corrupt.csv")

df_ff.display()

# COMMAND ----------

# MAGIC %md
# MAGIC How to save the corrupted records if found
# MAGIC -----------------------------
# MAGIC This process is important when the volume of data is high and you may expect corrupted records flowing in to be higher than expected.

# COMMAND ----------

# MAGIC %md
# MAGIC **Dont give mode when using bad records path**

# COMMAND ----------

# Thrown error because mode is mentioned while using bad record path
df_perm_rec_path = spark.read.format("csv")\
    .schema(expected_schema)\
    .option("mode", "PERMISSIVE") \
    .option("header", "true")\
    .option("badRecordsPath", "dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/corrupt_records")\
    .load("dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/emp_corrupt.csv")


df_perm_rec_path.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Bad record path correct syntax**

# COMMAND ----------

df_perm_rec_path = spark.read.format("csv")\
    .schema(expected_schema)\
    .option("header", "true")\
    .option("badRecordsPath", "dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/corrupt_records")\
    .load("dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/emp_corrupt.csv")


df_perm_rec_path.display()

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/corrupt_records/")

# COMMAND ----------

# MAGIC %md
# MAGIC In order to see the actual file keep running until backslash is gone

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/corrupt_records/20250122T175336/")

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/corrupt_records/20250122T175336/bad_records/")

# COMMAND ----------

# MAGIC %md
# MAGIC How to check the bad records

# COMMAND ----------

df_bad_records = spark.read.format("csv")\
    .schema(expected_schema)\
    .option("header", "true")\
    .load('dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/corrupt_records/20250122T175336/bad_records/part-00000-9def210b-5d5a-418e-bea7-a553e0c3eee5')

df_bad_records.display()

# COMMAND ----------

df_bad_records.show(truncate=False)

# COMMAND ----------


