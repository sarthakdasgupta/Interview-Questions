# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC Topics for today
# MAGIC ------
# MAGIC **UDF, Transform, Foreach()**

# COMMAND ----------

# MAGIC %md
# MAGIC UDF
# MAGIC ----
# MAGIC 1. udf() or user defined function is used to apply a custom functionality to a column in a DF
# MAGIC 2. They are the most expensive operations hence use them only you have no choice and when essential.
# MAGIC 3. One udf after it is registered can be used with multiple DF
# MAGIC 3. Example of a use case: When you want to convert the first alphabet of first and last name 

# COMMAND ----------

# MAGIC %md
# MAGIC **1. When you want to convert the first alphabet of first and last name :**

# COMMAND ----------

# Function to convert first alphabet to capital
def convert_first_to_upper(val):
    name_list = val.split(" ")
    first_name = name_list[0]
    last_name = name_list[1]
    res = first_name[0].upper() + first_name[1:] + " " + last_name[0].upper() + last_name[1:]
    return res

print(convert_first_to_upper("sarthak dasgupta"))

# COMMAND ----------

# Registering UDF
upper_udf = udf(lambda x: convert_first_to_upper(x), StringType())

# COMMAND ----------

columns = ["no","Name"]
data = [("1", "sam smith"),
    ("2", "tracey smith"),
    ("3", "amy sanders")]

df_src = spark.createDataFrame(data=data,schema=columns)
df_src.display()

# COMMAND ----------

# Applying UDF in withColumn
df_result = df_src.withColumn("Name", upper_udf(col("name")))
df_result.display()

# COMMAND ----------

# Applying UDF in select and sql
df_src.select(col("no"), upper_udf(col("name")).alias("name")).display()
# spark.sql("select no, upper_udf(name) from df_src_view")

# COMMAND ----------

# MAGIC %md
# MAGIC **2. For a given dataframe find out that if its column - "department" has palidrome values or not:**
# MAGIC
# MAGIC create a new column that indicates which department is palindrome
# MAGIC

# COMMAND ----------

columns = ["name","department", "salary"]
data = [("Harry", "HR", 12000),
    ("George", "ADA", 23000),
    ("Fred", "TQT", 21000),
    ("Sally", "IT", 25000),
    ("Neel", "SOS", 23000),
    ("Ashish", "IT", 27000)]

df_src = spark.createDataFrame(data=data,schema=columns)
df_src.display()

# COMMAND ----------

def is_palindrome(val):
    val_rev = val[::-1]
    print("reverse: ", val_rev)
    return val == val_rev

print("AQA is palindrome", is_palindrome("AQA"), "\n")
print("Bill is palindrome", is_palindrome("Bill"))

# COMMAND ----------

palindrome_udf = udf(lambda x: is_palindrome(x), BooleanType())
df_result = df_src.withColumn("is_palindrome", palindrome_udf(col("department")))
df_result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **3. In the below DF there is a column containing name with some special characters and numbers. Remove these special characters and numbers from name**
# MAGIC
# MAGIC In the existing name column remove the special characters using regex

# COMMAND ----------

columns = ["no","Name"]
data = [("1", "sa$m sm**ith"),
    ("2", "tr@#ce!y s^mith"),
    ("3", "amy%^ & sand(*ers")]

df_src = spark.createDataFrame(data=data,schema=columns)
df_src.display()

# COMMAND ----------

import re
def remove_sc(val):
    return re.sub('[^A-Za-z ]', '', val)

remove_sc_udf = udf(lambda x: remove_sc(x), StringType())
print(remove_sc("Sar5&*th@a(k) Da4%sgupt@a"))

# COMMAND ----------

df_src.select(col("no"), remove_sc_udf(col("name")).alias("name")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Transform
# MAGIC ----
# MAGIC 1. transform() is used to chain the custom transformation
# MAGIC 2. This operation is mainly used if you wanted to manipulate accumulators,
# MAGIC 3. Another use is to save the DataFrame results to RDBMS tables, Kafka topics, and other external sources.
# MAGIC
# MAGIC Parameters: transform(function, *args, **kwargs)

# COMMAND ----------

simpleData = (("Java",4000,5),
    ("Python", 4600,10),
    ("Scala", 4100,15),
    ("Scala", 4500,15),
    ("PHP", 3000,20),
  )
columns= ["CourseName", "fee", "discount"]

df = spark.createDataFrame(data = simpleData, schema = columns)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **1. DataFrame.transform:**
# MAGIC In this we pass DF as the first argument

# COMMAND ----------

def course_update(df):
    return df.withColumn("CourseName", upper(col("CourseName")))

def fee_after_discount(df):
    return df.withColumn("new_fee", col("fee") - lit(col("fee")*col("discount"))/100)

def discount_amount(df):
    return df.withColumn("discounted_fee", lit(col("fee")-col("new_fee")))

# COMMAND ----------

df_result = df.transform(course_update) \
        .transform(fee_after_discount) \
        .transform(discount_amount)

df_result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **2. sql.functions.transform:** This function applies the specified transformation on every element of the array and returns an object of ArrayType.
# MAGIC
# MAGIC Parameters: transform(col of array type, function to be applied)

# COMMAND ----------

data = [
 ("James,,Smith",["Java","Scala","C++"],["Spark","Java"]),
 ("Michael,Rose,",["Spark","Java","C++"],["Spark","Java"]),
 ("Robert,,Williams",["CSharp","VB"],["Spark","Python"])
]
df = spark.createDataFrame(data=data,schema=["Name","Languages1","Languages2"])
df.display()

# COMMAND ----------

df.select(transform("Languages1", lambda x: upper(x)).alias("languages1")) \
  .display()

# COMMAND ----------

df.select(transform("Languages1", lambda x: concat(x, lit(" programming"))).alias("languages1")) \
  .display()

# COMMAND ----------

# MAGIC %md
# MAGIC foreach()
# MAGIC -----
# MAGIC

# COMMAND ----------

1. PySpark foreach() is an action operation that is available in RDD, DataFram to iterate/loop over each element
2. When applied It executes a specified function for each of the element in a DataFrame. 

# COMMAND ----------

# foreach on rdd
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
def print_num(num):
    print(num)
rdd.foreach(lambda x: print_num(x))

# COMMAND ----------

columns = ["no","Name"]
data = [("1", "john jones"),
    ("2", "tracey smith"),
    ("3", "amy sanders")]

# Create DataFrame
df_src = spark.createDataFrame(data=data,schema=columns)
df_src.display()

# COMMAND ----------

res = []
def createList(df):
    df.display()
df_src.foreach(createList)


# COMMAND ----------

# Used with accumulators
accum=spark.sparkContext.accumulator(0)
df.foreach(lambda x:accum.add(int(x.no)))
print(accum.value) #Accessed by driver

# COMMAND ----------


