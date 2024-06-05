# Databricks notebook source
# MAGIC %md
# MAGIC Topics for today
# MAGIC ---
# MAGIC **map(), flatmap(), reduceBy(), reduceByKey(), sortByKey(), groupByKey()**

# COMMAND ----------

# MAGIC %md
# MAGIC Source Dataset
# MAGIC --

# COMMAND ----------

data = ["Project","Gutenberg’s","Alice’s","Adventures",
"in","Wonderland","Project","Gutenberg’s","Adventures",
"in","Wonderland","Project","Gutenberg’s"]
rdd=spark.sparkContext.parallelize(data)
rdd.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC map()
# MAGIC ------

# COMMAND ----------

# MAGIC %md
# MAGIC 1. map()in PySpark is a transformation function that is used to apply a function/lambda to each element of an RDD
# MAGIC 2. You can use this for simple to complex operations like deriving a new element from exising data, or transforming the data
# MAGIC 3. DataFrame doesn’t have map() transformation to use with DataFrame; hence, you need to convert DataFrame to RDD first.
# MAGIC

# COMMAND ----------

rdd_map=rdd.map(lambda x: (x,1))
for item in rdd_map.collect():
    print(item)

# COMMAND ----------

# MAGIC %md
# MAGIC **How to use map() on DataFrame**

# COMMAND ----------

data = [('James','Smith','M',3000),
  ('Anna','Rose','F',2500),
  ('Robert','Williams','M',1800), 
]

columns = ["firstname","lastname","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
df.show()

# COMMAND ----------

rdd_map=df.rdd.map(lambda x: (x[0]+","+x[1],x[2],x[3]*2))

# Referring Column Names
# rdd2=df.rdd.map(lambda x: (x["firstname"]+","+x["lastname"],x["gender"],x["salary"]*2)) 

df_result=rdd_map.toDF(["name","gender","salary_hike"])
df_result.show()


# COMMAND ----------

# MAGIC %md
# MAGIC **How to use custom map function**

# COMMAND ----------

def map_func(x):
    first_name=x.firstname
    last_name=x.lastname
    name=first_name+" "+last_name
    gender=x.gender.lower()
    salary=x.salary*2
    return (name,gender,salary)

rdd_result = df.rdd.map(lambda x: map_func(x))

# rdd_result = df.rdd.map(map_func)

rdd_result.collect()

# COMMAND ----------

cols = ["name", "gender", "s"]
df_result = rdd_result.toDF(schema=cols)
df_result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC flatmap()
# MAGIC --------

# COMMAND ----------

# MAGIC %md
# MAGIC 1. PySpark flatMap() is a transformation operation that flattens the RDD/DataFrame (array/map DataFrame columns) after applying the function on every element and returns a new RDD/DataFrame.
# MAGIC 2. It's particularly useful when you want to flatten the output into a single list of elements
# MAGIC 3. Another common usage when dealing with text data, where you may want to split lines into words

# COMMAND ----------

rdd.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC **Example 1: Used to create different cardinalities of input**

# COMMAND ----------

# cardinality: messaure of different number of an element in a set
range_rdd=spark.sparkContext.range(5,10,1)
range_rdd.collect()

# COMMAND ----------

# using map to create a list of lists
flatmap_rdd = range_rdd.flatMap(lambda x: [x, x*2, x*4])
flatmap_rdd.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC **Example 2: If you want to split a text file**

# COMMAND ----------

rdd_text = spark.sparkContext.parallelize(["If you want to split a text file"])
rdd_text.collect()

# COMMAND ----------

rdd_each_word = rdd_text.flatMap(lambda x: x.split(" "))
rdd_each_word.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC What is the difference between map() and flatMap()
# MAGIC -----

# COMMAND ----------

# MAGIC %md
# MAGIC 1. map() is used when you want a one-to-one transformation, where each input element corresponds to exactly one output element.
# MAGIC 2. flatMap() is used when you want to perform a transformation that can generate zero or more output elements for each input element, and you want to flatten the results into a single RDD or DataFrame.

# COMMAND ----------

range_rdd.collect()

# COMMAND ----------

# flatMap()
range_rdd.flatMap(lambda x: [x, x*2, x*4]).collect()

# COMMAND ----------

# map()
range_rdd.map(lambda x: [x, x*2, x*4]).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC reduce()
# MAGIC ----------

# COMMAND ----------

# MAGIC %md
# MAGIC 1. reduce() is an aggregate action function is used to reduce a dataset ro a single value.
# MAGIC 2. It is commonly used to calculate aggregate values like min, max, sum, etc

# COMMAND ----------

list_rdd = spark.sparkContext.parallelize([1,2,3,4,5,3,2])
print("Min : "+ str(list_rdd.reduce(lambda x, y: min(x, y))))
print("Max: "+ str(list_rdd.reduce(lambda x, y: max(x,y))))
print("Sum : "+ str(list_rdd.reduce(lambda x,y: x+y)))
print("Multiply : "+ str(list_rdd.reduce(lambda x,y: x*y)))

# COMMAND ----------

# MAGIC %md
# MAGIC reduceByKey()
# MAGIC ----------------

# COMMAND ----------

# MAGIC %md
# MAGIC 1. reduceByKey() is a transformation which is used to merge the values of each key using an associative reduce function.
# MAGIC 2. It is a wide transformation as it shuffles data across multiple partitions and it operates on pair RDD (key/value pair).

# COMMAND ----------

rdd.collect()

# COMMAND ----------

rdd_map = rdd.map(lambda x: (x,1))
rdd_map.collect()

# COMMAND ----------

# reduceByKey reduces each of the words by applying sum function to the value. Eg. (key,value): ('Gutenberg’s', 1)
rdd_reduce_key = rdd_map.reduceByKey(lambda x, y: x+y)
rdd_reduce_key.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC sortByKey()
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC 1. sortByKey() is a RDD transformation function which is used to sort the values of the key by ascending or descending order.
# MAGIC 2. It recieves key/value pair as an input and then sorts in the specified order

# COMMAND ----------

rdd_list = spark.sparkContext.parallelize([['C', 10], ['H', 4], ['A', 6], ['L', 8]])

# by default it will sort in asc order
rdd_sort = rdd_list.sortByKey()
rdd_sort.collect()

# COMMAND ----------

# use False to sort in descending order
rdd_list.sortByKey(False).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC groupByKey
# MAGIC ------

# COMMAND ----------

1. groupByKey() receives key-value pairs as an input, groups the values based on key and generates a dataset of (Key, Its iterable) pairs
2. It is a wide transformation that performs shuffling of data
3. It is suitable when you need to group all the values associated with each key. 
4. It can be memory-intensive since all values for each key are collected and stored in memory.

# COMMAND ----------

rdd_list = spark.sparkContext.parallelize([['C', 10], ['H', 4], ['A', 6], ['L', 8], ['A', 6], ['C', 12], ['L', 2], ['H', 4], ['A', 6], ['C', 13], ['L', 6], ['H', 4]])
rdd_list.collect()


# COMMAND ----------

rdd_group_key = rdd_list.groupByKey()
rdd_group_key.collect()


# COMMAND ----------

# How to get the iterable value and their keys after grouping
rdd_result = rdd_group_key.map(lambda x: (x[0], list(x[1])))
rdd_result.collect()

# COMMAND ----------

# You can also sort the final grouped set
rdd_result.sortByKey().collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ================================================================================================================================================

# COMMAND ----------

# MAGIC %md
# MAGIC Common Interview Question:
# MAGIC -----------

# COMMAND ----------

# MAGIC %md
# MAGIC ---------------------

# COMMAND ----------

# MAGIC %md 
# MAGIC Difference between reduce() and reduceByKey()
# MAGIC -------

# COMMAND ----------

# MAGIC %md
# MAGIC reduce() is an aggregate function that reduces the data into a single value while reducByKey() accepts key value pair and it reduces the values based on the key

# COMMAND ----------

# MAGIC %md
# MAGIC Difference between reduceByKey() and groupByKey()
# MAGIC --------

# COMMAND ----------

# MAGIC %md
# MAGIC reduceByKey() and groupByKey() are transformation operations on key-value RDDs, but they differ in how they combine the values corresponding to each key

# COMMAND ----------

rdd_list = spark.sparkContext.parallelize([['C', 10], ['H', 4], ['A', 6], ['L', 8], ['A', 6], ['C', 12], ['L', 2], ['H', 4], ['A', 6], ['C', 13], ['L', 6], ['H', 4]])
rdd_list.collect()

# COMMAND ----------

# groupByKey(): groups the data based on a key while keeping the values as iterable object
rdd_group_key = rdd_list.groupByKey()
rdd_result = rdd_group_key.map(lambda x: (x[0], list(x[1])))
rdd_result.collect()

# COMMAND ----------

# reduceByKey(): reduces (or adds if add operation is used) the values based on their keys
rdd_result = rdd_list.reduceByKey(lambda x,y: x+y)
rdd_result.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC Difference between sortBy() and sortByKey()
# MAGIC ----------------

# COMMAND ----------

data = [('a', 1), ('b', 2), ('1', 3), ('A', 4), ('2', 5)]
rdd = sc.parallelize(data)
rdd.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC **sortByKey()**
# MAGIC
# MAGIC 1. parameter: (order in bool, numPartitions(optional), keyfunction)
# MAGIC 2. By default no arguments are required and it will sort in asc order

# COMMAND ----------

rdd.sortByKey().collect()

# COMMAND ----------

rdd.sortByKey(True, 3, lambda x: x.lower()).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC **sortBy()**
# MAGIC
# MAGIC 1. parameter: (keyfunction, order in bool, numPartitions(optional))
# MAGIC 2. keyfunction is a required argument, without it you will get error

# COMMAND ----------

rdd.sortBy().collect()

# COMMAND ----------

rdd.sortBy(lambda x: x[1]).collect()

# COMMAND ----------

rdd.sortBy(lambda x: x[0], True, 2).collect()

# COMMAND ----------


