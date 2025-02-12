# Databricks notebook source
# MAGIC %md
# MAGIC From the given json data find the result:
# MAGIC ------------------------------------------
# MAGIC **1. Calculate total amount spent by each customer**<br>
# MAGIC **2. Find the most expensive product bought by each customer**<br>
# MAGIC **3. Calculate the average age of customers who made purchases greater than $300**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

data = """
[
{
  "name": "Jane Smith",
  "age": 28,
  "purchases": [
    {
      "product_id": "B001",
      "product_name": "Tablet",
      "price": 399.99,
      "quantity": 1,
      "purchase_date": "2023-06-20"
    },
    {
      "product_id": "B002",
      "product_name": "Smartwatch",
      "price": 249.99,
      "quantity": 1,
      "purchase_date": "2023-07-05"
    }
  ]
},
{
  "name": "Alice Johnson",
      "age": 35,
  "purchases": [
    {
      "product_id": "C001",
      "product_name": "Bluetooth Speaker",
      "price": 149.99,
      "quantity": 1,
      "purchase_date": "2023-08-10"
    },
    {
      "product_id": "C002",
      "product_name": "Camera",
      "price": 199.99,
      "quantity": 1,
      "purchase_date": "2023-08-25"
    },
    {
      "product_id": "C003",
      "product_name": "VR Headset",
      "price": 299.99,
      "quantity": 1,
      "purchase_date": "2023-09-12"
    }
  ]
},
{
  "name": "John Doe",
  "age": 30,
  "purchases": [
    {
      "product_id": "A001",
      "product_name": "Smartphone",
      "price": 699.99,]
      "quantity": 1,
      "purchase_date": "2023-07-15"
    },
    {
      "product_id": "A002",
      "product_name": "Laptop",
      "price": 999.99,
      "quantity": 1,
      "purchase_date": "2023-08-02"
    },
    {
      "product_id": "A003",
      "product_name": "Wireless Headphones",
      "price": 199.99,
      "quantity": 2,
      "purchase_date": "2023-09-10"
    }
  ]
}
]
"""

# COMMAND ----------



# COMMAND ----------

# Create DataFrame from JSON data using a scheme
json_schema = StructType([
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("purchases", ArrayType(StructType([
        StructField("product_id", StringType()),
        StructField("product_name", StringType()),
        StructField("price", DoubleType()),
        StructField("quantity", IntegerType()),
        StructField("purchase_date", StringType()),
    ]))),
])

df = spark.read.json(spark.sparkContext.parallelize([data]), schema=json_schema)
df.display()

# COMMAND ----------

# Explode the purchases array
exploded_df = df.select(
    col("name"),
    col("age"),
    explode(col("purchases")).alias("purchase"),
    col('purchase.product_id').alias('product_id'),
    col('purchase.product_name').alias('product_name'),
    col('purchase.price'),
    col('purchase.quantity'),
    col('purchase.purchase_date')
).drop(col('purchase'))

exploded_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Calculate total amount spent by each customer
# MAGIC -------------------------------------------------

# COMMAND ----------

total_spent_df = exploded_df\
    .withColumn("net_amount", col("price") * col("quantity"))\
    .groupBy("name")\
    .agg(sum("net_amount").alias("total_amount_spent"))

total_spent_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Find the most expensive product bought by each customer
# MAGIC -----------------------

# COMMAND ----------

# Maximum price paid by each customer
temp_df = exploded_df\
    .groupBy(col("name").alias("temp_name"))\
    .agg(max(col("price")).alias("max_price"))\
    
temp_df.display()

# COMMAND ----------

# which product has the max price
most_expensive_product_df = exploded_df\
    .join(temp_df, (exploded_df["price"] == temp_df["max_price"]) & (exploded_df["name"] == temp_df["temp_name"])) \
    .select(col("name"), col("max_price"), col("product_name"), col("purchase_date"))
    
most_expensive_product_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Calculate the average age of customers who made purchases greater than $300
# MAGIC --------------------------

# COMMAND ----------

high_value_customers_df = exploded_df.filter(col("price") > 300) \
    .select("name", "age").distinct()
high_value_customers_df.display()

# COMMAND ----------

average_age_df = high_value_customers_df.groupBy().agg(avg("age").alias("average_age_high_value_customers"))
average_age_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Create the final summary report
# MAGIC -------------------

# COMMAND ----------

final_report_df = total_spent_df.join(most_expensive_product_df, "name").join(average_age_df)

# Show the results
final_report_df.display()
