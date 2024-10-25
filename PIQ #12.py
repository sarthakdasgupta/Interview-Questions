# Databricks notebook source
# MAGIC %md
# MAGIC Write a PySpark code to clean up the names by removing any special characters (e.g., @, #, *, etc.)
# MAGIC ---------------
# MAGIC Additionally, normalize all email addresses to lowercase

# COMMAND ----------

from pyspark.sql.functions import *
# Sample input data
data = [
    ("John@Doe*", "John.Doe@Example.COM"),
    ("Jane#Smith@", "JaneSmith@EXAMPLE.com"),
    ("Alice!Johnson$", "AliceJohnson@Example.Com"),
    ("Bob%Stone&", "BOB.STONE@Gmail.COM")
]

# Create DataFrame
columns = ["name", "email"]
df = spark.createDataFrame(data, columns)
df.display()

# COMMAND ----------

pattern = r"[^a-zA-Z\s]"

^ (inside square brackets): This symbol means "negation" inside the brackets. It tells the regex to match anything except the characters that follow.

[a-zA-Z]: This part matches any uppercase (A-Z) or lowercase (a-z) letter in the alphabet.

\s: This matches any whitespace character, including spaces, tabs, or newlines.

# COMMAND ----------

# Apply regex_replace to remove special characters from names
pattern = r"[^a-zA-Z\s]"
df_cleaned = df.withColumn("cleaned_name", regexp_replace(col("name"), pattern, ""))\
    .withColumn("normalized_email", lower(col("email")))

# Select and display the cleaned name and normalized email
df_cleaned.select("cleaned_name", "normalized_email").display()

# COMMAND ----------


