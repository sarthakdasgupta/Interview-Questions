# Databricks notebook source
# MAGIC %md
# MAGIC Write a pyspark code to check if email is in correct format
# MAGIC ---------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC Regular Expression for Email Validation:<br>
# MAGIC A basic regex pattern for validating an email format could be:<br>
# MAGIC
# MAGIC Pattern: ^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$ <br>

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import *

# Sample input data with some invalid and valid emails
data = [
    ("John Doe", "John.Doe@Example.COM"),
    ("Jane Smith", "JaneSmith@EXAMPLEcom"),
    ("Alice Johnson", "AliceJohnson@example.co"),
    ("Bob Stone", "bob_stone@example"),
    ("Invalid Email", "invalid_email@wrong")
]

# Create DataFrame
columns = ["name", "email"]
df = spark.createDataFrame(data, columns)
df.display()

# COMMAND ----------



# COMMAND ----------

em]

sample = 'sarthak.dasgupta @ example-mail . com'

^: This means the pattern must match from the beginning of the string.

[a-zA-Z0-9._%+-]+:
    [a-zA-Z0-9]: This allows any letter (a-z, A-Z) or number (0-9).
    ._%+-: This allows the special characters dot (.), underscore (_), percent (%), plus (+), and dash (-).
    The + means that this part must repeat one or more times. This defines the local part (the part before the @ symbol) of the email.
    @: This specifies the @ symbol, which separates the local part of the email from the domain part.

[a-zA-Z0-9.-]+\.:
    [a-zA-Z0-9]: This allows letters and numbers.
    .-: This allows dots (.) and dashes (-) to appear in the domain part (the part after the @).
    The + means this part must repeat one or more times. This is the domain name part of the email.
    \.: This ensures there is a dot before the top-level domain (TLD) (like .com or .org).

[a-zA-Z]{2,}:
    This part checks for the TLD (top-level domain), like .com or .org.
    [a-zA-Z]: This allows only letters.
    {2,}: This ensures that there are at least two letters in the TLD (like .com has three letters, .co has two).
    $: This means the pattern must match until the end of the string.

# COMMAND ----------

# Regular expression for email validation
email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

# Add a new column 'valid_email' to check if the email matches the regex pattern
df_validated = df\
.withColumn("regex_extract", regexp_extract(col("email"), email_regex, 0))\
.withColumn("valid_email", regexp_extract(col("email"), email_regex, 0) != "")

# Show the result with the validation column
df_validated.display()

# COMMAND ----------


