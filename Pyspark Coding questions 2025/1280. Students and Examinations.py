# Databricks notebook source
# MAGIC %md
# MAGIC Write a solution to find the number of times each student attended each exam.
# MAGIC ---------------
# MAGIC Return the result table ordered by student_id and subject_name.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# COMMAND ----------

# Create DataFrames for Students, Subjects, and Examinations tables
students_data = [
    (1, "Alice"),
    (2, "Bob"),
    (13, "John"),
    (6, "Alex")
]
students_columns = ["student_id", "student_name"]

subjects_data = [
    ("Math",),
    ("Physics",),
    ("Programming",)
]
subjects_columns = ["subject_name"]

examinations_data = [
    (1, "Math"),
    (1, "Physics"),
    (1, "Programming"),
    (2, "Programming"),
    (1, "Physics"),
    (1, "Math"),
    (13, "Math"),
    (13, "Programming"),
    (13, "Physics"),
    (2, "Math"),
    (1, "Math"),
]
examinations_columns = ["student_id_e", "subject_name_e"]

students_df = spark.createDataFrame(students_data, students_columns)
subjects_df = spark.createDataFrame(subjects_data, subjects_columns)
examinations_df = spark.createDataFrame(examinations_data, examinations_columns)



# COMMAND ----------

students_df.display()

# COMMAND ----------

subjects_df.display()

# COMMAND ----------

examinations_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Output Explained
# MAGIC -----
# MAGIC

# COMMAND ----------

Output: 
+------------+--------------+--------------+----------------+
| student_id | student_name | subject_name | attended_exams |
+------------+--------------+--------------+----------------+
| 1          | Alice        | Math         | 3              |
| 1          | Alice        | Physics      | 2              |
| 1          | Alice        | Programming  | 1              |
| 2          | Bob          | Math         | 1              |
| 2          | Bob          | Physics      | 0              |
| 2          | Bob          | Programming  | 1              |
| 6          | Alex         | Math         | 0              |
| 6          | Alex         | Physics      | 0              |
| 6          | Alex         | Programming  | 0              |
| 13         | John         | Math         | 1              |
| 13         | John         | Physics      | 1              |
| 13         | John         | Programming  | 1              |
+------------+--------------+--------------+----------------+

Explanation: 
The result table should contain all students and all subjects.
Alice attended the Math exam 3 times, the Physics exam 2 times, and the Programming exam 1 time.
Bob attended the Math exam 1 time, the Programming exam 1 time, and did not attend the Physics exam.
Alex did not attend any exams.
John attended the Math exam 1 time, the Physics exam 1 time, and the Programming exam 1 time.


# COMMAND ----------



# COMMAND ----------

# Perform a CROSS JOIN between students and subjects
cross_join_df = students_df.crossJoin(subjects_df)
cross_join_df.display()

# COMMAND ----------

# Perform a LEFT JOIN between the CROSS JOIN result and the examinations table
joined_df = cross_join_df.join(
    examinations_df,
    (cross_join_df["student_id"] == examinations_df["student_id_e"]) &
    (cross_join_df["subject_name"] == examinations_df["subject_name_e"]),
    "left")\
    

joined_df.display()

# COMMAND ----------

# Group by student_id, student_name, and subject_name and count the number of attended exams
result_df = joined_df.groupBy("student_id", "student_name", "subject_name")\
    .agg(count(col("student_id_e")).alias("attended_exams"))\
    .orderBy("student_id", "subject_name")

# Show the final result
result_df.display()


# COMMAND ----------


