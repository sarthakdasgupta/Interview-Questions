# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC Question for today
# MAGIC ----
# MAGIC 1. Find out the no of words in a text file
# MAGIC 2. Find out the no of alphabets in a text file
# MAGIC 3. Find out how many times word 'of' occurs in the text file
# MAGIC 4. Convert 'Sherlock Holmes' & 'Holmes' all occurance to upper case and save as text file

# COMMAND ----------

# MAGIC %md
# MAGIC Dataset
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC 'Sherlock Holmes, fictional character created by the Scottish writer Arthur Conan Doyle. The prototype for the modern mastermind detective, Holmes first appeared in Conan Doyle’s A Study in Scarlet, published in Beeton’s Christmas Annual of 1887. The first collection of the Holmes’ tales, published as The Adventures of Sherlock Holmes, appeared in 1892. As the world’s first and only “consulting detective,” he pursued criminals throughout Victorian and Edwardian London, the south of England, and continental Europe. Although the fictional detective had been anticipated by Edgar Allan Poe’s C. Auguste Dupin and Émile Gaboriau’s Monsieur Lecoq, Holmes made a singular impact upon the popular imagination and has been the most enduring character of the detective story.'

# COMMAND ----------


dbutils.fs.ls('dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/sherlock_holmes.txt')

# COMMAND ----------

rdd_text = spark.sparkContext.textFile("dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/sherlock_holmes.txt")
rdd_text.collect()

# COMMAND ----------

df_text = spark.read.text("dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/sherlock_holmes.txt")
df_text.display()

# COMMAND ----------

r = rdd_text.flatMap(lambda x: x.split(','))
r.collect()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Find out the no of words and no of unique words in text file
# MAGIC ----

# COMMAND ----------

rdd_extract = rdd_text.flatMap(lambda x: x.split(" "))
rdd_extract.collect()

# COMMAND ----------

import re
def removePunctuation(word):
    res = re.sub('[^A-Za-z]', '', word)
    return res

removePunctuation("Holmes,")

# COMMAND ----------

# Remove special characters
rdd_no_punc =  rdd_extract.map(lambda x: removePunctuation(x))
rdd_no_punc.collect()

# COMMAND ----------

# remove spaces
rdd_word = rdd_no_punc.filter(lambda x: x.isalpha())
rdd_word.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC **No of words**

# COMMAND ----------

rdd_word.count()

# COMMAND ----------

# MAGIC %md
# MAGIC **No of unique words**

# COMMAND ----------

rdd_word_map = rdd_word.map(lambda x: (x,1))
rdd_word_map.collect()

# COMMAND ----------

rdd_word_reduce = rdd_word_map.reduceByKey(lambda x,y: x+y)
rdd_word_reduce.collect()

# COMMAND ----------

rdd_word_reduce.count()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Find the no of all alphabets and no of unique alphabets in the text file
# MAGIC -------

# COMMAND ----------

rdd_no_punc.collect()

# COMMAND ----------

rdd_no_punc.flatMap(lambda x: x.split('')).collect()

# COMMAND ----------



# COMMAND ----------

def getAlpha(word):
    res = ""
    for i in range(0, len(word)):
        if i==len(word)-1:
            res += word[i]
        else:
            res += word[i] + "-"
    return res

getAlpha("Word")


# COMMAND ----------

rdd_sep_add = rdd_no_punc.map(lambda x: getAlpha(x))
rdd_sep_add.collect()

# COMMAND ----------

rdd_alpha = rdd_sep_add.flatMap(lambda x: x.split("-"))
rdd_alpha.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC No of all the alphabets

# COMMAND ----------

rdd_alpha.count()

# COMMAND ----------

# MAGIC %md
# MAGIC No of unique alphabets

# COMMAND ----------

rdd_map = rdd_alpha.map(lambda x: (x,1))
rdd_map.collect()

# COMMAND ----------

rdd_map.reduceByKey(lambda x,y: x+y).collect()

# COMMAND ----------

rdd_map.reduceByKey(lambda x,y: x+y).count()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Find out how many times word 'of' occurs in the text file
# MAGIC --------

# COMMAND ----------

rdd_word_reduce.collect()

# COMMAND ----------

rdd_word_reduce.filter(lambda x: x[0].lower()=='of').collect()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC From the existing text file do the following
# MAGIC --------
# MAGIC 1. Convert 'Sherlock Holmes' & 'Holmes' all occurance to upper case
# MAGIC 2. Save as text file in paragraph format like source file
# MAGIC

# COMMAND ----------

rdd_extract.collect()

# COMMAND ----------

def sherlock(word):
    check_list = ["Sherlock", "Holmes", "Holmes,"]
    cap = ""
    if word in check_list:
        return word.upper()
    else:
        return word

w1 = sherlock("word")
w2 = sherlock("Sherlock")
w3 = sherlock("Holmes,")
print(w1 + ", " + w2 + ", " + w3)

# COMMAND ----------

rdd_caps = rdd_extract.map(lambda x: sherlock(x))
rdd_caps.collect()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

rdd_caps.saveAsTextFile('dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/sherlock_result_v0')

# COMMAND ----------

rdd_read = spark.sparkContext.textFile('dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/sherlock_result_v0')
rdd_read.collect()

# COMMAND ----------

# Source text
# -----------------
rdd_text = spark.sparkContext.textFile("dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/sherlock_holmes.txt")
rdd_text.collect()

# COMMAND ----------



# COMMAND ----------

# Steps
# ---------
# rdd = ['word1', 'word2']
# rdd_1 = [(1,'word1'), (1, 'word2')]
# combine all the elements based on key = 1

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **How to save rdd as the original source text file**

# COMMAND ----------

rdd_map = rdd_caps.map(lambda x: (1,x))
rdd_map.collect()

# COMMAND ----------

rdd_reduce = rdd_map.reduceByKey(lambda x,y: x+" "+y)
rdd_reduce.collect()

# COMMAND ----------

rdd_reduce.map(lambda x: x[1]).collect()

# COMMAND ----------

rdd_reduce.map(lambda x: x[1]).saveAsTextFile('dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/sherlock_result_v1')

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/sherlock_result_v1/')

# COMMAND ----------

rdd_read = spark.sparkContext.textFile('dbfs:/FileStore/shared_uploads/sarthakdasgupta1997@gmail.com/sherlock_result_v1/part-00001')
rdd_read.collect()

# COMMAND ----------


