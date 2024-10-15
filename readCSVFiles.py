# Databricks notebook source
###To read single file
df=spark.read.csv(path='dbfs:/FileStore/Data/daily_data.csv',header=True)
display(df)
df.printSchema()

# COMMAND ----------

###To read two files (passing in list)
df=spark.read.csv(path=['dbfs:/FileStore/Data/daily_data.csv','dbfs:/FileStore/Data/Weekly_data.csv'],header=True)
display(df)
df.printSchema()

# COMMAND ----------

###To read all the files in the same path
df=spark.read.csv(path='dbfs:/FileStore/Data/',header=True)
display(df)
df.printSchema()

# COMMAND ----------

###To read all the files in the same path and specify the schema
from pyspark.sql.types import *
schema= StructType().add('reported_date', DateType(), True)
df=spark.read.csv(path='dbfs:/FileStore/Data/',schema=schema,header=True)
display(df)
df.printSchema()
