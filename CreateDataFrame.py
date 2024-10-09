# Databricks notebook source
from pyspark.sql.types import *

data=[(1,'Izas'),(2,'AHamed')]
schema=StructType([StructField('id',IntegerType()),StructField('name',StringType())])
df=spark.createDataFrame(data=data,schema=schema)
df.show()
df.printSchema()

# COMMAND ----------


