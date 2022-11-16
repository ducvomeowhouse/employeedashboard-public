# Databricks notebook source
import dbutils as dbutils

dbutils.help()

# COMMAND ----------

dbutils.fs.ls("FileStore/tables/")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/tables/dt_employees.csv",header=True,inferSchema=True)

# COMMAND ----------

df.printSchema()