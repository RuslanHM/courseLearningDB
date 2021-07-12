# Databricks notebook source
# MAGIC %sql 
# MAGIC create database if not exists MyDatawarehouse

# COMMAND ----------

# create managable table
df = spark.read.option("header", "true").option("inferSchema", "true").csv("dbfs:/mnt/datalake3/annual-enterprise-survey-2020-financial-year-provisional-csv.csv")
df.write.mode('overwrite').saveAsTable('MyDatawarehouse.survey2020')


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC describe extended MyDatawarehouse.survey2020

# COMMAND ----------

# create unmanagable table
df = spark.read.option("header", "true").option("inferSchema", "true").csv("dbfs:/mnt/datalake3/annual-enterprise-survey-2020-financial-year-provisional-csv.csv")
df.write.mode('overwrite').option('path', '/mnt/databrick').saveAsTable('MyDatawarehouse.survey2020_ext')



# COMMAND ----------

# MAGIC %fs ls /mnt/

# COMMAND ----------


