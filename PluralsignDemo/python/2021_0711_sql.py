# Databricks notebook source
df = spark.read.option("header", "true").option("inferSchema", "true").csv("dbfs:/mnt/datalake3/annual-enterprise-survey-2020-financial-year-provisional-csv.csv")
df.show()

# COMMAND ----------

# table in local context
df.createOrReplaceTempView('my_table')

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select *
# MAGIC from my_table

# COMMAND ----------

# table in global context
# lets get it in data notebook
df.createOrReplaceGlobalTempView('global_table')

# COMMAND ----------

 # sys param:
spark.conf.get('spark.sql.shuffle.partitions')

# COMMAND ----------

spark.conf.set('spark.sql.shuffle.partitions', 10)

# COMMAND ----------


