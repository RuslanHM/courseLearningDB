# Databricks notebook source
# access to global objects use the "global_temp"

df = spark.sql("select * from global_temp.global_table")
df.show()

# COMMAND ----------


