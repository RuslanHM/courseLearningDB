// Databricks notebook source
// MAGIC %md ###Exploration Notebook
// MAGIC testing new commands

// COMMAND ----------

"Hello world"

// COMMAND ----------

val configs = Map(
  "dfs.adls.oauth2.access.token.provider.type" -> "ClientCredential",
  "dfs.adls.oauth2.client.id" -> "3d8dd768-9458-4fca-8e63-bbbd8a0186cb",
  "dfs.adls.oauth2.credential" -> "io~C8-XI2rYXiT0TPeV3IM_qaC15Rgm.N_",
  "dfs.adls.oauth2.refresh.url" -> "https://login.microsoftonline.com/6d52477f-c9b3-4511-a133-d93a73d4f499/oauth2/token"
)

// COMMAND ----------

dbutils.fs.mount(
  source = "adl://testdatas.azuredatalakestore.net/",
  mountPoint = "/mnt/datalake3",
  extraConfigs = configs
)

// COMMAND ----------

//look at directory
display(
  dbutils.fs.ls("/mnt/datalake3")
)

// COMMAND ----------

dbutils.fs.head("mnt/datalake3/annual-enterprise-survey-2020-financial-year-provisional-csv.csv")

// COMMAND ----------

// MAGIC %fs ls '/mnt/'

// COMMAND ----------

// MAGIC %fs rm '/mnt/datalake/'

// COMMAND ----------


