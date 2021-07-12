# Databricks notebook source
df = spark.read.option("header", "true").option("inferSchema", "true").csv("dbfs:/mnt/datalake3/annual-enterprise-survey-2020-financial-year-provisional-csv.csv")
df.show()


# COMMAND ----------

print (df.count())

# COMMAND ----------

# applying schema
from pyspark.sql.types import StructType, IntegerType, StructField, StringType, DecimalType

dataschema =  StructType([
    StructField('Year', IntegerType(), False),
    StructField('Industry_aggregation_NZSIOC', StringType(), False),
    StructField('Industry_code_NZSIOC', StringType(), False),
    StructField('Industry_name_NZSIOC', StringType(), False),
    StructField('Units', StringType(), True),
    StructField('Variable_code', StringType(), True),
    StructField('Variable_name', StringType(), True),
    StructField('Variable_category', StringType(), True),
    StructField('Value', DecimalType(), True),
    StructField('Industry_code_ANZSIC06', StringType(), True),
    ])
df_schema = spark.read.option("header", "true").option("inferSchema", "true").csv("dbfs:/mnt/datalake3/annual-enterprise-survey-2020-financial-year-provisional-csv.csv", schema=dataschema)


df_schema = df_schema.withColumnRenamed('Industry_aggregation_NZSIOC', 'level')
df_schema = df_schema.withColumnRenamed('Industry_code_NZSIOC', 'Industry_code')
df_schema = df_schema.withColumnRenamed('Industry_name_NZSIOC', 'Industry_name')
df_schema = df_schema.withColumnRenamed('Industry_code_ANZSIC06', 'Industry_id')

# COMMAND ----------

# let's extend our small set by other random columns for 
# ability to show different functions
from pyspark.sql.functions import rand
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, IntegerType, StructField, StringType, DecimalType, TimestampType

df_schema = df_schema.withColumn("start_date", (F.lit(1516364153) + rand() * 2000).cast('timestamp'))
df_schema = df_schema.withColumn("end_date", (F.unix_timestamp(df_schema.start_date) + rand() * 2000).cast('timestamp'))

df_schema = df_schema.withColumn("type_column", (rand() * 1000).cast('int'))
df_schema.show()

# COMMAND ----------

df_schema.describe('Value', 'Year', 'start_date', 'type_column').show()

# COMMAND ----------

df_filtered = df_schema.filter("Value>0")

# COMMAND ----------

df_filtered.describe('Value', 'Year').show()

# COMMAND ----------

#filter NULL value in some columns
df_filtered_withoutNull = df_filtered.na.drop(subset=["Variable_name","Industry_id"]) 

#alternative is .drop("any") or .drop("all")

# COMMAND ----------

#replace null value by  default value
defaultValueMap = {'Variable_name':'empty value','Industry_id':'empty id'}
df_filtered_withoutNull = df_filtered.na.replace(defaultValueMap, 1) 

# COMMAND ----------

# check data is uniq
# drop duplicates
df_filtered_withoutNull = df_filtered_withoutNull.dropDuplicates()

# COMMAND ----------

# filtered by reasonable date
df_filtered_withoutNull = df_filtered_withoutNull.where("Year>2014 AND Year<2020")

# COMMAND ----------

#select important fields
df_filtered_withoutNull = df_filtered_withoutNull.select(
                                                  'Year',
                                                  'Variable_name',
                                                  'Value'
                                                  )

# COMMAND ----------

df_filtered_withoutNull.show()

# COMMAND ----------

# changing value 1
# adding


df_schema = df_schema.withColumn('start_year', F.year(df_schema.start_date))
df_schema = df_schema.withColumn('start_month', F.month(df_schema.start_date))
df_schema = df_schema.withColumn('start_day', F.dayofmonth(df_schema.start_date))
df_schema.show()

# COMMAND ----------

# changing value 2
# adding delta in time periods

df_schema = df_schema.withColumn('delta_inminutes', F.round((F.unix_timestamp(df_schema.end_date) - F.unix_timestamp(df_schema.start_date))/60))
df_schema.show()

# COMMAND ----------

# changing value 3
# devide by range
# 1 -> df_schema.describe('type_column').show()

# 2 ->
df_schema = df_schema.withColumn('type', F.when(df_schema.type_column > 500, 'small').otherwise('big'))
df_schema.show()

# COMMAND ----------

#receiving new analitical view 
df_typeview = df_schema.groupBy('type_column').agg(F.sum('Value').alias('earning'), F.count('type_column').alias('cnt')).orderBy('type_column')
df_typeview.show()

# COMMAND ----------


