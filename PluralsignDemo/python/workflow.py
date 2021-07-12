# Databricks notebook source
# creating parameters for our workbook
 
dbutils.widgets.text('ProcessMonth', '201901', 'Process month(yyyymm)')

# COMMAND ----------

param1 = dbutils.widgets.get('ProcessMonth')
param1

# COMMAND ----------

# end of the notebook 
dbutils.notebook.exit('Success')

# COMMAND ----------

# lets create orchistration for our notebook
parameterMap = {'ProcessMonth':param1}
status = dbutils.notebook.run('path_to_notebook', 300, parameterMap) # -- path_to_notebook
if (status == 'Success'):
  print('Success')
else:
  print('Error')



# COMMAND ----------

# lets create orchistration for our notebook
parameterMap = {'ProcessMonth':param1}
status = dbutils.notebook.run('path_to_notebook', 300, parameterMap) # -- path_to_notebook
if (status == 'Success'):
  print('Success')
else:
  print('Error')


