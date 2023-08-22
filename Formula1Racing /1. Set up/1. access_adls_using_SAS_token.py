# Databricks notebook source
# MAGIC %md
# MAGIC  Access Azure Datalake using SAS Token
# MAGIC 1. Set the spark config SAS token
# MAGIC 2. List files from demo container 
# MAGIC 3. Read data from circuit.csv file 

# COMMAND ----------

dbutils.secrets.list('Formula1-scope')

# COMMAND ----------

SAS_secret_scope = dbutils.secrets.get(scope='Formula1-scope', key='formula1-demo-SAS-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.projectformular1dl.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.projectformular1dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.projectformular1dl.dfs.core.windows.net", SAS_secret_scope)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@projectformular1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@projectformular1dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


