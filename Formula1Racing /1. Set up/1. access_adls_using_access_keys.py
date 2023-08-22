# Databricks notebook source
# MAGIC %md
# MAGIC  Access Azure Datalake using the access keys
# MAGIC 1. Set the spark config fs.azure.account.Key
# MAGIC 2. List files from demo container 
# MAGIC 3. Read data from circuit.csv file 

# COMMAND ----------

spark.conf.set(

    "fs.azure.account.key.projectformular1dl.dfs.core.windows.net", 
    dbutils.secrets.get('Formula1-scope', 'formular1dl-account-key')
    "xVZ8jn7Z3vdN7Jd1LZwV9m1tXD7AdttYuW2hWAabTeosVZsFwfi33DOCuPsQazqdG9GqswNHtpTd+AStKLM7xQ=="
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@projectformular1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@projectformular1dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list('Formula1-scope')

# COMMAND ----------

dbutils.secrets.get('Formula1-scope', 'formular1dl-account-key')
