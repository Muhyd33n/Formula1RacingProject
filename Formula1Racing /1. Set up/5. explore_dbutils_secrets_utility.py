# Databricks notebook source
# MAGIC %md
# MAGIC Explore the capabilities of the dbutils.secrets utility

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'Formula1-scope')

# COMMAND ----------

dbutils.secrets.get('Formula1-scope', 'formular1dl-account-key')

# COMMAND ----------


