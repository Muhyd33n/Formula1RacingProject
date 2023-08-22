# Databricks notebook source
# MAGIC %md
# MAGIC  Access Azure Datalake using the access keys
# MAGIC 1. Set the spark config within the cluster 
# MAGIC 2. List files from demo container 
# MAGIC 3. Read data from circuit.csv file 

# COMMAND ----------

#before this line of codes can work, spark configuration must have been done within the cluster. Instead of the session scope where spark configuration is within the notebook 
display(dbutils.fs.ls("abfss://demo@projectformular1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@projectformular1dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


