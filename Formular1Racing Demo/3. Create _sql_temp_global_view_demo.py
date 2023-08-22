# Databricks notebook source
# MAGIC %md
# MAGIC Global Temporary view  - in the context of databrick, all notebooks attached to the cluster will have access to the global temp view. while temp view is only available for the notebook for which its created
# MAGIC
# MAGIC Objectives 
# MAGIC
# MAGIC 1.Create Global temporary views on dataFrames
# MAGIC
# MAGIC 2. Access the view from SQL cell
# MAGIC
# MAGIC 3. Access the view from python Cell

# COMMAND ----------

# MAGIC %run
# MAGIC
# MAGIC "../Formula 1/includes/configuration"

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers")

# COMMAND ----------

drivers_df.createOrReplaceGlobalTempView("gv_drivers_df")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TABLES IN global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM 
# MAGIC global_temp.gv_drivers_df

# COMMAND ----------

#using spark.sql allow us to create a dataframe from the SQL qeueries 
df_v_drivers_df = spark.sql("SELECT * FROM global_temp.gv_drivers_df WHERE nationality ='British'")

# COMMAND ----------


