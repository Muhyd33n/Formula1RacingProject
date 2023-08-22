# Databricks notebook source
# MAGIC %md
# MAGIC #Access Dataframe using SQL
# MAGIC
# MAGIC Objectives 
# MAGIC
# MAGIC 1.Create temporary views on dataFrames
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

drivers_df.createOrReplaceTempView("v_drivers_df")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM v_drivers_df

# COMMAND ----------

#using spark.sql allow us to create a dataframe from the SQL qeueries 
df_v_drivers_df = spark.sql("SELECT * FROM v_drivers_df WHERE nationality ='British'")

# COMMAND ----------

#This global view was created in another notebook. its written here to show how global temp view can work in other notebook within a cluster 
df_v_drivers_df = spark.sql("SELECT * FROM global_temp.gv_drivers_df WHERE nationality ='British'")

# COMMAND ----------


