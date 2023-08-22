-- Databricks notebook source
-- MAGIC %md
-- MAGIC Lesson Objective
-- MAGIC
-- MAGIC 1. Spark SQL Documentation (SQL reference)
-- MAGIC
-- MAGIC 2. Create Database Demo
-- MAGIC
-- MAGIC 3. Data tab in the UI 
-- MAGIC
-- MAGIC 4. SHOW command
-- MAGIC
-- MAGIC 5. DESCRIBE command
-- MAGIC
-- MAGIC 6. Find the current Database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

DESCRIBE DATABASE demo

-- COMMAND ----------

DESCRIBE DATABASE  EXTENDED demo

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

---'USE' can be used to change the current database 

USE demo

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Learning Objectives
-- MAGIC
-- MAGIC 1. Create managed table using Python
-- MAGIC
-- MAGIC 2. Create managed table using SQL
-- MAGIC
-- MAGIC 3. Effect of dropping a managed table
-- MAGIC
-- MAGIC 4. Describe table

-- COMMAND ----------

-- MAGIC %run 
-- MAGIC "../Formula 1/includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC drivers_df.write.format('parquet').saveAsTable('demo_drivers_df_python')

-- COMMAND ----------

USE  demo

SHOW TABLES demo;

-- COMMAND ----------


