# Databricks notebook source
# MAGIC %md
# MAGIC import the needed notebook

# COMMAND ----------

# MAGIC %run
# MAGIC
# MAGIC "../includes/configuration"

# COMMAND ----------

# MAGIC %run
# MAGIC
# MAGIC "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC using the dbutils.widgets to add a column to the dataFrame

# COMMAND ----------

dbutils.widgets.text('status', ' ')
status_value = dbutils.widgets.get('status')

# COMMAND ----------

# MAGIC %md
# MAGIC Ingest Constructors.json file

# COMMAND ----------

display(dbutils.fs.ls(raw_folder_path))

# COMMAND ----------

# MAGIC %md
# MAGIC Read the file Using the spark dataframe Reader

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df =  spark.read.schema(constructors_schema).json(f'{raw_folder_path}/constructors.json')

# COMMAND ----------

constructors_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Drop Unwanted Column - url

# COMMAND ----------

constructors_dropped_df = constructors_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC Rename Columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

constructors_renamed_df= constructors_dropped_df.withColumnRenamed('constructorId', 'constructor_id')\
                       .withColumnRenamed('constructorRef', 'constructor_ref') \
                       .withColumn('status', lit(status_value))

# COMMAND ----------

constructors_final_df = add_ingestion_date(constructors_renamed_df)

# COMMAND ----------

constructors_final_df.write.mode("overwrite").parquet(f'{processed_folder_path}/constructors')



# COMMAND ----------

dbutils.notebook.exit("Success")
