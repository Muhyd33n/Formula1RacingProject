# Databricks notebook source
# MAGIC %md
# MAGIC Import the necessary notebook

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
# MAGIC use the dbutils.widgets to add a column to the dataframe

# COMMAND ----------

dbutils.widgets.text('status', ' ')
status_value = dbutils.widgets.get('status')

# COMMAND ----------

display(dbutils.fs.ls(raw_folder_path))

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

schema = StructType([StructField('raceId', IntegerType(), False),
                           StructField('year',IntegerType(), True),
                           StructField('round', IntegerType(), True),
                           StructField('circuitId', IntegerType(), True),
                           StructField('name', StringType(), True),
                           StructField('date', DateType(), True),
                           StructField('Time',StringType(), True)])

# COMMAND ----------

races_df = spark.read.option('header', True).schema(schema).csv(f"{raw_folder_path}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC - Rename columns - raceid, circuitid 
# MAGIC - Add Ingestion date and race_timestamp(combine date and time in one column) to the dataFrame
# MAGIC - Drop date and time column

# COMMAND ----------

from pyspark.sql.functions import  lit, concat, col, to_timestamp

# COMMAND ----------

race_renamed_df  = races_df.withColumnRenamed('raceId', 'race_id') \
                                  .withColumnRenamed('circuitId', 'circuit_id')  \
                                  .withColumn('race_timestamp', to_timestamp(concat(col('date'),lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))  \
                                  .withColumn('status', lit(status_value))

# COMMAND ----------

race_ingested_df = add_ingestion_date(race_renamed_df)

# COMMAND ----------

race_final_df = race_ingested_df.drop('date', 'Time')

# COMMAND ----------

# MAGIC %md
# MAGIC - Partition the output by race year
# MAGIC -Write the output to processed container in parquet format

# COMMAND ----------

race_final_df.write.mode('overwrite').partitionBy('year').parquet(f'{processed_folder_path}/race')

# COMMAND ----------

dbutils.notebook.exit("Success")
