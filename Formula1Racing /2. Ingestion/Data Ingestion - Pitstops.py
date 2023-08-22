# Databricks notebook source
# MAGIC %md
# MAGIC Import the needed notebooks 

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run  "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC using the dbutils widget to add a new column to the dataframe 

# COMMAND ----------

dbutils.widgets.text('status', '  ')
status_value = dbutils.widgets.get('status')

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Define the Schema

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType

# COMMAND ----------

pit_stop_schema = StructType([StructField('raceId', IntegerType(), False),
                     StructField('driverId', IntegerType(), True),
                     StructField('stop', StringType(), True),
                     StructField('lap', StringType(), True),
                     StructField('time', StringType(), True),
                     StructField('duration', StringType(), True),
                     StructField('milliseconds', IntegerType(), True)
                    
                     
                     
                     
                     ])

# COMMAND ----------

# MAGIC %md
# MAGIC The file is  a multiline Json file and Multiline function is set as false by default in pyspark. you have to set it back to 'true' to read the dataset

# COMMAND ----------

pit_stop_df = spark.read.option('multiline', True).schema(pit_stop_schema).json(f"{raw_folder_path}/pit_stops.json")

# COMMAND ----------

display(pit_stop_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2 
# MAGIC
# MAGIC - Rename driverid and raceid
# MAGIC - Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

pit_stop_renamed_df = pit_stop_df.withColumnRenamed('driverid', 'driver_id') \
                                 .withColumnRenamed ('raceid', 'race_id') \
                                 .withColumn('status', lit(status_value))


# COMMAND ----------

pit_stop_final_df = add_ingestion_date(pit_stop_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3 
# MAGIC
# MAGIC - Write the output to processsed contain in parquet format
# MAGIC

# COMMAND ----------

pit_stop_final_df.write.mode('overwrite').parquet(f'{processed_folder_path}/pit_stops')

# COMMAND ----------

dbutils.notebook.exit("Success")
