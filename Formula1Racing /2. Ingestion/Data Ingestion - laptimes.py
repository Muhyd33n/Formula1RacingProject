# Databricks notebook source
# MAGIC %md
# MAGIC #Import the needed notebooks which contain the user_defined function and the variable defined
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run
# MAGIC "../includes/configuration"

# COMMAND ----------

# MAGIC %run
# MAGIC "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC Using the dbutils widget to add a column to the dataframe 

# COMMAND ----------

dbutils.widgets.text('Status', ' ')
status_value = dbutils.widgets.get('Status')

# COMMAND ----------

# MAGIC %md
# MAGIC #Read the csv file(folder) using the spark dataframe API reader
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

lap_time_schema = StructType([ StructField('raceId', IntegerType(), False), 
                              StructField('driverId', IntegerType(), True), 
                              StructField('lap', IntegerType(), True), 
                              StructField('position', IntegerType(), True), 
                              StructField('time', StringType(), True), 
                              StructField('milliseconds', IntegerType(), True), 
])

# COMMAND ----------

lap_time_df = spark.read.schema(lap_time_schema).csv(f'{raw_folder_path}/lap_times')

# COMMAND ----------

# MAGIC %md
# MAGIC #step 2
# MAGIC
# MAGIC - Rename raceId and driverId
# MAGIC - Add Ingestion_date column

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------


lap_time_renamed_df = lap_time_df.withColumnRenamed('raceId', 'race_id') \
                               .withColumnRenamed('driverId', 'driver_id') \
                               .withColumn('status', lit(status_value))
                               

# COMMAND ----------

lap_time_final_df = add_ingestion_date(lap_time_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Write to processsed contain in parquet format 

# COMMAND ----------


lap_time_final_df.write.mode('overwrite').parquet(f'{processed_folder_path}/lap_times')

# COMMAND ----------

dbutils.notebook.exit("Success")
