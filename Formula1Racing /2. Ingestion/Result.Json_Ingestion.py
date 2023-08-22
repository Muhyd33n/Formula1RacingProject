# Databricks notebook source
# MAGIC %md
# MAGIC import the needed notebooks

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
# MAGIC use of dbutils.widgets to add column to the dataframe

# COMMAND ----------

dbutils.widgets.text('status', ' ')
status_value = dbutils.widgets.get('status')

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Define Schema and Read data from abfs into dataframe

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, DateType, FloatType, StringType

# COMMAND ----------

schema = StructType([StructField('resultId', IntegerType(), False),
                     StructField('raceId', IntegerType(), True),
                     StructField('driverId', IntegerType(), True),
                     StructField('constructorId', IntegerType(), True),
                     StructField('number', IntegerType(), True),
                     StructField('grid', IntegerType(), True),
                     StructField('position', IntegerType(), True),
                     StructField('positionText', StringType(), True),
                     StructField('positionOrder', IntegerType(), True),
                     StructField('points', FloatType(), True),
                     StructField('laps', IntegerType(), True),
                     StructField('time', StringType(), True),
                     StructField('milliSeconds',DateType(), True),
                     StructField('fastestLap', IntegerType(), True),
                     StructField('rank', IntegerType(), True),
                     StructField('fastestLapTime', StringType(), True),
                     StructField('fastestLapSpeed', FloatType(), True),
                     StructField('statusId', IntegerType(), True)
                     
                     
                     
                     
                     
                     
                     
                     
                     
                     
                     ])

# COMMAND ----------

results_df = spark.read.schema(schema).json(f"{raw_folder_path}/results.json")



# COMMAND ----------

# MAGIC %md
# MAGIC 2. Drop unwanted column (statusid)

# COMMAND ----------

from pyspark.sql.functions import  col

# COMMAND ----------

results_drop_df = results_df.drop(col('statusid')) 

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Change column names and include a column for Ingestion date

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

results_renamed_df = results_drop_df.withColumnRenamed('resultId', 'result_id') \
                                    .withColumnRenamed('raceId', 'race_id') \
                                    .withColumnRenamed('driverId', 'driver_id') \
                                    .withColumnRenamed('constructorId', 'constructor_id') \
                                    .withColumnRenamed('positionText', 'position_text') \
                                    .withColumnRenamed('positionOrder', 'position_order') \
                                    .withColumnRenamed('fastestLap', 'fastest_lap') \
                                    .withColumnRenamed('fastestLapTime', 'fastest_lap_time') \
                                    .withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed') \
                                    .withColumn('status', lit(status_value))
                                    


# COMMAND ----------

 results_final_df= add_ingestion_date(results_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC -Write the Result to the processed container

# COMMAND ----------

results_final_df.write.mode('overwrite').partitionBy('race_id').parquet(f'{processed_folder_path}/results')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


