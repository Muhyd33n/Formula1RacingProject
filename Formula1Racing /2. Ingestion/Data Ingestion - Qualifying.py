# Databricks notebook source
# MAGIC %md
# MAGIC Import the needed notebook

# COMMAND ----------

# MAGIC %run "../includes/configuration "

# COMMAND ----------

# MAGIC %run "../includes/common_functions "

# COMMAND ----------

# MAGIC %md
# MAGIC using the dbutils.widgets to add a new column to the dataFrame 

# COMMAND ----------

dbutils.widgets.text('status', ' ')
status_value = dbutils.widgets.get('status')

# COMMAND ----------

# MAGIC %md
# MAGIC define the schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType([StructField('qualifyingId', IntegerType(), False),
                                StructField('raceId', IntegerType(), True),
                                StructField('driverId', IntegerType(), True),
                                StructField('constructorId', IntegerType(), True),
                                StructField('number', IntegerType(), True),
                                StructField('position', IntegerType(), True),
                                StructField('q1', StringType(), True),
                                StructField('q2', StringType(), True),
                                StructField('q3', StringType(), True),

                                ])

# COMMAND ----------

qualifying_df= spark.read.schema(qualifying_schema).option('multiLine', True).json(f'{raw_folder_path}/qualifying')

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

qualifying_renamed_df = qualifying_df.withColumnRenamed('qualifyingId', 'qualifying_id') \
                        .withColumnRenamed('raceId', 'race_id') \
                        .withColumnRenamed('driverId', 'driver_id') \
                        .withColumnRenamed('constructorId', 'constructor_id') \
                        .withColumn('status', lit(status_value))


# COMMAND ----------

final_df = add_ingestion_date(qualifying_renamed_df)

# COMMAND ----------

final_df.write.mode('overwrite').parquet(f'{processed_folder_path}/qualifying')

# COMMAND ----------

dbutils.notebook.exit("Success")
