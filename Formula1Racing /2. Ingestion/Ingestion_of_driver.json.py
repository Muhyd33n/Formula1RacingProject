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

display(dbutils.fs.ls(raw_folder_path))

# COMMAND ----------

# MAGIC %md
# MAGIC - Read the Json file
# MAGIC
# MAGIC -the json is a nested one, so we have to define the inner schema and the outer schema

# COMMAND ----------

from pyspark.sql.types import StringType, StructField,StructType, DateType, IntegerType

# COMMAND ----------

name_schema = StructType([StructField('forename', StringType(), True),
                                  StructField('surname', StringType(), True)
                                  
                                  ])

# COMMAND ----------

schema =  StructType([StructField('driverid', IntegerType(), False),
                      StructField('driverRef', StringType(), True),
                      StructField('number', IntegerType(), True),
                      StructField('code', StringType(), True), 
                      StructField('name', name_schema),
                      StructField('dob',  DateType(), True),
                      StructField('nationality', StringType(), True),
                      StructField('url', StringType(), True)

                              
                              ])

# COMMAND ----------

driver_df = spark.read.schema(schema).json(f"{raw_folder_path}/drivers.json")

# COMMAND ----------

driver_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC -Rename columns and new columns 

# COMMAND ----------

from pyspark.sql.functions import  col, concat, lit

# COMMAND ----------

driver_renamed_df = driver_df.withColumnRenamed('driverid', 'driver_id') \
                             .withColumnRenamed('driverRef', 'driver_ref') \
                             .withColumn ('name', concat(col('name.forename'),lit(' '),col('name.surname') ))  \
                             .withColumn('status', lit(status_value))



# COMMAND ----------

driver_ingested_df = add_ingestion_date(driver_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Drop Unwanted Column 

# COMMAND ----------

driver_final_df = driver_ingested_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC - write the output to the processed container

# COMMAND ----------

driver_final_df.write.mode('overwrite').parquet('{processed_folder_path}/drivers')

# COMMAND ----------

dbutils.notebook.exit("Success")
