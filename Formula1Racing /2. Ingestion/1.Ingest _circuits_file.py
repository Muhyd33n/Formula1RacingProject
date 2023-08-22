# Databricks notebook source
# MAGIC %md
# MAGIC use the dbutils widget to add a column to the notebooks 

# COMMAND ----------

 dbutils.widgets.text(name = 'status', defaultValue= ' ')
status_value =  dbutils.widgets.get('status')

# COMMAND ----------

status_value

# COMMAND ----------

# MAGIC %md
# MAGIC Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC -import the needed notebook in this child's notebook. this allow for reusing codes

# COMMAND ----------

# MAGIC %run 
# MAGIC
# MAGIC "../includes/configuration"

# COMMAND ----------

# MAGIC %run 
# MAGIC
# MAGIC "../includes/common_functions"

# COMMAND ----------

# DBTITLE 1,Import the file and read into a dataframe 
# MAGIC %md
# MAGIC STEP 1 - Read the CSV file using the spark dataframe reader. 
# MAGIC
# MAGIC - NOTE: the raw_folder_path, processed_folder_path and add_ingestion_date function were defined outside this notebook

# COMMAND ----------

display(dbutils.fs.ls(raw_folder_path))

# COMMAND ----------

# MAGIC %md
# MAGIC Define Schema 

# COMMAND ----------

from pyspark.sql.types import *

schema =  StructType([StructField("circuitId", IntegerType(), False),
                      StructField("circuitRef", StringType(), True),
                      StructField("name", StringType(), True),
                      StructField("location", StringType(), True),
                      StructField("country", StringType(), True),
                      StructField("lat", DoubleType(), True),
                      StructField("lng", DoubleType(), True),
                      StructField("alt", IntegerType(), True),
                      StructField("url", StringType(), True)])

# COMMAND ----------

raw_df = spark.read.option('Header', True).schema(schema).csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

raw_selected_df = raw_df.select(col('circuitId'), col('circuitRef'), col('name'), 
                                col('location'), col('country'), col('lat'), col('lng'), 
                                col('alt'))

# COMMAND ----------

renamed_raw_df = raw_selected_df.withColumnRenamed('circuitId', 'circuit_id') \
                 .withColumnRenamed('circuitRef', 'circuit_ref') \
                 .withColumnRenamed('lat', 'latitude') \
                 .withColumnRenamed('lng', 'longitude') \
                 .withColumnRenamed('alt', 'altitude') \
                 .withColumn('status', lit(status_value) )



# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

cleaned_raw_df = add_ingestion_date(renamed_raw_df )

# COMMAND ----------

cleaned_raw_df = renamed_raw_df.withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

cleaned_raw_df.write.mode('overwrite').parquet(f'{processed_folder_path}/circuits', 'overwrite')


# COMMAND ----------

# MAGIC  %fs
# MAGIC
# MAGIC  ls abfss://processsed@projectformular1dl.dfs.core.windows.net/circuits

# COMMAND ----------

display(spark.read.parquet('abfss://processsed@projectformular1dl.dfs.core.windows.net/circuits'))

# COMMAND ----------

dbutils.notebook.exit("Success")
