# Databricks notebook source
races_df = spark.read.parquet("abfss://processsed@projectformular1dl.dfs.core.windows.net/race") \
    .withColumnRenamed('name', 'race_name') \
    .withColumnRenamed('year', 'race_year') \
    .withColumnRenamed('race_timestamp', 'race_date')

# COMMAND ----------

circuits_df = spark.read.parquet("abfss://processsed@projectformular1dl.dfs.core.windows.net/circuits") \
    .withColumnRenamed('location', 'circuit_location')

# COMMAND ----------

drivers_df = spark.read.parquet("abfss://processsed@projectformular1dl.dfs.core.windows.net/drivers") \
    .withColumnRenamed('name', 'driver_name') \
    .withColumnRenamed('number', 'driver_number') \
    .withColumnRenamed('nationality', 'driver_nationality')

# COMMAND ----------

constructors_df = spark.read.parquet("abfss://processsed@projectformular1dl.dfs.core.windows.net/constructors") \
    .withColumnRenamed('name', 'team')


# COMMAND ----------

results_df = spark.read.parquet("abfss://processsed@projectformular1dl.dfs.core.windows.net/results")\
    .withColumnRenamed('time', 'race_time')

# COMMAND ----------

race_circuit_df = races_df.join(circuits_df, circuits_df.circuit_id ==races_df.circuit_id) \
    .select('race_id', 'race_year', 'race_name', 'race_date', 'circuit_location')


# COMMAND ----------

driver_results_df=results_df.join(drivers_df, drivers_df.driver_id == results_df.driver_id )

# COMMAND ----------

constructor_driver_results = driver_results_df.join(constructors_df, driver_results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

all_race_df = race_circuit_df.join(constructor_driver_results, constructor_driver_results.race_id == race_circuit_df.race_id )

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = all_race_df.select('race_year', 'race_name', 'race_date', 'circuit_location', 'driver_name', 'driver_number', 'driver_nationality',
                                   'team', 'grid', 'fastest_lap', 'race_time', 'points') \
                          .withColumn('created_date', current_timestamp())

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.write.mode('overwrite').parquet("abfss://presentation@projectformular1dl.dfs.core.windows.net/race_results")

# COMMAND ----------

spark.read.parquet("abfss://presentation@projectformular1dl.dfs.core.windows.net/race_results").display()

# COMMAND ----------


