# Databricks notebook source
# MAGIC %run
# MAGIC
# MAGIC "../Formula 1/includes/configuration"

# COMMAND ----------

race_demo_df= spark.read.parquet(f"{processed_folder_path}/race")

# COMMAND ----------

display(race_demo_df)

# COMMAND ----------

race_demo_df.filter(race_demo_df.year == 2020).display()

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct

# COMMAND ----------

race_demo_df.select(count('*')).show()

# COMMAND ----------

race_demo_df.select(countDistinct('race_id')).show()

# COMMAND ----------

race_demo_df.filter(race_demo_df.name == 'Abu Dhabi Grand Prix').select(count('round')).show()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC Window Function

# COMMAND ----------

from pyspark.sql.functions import window
from pyspark.sql.functions import col, rank


rank_window = window.partitionBy("year").orderBy(col('total_point'), ascending = False)
df.withcolumn('rank', rank().over(rank_window)).show()

# COMMAND ----------



# COMMAND ----------

race_demo_df

# COMMAND ----------


