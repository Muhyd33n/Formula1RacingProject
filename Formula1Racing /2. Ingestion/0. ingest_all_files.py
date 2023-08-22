# Databricks notebook source
dbutils.notebook.run("1.Ingest _circuits_file", 0, {"status" : "Ergast API"})

# COMMAND ----------

dbutils.notebook.run("/Users/o.m.olalekan@edu.salford.ac.uk/Formula 1/Ingestion/Data Ingestion - laptimes", 0, {"status" : "Ergast API"})

# COMMAND ----------

dbutils.notebook.run("/Users/o.m.olalekan@edu.salford.ac.uk/Formula 1/Ingestion/Data Ingestion - Pitstops", 0, {"status" : "Ergast API"})

# COMMAND ----------

dbutils.notebook.run("/Users/o.m.olalekan@edu.salford.ac.uk/Formula 1/Ingestion/Data Ingestion - Qualifying", 0, {"status" : "Ergast API"})

# COMMAND ----------

dbutils.notebook.run("/Users/o.m.olalekan@edu.salford.ac.uk/Formula 1/Ingestion/Ingest_Constructors.json", 0, {"status" : "Ergast API"})

# COMMAND ----------

dbutils.notebook.run("/Users/o.m.olalekan@edu.salford.ac.uk/Formula 1/Ingestion/ingest_race_file", 0, {"status" : "Ergast API"})

# COMMAND ----------

dbutils.notebook.run("/Users/o.m.olalekan@edu.salford.ac.uk/Formula 1/Ingestion/Ingestion_of_driver.json", 0, {"status" : "Ergast API"})

# COMMAND ----------

dbutils.notebook.run("/Users/o.m.olalekan@edu.salford.ac.uk/Formula 1/Ingestion/Result.Json_Ingestion", 0, {"status" : "Ergast API"})
